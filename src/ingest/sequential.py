"""Phase 1 baseline: Hacker News ingestion, ONE request at a time (Phase 3: with reliability).

Deliberately sequential: each `await` waits for the network with nothing else to do. Phase 3 adds
the same retries, dead letters and graceful stop as the concurrent pipeline, via shared helpers in
records.py, so the two runners stay comparable. Explainer: docs/explain/ingest/sequential.md
"""

import asyncio
import time
from dataclasses import dataclass

import httpx
import structlog
from structlog.typing import FilteringBoundLogger

from ingest.models.hn import SOURCE
from ingest.records import classify_hn_body, fetch_hn_body, finish_safely, make_retrier
from ingest.retry import DEFAULT_POLICY, Retrier, RetryPolicy
from ingest.shutdown import StopSignal
from ingest.sources.hn import fetch_top_ids
from ingest.storage import DeadLetter, RunCounts, RunStatus, Store

log = structlog.get_logger()


@dataclass(frozen=True)
class RunSummary:
    run_id: int
    status: RunStatus
    counts: RunCounts
    duration_s: float


async def run_hn_sequential(
    store: Store,
    client: httpx.AsyncClient,
    *,
    base_url: str,
    limit: int,
    policy: RetryPolicy = DEFAULT_POLICY,
    stop: StopSignal | None = None,
) -> RunSummary:
    """Fetch the top `limit` stories one by one, validate, upsert, and record the run.

    Rejected records are dead-lettered and the run continues. Transient errors are retried; an
    error that survives the retries marks the run `failed` and is re-raised. A stop request ends
    the run after the current item as `cancelled`.
    """
    run_id = await store.start_run(SOURCE)
    run_log = log.bind(run_id=run_id, source=SOURCE)
    counts = RunCounts()
    retrier = make_retrier(policy, counts, run_log)
    started = time.perf_counter()
    run_log.info("run_started", limit=limit)
    try:
        ids = await retrier.run(lambda: fetch_top_ids(client, base_url, limit=limit))
        for item_id in ids:
            if stop is not None and stop.is_set():
                break
            await _ingest_one(store, client, base_url, run_id, item_id, counts, run_log, retrier)
    except asyncio.CancelledError:
        # Forced shutdown (second signal): still record what happened, then let cancellation win.
        await finish_safely(store, run_id, RunStatus.CANCELLED, counts, "forced cancellation")
        run_log.warning("run_cancelled", forced=True, fetched=counts.fetched)
        raise
    except Exception as exc:
        await finish_safely(store, run_id, RunStatus.FAILED, counts, repr(exc))
        run_log.error("run_failed", error=repr(exc), fetched=counts.fetched)
        raise
    status = RunStatus.CANCELLED if stop is not None and stop.is_set() else RunStatus.SUCCEEDED
    await finish_safely(store, run_id, status, counts)
    summary = RunSummary(run_id, status, counts, time.perf_counter() - started)
    run_log.info(
        "run_finished",
        status=status.value,
        duration_s=round(summary.duration_s, 3),
        fetched=counts.fetched,
        inserted=counts.inserted,
        updated=counts.updated,
        unchanged=counts.unchanged,
        invalid=counts.invalid,
        missing=counts.missing,
        retries=counts.retries,
    )
    return summary


async def _ingest_one(
    store: Store,
    client: httpx.AsyncClient,
    base_url: str,
    run_id: int,
    item_id: int,
    counts: RunCounts,
    run_log: FilteringBoundLogger,
    retrier: Retrier,
) -> None:
    fetched = await fetch_hn_body(client, base_url, item_id, retrier, run_log)
    counts.fetched += 1
    result = classify_hn_body(fetched, item_id, counts, run_log)
    if isinstance(result, DeadLetter):
        await store.flush(run_id, SOURCE, [], [result])
    elif result is not None:
        flushed = await store.flush(
            run_id, SOURCE, [(str(result.id), result.model_dump_json())], []
        )
        for outcome in flushed.outcomes.values():
            counts.record(outcome)
