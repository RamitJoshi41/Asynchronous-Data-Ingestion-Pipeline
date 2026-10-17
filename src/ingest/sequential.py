"""Phase 1 baseline: Hacker News ingestion, ONE request at a time.

Deliberately sequential: each `await` waits for the network with nothing else to do. This is the
number Phase 2's concurrency is measured against. Explainer: docs/explain/ingest/sequential.md
"""

import time
from dataclasses import dataclass

import httpx
import structlog
from structlog.typing import FilteringBoundLogger

from ingest.models.hn import SOURCE
from ingest.records import classify_hn_body
from ingest.sources.hn import fetch_item, fetch_top_ids
from ingest.storage import RunCounts, RunStatus, Store

log = structlog.get_logger()


@dataclass(frozen=True)
class RunSummary:
    run_id: int
    status: RunStatus
    counts: RunCounts
    duration_s: float


async def run_hn_sequential(
    store: Store, client: httpx.AsyncClient, *, base_url: str, limit: int
) -> RunSummary:
    """Fetch the top `limit` stories one by one, validate, upsert, and record the run.

    Per-record problems (invalid, missing) are counted and skipped. Anything else (HTTP error,
    network failure, DB error) marks the run `failed` and is re-raised.
    """
    run_id = await store.start_run(SOURCE)
    run_log = log.bind(run_id=run_id, source=SOURCE)
    counts = RunCounts()
    started = time.perf_counter()
    run_log.info("run_started", limit=limit)
    try:
        ids = await fetch_top_ids(client, base_url, limit=limit)
        for item_id in ids:
            await _ingest_one(store, client, base_url, run_id, item_id, counts, run_log)
    except Exception as exc:
        # CancelledError is a BaseException and deliberately not caught: on Ctrl-C the run row
        # stays 'running' (known Phase 1 limitation, fixed by Phase 3's graceful shutdown).
        await store.finish_run(run_id, RunStatus.FAILED, counts, error=repr(exc))
        run_log.error("run_failed", error=repr(exc), fetched=counts.fetched)
        raise
    await store.finish_run(run_id, RunStatus.SUCCEEDED, counts)
    summary = RunSummary(run_id, RunStatus.SUCCEEDED, counts, time.perf_counter() - started)
    run_log.info(
        "run_finished",
        duration_s=round(summary.duration_s, 3),
        fetched=counts.fetched,
        inserted=counts.inserted,
        updated=counts.updated,
        unchanged=counts.unchanged,
        invalid=counts.invalid,
        missing=counts.missing,
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
) -> None:
    body = await fetch_item(client, base_url, item_id)
    counts.fetched += 1
    item = classify_hn_body(body, item_id, counts, run_log)
    if item is not None:
        outcome = await store.upsert_item(run_id, SOURCE, str(item.id), item.model_dump_json())
        counts.record(outcome)
