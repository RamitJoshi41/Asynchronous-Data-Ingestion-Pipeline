"""Phase 1 baseline: ONE request at a time (Phase 3: with reliability; Phase 4: any source).

Deliberately sequential: each `await` waits for the network with nothing else to do. It shares the
per-record rules (records.py) and the connector contract with the concurrent pipeline, so the two
runners stay comparable: same counts, same rows, same dead letters, same watermarks.
Explainer: docs/explain/ingest/sequential.md
"""

import asyncio
import time
from collections.abc import Callable
from contextlib import aclosing
from dataclasses import dataclass

import httpx
import structlog

from ingest.connector import Connector, Record
from ingest.observability import LatencyRecorder, LiveRun, LiveRuns
from ingest.records import (
    WatermarkTracker,
    classify,
    fetch_context,
    finish_safely,
    job_pages,
    make_retrier,
)
from ingest.retry import DEFAULT_POLICY, RetryPolicy
from ingest.shutdown import StopSignal
from ingest.storage import DeadLetter, RunCounts, RunMetrics, RunStatus, Store

log = structlog.get_logger()


@dataclass(frozen=True)
class RunSummary:
    run_id: int
    status: RunStatus
    counts: RunCounts
    duration_s: float


async def run_sequential(
    store: Store,
    client: httpx.AsyncClient,
    connector: Connector,
    *,
    policy: RetryPolicy = DEFAULT_POLICY,
    stop: StopSignal | None = None,
    live: LiveRuns | None = None,
    on_start: Callable[[str, int], None] | None = None,
) -> RunSummary:
    """Fetch every page of every job one by one, validate, write each page, and record the run.

    Rejected records are dead-lettered and the run continues. Transient errors are retried; an
    error that survives the retries marks the run `failed` and is re-raised. A stop request ends
    the run after the current page as `cancelled`.
    """
    source = connector.source
    run_id = await store.start_run(source)
    run_log = log.bind(run_id=run_id, source=source)
    counts, tracker = RunCounts(), WatermarkTracker()
    retrier = make_retrier(policy, counts, run_log, connector)
    latency = LatencyRecorder()
    ctx = fetch_context(client, retrier, run_log, latency)
    started = time.perf_counter()
    run_log.info("run_started")

    def stopping() -> bool:
        return stop is not None and stop.is_set()

    def metrics() -> RunMetrics:
        lat = latency.summary()  # no gauges: one request at a time, nothing queued
        return RunMetrics(
            duration_ms=round((time.perf_counter() - started) * 1000),
            requests=lat.requests,
            latency_p50_ms=lat.p50_ms,
            latency_p95_ms=lat.p95_ms,
            latency_max_ms=lat.max_ms,
        )

    if on_start is not None:
        on_start(source, run_id)
    if live is not None:
        live.add(LiveRun(run_id, source, None))
    try:
        jobs = await connector.plan(ctx, await store.watermarks(source))
        run_log.info("run_planned", jobs=len(jobs))
        for job in jobs:
            if stopping():
                break
            async with aclosing(job_pages(connector, ctx, job, retrier, tracker)) as pages:
                async for fetched in pages:
                    batch = classify(connector, fetched, counts, run_log, tracker)
                    items = [(r.source_id, r.payload_json) for r in batch if isinstance(r, Record)]
                    rejects = [r for r in batch if isinstance(r, DeadLetter)]
                    flushed = await store.flush(run_id, source, items, rejects)
                    for outcome in flushed.outcomes.values():
                        counts.record(outcome)
                    if stopping():
                        break
    except asyncio.CancelledError:
        # Forced shutdown (second signal): still record what happened, then let cancellation win.
        await finish_safely(
            store, run_id, RunStatus.CANCELLED, counts, "forced cancellation", metrics=metrics()
        )
        run_log.warning("run_cancelled", forced=True, fetched=counts.fetched)
        raise
    except Exception as exc:
        await finish_safely(store, run_id, RunStatus.FAILED, counts, repr(exc), metrics=metrics())
        run_log.error("run_failed", error=repr(exc), fetched=counts.fetched)
        raise
    finally:
        if live is not None:
            live.remove(run_id)
    if stopping():
        status = RunStatus.CANCELLED
        await finish_safely(store, run_id, status, counts, metrics=metrics())
    else:
        status = RunStatus.SUCCEEDED
        await finish_safely(
            store, run_id, status, counts, watermarks=tracker.advanced(), metrics=metrics()
        )
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
