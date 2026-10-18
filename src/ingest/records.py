"""Per-record rules shared by the sequential runner, the concurrent pipeline, and replay.

Source-agnostic (Phase 4): the connector says what a page contains; this module decides what
happens to it (retry, dead letter, count, log, watermark) identically for every source.
Explainer: docs/explain/ingest/records.md
"""

import asyncio
import time
from collections.abc import AsyncGenerator, Mapping
from contextlib import aclosing

import httpx
from structlog.typing import FilteringBoundLogger

from ingest.connector import (
    Connector,
    FetchContext,
    InvalidPage,
    Job,
    Missing,
    Page,
    Parsed,
    Record,
)
from ingest.observability import LatencyRecorder
from ingest.retry import Retrier, RetryPolicy, default_rate_limit
from ingest.storage import DeadLetter, RunCounts, RunMetrics, RunStatus, Store

type Fetched = Page | DeadLetter  # what a fetcher hands a worker: a page, or an HTTP-level reject


def make_retrier(
    policy: RetryPolicy,
    counts: RunCounts,
    run_log: FilteringBoundLogger,
    connector: Connector | None = None,
) -> Retrier:
    """A per-run Retrier whose retries are counted in the run and logged as `retrying`.

    The connector (if given) decides what counts as a rate limit for its API (ADR-0032).
    """

    def on_retry(attempt: int, delay: float, exc: BaseException) -> None:
        counts.retries += 1
        status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
        run_log.info(
            "retrying",
            attempt=attempt,
            delay_s=round(delay, 3),
            error=type(exc).__name__,
            status=status,
        )

    rate_limit = default_rate_limit if connector is None else connector.rate_limit_delay
    return Retrier(policy, rate_limit=rate_limit, on_retry=on_retry)


def fetch_context(
    client: httpx.AsyncClient,
    retrier: Retrier,
    run_log: FilteringBoundLogger,
    latency: LatencyRecorder | None = None,
) -> FetchContext:
    """The connector's only door to the network: GET with retries and raise_for_status.

    Every attempt (retries included, failures included) is timed into `latency` (ADR-0038).
    """

    async def get(url: str, params: Mapping[str, str] | None = None) -> httpx.Response:
        async def once() -> httpx.Response:
            started = time.perf_counter()
            try:
                response = await client.get(url, params=params)
            finally:
                if latency is not None:
                    latency.record(time.perf_counter() - started)
            response.raise_for_status()
            return response

        return await retrier.run(once)

    return FetchContext(get=get, log=run_log)


async def finish_safely(
    store: Store,
    run_id: int,
    status: RunStatus,
    counts: RunCounts,
    error: str | None = None,
    *,
    watermarks: Mapping[str, str] | None = None,
    metrics: RunMetrics | None = None,
) -> None:
    """Record the run's final state even while the calling task is being cancelled (ADR-0027).

    shield() lets the UPDATE complete if another cancellation arrives mid-await, so a hard
    shutdown can't leave a zombie 'running' row.
    """
    await asyncio.shield(
        store.finish_run(run_id, status, counts, error, watermarks=watermarks, metrics=metrics)
    )


class WatermarkTracker:
    """Per job key: the max record cursor seen, unless the job lost data (ADR-0031).

    A job is *blocked* when some of its records were never seen (an HTTP reject, an unreadable
    page): advancing its watermark past them would skip them forever.
    """

    def __init__(self) -> None:
        self._max: dict[str, str] = {}
        self._blocked: set[str] = set()

    def seen(self, key: str, cursor: str | None) -> None:
        if cursor is not None and cursor > self._max.get(key, ""):
            self._max[key] = cursor

    def block(self, key: str) -> None:
        self._blocked.add(key)

    def advanced(self) -> dict[str, str]:
        return {key: value for key, value in self._max.items() if key not in self._blocked}


def reject_key(job: Job, page_number: int) -> str:
    """Dead-letter key for a page-level failure. Page 1 uses the job key itself, so an HN 404 is
    keyed by the item id and resolves when that item later ingests."""
    return job.key if page_number == 1 else f"{job.key}@page{page_number}"


async def job_pages(
    connector: Connector,
    ctx: FetchContext,
    job: Job,
    retrier: Retrier,
    tracker: WatermarkTracker,
) -> AsyncGenerator[Fetched]:
    """One job's pages, with a non-retryable HTTP status turned into a dead letter.

    Retryable errors that survive the retries propagate and fail the run (upstream unhealthy).
    A non-retryable status (404, a plain 403, Open-Meteo's 400) is a problem with THIS job, not the
    source: it becomes a dead letter, the job ends, and its watermark is held back.
    """
    number = 0
    try:
        # aclosing: if our consumer stops early (shutdown), the connector's generator is closed
        # right away instead of whenever the garbage collector gets to it.
        async with aclosing(connector.pages(ctx, job)) as pages:
            async for page in pages:
                number = page.number
                yield page
    except httpx.HTTPStatusError as exc:
        if retrier.is_retryable(exc):
            raise
        status = exc.response.status_code
        key = reject_key(job, number + 1)
        ctx.log.warning("page_http_rejected", job=job.key, page=number + 1, status=status)
        tracker.block(job.key)
        yield DeadLetter(key, f"http_{status}", [f"http:{status}"], exc.response.content)


def classify(
    connector: Connector,
    fetched: Fetched,
    counts: RunCounts,
    run_log: FilteringBoundLogger,
    tracker: WatermarkTracker,
) -> list[Record | DeadLetter]:
    """Validate a page into records to store and rejects to dead-letter, counting each one.

    Never raises for bad data: rejected records are per-record outcomes, not run failures.
    `fetched` counts records received, plus one per page-level reject.
    """
    if isinstance(fetched, DeadLetter):
        counts.fetched += 1
        counts.invalid += 1
        return [fetched]
    job = fetched.job
    try:
        parsed = connector.parse(fetched)
    except InvalidPage as exc:
        counts.fetched += 1
        counts.invalid += 1
        tracker.block(job.key)
        key = reject_key(job, fetched.number)
        run_log.warning("page_invalid", job=job.key, page=fetched.number, problems=exc.errors)
        return [DeadLetter(key, "invalid_page", exc.errors, fetched.body)]
    out: list[Record | DeadLetter] = []
    for item in parsed:
        counts.fetched += 1
        kept = account(item, counts, run_log)
        if isinstance(kept, Record):
            tracker.seen(job.key, kept.cursor)
        if kept is not None:
            out.append(kept)
    return out


def account(
    item: Parsed, counts: RunCounts, run_log: FilteringBoundLogger
) -> Record | DeadLetter | None:
    """Count and log one parsed record; return what must be stored (None for missing)."""
    match item:
        case Missing():
            counts.missing += 1
            run_log.info("record_missing", source_id=item.source_id)
            return None
        case DeadLetter():
            counts.invalid += 1
            # Locations, types and reasons only: never the raw payload at INFO/WARNING.
            run_log.warning(
                "record_invalid", source_id=item.source_id, reason=item.reason, problems=item.errors
            )
            return item
        case _:
            return item
