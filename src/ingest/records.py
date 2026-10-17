"""Per-record outcomes shared by the sequential runner, the concurrent pipeline, and replay.

One place decides what a fetched body *is* (valid / dead letter / missing), so every runner counts,
logs and dead-letters identically. Explainer: docs/explain/ingest/records.md
"""

import asyncio

import httpx
from pydantic import ValidationError
from structlog.typing import FilteringBoundLogger

from ingest.models.hn import HNItem, parse_hn_item
from ingest.retry import Retrier, RetryPolicy
from ingest.sources.hn import fetch_item
from ingest.storage import DeadLetter, RunCounts, RunStatus, Store


def make_retrier(policy: RetryPolicy, counts: RunCounts, run_log: FilteringBoundLogger) -> Retrier:
    """A per-run Retrier whose retries are counted in the run and logged as `retrying`."""

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

    return Retrier(policy, on_retry=on_retry)


async def finish_safely(
    store: Store, run_id: int, status: RunStatus, counts: RunCounts, error: str | None = None
) -> None:
    """Record the run's final state even while the calling task is being cancelled (ADR-0027).

    shield() lets the UPDATE complete if another cancellation arrives mid-await, so a hard
    shutdown can't leave a zombie 'running' row.
    """
    await asyncio.shield(store.finish_run(run_id, status, counts, error))


async def fetch_hn_body(
    client: httpx.AsyncClient,
    base_url: str,
    item_id: int,
    retrier: Retrier,
    run_log: FilteringBoundLogger,
) -> bytes | DeadLetter:
    """Fetch one item with retries (ADR-0021/0022).

    Transient failures are retried; once retries run out the error propagates and fails the run.
    A non-retryable HTTP status (e.g. 404) is a problem with THIS record, not the source, so it
    becomes a dead letter and the run continues.
    """
    try:
        return await retrier.run(lambda: fetch_item(client, base_url, item_id))
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if retrier.is_retryable(exc):
            raise  # retryable but out of attempts: upstream unhealthy
        run_log.warning("item_http_rejected", item_id=item_id, status=status)
        return DeadLetter(str(item_id), f"http_{status}", [f"http:{status}"], exc.response.content)


def classify_hn_body(
    fetched: bytes | DeadLetter, item_id: int, counts: RunCounts, run_log: FilteringBoundLogger
) -> HNItem | DeadLetter | None:
    """Return the validated item, a DeadLetter (counted invalid), or None (counted missing).

    Never raises for bad data: rejected records are per-record outcomes, not run failures.
    """
    if isinstance(fetched, DeadLetter):
        counts.invalid += 1
        return fetched
    try:
        item = parse_hn_item(fetched)
    except ValidationError as exc:
        counts.invalid += 1
        # Field locations and error types only: never the raw payload at INFO/WARNING.
        problems = [f"{'.'.join(map(str, e['loc']))}:{e['type']}" for e in exc.errors()]
        run_log.warning("item_invalid", item_id=item_id, problems=problems)
        return DeadLetter(str(item_id), "invalid", problems, fetched)
    if item is None:
        counts.missing += 1
        run_log.info("item_missing", item_id=item_id)
        return None
    if item.id != item_id:
        # A body for a different record than requested is not trustworthy data for this key.
        counts.invalid += 1
        run_log.warning("item_id_mismatch", item_id=item_id, body_id=item.id)
        return DeadLetter(str(item_id), "id_mismatch", [f"id:{item.id}"], fetched)
    return item
