from collections.abc import AsyncGenerator, Mapping

import httpx
import pytest
import respx
import structlog
from structlog.testing import capture_logs

from ingest.config import Settings
from ingest.connector import FetchContext, InvalidPage, Job, Missing, Page, Parsed, Record
from ingest.connectors.hn import HackerNewsConnector
from ingest.httpclient import build_http_client
from ingest.records import (
    WatermarkTracker,
    account,
    classify,
    fetch_context,
    job_pages,
    make_retrier,
    reject_key,
)
from ingest.retry import RetryPolicy, default_rate_limit
from ingest.storage import DeadLetter, RunCounts

LOG = structlog.get_logger().bind(run_id=1, source="test")
BASE = "https://hn.test/v0"
FAST = RetryPolicy(max_attempts=3, base_delay_s=0.001, max_delay_s=0.002, max_retry_after_s=1)
HN = HackerNewsConnector(BASE, limit=5)
JOB = Job("repo", f"{BASE}/repo")


class FakeConnector:
    """A connector whose pages and parse results are scripted by the test."""

    def __init__(self, parsed: list[Parsed] | None = None, *, invalid: bool = False) -> None:
        self.parsed, self.invalid = parsed or [], invalid
        self.closed = False

    source = "fake"
    headers: Mapping[str, str] = {}

    async def plan(self, ctx: FetchContext, watermarks: Mapping[str, str]) -> list[Job]:
        return [JOB]

    async def pages(self, ctx: FetchContext, job: Job) -> AsyncGenerator[Page]:
        try:
            for number in (1, 2, 3):
                response = await ctx.get(f"{job.url}/{number}", None)
                yield Page(job, number, response.content)
        finally:
            self.closed = True

    def parse(self, page: Page) -> list[Parsed]:
        if self.invalid:
            raise InvalidPage(["page:not_a_list"])
        return self.parsed

    def revalidate(self, source_id: str, raw: bytes) -> Parsed:
        return Missing(source_id)

    def rate_limit_delay(self, response: httpx.Response) -> float | None:
        return default_rate_limit(response)


async def collect(gen: AsyncGenerator[Page | DeadLetter]) -> list[Page | DeadLetter]:
    return [item async for item in gen]


# --- classify / account ---------------------------------------------------------------------


def test_records_are_returned_and_counted_as_fetched_only() -> None:
    counts, tracker = RunCounts(), WatermarkTracker()
    record = Record("r#1", "{}", "2026-01-01T00:00:00Z")
    out = classify(FakeConnector([record]), Page(JOB, 1, b"[]"), counts, LOG, tracker)
    assert out == [record]
    assert counts == RunCounts(fetched=1)  # the upsert outcome is counted by the writer
    assert tracker.advanced() == {"repo": "2026-01-01T00:00:00Z"}


def test_rejects_are_counted_invalid_and_logged_without_values() -> None:
    counts = RunCounts()
    reject = DeadLetter("5", "invalid", ["score:int_type"], b'{"score": "secret-ish"}')
    with capture_logs() as logs:
        out = classify(FakeConnector([reject]), Page(JOB, 1, b""), counts, LOG, WatermarkTracker())
    assert out == [reject] and (counts.fetched, counts.invalid) == (1, 1)
    assert logs[0]["event"] == "record_invalid" and logs[0]["problems"] == ["score:int_type"]
    assert "secret-ish" not in repr(logs)


def test_missing_is_counted_and_not_stored() -> None:
    counts = RunCounts()
    assert account(Missing("5"), counts, LOG) is None
    assert (counts.missing, counts.invalid) == (1, 0)


def test_an_http_reject_from_the_fetcher_is_counted_invalid() -> None:
    counts = RunCounts()
    reject = DeadLetter("5", "http_404", ["http:404"], b"Not Found")
    assert classify(HN, reject, counts, LOG, WatermarkTracker()) == [reject]
    assert (counts.fetched, counts.invalid) == (1, 1)


def test_an_unreadable_page_is_one_dead_letter_and_blocks_the_watermark() -> None:
    counts, tracker = RunCounts(), WatermarkTracker()
    tracker.seen("repo", "2026-01-01T00:00:00Z")  # page 1 was fine
    out = classify(FakeConnector(invalid=True), Page(JOB, 2, b"<html>"), counts, LOG, tracker)
    assert out == [DeadLetter("repo@page2", "invalid_page", ["page:not_a_list"], b"<html>")]
    assert (counts.fetched, counts.invalid) == (1, 1)
    assert tracker.advanced() == {}  # records on page 2 were never seen: don't skip past them


# --- WatermarkTracker ------------------------------------------------------------------------


def test_watermark_is_the_max_cursor_per_key_and_ignores_none() -> None:
    tracker = WatermarkTracker()
    for key, cursor in [("a", "2026-01-02T00:00:00Z"), ("a", "2026-01-01T00:00:00Z"),
                        ("b", None), ("c", "2025-12-31T23:59:59Z")]:  # fmt: skip
        tracker.seen(key, cursor)
    tracker.block("c")
    assert tracker.advanced() == {"a": "2026-01-02T00:00:00Z"}


def test_reject_key_names_the_job_then_the_page() -> None:
    assert (reject_key(JOB, 1), reject_key(JOB, 3)) == ("repo", "repo@page3")


# --- fetch_context / job_pages ---------------------------------------------------------------


async def test_get_retries_transient_errors_and_counts_them(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(f"{BASE}/item/5.json").mock(
        side_effect=[httpx.ConnectError("drop"), httpx.Response(503), httpx.Response(200, json={})]
    )
    counts = RunCounts()
    async with build_http_client(Settings(_env_file=None), concurrency=1) as client:
        ctx = fetch_context(client, make_retrier(FAST, counts, LOG), LOG)
        with capture_logs() as logs:
            response = await ctx.get(f"{BASE}/item/5.json", None)
    assert response.content == b"{}" and counts.retries == 2
    retry_events = [e for e in logs if e["event"] == "retrying"]
    assert [(e["attempt"], e["error"], e["status"]) for e in retry_events] == [
        (1, "ConnectError", None),
        (2, "HTTPStatusError", 503),
    ]


async def test_non_retryable_status_ends_the_job_as_a_dead_letter(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get(f"{BASE}/repo/1").respond(content=b"[]")
    second = respx_mock.get(f"{BASE}/repo/2").respond(status_code=404, content=b"gone")
    fake, tracker = FakeConnector(), WatermarkTracker()
    async with build_http_client(Settings(_env_file=None), concurrency=1) as client:
        retrier = make_retrier(FAST, RunCounts(), LOG)
        ctx = fetch_context(client, retrier, LOG)
        got = await collect(job_pages(fake, ctx, JOB, retrier, tracker))
    assert got == [Page(JOB, 1, b"[]"), DeadLetter("repo@page2", "http_404", ["http:404"], b"gone")]
    assert second.call_count == 1  # not retried
    tracker.seen("repo", "2026-01-01T00:00:00Z")
    assert tracker.advanced() == {}  # page 3 was never fetched: the watermark must not move
    assert fake.closed


async def test_an_hn_404_is_keyed_by_the_item_id(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(f"{BASE}/item/5.json").respond(status_code=404, content=b"gone")
    async with build_http_client(Settings(_env_file=None), concurrency=1) as client:
        retrier = make_retrier(FAST, RunCounts(), LOG, HN)
        ctx = fetch_context(client, retrier, LOG)
        job = Job("5", f"{BASE}/item/5.json")
        got = await collect(job_pages(HN, ctx, job, retrier, WatermarkTracker()))
    assert got == [DeadLetter("5", "http_404", ["http:404"], b"gone")]  # same key as Phase 3


async def test_retries_exhausted_raises(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(f"{BASE}/repo/1").respond(status_code=500)
    async with build_http_client(Settings(_env_file=None), concurrency=1) as client:
        retrier = make_retrier(FAST, RunCounts(), LOG)
        ctx = fetch_context(client, retrier, LOG)
        with pytest.raises(httpx.HTTPStatusError):
            await collect(job_pages(FakeConnector(), ctx, JOB, retrier, WatermarkTracker()))
    assert route.call_count == FAST.max_attempts


async def test_stopping_early_closes_the_connector_generator(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(url__regex=rf"{BASE}/repo/\d").respond(content=b"[]")
    fake = FakeConnector()
    async with build_http_client(Settings(_env_file=None), concurrency=1) as client:
        retrier = make_retrier(FAST, RunCounts(), LOG)
        pages = job_pages(
            fake, fetch_context(client, retrier, LOG), JOB, retrier, WatermarkTracker()
        )
        assert isinstance(await anext(pages), Page)
        await pages.aclose()  # what a draining fetcher does (via aclosing)
    assert fake.closed
