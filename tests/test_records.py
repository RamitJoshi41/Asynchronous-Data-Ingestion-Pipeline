import httpx
import pytest
import respx
import structlog
from structlog.testing import capture_logs

from ingest.config import Settings
from ingest.httpclient import build_http_client
from ingest.models.hn import HNItem
from ingest.records import classify_hn_body, fetch_hn_body, make_retrier
from ingest.retry import RetryPolicy
from ingest.storage import DeadLetter, RunCounts

LOG = structlog.get_logger().bind(run_id=1, source="hacker_news")
BASE = "https://hn.test/v0"
FAST = RetryPolicy(max_attempts=3, base_delay_s=0.001, max_delay_s=0.002, max_retry_after_s=1)


def test_valid_body_returns_item_without_counting() -> None:
    counts = RunCounts()
    item = classify_hn_body(b'{"id": 5, "type": "story"}', 5, counts, LOG)
    assert isinstance(item, HNItem) and item.id == 5
    assert counts == RunCounts()  # the caller counts the upsert outcome, not the classifier


def test_invalid_body_becomes_dead_letter_without_logging_the_value() -> None:
    counts = RunCounts()
    body = b'{"id": 5, "score": "secret-ish"}'
    with capture_logs() as logs:
        result = classify_hn_body(body, 5, counts, LOG)
    assert result == DeadLetter(
        "5", "invalid", ["score:int_type"], body
    )  # raw body kept for replay
    assert counts.invalid == 1
    assert logs[0]["event"] == "item_invalid" and logs[0]["problems"] == ["score:int_type"]
    assert "secret-ish" not in repr(logs)


def test_null_body_is_counted_missing_not_dead_lettered() -> None:
    counts = RunCounts()
    assert classify_hn_body(b"null", 5, counts, LOG) is None
    assert counts.missing == 1 and counts.invalid == 0


def test_body_for_another_id_becomes_id_mismatch_dead_letter() -> None:
    counts = RunCounts()
    with capture_logs() as logs:
        result = classify_hn_body(b'{"id": 6}', 5, counts, LOG)
    assert isinstance(result, DeadLetter) and result.reason == "id_mismatch"
    assert result.errors == ["id:6"] and counts.invalid == 1
    assert logs[0]["event"] == "item_id_mismatch" and logs[0]["body_id"] == 6


def test_malformed_json_never_raises() -> None:
    counts = RunCounts()
    result = classify_hn_body(b"{not json", 5, counts, LOG)
    assert isinstance(result, DeadLetter) and result.raw_body == b"{not json"
    assert counts.invalid == 1


def test_http_reject_from_fetcher_is_counted_invalid() -> None:
    counts = RunCounts()
    reject = DeadLetter("5", "http_404", ["http:404"], b"Not Found")
    assert classify_hn_body(reject, 5, counts, LOG) is reject
    assert counts.invalid == 1


async def test_fetch_retries_transient_errors_and_counts_them(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(f"{BASE}/item/5.json").mock(
        side_effect=[httpx.ConnectError("drop"), httpx.Response(503), httpx.Response(200, json={})]
    )
    counts = RunCounts()
    async with build_http_client(Settings(_env_file=None)) as client:
        with capture_logs() as logs:
            body = await fetch_hn_body(client, BASE, 5, make_retrier(FAST, counts, LOG), LOG)
    assert body == b"{}"
    assert counts.retries == 2
    retry_events = [e for e in logs if e["event"] == "retrying"]
    assert [(e["attempt"], e["error"], e["status"]) for e in retry_events] == [
        (1, "ConnectError", None),
        (2, "HTTPStatusError", 503),
    ]


async def test_non_retryable_status_becomes_dead_letter(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(f"{BASE}/item/5.json").respond(status_code=404, content=b"gone")
    async with build_http_client(Settings(_env_file=None)) as client:
        result = await fetch_hn_body(client, BASE, 5, make_retrier(FAST, RunCounts(), LOG), LOG)
    assert result == DeadLetter("5", "http_404", ["http:404"], b"gone")
    assert route.call_count == 1  # not retried


async def test_retries_exhausted_raises(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(f"{BASE}/item/5.json").respond(status_code=500)
    async with build_http_client(Settings(_env_file=None)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_hn_body(client, BASE, 5, make_retrier(FAST, RunCounts(), LOG), LOG)
    assert route.call_count == FAST.max_attempts
