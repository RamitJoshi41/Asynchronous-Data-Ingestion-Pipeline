import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from ingest.retry import (
    RateLimitGate,
    Retrier,
    RetryAfterTooLongError,
    RetryPolicy,
    is_retryable,
    retry_after_seconds,
)

POLICY = RetryPolicy(max_attempts=4, base_delay_s=0.5, max_delay_s=10.0, max_retry_after_s=60.0)
NOW = datetime(2026, 10, 17, 12, 0, 0, tzinfo=UTC)


def status_error(code: int, retry_after: str | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://hn.test/v0/item/1.json")
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    response = httpx.Response(code, headers=headers, request=request)
    return httpx.HTTPStatusError(f"{code}", request=request, response=response)


class FakeTime:
    """A clock plus a sleep that advances it instantly, recording every requested delay."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def flaky(
    failures: list[BaseException], result: str = "ok"
) -> tuple[list[int], Callable[[], Awaitable[str]]]:
    calls: list[int] = []

    async def call() -> str:
        calls.append(1)
        if len(calls) <= len(failures):
            raise failures[len(calls) - 1]
        return result

    return calls, call


# --- classification ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
        httpx.ConnectTimeout("slow"),
        httpx.ReadError("reset"),
        httpx.RemoteProtocolError("server hung up"),
        status_error(429),
        status_error(500),
        status_error(502),
        status_error(503),
        status_error(504),
    ],
)
def test_transient_errors_are_retryable(exc: BaseException) -> None:
    assert is_retryable(exc)


@pytest.mark.parametrize(
    "exc",
    [
        status_error(400),
        status_error(401),
        status_error(404),
        status_error(410),
        status_error(301),
        httpx.LocalProtocolError("our bug"),
        httpx.UnsupportedProtocol("ftp://"),
        ValueError("not an http error"),
    ],
)
def test_permanent_errors_are_not_retryable(exc: BaseException) -> None:
    assert not is_retryable(exc)


# --- Retry-After ---------------------------------------------------------------------------


def test_retry_after_seconds_form() -> None:
    assert retry_after_seconds("120") == 120.0
    assert retry_after_seconds(" 5 ") == 5.0


def test_retry_after_http_date_form() -> None:
    later = format_datetime(
        NOW + timedelta(seconds=30), usegmt=True
    )  # "Sat, 17 Oct 2026 12:00:30 GMT"
    assert retry_after_seconds(later, now=NOW) == 30.0


def test_retry_after_in_the_past_means_now() -> None:
    earlier = format_datetime(NOW - timedelta(hours=1), usegmt=True)
    assert retry_after_seconds(earlier, now=NOW) == 0.0


@pytest.mark.parametrize("value", [None, "", "soon", "-5", "1.5", "Mon, 99 Foo 2026"])
def test_unparseable_retry_after_is_ignored(value: str | None) -> None:
    assert retry_after_seconds(value, now=NOW) is None


# --- backoff -------------------------------------------------------------------------------


def test_backoff_cap_doubles_up_to_the_maximum() -> None:
    assert [POLICY.backoff_cap(n) for n in range(1, 8)] == [0.5, 1, 2, 4, 8, 10, 10]


def test_full_jitter_stays_within_zero_and_cap() -> None:
    rng = random.Random(42)
    retrier = Retrier(POLICY, rng=rng.random)
    for n in range(1, 8):
        delays = [retrier.delay_for(n, httpx.ConnectError("x")) for _ in range(200)]
        assert all(0 <= d < POLICY.backoff_cap(n) for d in delays)
        # Jitter actually spreads: not all retries land on the same instant.
        assert max(delays) - min(delays) > POLICY.backoff_cap(n) * 0.5


# --- the retry loop ------------------------------------------------------------------------


async def test_succeeds_after_transient_failures() -> None:
    t = FakeTime()
    seen: list[tuple[int, float]] = []
    retrier = Retrier(
        POLICY, sleep=t.sleep, rng=lambda: 0.5, on_retry=lambda n, d, e: seen.append((n, d))
    )
    calls, call = flaky([httpx.ConnectError("drop"), httpx.ReadTimeout("slow")])
    assert await retrier.run(call) == "ok"
    assert len(calls) == 3
    assert seen == [(1, 0.25), (2, 0.5)]  # rng 0.5 x caps 0.5 and 1.0
    assert t.sleeps == [0.25, 0.5]


async def test_gives_up_after_max_attempts() -> None:
    t = FakeTime()
    retrier = Retrier(POLICY, sleep=t.sleep, rng=lambda: 0.0)
    calls, call = flaky([httpx.ConnectError("drop")] * 10)
    with pytest.raises(httpx.ConnectError):
        await retrier.run(call)
    assert len(calls) == POLICY.max_attempts  # 4 tries = 3 retries, never unlimited
    assert len(t.sleeps) == POLICY.max_attempts - 1


async def test_non_retryable_error_is_raised_immediately() -> None:
    t = FakeTime()
    retrier = Retrier(POLICY, sleep=t.sleep)
    calls, call = flaky([status_error(404)])
    with pytest.raises(httpx.HTTPStatusError):
        await retrier.run(call)
    assert len(calls) == 1 and t.sleeps == []


async def test_429_retry_after_seconds_is_honoured_exactly() -> None:
    t = FakeTime()
    retrier = Retrier(POLICY, gate=RateLimitGate(clock=t.clock), sleep=t.sleep, rng=lambda: 0.9)
    calls, call = flaky([status_error(429, retry_after="7")])
    assert await retrier.run(call) == "ok"
    assert t.sleeps == [7.0]  # the server's number, not our jittered guess


async def test_503_retry_after_http_date_is_honoured() -> None:
    t = FakeTime()
    retrier = Retrier(POLICY, gate=RateLimitGate(clock=t.clock), sleep=t.sleep)
    date = format_datetime(datetime.now(UTC) + timedelta(seconds=20), usegmt=True)
    calls, call = flaky([status_error(503, retry_after=date)])
    assert await retrier.run(call) == "ok"
    assert 18.0 <= t.sleeps[0] <= 20.0  # whole-second date precision, minus elapsed time


async def test_retry_after_over_the_cap_gives_up() -> None:
    t = FakeTime()
    retrier = Retrier(POLICY, sleep=t.sleep)
    calls, call = flaky([status_error(429, retry_after="3600")])
    with pytest.raises(RetryAfterTooLongError) as exc_info:
        await retrier.run(call)
    assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)
    assert t.sleeps == [] and len(calls) == 1


async def test_429_pauses_other_callers_through_the_shared_gate() -> None:
    t = FakeTime()
    gate = RateLimitGate(clock=t.clock)
    retrier = Retrier(POLICY, gate=gate, sleep=t.sleep, rng=lambda: 0.0)
    _, rate_limited = flaky([status_error(429, retry_after="5")])
    await retrier.run(rate_limited)

    # A different fetcher starts a fresh call while the pause is still in force.
    t.now -= 3  # pretend only 2 of the 5 seconds have passed for the second caller
    t.sleeps.clear()
    _, healthy = flaky([])
    await retrier.run(healthy)
    assert t.sleeps == [pytest.approx(3.0)]  # waited out the remainder before calling


def test_gate_never_shortens_an_existing_pause() -> None:
    t = FakeTime()
    gate = RateLimitGate(clock=t.clock)
    gate.pause_for(10)
    gate.pause_for(2)
    assert gate.remaining() == 10
