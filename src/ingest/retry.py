"""Hand-rolled retries: classification, full-jitter backoff, Retry-After, and a shared gate.

Source-agnostic. `sleep`, `rng` and `now` are injectable so tests are instant and deterministic.
Explainer: docs/explain/ingest/retry.md   Decisions: ADR-0021, ADR-0022, ADR-0023
"""

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from ingest.config import Settings

RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

# Transient by nature: the same request may well succeed a moment later.
_RETRYABLE_TRANSPORT = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)


class RetryAfterTooLongError(Exception):
    """The server asked us to wait longer than we are willing to; give up instead of stalling."""


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int  # total tries, including the first
    base_delay_s: float
    max_delay_s: float
    max_retry_after_s: float

    @classmethod
    def from_settings(cls, settings: Settings) -> "RetryPolicy":
        return cls(
            max_attempts=settings.retry_max_attempts,
            base_delay_s=settings.retry_base_delay_s,
            max_delay_s=settings.retry_max_delay_s,
            max_retry_after_s=settings.retry_after_max_s,
        )

    def backoff_cap(self, retry_number: int) -> float:
        """Upper bound of the jittered delay before retry n (1-based): min(max, base * 2^(n-1))."""
        return float(min(self.max_delay_s, self.base_delay_s * 2 ** (retry_number - 1)))


# Same values as the Settings defaults; used when a caller doesn't pass its own policy.
DEFAULT_POLICY = RetryPolicy(
    max_attempts=4, base_delay_s=0.5, max_delay_s=10.0, max_retry_after_s=60.0
)


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRYABLE_STATUSES
    return isinstance(exc, _RETRYABLE_TRANSPORT)


def retry_after_seconds(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse Retry-After: delta-seconds ("120") or an HTTP-date. None if absent or unparseable."""
    if value is None:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:  # RFC 9110 dates are GMT; be lenient with naive ones
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - (now or datetime.now(UTC))).total_seconds())


class RateLimitGate:
    """Shared pause: one 429 with Retry-After makes EVERY fetcher wait, not just the one hit."""

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._resume_at = 0.0

    def pause_for(self, seconds: float) -> None:
        self._resume_at = max(self._resume_at, self._clock() + seconds)  # never shorten a pause

    def remaining(self) -> float:
        return max(0.0, self._resume_at - self._clock())


type OnRetry = Callable[[int, float, BaseException], None]


class Retrier:
    """Runs a call with retries. One instance per run, shared by all fetchers of that run."""

    def __init__(
        self,
        policy: RetryPolicy,
        *,
        gate: RateLimitGate | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: Callable[[], float] = random.random,
        on_retry: OnRetry | None = None,
    ) -> None:
        self.policy = policy
        self.gate = gate or RateLimitGate()
        self._sleep, self._rng, self._on_retry = sleep, rng, on_retry

    @staticmethod
    def is_retryable(exc: BaseException) -> bool:
        return is_retryable(exc)

    def delay_for(self, retry_number: int, exc: BaseException) -> float:
        """Retry-After (if the server sent one) beats our guess; otherwise full jitter."""
        if isinstance(exc, httpx.HTTPStatusError):
            server_says = retry_after_seconds(exc.response.headers.get("Retry-After"))
            if server_says is not None:
                if server_says > self.policy.max_retry_after_s:
                    limit = self.policy.max_retry_after_s
                    raise RetryAfterTooLongError(
                        f"Retry-After {server_says:.0f}s exceeds {limit:.0f}s"
                    ) from exc
                if exc.response.status_code == 429:
                    self.gate.pause_for(server_says)
                return server_says
        # Full jitter: uniform in [0, cap). Spreads retries out so clients don't stampede together.
        return self._rng() * self.policy.backoff_cap(retry_number)

    async def run[T](self, call: Callable[[], Awaitable[T]]) -> T:
        attempt = 1
        while True:
            wait = self.gate.remaining()
            if wait > 0:
                await self._sleep(wait)  # somebody got a 429: everyone holds off
            try:
                return await call()
            except Exception as exc:
                if not is_retryable(exc) or attempt >= self.policy.max_attempts:
                    raise
                delay = self.delay_for(attempt, exc)
                if self._on_retry is not None:
                    self._on_retry(attempt, delay, exc)
                await self._sleep(delay)
                attempt += 1
