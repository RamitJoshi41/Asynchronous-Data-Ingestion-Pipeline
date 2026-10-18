"""Logging setup and run metrics: exact latency percentiles and sampled gauges (ADR-0038/0039).

Core module: knows nothing about sources. `LiveRuns` is how the API sees the gauges of runs that are
still going (same process, same event loop).
Explainer: docs/explain/ingest/observability.md
"""

import asyncio
import logging
import math
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

import structlog

from ingest.config import Settings


def configure_logging(settings: Settings) -> None:
    """Configure structlog once per process (CLI and API entry points; tests keep the defaults)."""
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer()
        if settings.log_format == "json"
        else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[settings.log_level.upper()]
        ),
        # Not cached: structlog.testing.capture_logs (and reconfiguration) must keep working.
        cache_logger_on_first_use=False,
    )


def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile: the smallest value with at least p% of samples at or below it."""
    if not sorted_values:
        raise ValueError("no samples")
    rank = max(1, math.ceil(p / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


@dataclass(frozen=True)
class LatencySummary:
    requests: int
    p50_ms: float | None
    p95_ms: float | None
    max_ms: float | None


class LatencyRecorder:
    """Every HTTP attempt's duration, kept in memory: a run makes at most a few thousand requests,
    so exact percentiles are cheaper than approximating them."""

    def __init__(self) -> None:
        self._seconds: list[float] = []

    def record(self, seconds: float) -> None:
        self._seconds.append(seconds)

    def summary(self) -> LatencySummary:
        if not self._seconds:
            return LatencySummary(0, None, None, None)
        values = sorted(self._seconds)
        return LatencySummary(
            requests=len(values),
            p50_ms=round(percentile(values, 50) * 1000, 2),
            p95_ms=round(percentile(values, 95) * 1000, 2),
            max_ms=round(values[-1] * 1000, 2),
        )


@dataclass
class Gauge:
    """Running peak and mean of one sampled quantity, plus its latest value."""

    latest: int = 0
    peak: int = 0
    total: int = 0
    samples: int = 0

    def observe(self, value: int) -> None:
        self.latest = value
        self.peak = max(self.peak, value)
        self.total += value
        self.samples += 1

    @property
    def mean(self) -> float | None:
        return round(self.total / self.samples, 2) if self.samples else None


class GaugeSampler:
    """Samples queue depth and in-flight requests every `interval_s` until `stop()` (ADR-0038)."""

    def __init__(
        self, queue_depth: Callable[[], int], in_flight: Callable[[], int], *, interval_s: float
    ) -> None:
        self._read_depth, self._read_in_flight = queue_depth, in_flight
        self._interval_s = interval_s
        self._stopped = asyncio.Event()
        self.queue_depth, self.in_flight = Gauge(), Gauge()

    def sample(self) -> None:
        self.queue_depth.observe(self._read_depth())
        self.in_flight.observe(self._read_in_flight())

    def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        # An Event rather than cancellation, so stopping is an ordinary return inside the TaskGroup.
        while True:
            self.sample()
            try:
                async with asyncio.timeout(self._interval_s):
                    await self._stopped.wait()
            except TimeoutError:
                continue
            self.sample()  # one last reading at the end of the run
            return


@dataclass(frozen=True)
class LiveRun:
    run_id: int
    source: str
    gauges: GaugeSampler | None  # None for the sequential runner (nothing to sample)


@dataclass
class LiveRuns:
    """Runs in progress in THIS process, for the API live view. Single-threaded: no lock needed."""

    _runs: dict[int, LiveRun] = field(default_factory=dict)

    def add(self, run: LiveRun) -> None:
        self._runs[run.run_id] = run

    def remove(self, run_id: int) -> None:
        self._runs.pop(run_id, None)

    def get(self, run_id: int) -> LiveRun | None:
        return self._runs.get(run_id)

    def __iter__(self) -> Iterator[LiveRun]:
        return iter(list(self._runs.values()))
