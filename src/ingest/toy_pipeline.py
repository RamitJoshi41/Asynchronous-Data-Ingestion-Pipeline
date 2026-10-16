"""Phase 0 toy pipeline. THIS IS A SIMULATION: fetches and processing are `asyncio.sleep` calls.

It exists to demonstrate the concurrency skeleton the real pipeline will use, with no network
and no database: a Semaphore caps in-flight fetches, a bounded Queue caps buffered items and
applies backpressure, N workers consume, and sentinels shut the workers down cleanly.
Full explanation: docs/explain/ingest/toy_pipeline.md.

Run the demo:  uv run python -m ingest.toy_pipeline
"""

import argparse
import asyncio
import time
from collections.abc import Iterable
from dataclasses import dataclass

import structlog

log = structlog.get_logger()


class FetchError(Exception):
    """Simulated unrecoverable fetch failure. It aborts the whole run (no retries until Phase 3)."""


class InvalidRecordError(ValueError):
    """Simulated bad record. Expected: it is recorded as failed and the run continues."""


@dataclass(frozen=True)
class RunReport:
    items_in: int
    processed: tuple[int, ...]
    failed: tuple[int, ...]
    peak_in_flight: int
    peak_queue_depth: int
    blocked_puts: int
    duration_s: float


class ToyPipeline:
    """Single-use: one instance runs once, so its counters always describe exactly one run."""

    def __init__(
        self,
        *,
        concurrency: int,
        queue_maxsize: int,
        num_workers: int,
        fetch_delay: float = 0.01,
        process_delay: float = 0.01,
        fail_fetch: Iterable[int] = (),
        fail_process: Iterable[int] = (),
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        # asyncio.Queue(maxsize=0) means UNBOUNDED, which would silently disable backpressure.
        if queue_maxsize < 1:
            raise ValueError("queue_maxsize must be >= 1")
        if num_workers < 1:
            raise ValueError("num_workers must be >= 1")

        self._num_workers = num_workers
        self._fetch_delay = fetch_delay
        self._process_delay = process_delay
        self._fail_fetch = frozenset(fail_fetch)
        self._fail_process = frozenset(fail_process)

        self._semaphore = asyncio.Semaphore(concurrency)
        # None is the sentinel: "no more items, exit your loop". One is sent per worker.
        self._queue: asyncio.Queue[int | None] = asyncio.Queue(maxsize=queue_maxsize)

        self._started = False
        # No lock needed: these are only touched between awaits, on the single event-loop thread.
        self._in_flight = 0
        self._peak_in_flight = 0
        self._peak_queue_depth = 0
        self._blocked_puts = 0
        self._processed: list[int] = []
        self._failed: list[int] = []

    async def run(self, items: Iterable[int]) -> RunReport:
        if self._started:
            raise RuntimeError("ToyPipeline is single-use; create a new instance per run")
        self._started = True
        items = list(items)
        started = time.monotonic()
        log.info("run_started", items=len(items), workers=self._num_workers)

        # If any task here raises, the TaskGroup cancels all the others and waits for them,
        # so nothing outlives run(). That is the guarantee gather() does not give (ADR-0001).
        async with asyncio.TaskGroup() as tg:
            for worker_id in range(self._num_workers):
                tg.create_task(self._worker(worker_id), name=f"worker-{worker_id}")
            tg.create_task(self._produce(items), name="producer")

        report = RunReport(
            items_in=len(items),
            processed=tuple(sorted(self._processed)),
            failed=tuple(sorted(self._failed)),
            peak_in_flight=self._peak_in_flight,
            peak_queue_depth=self._peak_queue_depth,
            blocked_puts=self._blocked_puts,
            duration_s=time.monotonic() - started,
        )
        log.info(
            "run_finished",
            processed=len(report.processed),
            failed=len(report.failed),
            peak_in_flight=report.peak_in_flight,
            peak_queue_depth=report.peak_queue_depth,
            blocked_puts=report.blocked_puts,
            duration_s=round(report.duration_s, 3),
        )
        return report

    async def _produce(self, items: list[int]) -> None:
        # One task per item; the Semaphore, not the task count, bounds concurrency (ADR-0002).
        async with asyncio.TaskGroup() as fetches:
            for item in items:
                fetches.create_task(self._fetch(item), name=f"fetch-{item}")
        # Every real item is in the queue now. The queue is FIFO, so sentinels put behind them
        # are only seen after all real items have been taken.
        for _ in range(self._num_workers):
            await self._queue.put(None)

    async def _fetch(self, item: int) -> None:
        async with self._semaphore:
            self._in_flight += 1
            self._peak_in_flight = max(self._peak_in_flight, self._in_flight)
            try:
                await asyncio.sleep(self._fetch_delay)  # stands in for an HTTP request
                if item in self._fail_fetch:
                    raise FetchError(f"simulated fetch failure for item {item}")
            finally:
                self._in_flight -= 1
        # Outside the semaphore on purpose: waiting for queue space must not hold a fetch slot.
        await self._put(item)

    async def _put(self, item: int) -> None:
        if not self._queue.full():
            # put() on a non-full queue completes without suspending.
            await self._queue.put(item)
        else:
            self._blocked_puts += 1
            log.info("producer_blocked", item=item, queue_depth=self._queue.qsize())
            waited_from = time.monotonic()
            await self._queue.put(item)  # suspends here until a worker frees a slot
            waited_ms = round((time.monotonic() - waited_from) * 1000, 1)
            log.info("producer_unblocked", item=item, waited_ms=waited_ms)
        self._peak_queue_depth = max(self._peak_queue_depth, self._queue.qsize())

    async def _worker(self, worker_id: int) -> None:
        while True:
            item = await self._queue.get()
            if item is None:
                log.debug("worker_stopped", worker_id=worker_id)
                return
            try:
                await self._process(item)
            except InvalidRecordError as exc:
                # Expected, per-record failure: record it and keep consuming. Anything else is a
                # bug and propagates, which makes the TaskGroup abort the run.
                self._failed.append(item)
                log.warning("item_failed", worker_id=worker_id, item=item, error=str(exc))
            else:
                self._processed.append(item)

    async def _process(self, item: int) -> None:
        await asyncio.sleep(self._process_delay)  # stands in for parse + validate + buffer
        if item in self._fail_process:
            raise InvalidRecordError(f"simulated invalid record {item}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 0 toy pipeline. SIMULATION: fake sleeps, no network, no database."
    )
    parser.add_argument("--items", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--queue-size", type=int, default=5)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--fetch-delay", type=float, default=0.01)
    # Defaults make workers ~20x slower than fetchers so backpressure is visible.
    parser.add_argument("--process-delay", type=float, default=0.05)
    parser.add_argument("--fail-process", type=int, nargs="*", default=[7])
    args = parser.parse_args()

    pipeline = ToyPipeline(
        concurrency=args.concurrency,
        queue_maxsize=args.queue_size,
        num_workers=args.workers,
        fetch_delay=args.fetch_delay,
        process_delay=args.process_delay,
        fail_process=args.fail_process,
    )
    report = asyncio.run(pipeline.run(range(args.items)))
    print(report)


if __name__ == "__main__":
    main()
