"""Concurrent ingestion (Phase 2), made reliable (Phase 3), generic over a connector (Phase 4).

    feeder ─► job_queue ─► C fetchers ─► raw_queue ─► W workers (validate, per-worker batch) ─► DB

Every queue is bounded (full ⇒ the producer waits: backpressure). All tasks live in TaskGroups, so
any unexpected error cancels everything and nothing outlives the run. Shutdown is sentinel-driven:
the feeder sends C sentinels to the fetchers; after all fetchers finish, W sentinels go to the
workers, which flush their last partial batch and exit.
Phase 3: fetches retry (shared Retrier + rate-limit gate), rejects are dead-lettered in the same
flush transaction, a stop request drains the pipeline under a deadline, and flushes are
cancellation-safe so item counts always match committed rows.
Phase 4: what to fetch and how to read it comes from a Connector (ADR-0028); a job may have many
pages (one fetcher follows them in order), and a succeeded run advances watermarks (ADR-0031).
Explainer: docs/explain/ingest/pipeline.md   Decisions: ADR-0014..0031
"""

import asyncio
import time
from contextlib import aclosing
from dataclasses import dataclass, field
from typing import Self

import httpx
import structlog
from structlog.typing import FilteringBoundLogger

from ingest.config import Settings
from ingest.connector import Connector, FetchContext, Job, Record
from ingest.records import (
    Fetched,
    WatermarkTracker,
    classify,
    fetch_context,
    finish_safely,
    job_pages,
    make_retrier,
)
from ingest.retry import DEFAULT_POLICY, Retrier, RetryPolicy
from ingest.sequential import RunSummary
from ingest.shutdown import StopSignal
from ingest.storage import DeadLetter, RunCounts, RunStatus, Store

log = structlog.get_logger()


@dataclass(frozen=True)
class PipelineConfig:
    concurrency: int
    workers: int
    batch_size: int
    flush_interval_s: float
    job_queue_maxsize: int
    raw_queue_maxsize: int

    def __post_init__(self) -> None:
        sizes = (self.concurrency, self.workers, self.batch_size)
        queues = (self.job_queue_maxsize, self.raw_queue_maxsize)
        # Queue maxsize 0 would mean UNBOUNDED in asyncio: forbidden (CLAUDE.md §7).
        if min(*sizes, *queues) < 1 or self.flush_interval_s <= 0:
            raise ValueError(f"invalid pipeline config: {self}")

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        concurrency: int,
        workers: int | None = None,
        batch_size: int | None = None,
    ) -> Self:
        """Defaults from Settings. Concurrency is per source (ADR-0030): the caller passes it."""
        return cls(
            concurrency=concurrency,
            workers=settings.pipeline_workers if workers is None else workers,
            batch_size=settings.batch_size if batch_size is None else batch_size,
            flush_interval_s=settings.flush_interval_s,
            job_queue_maxsize=settings.job_queue_maxsize,
            raw_queue_maxsize=settings.raw_queue_maxsize,
        )


@dataclass
class PipelineStats:
    """Concurrency facts about one run (logged, returned, asserted by tests)."""

    in_flight: int = 0
    peak_in_flight: int = 0
    peak_raw_queue_depth: int = 0
    blocked_puts: int = 0
    flushes: int = 0
    flush_sizes: list[int] = field(default_factory=list)
    skipped_on_stop: int = 0  # jobs left unfetched because a stop was requested


@dataclass(frozen=True)
class ConcurrentRunSummary(RunSummary):
    stats: PipelineStats = field(default_factory=PipelineStats)


async def run_concurrent(
    store: Store,
    client: httpx.AsyncClient,
    connector: Connector,
    *,
    cfg: PipelineConfig,
    policy: RetryPolicy = DEFAULT_POLICY,
    stop: StopSignal | None = None,
    drain_timeout_s: float = 10.0,
) -> ConcurrentRunSummary:
    """Concurrent version of run_sequential: same classification, counts and run lifecycle.

    Outcomes: `succeeded` (watermarks advanced); `cancelled` (stop requested and drained, drain
    deadline exceeded, or forced cancellation); `failed` (an error survived retries; re-raised).
    """
    # Re-checked here because configs can be built in code, bypassing Settings validation.
    if store.pool_max_size < cfg.workers + 1:
        raise ValueError(
            f"DB pool max size {store.pool_max_size} < workers + 1 ({cfg.workers + 1}): "
            "flushes would exhaust the pool (ADR-0018)"
        )
    source = connector.source
    run_id = await store.start_run(source)
    run_log = log.bind(run_id=run_id, source=source)
    counts, stats, tracker = RunCounts(), PipelineStats(), WatermarkTracker()
    retrier = make_retrier(policy, counts, run_log, connector)
    ctx = fetch_context(client, retrier, run_log)
    stages = _Stages(
        store, connector, ctx, run_id, cfg, counts, stats, run_log, retrier, tracker, stop
    )
    started = time.perf_counter()
    run_log.info(
        "run_started",
        concurrency=cfg.concurrency,
        workers=cfg.workers,
        batch_size=cfg.batch_size,
    )

    def summary(status: RunStatus) -> ConcurrentRunSummary:
        return ConcurrentRunSummary(run_id, status, counts, time.perf_counter() - started, stats)

    loop = asyncio.get_running_loop()
    drain: asyncio.Timeout | None = None
    finished = False

    def arm_drain_deadline() -> None:
        # Called by StopSignal (a loop callback): from now on the drain has a hard deadline.
        if drain is not None and not finished:
            run_log.warning("drain_started", deadline_s=drain_timeout_s)
            drain.reschedule(loop.time() + drain_timeout_s)

    try:
        jobs = await connector.plan(ctx, await store.watermarks(source))
        run_log.info("run_planned", jobs=len(jobs))
        async with asyncio.timeout(None) as drain:
            if stop is not None:
                stop.on_stop(arm_drain_deadline)
            await stages.run(jobs)
        finished = True
    except asyncio.CancelledError:
        # Forced shutdown (second signal): let in-progress flushes land, record, re-raise.
        finished = True
        await stages.settle()
        await finish_safely(store, run_id, RunStatus.CANCELLED, counts, "forced cancellation")
        run_log.warning("run_cancelled", forced=True, **_count_fields(counts))
        raise
    except Exception as exc:  # includes ExceptionGroup from the TaskGroup
        finished = True
        await stages.settle()
        if drain is not None and drain.expired():
            error = f"drain exceeded {drain_timeout_s}s; remaining work cancelled"
            await finish_safely(store, run_id, RunStatus.CANCELLED, counts, error)
            run_log.warning("run_cancelled", drain_timed_out=True, **_count_fields(counts))
            return summary(RunStatus.CANCELLED)
        await finish_safely(store, run_id, RunStatus.FAILED, counts, repr(exc))
        run_log.error("run_failed", error=repr(exc), **_count_fields(counts))
        raise
    if stop is not None and stop.is_set():
        status = RunStatus.CANCELLED  # not every job ran to the end: watermarks stay put
        await finish_safely(store, run_id, status, counts)
    else:
        status = RunStatus.SUCCEEDED
        await finish_safely(store, run_id, status, counts, watermarks=tracker.advanced())
    result = summary(status)
    run_log.info(
        "run_finished",
        status=status.value,
        duration_s=round(result.duration_s, 3),
        **_count_fields(counts),
        peak_in_flight=stats.peak_in_flight,
        peak_raw_queue_depth=stats.peak_raw_queue_depth,
        blocked_puts=stats.blocked_puts,
        flushes=stats.flushes,
        skipped_on_stop=stats.skipped_on_stop,
    )
    return result


def _count_fields(counts: RunCounts) -> dict[str, int]:
    return {
        "fetched": counts.fetched,
        "inserted": counts.inserted,
        "updated": counts.updated,
        "unchanged": counts.unchanged,
        "invalid": counts.invalid,
        "missing": counts.missing,
        "retries": counts.retries,
    }


class _Stages:
    """The task bodies of one run. Shared state is only mutated between awaits (single thread)."""

    def __init__(
        self,
        store: Store,
        connector: Connector,
        ctx: FetchContext,
        run_id: int,
        cfg: PipelineConfig,
        counts: RunCounts,
        stats: PipelineStats,
        run_log: FilteringBoundLogger,
        retrier: Retrier,
        tracker: WatermarkTracker,
        stop: StopSignal | None,
    ) -> None:
        self._store, self._connector, self._ctx = store, connector, ctx
        self._run_id, self._cfg = run_id, cfg
        self._counts, self._stats, self._log = counts, stats, run_log
        self._retrier, self._tracker, self._stop = retrier, tracker, stop
        # None is the sentinel in both queues: "no more work, exit your loop".
        self._jobs: asyncio.Queue[Job | None] = asyncio.Queue(cfg.job_queue_maxsize)
        self._raw: asyncio.Queue[Fetched | None] = asyncio.Queue(cfg.raw_queue_maxsize)
        # Flush tasks in progress. They outlive a cancelled worker on purpose (see _flush).
        self._inflight: set[asyncio.Task[None]] = set()

    def _stopping(self) -> bool:
        return self._stop is not None and self._stop.is_set()

    async def run(self, jobs: list[Job]) -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._feed(jobs), name="feeder")
            tg.create_task(self._fetch_stage(), name="fetch-stage")
            for worker_id in range(self._cfg.workers):
                tg.create_task(self._worker(worker_id), name=f"worker-{worker_id}")

    async def settle(self) -> None:
        """Wait for flushes that were mid-write when the run was cancelled or failed (ADR-0027)."""
        if self._inflight:
            await asyncio.shield(asyncio.gather(*self._inflight, return_exceptions=True))

    async def _feed(self, jobs: list[Job]) -> None:
        for fed, job in enumerate(jobs):
            if self._stopping():
                self._stats.skipped_on_stop = len(jobs) - fed
                self._log.info("feeder_stopped", unfed=len(jobs) - fed)
                break
            # If the queue is full this waits; fetchers keep draining it (even while stopping),
            # so the put always completes and the stop check above runs.
            await self._jobs.put(job)
        for _ in range(self._cfg.concurrency):
            await self._jobs.put(None)

    async def _fetch_stage(self) -> None:
        async with asyncio.TaskGroup() as fetchers:
            for fetcher_id in range(self._cfg.concurrency):
                fetchers.create_task(self._fetcher(), name=f"fetcher-{fetcher_id}")
        # Every fetched page is now in raw_queue; FIFO puts the sentinels behind all of them.
        for _ in range(self._cfg.workers):
            await self._raw.put(None)

    async def _fetcher(self) -> None:
        while True:
            job = await self._jobs.get()
            if job is None:
                return
            if self._stopping():
                self._stats.skipped_on_stop += 1  # drain: take jobs off the queue, don't fetch
                continue
            await self._fetch_job(job)

    async def _fetch_job(self, job: Job) -> None:
        """Fetch one job's pages in order. Only this fetcher touches the job (ADR-0028)."""
        pages = job_pages(self._connector, self._ctx, job, self._retrier, self._tracker)
        async with aclosing(pages):
            while True:
                self._stats.in_flight += 1
                self._stats.peak_in_flight = max(self._stats.peak_in_flight, self._stats.in_flight)
                try:
                    fetched = await anext(pages, None)
                finally:
                    self._stats.in_flight -= 1
                if fetched is None:
                    return
                await self._put_raw(fetched)
                if self._stopping():
                    return  # drain: the page in hand is queued; don't start the next one

    async def _put_raw(self, entry: Fetched) -> None:
        if not self._raw.full():
            await self._raw.put(entry)  # does not suspend when there is room
        else:
            key = entry.job.key if not isinstance(entry, DeadLetter) else entry.source_id
            self._stats.blocked_puts += 1
            self._log.info("producer_blocked", job=key, queue_depth=self._raw.qsize())
            waited_from = time.monotonic()
            await self._raw.put(entry)
            waited_ms = round((time.monotonic() - waited_from) * 1000, 1)
            self._log.info("producer_unblocked", job=key, waited_ms=waited_ms)
        self._stats.peak_raw_queue_depth = max(self._stats.peak_raw_queue_depth, self._raw.qsize())

    async def _worker(self, worker_id: int) -> None:
        loop = asyncio.get_running_loop()
        buffer: list[Record | DeadLetter] = []
        deadline = 0.0  # loop time by which the oldest buffered record must be flushed
        while True:
            # get() on a non-empty queue returns without suspending, so the timeout below can't
            # fire under sustained load: honour the deadline explicitly as well.
            if buffer and loop.time() >= deadline:
                await self._flush(worker_id, buffer, reason="interval")
                buffer = []
                continue
            try:
                # No timeout while the buffer is empty; otherwise wake up at the flush deadline.
                # Cancelling a waiting get() never loses an item (CPython queues.py).
                async with asyncio.timeout_at(deadline if buffer else None):
                    entry = await self._raw.get()
            except TimeoutError:
                await self._flush(worker_id, buffer, reason="interval")
                buffer = []
                continue
            if entry is None:
                if buffer:
                    await self._flush(worker_id, buffer, reason="final")
                return
            results = classify(self._connector, entry, self._counts, self._log, self._tracker)
            if results and not buffer:
                deadline = loop.time() + self._cfg.flush_interval_s
            buffer.extend(results)
            # One page can hold many records (GitHub: 100), so write exact-size chunks.
            size = self._cfg.batch_size
            while len(buffer) >= size:
                await self._flush(worker_id, buffer[:size], reason="size")
                buffer = buffer[size:]
                if buffer:
                    deadline = loop.time() + self._cfg.flush_interval_s

    async def _flush(
        self, worker_id: int, batch: list[Record | DeadLetter], *, reason: str
    ) -> None:
        # Cancellation-safe (ADR-0027): the write AND the counting run as one task that a
        # cancelled worker can't interrupt halfway; settle() awaits it before the run is recorded.
        write = asyncio.create_task(
            self._write(worker_id, batch, reason), name=f"flush-w{worker_id}"
        )
        self._inflight.add(write)
        write.add_done_callback(self._write_done)
        await asyncio.shield(write)

    def _write_done(self, task: asyncio.Task[None]) -> None:
        self._inflight.discard(task)
        # Retrieve the exception so it is never "unretrieved"; the awaiting worker (if still
        # alive) gets it through shield() and fails the run.
        if not task.cancelled() and (exc := task.exception()) is not None:
            self._log.warning("flush_failed", task=task.get_name(), error=repr(exc))

    async def _write(self, worker_id: int, batch: list[Record | DeadLetter], reason: str) -> None:
        items = [(r.source_id, r.payload_json) for r in batch if isinstance(r, Record)]
        rejects = [r for r in batch if isinstance(r, DeadLetter)]
        result = await self._store.flush(self._run_id, self._connector.source, items, rejects)
        for outcome in result.outcomes.values():
            self._counts.record(outcome)
        if len(result.outcomes) < len(items):
            dropped = len(items) - len(result.outcomes)
            self._log.warning("batch_duplicates", worker_id=worker_id, dropped=dropped)
        self._stats.flushes += 1
        self._stats.flush_sizes.append(len(batch))
        self._log.debug(
            "batch_flushed",
            worker_id=worker_id,
            size=len(batch),
            dead_lettered=result.dead_lettered,
            resolved=result.resolved,
            reason=reason,
        )
