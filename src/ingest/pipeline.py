"""Concurrent Hacker News ingestion (Phase 2), made reliable (Phase 3).

    feeder ─► id_queue ─► C fetchers ─► raw_queue ─► W workers (validate, per-worker batch) ─► DB

Every queue is bounded (full ⇒ the producer waits: backpressure). All tasks live in TaskGroups, so
any unexpected error cancels everything and nothing outlives the run. Shutdown is sentinel-driven:
the feeder sends C sentinels to the fetchers; after all fetchers finish, W sentinels go to the
workers, which flush their last partial batch and exit.
Phase 3: fetches retry (shared Retrier + rate-limit gate), rejects are dead-lettered in the same
flush transaction, a stop request drains the pipeline under a deadline, and flushes are
cancellation-safe so item counts always match committed rows.
Explainer: docs/explain/ingest/pipeline.md   Decisions: ADR-0014..0027
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Self

import httpx
import structlog
from structlog.typing import FilteringBoundLogger

from ingest.config import Settings
from ingest.models.hn import SOURCE
from ingest.records import classify_hn_body, fetch_hn_body, finish_safely, make_retrier
from ingest.retry import DEFAULT_POLICY, Retrier, RetryPolicy
from ingest.sequential import RunSummary
from ingest.shutdown import StopSignal
from ingest.sources.hn import fetch_top_ids
from ingest.storage import DeadLetter, RunCounts, RunStatus, Store

log = structlog.get_logger()

type RawEntry = tuple[int, bytes | DeadLetter]  # (requested id, body or an HTTP-level reject)


@dataclass(frozen=True)
class PipelineConfig:
    concurrency: int
    workers: int
    batch_size: int
    flush_interval_s: float
    id_queue_maxsize: int
    raw_queue_maxsize: int

    def __post_init__(self) -> None:
        sizes = (self.concurrency, self.workers, self.batch_size)
        queues = (self.id_queue_maxsize, self.raw_queue_maxsize)
        # Queue maxsize 0 would mean UNBOUNDED in asyncio: forbidden (CLAUDE.md §7).
        if min(*sizes, *queues) < 1 or self.flush_interval_s <= 0:
            raise ValueError(f"invalid pipeline config: {self}")

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        concurrency: int | None = None,
        workers: int | None = None,
        batch_size: int | None = None,
    ) -> Self:
        """Defaults from Settings; the CLI and benchmark override the three tuning knobs."""
        return cls(
            concurrency=settings.fetch_concurrency if concurrency is None else concurrency,
            workers=settings.pipeline_workers if workers is None else workers,
            batch_size=settings.batch_size if batch_size is None else batch_size,
            flush_interval_s=settings.flush_interval_s,
            id_queue_maxsize=settings.id_queue_maxsize,
            raw_queue_maxsize=settings.raw_queue_maxsize,
        )


def settings_for(settings: Settings, cfg: PipelineConfig) -> Settings:
    """Settings consistent with a (possibly overridden) config, for building the client and pool.

    httpx limits come from `fetch_concurrency` (ADR-0019) and the pool must be >= workers + 1
    (ADR-0018). If a caller overrides the config but builds resources from the original settings,
    httpx's pool silently caps concurrency or flushes exhaust the DB pool.
    """
    return settings.model_copy(
        update={
            "fetch_concurrency": cfg.concurrency,
            "pipeline_workers": cfg.workers,
            "db_pool_max_size": max(settings.db_pool_max_size, cfg.workers + 1),
        }
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
    skipped_on_stop: int = 0  # ids left unfetched because a stop was requested


@dataclass(frozen=True)
class ConcurrentRunSummary(RunSummary):
    stats: PipelineStats = field(default_factory=PipelineStats)


async def run_hn_concurrent(
    store: Store,
    client: httpx.AsyncClient,
    *,
    base_url: str,
    limit: int,
    cfg: PipelineConfig,
    policy: RetryPolicy = DEFAULT_POLICY,
    stop: StopSignal | None = None,
    drain_timeout_s: float = 10.0,
) -> ConcurrentRunSummary:
    """Concurrent version of run_hn_sequential: same classification, counts and run lifecycle.

    Outcomes: `succeeded`; `cancelled` (stop requested and drained, drain deadline exceeded, or
    forced cancellation); `failed` (an error survived retries; the ExceptionGroup is re-raised).
    """
    # Re-checked here because configs can be built in code, bypassing Settings validation.
    if store.pool_max_size < cfg.workers + 1:
        raise ValueError(
            f"DB pool max size {store.pool_max_size} < workers + 1 ({cfg.workers + 1}): "
            "flushes would exhaust the pool (ADR-0018)"
        )
    run_id = await store.start_run(SOURCE)
    run_log = log.bind(run_id=run_id, source=SOURCE)
    counts, stats = RunCounts(), PipelineStats()
    retrier = make_retrier(policy, counts, run_log)
    stages = _Stages(store, client, base_url, run_id, cfg, counts, stats, run_log, retrier, stop)
    started = time.perf_counter()
    run_log.info(
        "run_started",
        limit=limit,
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
        ids = await retrier.run(lambda: fetch_top_ids(client, base_url, limit=limit))
        async with asyncio.timeout(None) as drain:
            if stop is not None:
                stop.on_stop(arm_drain_deadline)
            await stages.run(ids)
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
    status = RunStatus.CANCELLED if stop is not None and stop.is_set() else RunStatus.SUCCEEDED
    await finish_safely(store, run_id, status, counts)
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
        client: httpx.AsyncClient,
        base_url: str,
        run_id: int,
        cfg: PipelineConfig,
        counts: RunCounts,
        stats: PipelineStats,
        run_log: FilteringBoundLogger,
        retrier: Retrier,
        stop: StopSignal | None,
    ) -> None:
        self._store, self._client, self._base_url = store, client, base_url
        self._run_id, self._cfg = run_id, cfg
        self._counts, self._stats, self._log = counts, stats, run_log
        self._retrier, self._stop = retrier, stop
        # None is the sentinel in both queues: "no more work, exit your loop".
        self._ids: asyncio.Queue[int | None] = asyncio.Queue(cfg.id_queue_maxsize)
        self._raw: asyncio.Queue[RawEntry | None] = asyncio.Queue(cfg.raw_queue_maxsize)
        # Flush tasks in progress. They outlive a cancelled worker on purpose (see _flush).
        self._inflight: set[asyncio.Task[None]] = set()

    def _stopping(self) -> bool:
        return self._stop is not None and self._stop.is_set()

    async def run(self, ids: list[int]) -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._feed(ids), name="feeder")
            tg.create_task(self._fetch_stage(), name="fetch-stage")
            for worker_id in range(self._cfg.workers):
                tg.create_task(self._worker(worker_id), name=f"worker-{worker_id}")

    async def settle(self) -> None:
        """Wait for flushes that were mid-write when the run was cancelled or failed (ADR-0027)."""
        if self._inflight:
            await asyncio.shield(asyncio.gather(*self._inflight, return_exceptions=True))

    async def _feed(self, ids: list[int]) -> None:
        for fed, item_id in enumerate(ids):
            if self._stopping():
                self._stats.skipped_on_stop = len(ids) - fed
                self._log.info("feeder_stopped", unfed=len(ids) - fed)
                break
            # If the queue is full this waits; fetchers keep draining it (even while stopping),
            # so the put always completes and the stop check above runs.
            await self._ids.put(item_id)
        for _ in range(self._cfg.concurrency):
            await self._ids.put(None)

    async def _fetch_stage(self) -> None:
        async with asyncio.TaskGroup() as fetchers:
            for fetcher_id in range(self._cfg.concurrency):
                fetchers.create_task(self._fetcher(), name=f"fetcher-{fetcher_id}")
        # Every fetched body is now in raw_queue; FIFO puts the sentinels behind all of them.
        for _ in range(self._cfg.workers):
            await self._raw.put(None)

    async def _fetcher(self) -> None:
        while True:
            item_id = await self._ids.get()
            if item_id is None:
                return
            if self._stopping():
                self._stats.skipped_on_stop += 1  # drain: take ids off the queue, don't fetch
                continue
            self._stats.in_flight += 1
            self._stats.peak_in_flight = max(self._stats.peak_in_flight, self._stats.in_flight)
            try:
                fetched = await fetch_hn_body(
                    self._client, self._base_url, item_id, self._retrier, self._log
                )
            finally:
                self._stats.in_flight -= 1
            self._counts.fetched += 1
            await self._put_raw((item_id, fetched))

    async def _put_raw(self, entry: RawEntry) -> None:
        if not self._raw.full():
            await self._raw.put(entry)  # does not suspend when there is room
        else:
            self._stats.blocked_puts += 1
            self._log.info("producer_blocked", item_id=entry[0], queue_depth=self._raw.qsize())
            waited_from = time.monotonic()
            await self._raw.put(entry)
            waited_ms = round((time.monotonic() - waited_from) * 1000, 1)
            self._log.info("producer_unblocked", item_id=entry[0], waited_ms=waited_ms)
        self._stats.peak_raw_queue_depth = max(self._stats.peak_raw_queue_depth, self._raw.qsize())

    async def _worker(self, worker_id: int) -> None:
        loop = asyncio.get_running_loop()
        items: list[tuple[str, str]] = []
        rejects: list[DeadLetter] = []
        deadline = 0.0  # loop time by which the oldest buffered record must be flushed
        while True:
            pending = len(items) + len(rejects)
            # get() on a non-empty queue returns without suspending, so the timeout below can't
            # fire under sustained load: honour the deadline explicitly as well.
            if pending and loop.time() >= deadline:
                await self._flush(worker_id, items, rejects, reason="interval")
                items, rejects = [], []
                continue
            try:
                # No timeout while the buffer is empty; otherwise wake up at the flush deadline.
                # Cancelling a waiting get() never loses an item (CPython queues.py).
                async with asyncio.timeout_at(deadline if pending else None):
                    entry = await self._raw.get()
            except TimeoutError:
                await self._flush(worker_id, items, rejects, reason="interval")
                items, rejects = [], []
                continue
            if entry is None:
                if pending:
                    await self._flush(worker_id, items, rejects, reason="final")
                return
            result = classify_hn_body(entry[1], entry[0], self._counts, self._log)
            if result is None:
                continue
            if not pending:
                deadline = loop.time() + self._cfg.flush_interval_s
            if isinstance(result, DeadLetter):
                rejects.append(result)
            else:
                items.append((str(result.id), result.model_dump_json()))
            if len(items) + len(rejects) >= self._cfg.batch_size:
                await self._flush(worker_id, items, rejects, reason="size")
                items, rejects = [], []

    async def _flush(
        self,
        worker_id: int,
        items: list[tuple[str, str]],
        rejects: list[DeadLetter],
        *,
        reason: str,
    ) -> None:
        # Cancellation-safe (ADR-0027): the write AND the counting run as one task that a
        # cancelled worker can't interrupt halfway; settle() awaits it before the run is recorded.
        write = asyncio.create_task(
            self._write(worker_id, items, rejects, reason), name=f"flush-w{worker_id}"
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

    async def _write(
        self, worker_id: int, items: list[tuple[str, str]], rejects: list[DeadLetter], reason: str
    ) -> None:
        result = await self._store.flush(self._run_id, SOURCE, items, rejects)
        for outcome in result.outcomes.values():
            self._counts.record(outcome)
        if len(result.outcomes) < len(items):
            dropped = len(items) - len(result.outcomes)
            self._log.warning("batch_duplicates", worker_id=worker_id, dropped=dropped)
        self._stats.flushes += 1
        self._stats.flush_sizes.append(len(items) + len(rejects))
        self._log.debug(
            "batch_flushed",
            worker_id=worker_id,
            size=len(items) + len(rejects),
            dead_lettered=result.dead_lettered,
            resolved=result.resolved,
            reason=reason,
        )
