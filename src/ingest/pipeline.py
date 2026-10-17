"""Phase 2: concurrent Hacker News ingestion (the Phase 0 skeleton with real I/O).

    feeder ─► id_queue ─► C fetchers ─► raw_queue ─► W workers (validate, per-worker batch) ─► DB

Every queue is bounded (full ⇒ the producer waits: backpressure). All tasks live in TaskGroups, so
any unexpected error cancels everything and nothing outlives the run. Shutdown is sentinel-driven:
the feeder sends C sentinels to the fetchers; after all fetchers finish, W sentinels go to the
workers, which flush their last partial batch and exit.
Explainer: docs/explain/ingest/pipeline.md   Decisions: ADR-0014..0020
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
from ingest.records import classify_hn_body
from ingest.sequential import RunSummary
from ingest.sources.hn import fetch_item, fetch_top_ids
from ingest.storage import RunCounts, RunStatus, Store

log = structlog.get_logger()

type RawEntry = tuple[int, bytes]  # (requested id, response body)


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


@dataclass
class PipelineStats:
    """Concurrency facts about one run (logged, returned, asserted by tests)."""

    in_flight: int = 0
    peak_in_flight: int = 0
    peak_raw_queue_depth: int = 0
    blocked_puts: int = 0
    flushes: int = 0
    flush_sizes: list[int] = field(default_factory=list)


@dataclass(frozen=True)
class ConcurrentRunSummary(RunSummary):
    stats: PipelineStats = field(default_factory=PipelineStats)


async def run_hn_concurrent(
    store: Store, client: httpx.AsyncClient, *, base_url: str, limit: int, cfg: PipelineConfig
) -> ConcurrentRunSummary:
    """Concurrent version of run_hn_sequential: same classification, counts and run lifecycle."""
    # Re-checked here because configs can be built in code, bypassing Settings validation.
    if store.pool_max_size < cfg.workers + 1:
        raise ValueError(
            f"DB pool max size {store.pool_max_size} < workers + 1 ({cfg.workers + 1}): "
            "flushes would exhaust the pool (ADR-0018)"
        )
    run_id = await store.start_run(SOURCE)
    run_log = log.bind(run_id=run_id, source=SOURCE)
    counts, stats = RunCounts(), PipelineStats()
    started = time.perf_counter()
    run_log.info(
        "run_started",
        limit=limit,
        concurrency=cfg.concurrency,
        workers=cfg.workers,
        batch_size=cfg.batch_size,
    )
    try:
        ids = await fetch_top_ids(client, base_url, limit=limit)
        stages = _Stages(store, client, base_url, run_id, cfg, counts, stats, run_log)
        await stages.run(ids)
    except Exception as exc:  # includes ExceptionGroup from the TaskGroup
        # CancelledError is not an Exception: on Ctrl-C the row stays 'running' (Phase 3 fixes).
        await store.finish_run(run_id, RunStatus.FAILED, counts, error=repr(exc))
        run_log.error("run_failed", error=repr(exc), fetched=counts.fetched)
        raise
    await store.finish_run(run_id, RunStatus.SUCCEEDED, counts)
    duration = time.perf_counter() - started
    run_log.info(
        "run_finished",
        duration_s=round(duration, 3),
        fetched=counts.fetched,
        inserted=counts.inserted,
        updated=counts.updated,
        unchanged=counts.unchanged,
        invalid=counts.invalid,
        missing=counts.missing,
        peak_in_flight=stats.peak_in_flight,
        peak_raw_queue_depth=stats.peak_raw_queue_depth,
        blocked_puts=stats.blocked_puts,
        flushes=stats.flushes,
    )
    return ConcurrentRunSummary(run_id, RunStatus.SUCCEEDED, counts, duration, stats)


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
    ) -> None:
        self._store, self._client, self._base_url = store, client, base_url
        self._run_id, self._cfg = run_id, cfg
        self._counts, self._stats, self._log = counts, stats, run_log
        # None is the sentinel in both queues: "no more work, exit your loop".
        self._ids: asyncio.Queue[int | None] = asyncio.Queue(cfg.id_queue_maxsize)
        self._raw: asyncio.Queue[RawEntry | None] = asyncio.Queue(cfg.raw_queue_maxsize)

    async def run(self, ids: list[int]) -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._feed(ids), name="feeder")
            tg.create_task(self._fetch_stage(), name="fetch-stage")
            for worker_id in range(self._cfg.workers):
                tg.create_task(self._worker(worker_id), name=f"worker-{worker_id}")

    async def _feed(self, ids: list[int]) -> None:
        for item_id in ids:
            await self._ids.put(item_id)  # waits while id_queue is full
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
            self._stats.in_flight += 1
            self._stats.peak_in_flight = max(self._stats.peak_in_flight, self._stats.in_flight)
            try:
                body = await fetch_item(self._client, self._base_url, item_id)
            finally:
                self._stats.in_flight -= 1
            self._counts.fetched += 1
            await self._put_raw((item_id, body))

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
        batch: list[tuple[str, str]] = []
        deadline = 0.0  # loop time by which the oldest buffered record must be flushed
        while True:
            # get() on a non-empty queue returns without suspending, so the timeout below can't
            # fire under sustained load: honour the deadline explicitly as well.
            if batch and loop.time() >= deadline:
                await self._flush(worker_id, batch, reason="interval")
                batch = []
            try:
                # No timeout while the buffer is empty; otherwise wake up at the flush deadline.
                # Cancelling a waiting get() never loses an item (CPython queues.py).
                async with asyncio.timeout_at(deadline if batch else None):
                    entry = await self._raw.get()
            except TimeoutError:
                await self._flush(worker_id, batch, reason="interval")
                batch = []
                continue
            if entry is None:
                if batch:
                    await self._flush(worker_id, batch, reason="final")
                return
            item = classify_hn_body(entry[1], entry[0], self._counts, self._log)
            if item is None:
                continue
            if not batch:
                deadline = loop.time() + self._cfg.flush_interval_s
            batch.append((str(item.id), item.model_dump_json()))
            if len(batch) >= self._cfg.batch_size:
                await self._flush(worker_id, batch, reason="size")
                batch = []

    async def _flush(self, worker_id: int, batch: list[tuple[str, str]], *, reason: str) -> None:
        outcomes = await self._store.upsert_items(self._run_id, SOURCE, batch)
        for outcome in outcomes.values():
            self._counts.record(outcome)
        if len(outcomes) < len(batch):
            self._log.warning(
                "batch_duplicates", worker_id=worker_id, dropped=len(batch) - len(outcomes)
            )
        self._stats.flushes += 1
        self._stats.flush_sizes.append(len(batch))
        self._log.debug("batch_flushed", worker_id=worker_id, size=len(batch), reason=reason)
