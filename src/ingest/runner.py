"""Run one or more sources: per-source HTTP clients, one sized DB pool, signals (ADR-0029/0030),
and the per-source overlap guard (ADR-0036).

`run all` starts one independent pipeline instance per source in a TaskGroup. A source that FAILS
doesn't cancel the others: its run is already recorded `failed` and logged by the pipeline, and
the failure comes back as a SourceOutcome. A forced cancellation (second Ctrl-C) is not caught,
so it still stops everything.
Explainer: docs/explain/ingest/runner.md
"""

import asyncio
import contextlib
import zlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import structlog

from ingest.config import Settings
from ingest.connector import Connector
from ingest.db import DbPool, PoolConnection, create_pool
from ingest.httpclient import build_http_client
from ingest.observability import LiveRuns
from ingest.pipeline import PipelineConfig, run_concurrent
from ingest.retry import RetryPolicy
from ingest.sequential import RunSummary, run_sequential
from ingest.shutdown import StopSignal, handle_signals
from ingest.storage import Store

log = structlog.get_logger()

# Two-int advisory keys live in a different key space from the migrator's single-bigint lock
# (Postgres docs, "Advisory Locks"); the first int namespaces PipeLine's per-source run locks.
RUN_LOCK_NAMESPACE = 74_201_002


@dataclass(frozen=True)
class SourcePlan:
    connector: Connector
    cfg: PipelineConfig | None  # None: the sequential baseline runner


@dataclass(frozen=True)
class SourceOutcome:
    source: str
    summary: RunSummary | None  # None when the run failed
    error: Exception | None = None  # what failed it (already recorded in its run row)


class SourceBusyError(Exception):
    """Another session (this process or any other) is already running one of these sources."""

    def __init__(self, sources: list[str]) -> None:
        super().__init__(f"already running: {', '.join(sources)}")
        self.sources = sources


def lock_key(source: str) -> int:
    """A stable int4 per source name (crc32, made signed to fit Postgres' int)."""
    value = zlib.crc32(source.encode())
    return value - 2**32 if value >= 2**31 else value


class SourceLocks:
    """Session-level advisory locks, one per source, each on its own pooled connection.

    Held for the whole run; released explicitly (asyncpg's connection reset on release also runs
    pg_advisory_unlock_all, and Postgres drops them if the process dies). That last property makes a
    free lock proof that a 'running' row is abandoned.
    """

    def __init__(self, held: dict[str, PoolConnection], pool: DbPool) -> None:
        self._held, self._pool = held, pool

    @classmethod
    async def acquire(
        cls, pool: DbPool, sources: Sequence[str], *, acquire_timeout: float
    ) -> "SourceLocks":
        """All or nothing: SourceBusyError (and nothing held) if any source is locked."""
        held: dict[str, PoolConnection] = {}
        busy: list[str] = []
        try:
            for source in sources:
                conn = await pool.acquire(timeout=acquire_timeout)
                got = await conn.fetchval(
                    "SELECT pg_try_advisory_lock($1, $2)", RUN_LOCK_NAMESPACE, lock_key(source)
                )
                if got:
                    held[source] = conn
                else:
                    await pool.release(conn)
                    busy.append(source)
            if busy:
                raise SourceBusyError(busy)
            store = Store(pool, acquire_timeout=acquire_timeout)
            for source in sources:
                abandoned = await store.abandon_stale_runs(source, "process died mid-run")
                if abandoned:
                    log.warning("stale_runs_abandoned", source=source, run_ids=abandoned)
        except BaseException:
            await cls(held, pool).release()
            raise
        return cls(held, pool)

    async def release(self) -> None:
        while self._held:
            source, conn = self._held.popitem()
            try:
                await conn.execute(
                    "SELECT pg_advisory_unlock($1, $2)", RUN_LOCK_NAMESPACE, lock_key(source)
                )
            finally:
                await self._pool.release(conn)


def pool_settings(settings: Settings, plans: Sequence[SourcePlan], *, extra: int = 0) -> Settings:
    """Settings whose DB pool fits every plan at once (ADR-0018, generalised in ADR-0030/0036).

    Per source: its lock connection, its run bookkeeping, and one connection per flushing worker
    (a sequential source flushes on its bookkeeping connection's turn). `extra` covers API reads.
    """
    needed = extra + sum(p.cfg.workers + 2 if p.cfg is not None else 2 for p in plans)
    return settings.model_copy(update={"db_pool_max_size": max(settings.db_pool_max_size, needed)})


async def run_sources(
    settings: Settings,
    plans: Sequence[SourcePlan],
    *,
    signals: bool = False,
    stop: StopSignal | None = None,
    pool: DbPool | None = None,
    locks: SourceLocks | None = None,
    live: LiveRuns | None = None,
    on_start: Callable[[str, int], None] | None = None,
) -> list[SourceOutcome]:
    """Run every plan concurrently; return one outcome per plan, in order.

    `signals=True` (the CLI) turns SIGINT/SIGTERM into a graceful drain of all sources (ADR-0026).
    The API passes its own `pool` (owned by its lifespan) and the `locks` it already took (so it
    can answer 409 before starting); otherwise both are created and released here.
    Raises SourceBusyError if a source is already running elsewhere.
    """
    stop = stop or StopSignal()
    policy = RetryPolicy.from_settings(settings)
    main_task = asyncio.current_task()
    assert main_task is not None
    owned_pool = pool is None
    if pool is None:
        settings = pool_settings(settings, plans)
        pool = await create_pool(settings)
    try:
        if locks is None:
            sources = [p.connector.source for p in plans]
            locks = await SourceLocks.acquire(
                pool, sources, acquire_timeout=settings.db_acquire_timeout_s
            )
        try:
            with handle_signals(stop, main_task) if signals else contextlib.nullcontext():
                store = Store(pool, acquire_timeout=settings.db_acquire_timeout_s)
                async with asyncio.TaskGroup() as tg:
                    tasks = [
                        tg.create_task(
                            _run_one(settings, store, plan, policy, stop, live, on_start),
                            name=f"source-{plan.connector.source}",
                        )
                        for plan in plans
                    ]
                return [task.result() for task in tasks]
        finally:
            await locks.release()
    finally:
        if owned_pool:
            await pool.close()


async def _run_one(
    settings: Settings,
    store: Store,
    plan: SourcePlan,
    policy: RetryPolicy,
    stop: StopSignal,
    live: LiveRuns | None,
    on_start: Callable[[str, int], None] | None,
) -> SourceOutcome:
    connector = plan.connector
    concurrency = 1 if plan.cfg is None else plan.cfg.concurrency
    # This source's own client: its limits match its concurrency (ADR-0019) and its headers
    # (e.g. a token) can't reach any other host (ADR-0029).
    async with build_http_client(
        settings, concurrency=concurrency, headers=connector.headers
    ) as client:
        try:
            if plan.cfg is None:
                summary = await run_sequential(
                    store, client, connector, policy=policy, stop=stop, live=live, on_start=on_start
                )
            else:
                summary = await run_concurrent(
                    store,
                    client,
                    connector,
                    cfg=plan.cfg,
                    policy=policy,
                    stop=stop,
                    drain_timeout_s=settings.shutdown_grace_s,
                    gauge_interval_s=settings.gauge_interval_s,
                    live=live,
                    on_start=on_start,
                )
        except Exception as exc:  # includes ExceptionGroup; CancelledError is NOT caught
            # Isolation, not swallowing: the run row says 'failed', the error was logged by the
            # runner, and the caller turns this outcome into a non-zero exit code.
            return SourceOutcome(connector.source, None, exc)
    return SourceOutcome(connector.source, summary)
