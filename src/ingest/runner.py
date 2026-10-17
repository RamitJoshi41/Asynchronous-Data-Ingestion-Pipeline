"""Run one or more sources: per-source HTTP clients, one sized DB pool, signals (ADR-0029/0030).

`run all` starts one independent pipeline instance per source in a TaskGroup. A source that FAILS
doesn't cancel the others: its run is already recorded `failed` and logged by the pipeline, and
the failure comes back as a SourceOutcome. A forced cancellation (second Ctrl-C) is not caught,
so it still stops everything.
Explainer: docs/explain/ingest/runner.md
"""

import asyncio
import contextlib
from collections.abc import Sequence
from dataclasses import dataclass

from ingest.config import Settings
from ingest.connector import Connector
from ingest.db import create_pool
from ingest.httpclient import build_http_client
from ingest.pipeline import PipelineConfig, run_concurrent
from ingest.retry import RetryPolicy
from ingest.sequential import RunSummary, run_sequential
from ingest.shutdown import StopSignal, handle_signals
from ingest.storage import Store


@dataclass(frozen=True)
class SourcePlan:
    connector: Connector
    cfg: PipelineConfig | None  # None: the sequential baseline runner


@dataclass(frozen=True)
class SourceOutcome:
    source: str
    summary: RunSummary | None  # None when the run failed
    error: Exception | None = None  # what failed it (already recorded in its run row)


def pool_settings(settings: Settings, plans: Sequence[SourcePlan]) -> Settings:
    """Settings whose DB pool fits every plan at once (ADR-0018 generalised in ADR-0030).

    A concurrent source needs one connection per flushing worker plus one for its run row; a
    sequential source uses one at a time.
    """
    needed = sum(p.cfg.workers + 1 if p.cfg is not None else 1 for p in plans)
    return settings.model_copy(update={"db_pool_max_size": max(settings.db_pool_max_size, needed)})


async def run_sources(
    settings: Settings,
    plans: Sequence[SourcePlan],
    *,
    signals: bool = False,
    stop: StopSignal | None = None,
) -> list[SourceOutcome]:
    """Run every plan concurrently; return one outcome per plan, in order.

    `signals=True` (the CLI) turns SIGINT/SIGTERM into a graceful drain of all sources (ADR-0026).
    """
    settings = pool_settings(settings, plans)
    stop = stop or StopSignal()
    policy = RetryPolicy.from_settings(settings)
    main_task = asyncio.current_task()
    assert main_task is not None
    pool = await create_pool(settings)
    try:
        with handle_signals(stop, main_task) if signals else contextlib.nullcontext():
            store = Store(pool, acquire_timeout=settings.db_acquire_timeout_s)
            async with asyncio.TaskGroup() as tg:
                tasks = [
                    tg.create_task(
                        _run_one(settings, store, plan, policy, stop),
                        name=f"source-{plan.connector.source}",
                    )
                    for plan in plans
                ]
            return [task.result() for task in tasks]
    finally:
        await pool.close()


async def _run_one(
    settings: Settings, store: Store, plan: SourcePlan, policy: RetryPolicy, stop: StopSignal
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
                summary = await run_sequential(store, client, connector, policy=policy, stop=stop)
            else:
                summary = await run_concurrent(
                    store,
                    client,
                    connector,
                    cfg=plan.cfg,
                    policy=policy,
                    stop=stop,
                    drain_timeout_s=settings.shutdown_grace_s,
                )
        except Exception as exc:  # includes ExceptionGroup; CancelledError is NOT caught
            # Isolation, not swallowing: the run row says 'failed', the error was logged by the
            # runner, and the caller turns this outcome into a non-zero exit code.
            return SourceOutcome(connector.source, None, exc)
    return SourceOutcome(connector.source, summary)
