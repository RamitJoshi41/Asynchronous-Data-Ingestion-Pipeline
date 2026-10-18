"""FastAPI status + trigger layer (ADR-0036, ADR-0037).

The API and the runs it triggers share ONE event loop and ONE DB pool: a trigger starts
`runner.run_sources` as a background task, and request handlers keep being served while it runs
(everything is non-blocking I/O). The overlap guard is the per-source Postgres advisory lock, so a
CLI run in another process is seen too. Shutting the API down drains active runs.
Explainer: docs/explain/ingest/api.md
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Any

import structlog
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ingest.config import Settings
from ingest.connectors.registry import SOURCES, build_plans
from ingest.db import DbPool, create_pool
from ingest.observability import LiveRuns
from ingest.runner import SourceBusyError, SourceLocks, pool_settings, run_sources
from ingest.shutdown import StopSignal
from ingest.storage import Store

log = structlog.get_logger()

API_READ_CONNECTIONS = 2  # headroom in the pool for request handlers on top of every run
TRIGGER_START_TIMEOUT_S = 10.0  # how long POST /trigger waits for run ids


class TriggerRequest(BaseModel):
    sources: list[str] = Field(min_length=1, examples=[["hn"], ["all"]])


class TriggerResponse(BaseModel):
    runs: dict[str, int]  # stored source name -> run id


class LiveGauges(BaseModel):
    queue_depth: int
    in_flight: int
    queue_depth_peak: int
    in_flight_peak: int


class RunOut(BaseModel):
    id: int
    source: str
    kind: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    items_fetched: int
    items_inserted: int
    items_updated: int
    items_unchanged: int
    items_invalid: int
    items_missing: int
    retries: int
    error: str | None
    duration_ms: int | None
    requests: int | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_max_ms: float | None
    queue_depth_peak: int | None
    queue_depth_mean: float | None
    in_flight_peak: int | None
    in_flight_mean: float | None
    live: LiveGauges | None = None  # only while the run is going, in this process


@dataclass
class AppState:
    settings: Settings
    pool: DbPool
    live: LiveRuns = field(default_factory=LiveRuns)
    stop: StopSignal = field(default_factory=StopSignal)
    tasks: set[asyncio.Task[Any]] = field(default_factory=set)

    @property
    def store(self) -> Store:
        return Store(self.pool, acquire_timeout=self.settings.db_acquire_timeout_s)


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Sized for the worst case: every source running at once, plus request handlers.
        sized = pool_settings(
            settings, build_plans(settings, list(SOURCES)), extra=API_READ_CONNECTIONS
        )
        pool = await create_pool(sized)
        state = AppState(sized, pool)
        app.state.pipeline = state
        try:
            yield
        finally:
            # Graceful shutdown: every active run drains and is recorded 'cancelled' (ADR-0026).
            state.stop.request("api shutdown")
            if state.tasks:
                log.info("api_draining_runs", runs=len(state.tasks))
                await asyncio.gather(*state.tasks, return_exceptions=True)
            await pool.close()

    app = FastAPI(title="PipeLine", version="0.5", lifespan=lifespan)

    def state_of(request: Request) -> AppState:
        state: AppState = request.app.state.pipeline
        return state

    @app.get("/health")
    async def health(request: Request) -> JSONResponse:
        state = state_of(request)
        try:
            async with state.pool.acquire(timeout=state.settings.db_acquire_timeout_s) as conn:
                await conn.fetchval("SELECT 1")
        except Exception as exc:  # report, don't crash: health checks must always answer
            body = {"status": "degraded", "database": type(exc).__name__}
            return JSONResponse(body, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
        active = len(list(state.live))
        return JSONResponse({"status": "ok", "database": "ok", "active_runs": active})

    @app.get("/runs")
    async def list_runs(
        request: Request,
        source: str | None = None,
        before_id: Annotated[int | None, Query(ge=1)] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> list[RunOut]:
        """Newest first. Page with `before_id` = the last id of the previous page."""
        state = state_of(request)
        rows = await state.store.list_runs(source=source, before_id=before_id, limit=limit)
        return [_run_out(row, state.live) for row in rows]

    @app.get("/runs/{run_id}")
    async def get_run(request: Request, run_id: int) -> RunOut:
        state = state_of(request)
        row = await state.store.get_run(run_id)
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"run {run_id} not found")
        return _run_out(row, state.live)

    @app.post("/trigger", status_code=status.HTTP_202_ACCEPTED)
    async def trigger(request: Request, body: TriggerRequest) -> TriggerResponse:
        state = state_of(request)
        names = list(SOURCES) if "all" in body.sources else list(dict.fromkeys(body.sources))
        unknown = [n for n in names if n not in SOURCES]
        if unknown:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT, f"unknown sources: {unknown}"
            )
        plans = build_plans(state.settings, names)
        try:
            locks = await SourceLocks.acquire(
                state.pool,
                [p.connector.source for p in plans],
                acquire_timeout=state.settings.db_acquire_timeout_s,
            )
        except SourceBusyError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        started: dict[str, int] = {}
        ready = asyncio.Event()

        def on_start(source: str, run_id: int) -> None:
            started[source] = run_id
            if len(started) == len(plans):
                ready.set()

        run = asyncio.create_task(
            run_sources(
                state.settings,
                plans,
                stop=state.stop,
                pool=state.pool,
                locks=locks,  # released by run_sources when the runs end
                live=state.live,
                on_start=on_start,
            ),
            name=f"trigger-{'+'.join(names)}",
        )
        state.tasks.add(run)
        run.add_done_callback(lambda task: _trigger_done(state, task))
        waiter = asyncio.create_task(ready.wait())
        try:
            await asyncio.wait(
                {waiter, run}, timeout=TRIGGER_START_TIMEOUT_S, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            waiter.cancel()
        if not started and run.done() and run.exception() is not None:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "runs failed to start")
        return TriggerResponse(runs=started)

    return app


def _trigger_done(state: AppState, task: asyncio.Task[Any]) -> None:
    state.tasks.discard(task)
    # Retrieve the exception so it's never "unretrieved". Per-source failures were already recorded
    # and logged by their pipelines; this catches errors outside them (e.g. the DB pool failing).
    if not task.cancelled() and (exc := task.exception()) is not None:
        log.error("trigger_failed", task=task.get_name(), error=repr(exc))


def _run_out(row: dict[str, object], live: LiveRuns) -> RunOut:
    out = RunOut.model_validate(row)
    active = live.get(out.id)
    if active is not None and active.gauges is not None:
        g = active.gauges
        out.live = LiveGauges(
            queue_depth=g.queue_depth.latest,
            in_flight=g.in_flight.latest,
            queue_depth_peak=g.queue_depth.peak,
            in_flight_peak=g.in_flight.peak,
        )
    return out
