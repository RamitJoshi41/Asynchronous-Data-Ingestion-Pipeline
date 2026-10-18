"""FastAPI layer: health, runs, trigger with the overlap guard, live gauges, drain on shutdown."""

import asyncio
from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
import respx

from ingest.__main__ import migrate_command
from ingest.api import create_app
from ingest.config import Settings
from ingest.db import create_pool
from ingest.runner import SourceLocks

HN = "https://hn.test/v0"
TIMEOUT_S = 10.0


class SlowHN:
    def __init__(self, respx_mock: respx.MockRouter, n: int, latency: float) -> None:
        self.latency = latency
        respx_mock.get(f"{HN}/topstories.json").respond(json=list(range(1, n + 1)))
        respx_mock.get(url__regex=rf"{HN}/item/(?P<item_id>\d+)\.json").mock(side_effect=self._item)

    async def _item(self, request: httpx.Request, item_id: str) -> httpx.Response:
        await asyncio.sleep(self.latency)
        return httpx.Response(200, json={"id": int(item_id), "type": "story"})


@pytest.fixture
async def settings(schema_settings: Settings) -> Settings:
    await migrate_command(schema_settings)
    return schema_settings.model_copy(
        update={
            "hn_base_url": HN,
            "hn_top_limit": 10,
            "hn_concurrency": 2,
            "gauge_interval_s": 0.01,
            "shutdown_grace_s": 5.0,
        }
    )


@pytest.fixture
async def api(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    # httpx's ASGI transport doesn't run lifespan events, so drive the lifespan explicitly.
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://api.test") as client:
            yield client


async def wait_finished(api: httpx.AsyncClient, run_id: int) -> dict[str, object]:
    async with asyncio.timeout(TIMEOUT_S):
        while True:
            run: dict[str, object] = (await api.get(f"/runs/{run_id}")).json()
            if run["status"] != "running":
                return run
            await asyncio.sleep(0.02)


async def test_health(api: httpx.AsyncClient) -> None:
    response = await api.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok", "active_runs": 0}


async def test_trigger_runs_in_the_background_and_records_metrics(
    api: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    SlowHN(respx_mock, 10, latency=0.01)
    response = await api.post("/trigger", json={"sources": ["hn"]})
    assert response.status_code == 202
    run_id = response.json()["runs"]["hacker_news"]
    run = await wait_finished(api, run_id)
    assert run["status"] == "succeeded" and run["items_inserted"] == 10
    assert run["requests"] == 11 and run["latency_p95_ms"] is not None
    assert run["in_flight_peak"] is not None and run["live"] is None  # finished: no live view


async def test_live_gauges_while_running_then_409_for_a_second_trigger(
    api: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    SlowHN(respx_mock, 10, latency=0.2)
    run_id = (await api.post("/trigger", json={"sources": ["hn"]})).json()["runs"]["hacker_news"]
    await asyncio.sleep(0.1)
    running = (await api.get(f"/runs/{run_id}")).json()
    assert running["status"] == "running"
    assert running["live"]["in_flight"] == 2  # both fetchers waiting on the slow mock
    second = await api.post("/trigger", json={"sources": ["all"]})
    assert second.status_code == 409 and "hacker_news" in second.json()["detail"]
    assert (await api.get("/health")).json()["active_runs"] == 1
    await wait_finished(api, run_id)


async def test_409_when_another_process_holds_the_source(
    api: httpx.AsyncClient, settings: Settings
) -> None:
    other = await create_pool(settings)  # other sessions, like a CLI run in another terminal
    try:
        locks = await SourceLocks.acquire(other, ["hacker_news"], acquire_timeout=5.0)
        try:
            response = await api.post("/trigger", json={"sources": ["hn"]})
            assert response.status_code == 409
        finally:
            await locks.release()
    finally:
        await other.close()


async def test_unknown_source_is_422(api: httpx.AsyncClient) -> None:
    response = await api.post("/trigger", json={"sources": ["twitter"]})
    assert response.status_code == 422
    assert (await api.post("/trigger", json={"sources": []})).status_code == 422


async def test_runs_listing_is_newest_first_filtered_and_paged(
    api: httpx.AsyncClient, settings: Settings
) -> None:
    conn = await asyncpg.connect(settings.db_dsn.get_secret_value())
    try:
        for source in ["github", "hacker_news", "github"]:
            await conn.execute(
                "INSERT INTO ingestion_runs (source, status, finished_at) "
                "VALUES ($1, 'succeeded', now())",
                source,
            )
    finally:
        await conn.close()
    runs = (await api.get("/runs")).json()
    ids = [r["id"] for r in runs]
    assert ids == sorted(ids, reverse=True) and len(ids) == 3
    github = (await api.get("/runs", params={"source": "github", "limit": 1})).json()
    assert len(github) == 1 and github[0]["source"] == "github"
    older = (
        await api.get("/runs", params={"source": "github", "before_id": github[0]["id"]})
    ).json()
    assert [r["source"] for r in older] == ["github"] and older[0]["id"] < github[0]["id"]
    assert (await api.get("/runs/999999")).status_code == 404
    assert (await api.get("/runs", params={"limit": 0})).status_code == 422


async def test_shutdown_drains_active_runs(
    settings: Settings, respx_mock: respx.MockRouter
) -> None:
    SlowHN(respx_mock, 10, latency=0.1)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://api.test") as client:
            run_id = (await client.post("/trigger", json={"sources": ["hn"]})).json()["runs"][
                "hacker_news"
            ]
            await asyncio.sleep(0.15)
    # Leaving the lifespan = API shutdown: the run was drained and recorded, never left running.
    conn = await asyncpg.connect(settings.db_dsn.get_secret_value())
    try:
        row = await conn.fetchrow(
            "SELECT status, items_fetched, items_inserted FROM ingestion_runs WHERE id = $1", run_id
        )
    finally:
        await conn.close()
    assert row is not None and row["status"] == "cancelled"
    assert row["items_inserted"] == row["items_fetched"] < 10
