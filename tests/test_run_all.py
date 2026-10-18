"""`run all`: independent pipeline instances per source in one event loop (ADR-0029, ADR-0030)."""

import asyncio
import json
from collections import defaultdict

import asyncpg
import httpx
import pytest
import respx
from pydantic import SecretStr

from ingest.__main__ import migrate_command
from ingest.config import Settings
from ingest.connectors.registry import build_plans
from ingest.runner import run_sources
from ingest.shutdown import StopSignal
from ingest.storage import RunStatus

HN = "https://hn.test/v0"
GH = "https://api.github.test"
TIMEOUT_S = 10.0


class Server:
    """Both mock APIs, measuring in-flight requests per host from the server side."""

    def __init__(self, respx_mock: respx.MockRouter, latency: float = 0.02) -> None:
        self.latency = latency
        self.in_flight: dict[str, int] = defaultdict(int)
        self.peak: dict[str, int] = defaultdict(int)
        self.requests: list[httpx.Request] = []
        self.github_status = 200
        respx_mock.get(f"{HN}/topstories.json").respond(json=list(range(1, 21)))
        respx_mock.get(url__regex=rf"{HN}/item/(?P<n>\d+)\.json").mock(side_effect=self._hn)
        respx_mock.get(url__regex=rf"{GH}/repos/o/(?P<n>\w+)/issues").mock(side_effect=self._gh)

    async def _timed(self, request: httpx.Request) -> None:
        host = request.url.host
        self.requests.append(request)
        self.in_flight[host] += 1
        self.peak[host] = max(self.peak[host], self.in_flight[host])
        try:
            await asyncio.sleep(self.latency)
        finally:
            self.in_flight[host] -= 1

    async def _hn(self, request: httpx.Request, n: str) -> httpx.Response:
        await self._timed(request)
        body = {"id": int(n), "type": "story", "time": 1175714200}
        return httpx.Response(200, content=json.dumps(body).encode())

    async def _gh(self, request: httpx.Request, n: str) -> httpx.Response:
        await self._timed(request)
        if self.github_status != 200:
            return httpx.Response(self.github_status)
        issue = {
            "id": 7,
            "number": 1,
            "title": n,
            "state": "open",
            "comments": 0,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "html_url": "https://github.com/o/r/issues/1",
        }
        return httpx.Response(200, json=[issue])


@pytest.fixture
async def settings(schema_settings: Settings) -> Settings:
    await migrate_command(schema_settings)
    return schema_settings.model_copy(
        update={
            "hn_base_url": HN,
            "hn_top_limit": 20,
            "hn_concurrency": 4,
            "github_base_url": GH,
            "github_repos": ["o/a", "o/b", "o/c", "o/d"],
            "github_concurrency": 1,
            "github_token": SecretStr("ghp_only_for_github"),
            "retry_max_attempts": 1,
        }
    )


async def run_rows(settings: Settings) -> dict[str, str]:
    conn = await asyncpg.connect(settings.db_dsn.get_secret_value())
    try:
        rows = await conn.fetch("SELECT source, status FROM ingestion_runs ORDER BY id")
    finally:
        await conn.close()
    return {r["source"]: r["status"] for r in rows}


async def test_sources_run_concurrently_each_within_its_own_limit(
    settings: Settings, respx_mock: respx.MockRouter
) -> None:
    server = Server(respx_mock)
    async with asyncio.timeout(TIMEOUT_S):
        outcomes = await run_sources(settings, build_plans(settings, ["github", "hn"]))
    assert [(o.source, o.error) for o in outcomes] == [("github", None), ("hacker_news", None)]
    assert server.peak == {"api.github.test": 1, "hn.test": 4}  # each source's own limit
    assert await run_rows(settings) == {"github": "succeeded", "hacker_news": "succeeded"}


async def test_the_token_is_never_sent_to_another_host(
    settings: Settings, respx_mock: respx.MockRouter
) -> None:
    server = Server(respx_mock, latency=0)
    await run_sources(settings, build_plans(settings, ["github", "hn"]))
    by_host = defaultdict(set)
    for request in server.requests:
        by_host[request.url.host].add(request.headers.get("Authorization"))
    assert by_host == {"api.github.test": {"Bearer ghp_only_for_github"}, "hn.test": {None}}


async def test_a_failing_source_does_not_cancel_the_others(
    settings: Settings, respx_mock: respx.MockRouter
) -> None:
    server = Server(respx_mock)
    server.github_status = 500  # retries are off in these settings: GitHub fails at once
    async with asyncio.timeout(TIMEOUT_S):
        github, hn = await run_sources(settings, build_plans(settings, ["github", "hn"]))
    assert isinstance(github.error, ExceptionGroup) and github.summary is None
    assert hn.error is None and hn.summary is not None
    assert hn.summary.counts.inserted == 20  # HN finished its whole run
    assert await run_rows(settings) == {"github": "failed", "hacker_news": "succeeded"}


async def test_one_stop_request_drains_every_source(
    settings: Settings, respx_mock: respx.MockRouter
) -> None:
    Server(respx_mock, latency=0.1)
    stop = StopSignal()
    asyncio.get_running_loop().call_later(0.15, stop.request, "test")
    async with asyncio.timeout(TIMEOUT_S):
        outcomes = await run_sources(settings, build_plans(settings, ["github", "hn"]), stop=stop)
    assert all(o.summary is not None for o in outcomes)
    assert {o.source: o.summary.status for o in outcomes if o.summary} == {
        "github": RunStatus.CANCELLED,
        "hacker_news": RunStatus.CANCELLED,
    }
    assert await run_rows(settings) == {"github": "cancelled", "hacker_news": "cancelled"}
