from collections.abc import Iterator

import asyncpg
import httpx
import pytest
import respx

from ingest.__main__ import main, migrate_command, replay_command, run_hn_command
from ingest.config import Settings, get_settings
from ingest.db import DbPool, create_pool
from ingest.httpclient import build_http_client, http_limits
from ingest.pipeline import PipelineConfig, settings_for

BASE = "https://hn.test/v0"


@pytest.fixture(autouse=True)
def fresh_settings_cache() -> Iterator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_migrate_command_is_idempotent(schema_settings: Settings) -> None:
    assert await migrate_command(schema_settings) == [1, 2, 3, 4]
    assert await migrate_command(schema_settings) == []


async def test_run_command_end_to_end(
    schema_settings: Settings, respx_mock: respx.MockRouter
) -> None:
    await migrate_command(schema_settings)
    respx_mock.get(f"{BASE}/topstories.json").respond(json=[11, 12])
    respx_mock.get(f"{BASE}/item/11.json").respond(json={"id": 11, "type": "story"})
    respx_mock.get(f"{BASE}/item/12.json").respond(json={"id": 12, "type": "job"})

    settings = schema_settings.model_copy(update={"hn_base_url": BASE})
    summary = await run_hn_command(settings, limit=2)
    assert summary.counts.inserted == 2

    conn = await asyncpg.connect(settings.db_dsn.get_secret_value())
    try:
        assert await conn.fetchval("SELECT count(*) FROM items") == 2
    finally:
        await conn.close()


def test_main_migrate_reads_db_dsn_from_env(
    schema_settings: Settings, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DB_DSN", schema_settings.db_dsn.get_secret_value())
    assert main(["migrate"]) == 0
    assert main(["migrate"]) == 0
    out = capsys.readouterr().out
    assert "applied migrations: [1, 2, 3, 4]" in out and "schema up to date" in out


@pytest.mark.parametrize("argv", [["run", "hn", "--limit", "0"], ["run", "hn", "--limit", "501"]])
def test_main_rejects_out_of_range_limit(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(argv)
    assert exc_info.value.code == 2  # argparse usage error


def test_main_rejects_unknown_source() -> None:
    with pytest.raises(SystemExit):
        main(["run", "github"])


async def test_concurrency_override_reaches_httpx_limits_and_pool(
    schema_settings: Settings, respx_mock: respx.MockRouter, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Settings say 2 fetchers / 1 worker; the override asks for 8 / 3. If the client were built from
    # the original settings, httpx's pool (2 connections) would silently cap concurrency at 2.
    # respx intercepts httpcore's AsyncConnectionPool.handle_async_request, i.e. ABOVE the point
    # where httpx enforces its limits, so a mocked run cannot observe that cap. Instead, spy on the
    # settings the client and pool are actually built from.
    await migrate_command(schema_settings)
    built_with: dict[str, Settings] = {}

    def spy_client(settings: Settings) -> httpx.AsyncClient:
        built_with["client"] = settings
        return build_http_client(settings)

    async def spy_pool(settings: Settings, **kwargs: str | None) -> DbPool:
        built_with["pool"] = settings
        return await create_pool(settings, **kwargs)

    monkeypatch.setattr("ingest.__main__.build_http_client", spy_client)
    monkeypatch.setattr("ingest.__main__.create_pool", spy_pool)
    respx_mock.get(f"{BASE}/topstories.json").respond(json=[1, 2, 3])
    respx_mock.get(url__regex=r"https://hn\.test/v0/item/\d+\.json").respond(
        json={"id": 1, "type": "story"}
    )
    settings = schema_settings.model_copy(
        update={"hn_base_url": BASE, "fetch_concurrency": 2, "pipeline_workers": 1}
    )
    await run_hn_command(
        settings, limit=1, cfg=PipelineConfig.from_settings(settings, concurrency=8, workers=3)
    )

    assert http_limits(built_with["client"]) == httpx.Limits(
        max_connections=8, max_keepalive_connections=8
    )
    assert built_with["pool"].db_pool_max_size >= 3 + 1


def test_settings_for_aligns_limits_and_pool() -> None:
    base = Settings(_env_file=None)
    aligned = settings_for(base, PipelineConfig.from_settings(base, concurrency=40, workers=6))
    assert (aligned.fetch_concurrency, aligned.pipeline_workers) == (40, 6)
    assert aligned.db_pool_max_size == 7


def test_main_rejects_invalid_pipeline_override() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["run", "hn", "--concurrency", "0"])
    assert exc_info.value.code == 2


async def test_replay_command_runs_a_replay(schema_settings: Settings) -> None:
    await migrate_command(schema_settings)
    summary = await replay_command(schema_settings)
    assert summary.counts.fetched == 0  # nothing dead-lettered yet: a clean no-op run


def test_main_replay(
    schema_settings: Settings, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DB_DSN", schema_settings.db_dsn.get_secret_value())
    assert main(["migrate"]) == 0
    assert main(["replay", "--batch-size", "10"]) == 0
    assert "status=<RunStatus.SUCCEEDED" in capsys.readouterr().out
