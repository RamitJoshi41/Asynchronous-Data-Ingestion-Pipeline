from collections.abc import Iterator

import asyncpg
import pytest
import respx

import ingest.httpclient
import ingest.runner
from ingest.__main__ import (
    EXIT_CANCELLED,
    EXIT_FAILED,
    build_plans,
    exit_code,
    main,
    migrate_command,
    replay_command,
    run_command,
)
from ingest.config import Settings, get_settings
from ingest.connectors.registry import SOURCES
from ingest.db import DbPool, create_pool
from ingest.runner import SourceOutcome, pool_settings
from ingest.sequential import RunSummary
from ingest.storage import RunCounts, RunStatus

BASE = "https://hn.test/v0"


@pytest.fixture(autouse=True)
def fresh_settings_cache() -> Iterator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_migrate_command_is_idempotent(schema_settings: Settings) -> None:
    assert await migrate_command(schema_settings) == [1, 2, 3, 4, 5]
    assert await migrate_command(schema_settings) == []


async def test_run_command_end_to_end(
    schema_settings: Settings, respx_mock: respx.MockRouter
) -> None:
    await migrate_command(schema_settings)
    respx_mock.get(f"{BASE}/topstories.json").respond(json=[11, 12])
    respx_mock.get(f"{BASE}/item/11.json").respond(json={"id": 11, "type": "story"})
    respx_mock.get(f"{BASE}/item/12.json").respond(json={"id": 12, "type": "job"})

    settings = schema_settings.model_copy(update={"hn_base_url": BASE, "hn_top_limit": 2})
    [outcome] = await run_command(settings, ["hn"])
    assert outcome.source == "hacker_news" and outcome.error is None
    assert outcome.summary is not None and outcome.summary.counts.inserted == 2

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
    assert "applied migrations: [1, 2, 3, 4, 5]" in out and "schema up to date" in out


@pytest.mark.parametrize("argv", [["run", "hn", "--limit", "0"], ["run", "hn", "--limit", "501"]])
def test_main_rejects_out_of_range_limit(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(argv)
    assert exc_info.value.code == 2  # argparse usage error


def test_main_rejects_unknown_source() -> None:
    with pytest.raises(SystemExit):
        main(["run", "twitter"])


@pytest.mark.parametrize(
    "argv",
    [["run", "all", "--concurrency", "5"], ["run", "all", "--sequential"],
     ["run", "github", "--limit", "5"]],
)  # fmt: skip
def test_main_rejects_options_that_dont_fit_the_sources(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(argv)
    assert exc_info.value.code == 2


async def test_concurrency_override_reaches_httpx_limits_and_pool(
    schema_settings: Settings, respx_mock: respx.MockRouter, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Settings say 2 fetchers / 1 worker; the override asks for 8 / 3. If the client were built from
    # the settings default, httpx's pool (2 connections) would silently cap concurrency at 2.
    # respx intercepts httpcore's AsyncConnectionPool.handle_async_request, i.e. ABOVE the point
    # where httpx enforces its limits, so a mocked run cannot observe that cap. Instead, spy on
    # what the client and pool are actually built with.
    await migrate_command(schema_settings)
    built_with: dict[str, object] = {}
    real_client = ingest.httpclient.build_http_client

    def spy_client(settings: Settings, *, concurrency: int, headers: object = None) -> object:
        built_with["concurrency"] = concurrency
        return real_client(settings, concurrency=concurrency)

    async def spy_pool(settings: Settings, **kwargs: str | None) -> DbPool:
        built_with["pool"] = settings.db_pool_max_size
        return await create_pool(settings, **kwargs)

    monkeypatch.setattr("ingest.runner.build_http_client", spy_client)
    monkeypatch.setattr("ingest.runner.create_pool", spy_pool)
    respx_mock.get(f"{BASE}/topstories.json").respond(json=[1, 2, 3])
    respx_mock.get(url__regex=r"https://hn\.test/v0/item/\d+\.json").respond(
        json={"id": 1, "type": "story"}
    )
    settings = schema_settings.model_copy(
        update={"hn_base_url": BASE, "hn_concurrency": 2, "pipeline_workers": 1, "hn_top_limit": 1}
    )
    plans = build_plans(settings, ["hn"], concurrency=8, workers=3)
    await ingest.runner.run_sources(settings, plans)

    assert built_with["concurrency"] == 8
    assert built_with["pool"] >= 3 + 1  # type: ignore[operator]


def test_pool_fits_every_source_at_once() -> None:
    base = Settings(_env_file=None)
    plans = build_plans(base, ["github", "hn"], workers=6)
    assert pool_settings(base, plans).db_pool_max_size == 2 * (6 + 1)
    sequential = build_plans(base, ["hn"], sequential=True)
    assert pool_settings(base, sequential).db_pool_max_size == base.db_pool_max_size


def test_each_source_gets_its_own_concurrency_setting() -> None:
    base = Settings(_env_file=None, hn_concurrency=7, github_concurrency=3)
    plans = {p.connector.source: p.cfg for p in build_plans(base, ["hn", "github"])}
    assert {k: v.concurrency for k, v in plans.items() if v} == {"hacker_news": 7, "github": 3}


def summary(status: RunStatus) -> RunSummary:
    return RunSummary(1, status, RunCounts(), 0.1)


@pytest.mark.parametrize(
    ("outcomes", "code"),
    [([SourceOutcome("a", summary(RunStatus.SUCCEEDED))], 0),
     ([SourceOutcome("a", summary(RunStatus.CANCELLED))], EXIT_CANCELLED),
     ([SourceOutcome("a", summary(RunStatus.CANCELLED)), SourceOutcome("b", None, ValueError())],
      EXIT_FAILED)],
)  # fmt: skip
def test_exit_code_failed_beats_cancelled_beats_ok(
    outcomes: list[SourceOutcome], code: int
) -> None:
    assert exit_code(outcomes) == code


def test_main_rejects_invalid_pipeline_override() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["run", "hn", "--concurrency", "0"])
    assert exc_info.value.code == 2


async def test_replay_command_runs_a_replay(schema_settings: Settings) -> None:
    await migrate_command(schema_settings)
    [replayed] = await replay_command(schema_settings, ["hn"])
    assert replayed.counts.fetched == 0  # nothing dead-lettered yet: a clean no-op run


def test_main_replay(
    schema_settings: Settings, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DB_DSN", schema_settings.db_dsn.get_secret_value())
    assert main(["migrate"]) == 0
    assert main(["replay", "--batch-size", "10"]) == 0  # every source
    out = capsys.readouterr().out
    assert out.count("status=<RunStatus.SUCCEEDED") == len(SOURCES)  # one replay per source
