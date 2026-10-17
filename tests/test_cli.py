from collections.abc import Iterator

import asyncpg
import pytest
import respx

from ingest.__main__ import main, migrate_command, run_hn_command
from ingest.config import Settings, get_settings

BASE = "https://hn.test/v0"


@pytest.fixture(autouse=True)
def fresh_settings_cache() -> Iterator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_migrate_command_is_idempotent(schema_settings: Settings) -> None:
    assert await migrate_command(schema_settings) == [1, 2]
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
    assert "applied migrations: [1, 2]" in out and "schema up to date" in out


@pytest.mark.parametrize("argv", [["run", "hn", "--limit", "0"], ["run", "hn", "--limit", "501"]])
def test_main_rejects_out_of_range_limit(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(argv)
    assert exc_info.value.code == 2  # argparse usage error


def test_main_rejects_unknown_source() -> None:
    with pytest.raises(SystemExit):
        main(["run", "github"])
