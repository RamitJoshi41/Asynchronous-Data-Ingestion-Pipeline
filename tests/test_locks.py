"""The per-source overlap guard (ADR-0036): advisory locks, all-or-nothing, stale-run cleanup."""

import asyncpg
import pytest
import respx

from ingest.__main__ import migrate_command, replay_command, run_command
from ingest.config import Settings
from ingest.db import DbPool
from ingest.runner import SourceBusyError, SourceLocks, lock_key

T = 5.0


@pytest.fixture
async def settings(schema_settings: Settings) -> Settings:
    await migrate_command(schema_settings)
    return schema_settings


def test_lock_keys_are_stable_signed_int4() -> None:
    assert lock_key("hacker_news") == lock_key("hacker_news")
    assert lock_key("github") != lock_key("hacker_news")
    assert all(-(2**31) <= lock_key(s) < 2**31 for s in ["a", "github", "open_meteo", "x" * 100])


async def test_second_session_sees_the_source_busy(
    migrated_pool: DbPool, other_session: DbPool
) -> None:
    locks = await SourceLocks.acquire(migrated_pool, ["github"], acquire_timeout=T)
    try:  # release even if the assertion fails, or pool.close() waits forever for the connection
        with pytest.raises(SourceBusyError) as exc_info:
            await SourceLocks.acquire(other_session, ["github"], acquire_timeout=T)
        assert exc_info.value.sources == ["github"]
    finally:
        await locks.release()
    again = await SourceLocks.acquire(other_session, ["github"], acquire_timeout=T)
    await again.release()


async def test_all_or_nothing(migrated_pool: DbPool, other_session: DbPool) -> None:
    held = await SourceLocks.acquire(other_session, ["github"], acquire_timeout=T)
    try:
        with pytest.raises(SourceBusyError):
            await SourceLocks.acquire(migrated_pool, ["hacker_news", "github"], acquire_timeout=T)
        # hacker_news must not stay locked just because github was busy.
        free = await SourceLocks.acquire(migrated_pool, ["hacker_news"], acquire_timeout=T)
        await free.release()
    finally:
        await held.release()


async def test_a_dead_sessions_lock_is_released_by_postgres(
    settings: Settings, migrated_pool: DbPool
) -> None:
    conn = await asyncpg.connect(settings.db_dsn.get_secret_value())
    assert await conn.fetchval("SELECT pg_try_advisory_lock(74201002, $1)", lock_key("github"))
    conn.terminate()  # like SIGKILL: no unlock, the socket just goes away
    for _ in range(50):  # the server notices the closed connection almost immediately
        try:
            locks = await SourceLocks.acquire(migrated_pool, ["github"], acquire_timeout=T)
        except SourceBusyError:
            continue
        await locks.release()
        return
    pytest.fail("lock was never released")


async def test_stale_running_rows_are_abandoned_when_the_lock_is_free(
    migrated_pool: DbPool,
) -> None:
    zombie = await migrated_pool.fetchval(
        "INSERT INTO ingestion_runs (source, status) VALUES ('github', 'running') RETURNING id"
    )
    other = await migrated_pool.fetchval(
        "INSERT INTO ingestion_runs (source, status) VALUES ('hacker_news', 'running') RETURNING id"
    )
    locks = await SourceLocks.acquire(migrated_pool, ["github"], acquire_timeout=T)
    await locks.release()
    rows = {
        r["id"]: (r["status"], r["error"])
        for r in await migrated_pool.fetch("SELECT id, status, error FROM ingestion_runs")
    }
    assert rows[zombie] == ("failed", "abandoned: process died mid-run")
    assert rows[other] == ("running", None)  # a different source's lock wasn't taken


async def test_cli_run_refuses_a_busy_source(
    settings: Settings, other_session: DbPool, respx_mock: respx.MockRouter
) -> None:
    held = await SourceLocks.acquire(other_session, ["hacker_news"], acquire_timeout=T)
    try:
        with pytest.raises(SourceBusyError):
            await run_command(
                settings.model_copy(update={"hn_base_url": "https://hn.test/v0"}), ["hn"]
            )
        with pytest.raises(SourceBusyError):
            await replay_command(settings, ["hn"])
    finally:
        await held.release()
    assert not respx_mock.calls  # refused before any HTTP request
