import asyncio
from pathlib import Path

import asyncpg
import pytest

from ingest.db import DbPool
from ingest.migrate import Migration, MigrationError, apply_migrations, load_migrations

T = 5.0  # acquire timeout for tests


async def table_names(pool: DbPool) -> set[str]:
    rows = await pool.fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
    )
    return {r["table_name"] for r in rows}


async def applied_versions(pool: DbPool) -> list[int]:
    rows = await pool.fetch("SELECT version FROM schema_migrations ORDER BY version")
    return [r["version"] for r in rows]


M1 = Migration(1, "one", "CREATE TABLE one (id int)")
M2 = Migration(2, "two", "CREATE TABLE two (id int)")
M3 = Migration(3, "three", "CREATE TABLE three (id int)")


def test_loads_real_migrations_in_version_order() -> None:
    migrations = load_migrations()
    assert [(m.version, m.name) for m in migrations] == [(1, "ingestion_runs"), (2, "items")]
    assert all(len(m.checksum) == 64 for m in migrations)


def test_rejects_bad_filename(tmp_path: Path) -> None:
    (tmp_path / "1_too_short.sql").write_text("SELECT 1")
    with pytest.raises(MigrationError, match="bad migration filename"):
        load_migrations(tmp_path)


def test_rejects_duplicate_versions(tmp_path: Path) -> None:
    (tmp_path / "0001_a.sql").write_text("SELECT 1")
    (tmp_path / "0001_b.sql").write_text("SELECT 2")
    with pytest.raises(MigrationError, match="duplicate"):
        load_migrations(tmp_path)


async def test_applies_real_migrations_once(pool: DbPool) -> None:
    migrations = load_migrations()
    assert await apply_migrations(pool, migrations, acquire_timeout=T) == [1, 2]
    assert {"ingestion_runs", "items", "schema_migrations"} <= await table_names(pool)
    assert await apply_migrations(pool, migrations, acquire_timeout=T) == []  # idempotent


async def test_failing_migration_is_rolled_back_and_not_recorded(pool: DbPool) -> None:
    broken = Migration(2, "broken", "CREATE TABLE half (id int); SELECT 1 / 0")
    with pytest.raises(asyncpg.DivisionByZeroError):
        await apply_migrations(pool, [M1, broken], acquire_timeout=T)
    assert await applied_versions(pool) == [1]
    assert "half" not in await table_names(pool)  # the CREATE TABLE was rolled back too

    # Fixing the file and re-running works, which also proves the advisory lock was released.
    async with asyncio.timeout(T):
        assert await apply_migrations(pool, [M1, M2], acquire_timeout=T) == [2]


async def test_edited_applied_migration_is_refused(pool: DbPool) -> None:
    await apply_migrations(pool, [M1], acquire_timeout=T)
    edited = Migration(1, "one", "CREATE TABLE one (id bigint)")
    with pytest.raises(MigrationError, match="edited after being applied"):
        await apply_migrations(pool, [edited, M2], acquire_timeout=T)
    assert await applied_versions(pool) == [1]  # nothing else ran


async def test_applied_migration_without_file_is_refused(pool: DbPool) -> None:
    await apply_migrations(pool, [M1, M2], acquire_timeout=T)
    with pytest.raises(MigrationError, match="no file"):
        await apply_migrations(pool, [M1], acquire_timeout=T)


async def test_out_of_order_migration_is_refused(pool: DbPool) -> None:
    await apply_migrations(pool, [M1, M3], acquire_timeout=T)
    with pytest.raises(MigrationError, match="older than applied"):
        await apply_migrations(pool, [M1, M2, M3], acquire_timeout=T)


async def test_concurrent_migrators_apply_each_migration_exactly_once(pool: DbPool) -> None:
    # Two migrators race on the same schema. The advisory lock serialises them, so one applies
    # everything and the other finds nothing pending (no duplicate-key or "already exists" error).
    async with asyncio.timeout(T), asyncio.TaskGroup() as tg:
        a = tg.create_task(apply_migrations(pool, [M1, M2], acquire_timeout=T))
        b = tg.create_task(apply_migrations(pool, [M1, M2], acquire_timeout=T))
    assert sorted([a.result(), b.result()]) == [[], [1, 2]]
    assert await applied_versions(pool) == [1, 2]
