"""Shared fixtures: a real Postgres (compose `db` service) with one throwaway schema per test."""

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import asyncpg
import pytest
from pydantic import SecretStr

from ingest.config import Settings
from ingest.db import DbPool, create_pool
from ingest.migrate import apply_migrations, load_migrations

# Tests get their own database on the compose server so they never touch dev data.
# TEST_DB_DSN overrides it (CI, Phase 6). Tests are not app code, so reading env here is fine.
TEST_DB_DSN = os.environ.get("TEST_DB_DSN", "postgresql://ingest:ingest@localhost:5433/ingest_test")


async def _ensure_test_database(dsn: str) -> None:
    target = dsn.rsplit("/", 1)[1]
    admin_dsn = dsn.rsplit("/", 1)[0] + "/postgres"
    try:
        conn = await asyncpg.connect(admin_dsn, timeout=5)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.exit(f"Postgres not reachable ({exc!r}). Start it: docker compose up -d --wait db")
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", target)
        if not exists:
            # Identifiers can't be bind parameters; the name comes from our own constant.
            await conn.execute(f'CREATE DATABASE "{target}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    # Sync session fixture: asyncio.run here happens before any test's event loop exists.
    asyncio.run(_ensure_test_database(TEST_DB_DSN))
    return Settings(_env_file=None, db_dsn=SecretStr(TEST_DB_DSN))


@pytest.fixture
async def pool(test_settings: Settings) -> AsyncIterator[DbPool]:
    """An empty schema of its own: tests can't see each other's tables or rows."""
    schema = f"t_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(TEST_DB_DSN, timeout=5)
    try:
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        db_pool = await create_pool(test_settings, search_path=schema)
        try:
            yield db_pool
        finally:
            await db_pool.close()
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
    finally:
        await admin.close()


@pytest.fixture
async def migrated_pool(pool: DbPool, test_settings: Settings) -> DbPool:
    migrations = await asyncio.to_thread(load_migrations)
    await apply_migrations(pool, migrations, acquire_timeout=test_settings.db_acquire_timeout_s)
    return pool
