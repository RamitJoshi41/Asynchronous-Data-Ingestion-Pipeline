import pytest

from ingest.config import Settings
from ingest.db import DbPool, create_pool


async def test_pool_uses_requested_search_path(pool: DbPool) -> None:
    # The conftest `pool` fixture asks for a per-test schema; it must be the active one.
    schema = await pool.fetchval("SELECT current_schema()")
    assert isinstance(schema, str) and schema.startswith("t_")


async def test_pool_applies_default_command_timeout(test_settings: Settings) -> None:
    fast_fail = test_settings.model_copy(update={"db_command_timeout_s": 0.2})
    db_pool = await create_pool(fast_fail)
    try:
        # No per-call timeout given: the pool's command_timeout must cancel the slow statement.
        with pytest.raises(TimeoutError):
            await db_pool.execute("SELECT pg_sleep(1)")
    finally:
        await db_pool.close()


async def test_rejects_min_size_above_max_size(test_settings: Settings) -> None:
    bad = test_settings.model_copy(update={"db_pool_min_size": 3, "db_pool_max_size": 2})
    with pytest.raises(ValueError, match="min_size"):
        await create_pool(bad)
