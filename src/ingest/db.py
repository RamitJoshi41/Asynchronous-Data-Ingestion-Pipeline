"""asyncpg connection-pool construction. The one place that knows how we connect to Postgres."""

import asyncpg

from ingest.config import Settings

# PEP 695 alias: evaluated lazily, so subscripting asyncpg.Pool (generic only in the stubs) is
# safe at runtime.
type DbPool = asyncpg.Pool[asyncpg.Record]


async def create_pool(settings: Settings, *, search_path: str | None = None) -> DbPool:
    """Open a pool with every timeout set explicitly.

    `search_path` lets tests point each pool at its own throwaway schema; the app leaves it unset.
    The caller owns the pool and must close it (`async with` or `await pool.close()`).
    """
    if settings.db_pool_min_size > settings.db_pool_max_size:
        raise ValueError("db_pool_min_size must be <= db_pool_max_size")
    return await asyncpg.create_pool(
        settings.db_dsn.get_secret_value(),
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        timeout=settings.db_connect_timeout_s,  # per-connection connect timeout
        command_timeout=settings.db_command_timeout_s,  # default per-statement timeout
        server_settings={"search_path": search_path} if search_path else None,
    )
