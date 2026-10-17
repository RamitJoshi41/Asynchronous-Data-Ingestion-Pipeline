"""Command-line entry point.

    uv run python -m ingest migrate              apply pending SQL migrations
    uv run python -m ingest run hn [--limit N]   sequential HN ingestion (Phase 1 baseline)

Explainer: docs/explain/ingest/__main__.md
"""

import argparse
import asyncio

from ingest.config import Settings, get_settings
from ingest.db import create_pool
from ingest.httpclient import build_http_client
from ingest.migrate import apply_migrations, load_migrations
from ingest.sequential import RunSummary, run_hn_sequential
from ingest.storage import Store

HN_MAX_LIMIT = 500  # /topstories returns at most 500 ids


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ingest")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="apply pending SQL migrations")
    run = commands.add_parser("run", help="run one ingestion")
    run.add_argument("source", choices=["hn"])
    run.add_argument("--limit", type=int, help="number of top stories (default: HN_TOP_LIMIT)")
    args = parser.parse_args(argv)

    settings = get_settings()
    if args.command == "migrate":
        applied = asyncio.run(migrate_command(settings))
        print(f"applied migrations: {applied}" if applied else "schema up to date")
        return 0

    limit = settings.hn_top_limit if args.limit is None else args.limit
    if not 1 <= limit <= HN_MAX_LIMIT:
        parser.error(f"--limit must be between 1 and {HN_MAX_LIMIT}")
    summary = asyncio.run(run_hn_command(settings, limit))
    print(summary)
    return 0


async def migrate_command(settings: Settings) -> list[int]:
    migrations = await asyncio.to_thread(load_migrations)  # file I/O off the event loop
    pool = await create_pool(settings)
    try:
        return await apply_migrations(
            pool, migrations, acquire_timeout=settings.db_acquire_timeout_s
        )
    finally:
        await pool.close()


async def run_hn_command(settings: Settings, limit: int) -> RunSummary:
    pool = await create_pool(settings)
    try:
        async with build_http_client(settings) as client:
            store = Store(pool, acquire_timeout=settings.db_acquire_timeout_s)
            return await run_hn_sequential(
                store, client, base_url=settings.hn_base_url, limit=limit
            )
    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(main())
