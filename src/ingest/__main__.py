"""Command-line entry point.

    uv run python -m ingest migrate                          apply pending SQL migrations
    uv run python -m ingest run hn [--limit N]               concurrent HN ingestion (Phase 2)
        [--concurrency C] [--workers W] [--batch-size B]     override the Settings defaults
        [--sequential]                                       Phase 1 baseline runner instead

Explainer: docs/explain/ingest/__main__.md
"""

import argparse
import asyncio

from ingest.config import Settings, get_settings
from ingest.db import create_pool
from ingest.httpclient import build_http_client
from ingest.migrate import apply_migrations, load_migrations
from ingest.pipeline import PipelineConfig, run_hn_concurrent, settings_for
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
    run.add_argument("--sequential", action="store_true", help="Phase 1 one-at-a-time baseline")
    run.add_argument("--concurrency", type=int, help="fetcher pool size (FETCH_CONCURRENCY)")
    run.add_argument("--workers", type=int, help="validating/flushing workers (PIPELINE_WORKERS)")
    run.add_argument("--batch-size", type=int, help="rows per upsert (BATCH_SIZE)")
    args = parser.parse_args(argv)

    settings = get_settings()
    if args.command == "migrate":
        applied = asyncio.run(migrate_command(settings))
        print(f"applied migrations: {applied}" if applied else "schema up to date")
        return 0

    limit = settings.hn_top_limit if args.limit is None else args.limit
    if not 1 <= limit <= HN_MAX_LIMIT:
        parser.error(f"--limit must be between 1 and {HN_MAX_LIMIT}")
    try:
        cfg = PipelineConfig.from_settings(
            settings, concurrency=args.concurrency, workers=args.workers, batch_size=args.batch_size
        )
    except ValueError as exc:
        parser.error(str(exc))
    summary = asyncio.run(run_hn_command(settings, limit, cfg=None if args.sequential else cfg))
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


async def run_hn_command(
    settings: Settings, limit: int, *, cfg: PipelineConfig | None = None
) -> RunSummary:
    """Run HN ingestion: concurrent with `cfg`, the sequential baseline when `cfg` is None."""
    if cfg is not None:
        # Overrides must reach the httpx limits and the pool size too (ADR-0018/0019).
        settings = settings_for(settings, cfg)
    pool = await create_pool(settings)
    try:
        async with build_http_client(settings) as client:
            store = Store(pool, acquire_timeout=settings.db_acquire_timeout_s)
            base_url = settings.hn_base_url
            if cfg is None:
                return await run_hn_sequential(store, client, base_url=base_url, limit=limit)
            return await run_hn_concurrent(store, client, base_url=base_url, limit=limit, cfg=cfg)
    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(main())
