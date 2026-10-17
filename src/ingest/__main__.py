"""Command-line entry point.

    uv run python -m ingest migrate                          apply pending SQL migrations
    uv run python -m ingest run hn [--limit N]               concurrent HN ingestion (Phase 2)
        [--concurrency C] [--workers W] [--batch-size B]     override the Settings defaults
        [--sequential]                                       Phase 1 baseline runner instead
    uv run python -m ingest replay [--batch-size B]          re-validate open dead letters

Ctrl-C / SIGTERM during `run`: first signal drains (finish in-flight work, flush, mark the run
'cancelled'); a second signal cancels immediately. Cancelled runs exit with code 130.

Explainer: docs/explain/ingest/__main__.md
"""

import argparse
import asyncio
import contextlib

from ingest.config import Settings, get_settings
from ingest.db import create_pool
from ingest.httpclient import build_http_client
from ingest.migrate import apply_migrations, load_migrations
from ingest.pipeline import PipelineConfig, run_hn_concurrent, settings_for
from ingest.replay import replay_dead_letters
from ingest.retry import RetryPolicy
from ingest.sequential import RunSummary, run_hn_sequential
from ingest.shutdown import StopSignal, handle_signals
from ingest.storage import RunStatus, Store

HN_MAX_LIMIT = 500  # /topstories returns at most 500 ids
EXIT_CANCELLED = 130  # shell convention for "terminated by Ctrl-C" (128 + SIGINT)


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
    replay = commands.add_parser("replay", help="re-validate open dead letters")
    replay.add_argument("--batch-size", type=int, default=100, help="dead letters per batch")
    args = parser.parse_args(argv)

    settings = get_settings()
    if args.command == "migrate":
        applied = asyncio.run(migrate_command(settings))
        print(f"applied migrations: {applied}" if applied else "schema up to date")
        return 0
    if args.command == "replay":
        print(asyncio.run(replay_command(settings, batch_size=args.batch_size)))
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
    run_cfg = None if args.sequential else cfg
    try:
        summary = asyncio.run(run_hn_command(settings, limit, cfg=run_cfg, signals=True))
    except asyncio.CancelledError:
        print("forced shutdown: run recorded as 'cancelled'")
        return EXIT_CANCELLED
    print(summary)
    return EXIT_CANCELLED if summary.status is RunStatus.CANCELLED else 0


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
    settings: Settings,
    limit: int,
    *,
    cfg: PipelineConfig | None = None,
    signals: bool = False,
    stop: StopSignal | None = None,
) -> RunSummary:
    """Run HN ingestion: concurrent with `cfg`, the sequential baseline when `cfg` is None.

    `signals=True` (the CLI) turns SIGINT/SIGTERM into a graceful drain (ADR-0026).
    """
    if cfg is not None:
        # Overrides must reach the httpx limits and the pool size too (ADR-0018/0019).
        settings = settings_for(settings, cfg)
    stop = stop or StopSignal()
    policy = RetryPolicy.from_settings(settings)
    main_task = asyncio.current_task()
    assert main_task is not None
    pool = await create_pool(settings)
    try:
        with handle_signals(stop, main_task) if signals else contextlib.nullcontext():
            async with build_http_client(settings) as client:
                store = Store(pool, acquire_timeout=settings.db_acquire_timeout_s)
                base_url = settings.hn_base_url
                if cfg is None:
                    return await run_hn_sequential(
                        store, client, base_url=base_url, limit=limit, policy=policy, stop=stop
                    )
                return await run_hn_concurrent(
                    store,
                    client,
                    base_url=base_url,
                    limit=limit,
                    cfg=cfg,
                    policy=policy,
                    stop=stop,
                    drain_timeout_s=settings.shutdown_grace_s,
                )
    finally:
        await pool.close()


async def replay_command(settings: Settings, *, batch_size: int = 100) -> RunSummary:
    pool = await create_pool(settings)
    try:
        store = Store(pool, acquire_timeout=settings.db_acquire_timeout_s)
        return await replay_dead_letters(store, batch_size=batch_size)
    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(main())
