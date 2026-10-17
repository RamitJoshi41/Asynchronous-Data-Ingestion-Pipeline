"""Command-line entry point.

    uv run python -m ingest migrate                          apply pending SQL migrations
    uv run python -m ingest run hn|github|all                concurrent ingestion (Phase 2/4)
        [--limit N]                                          HN: number of top stories
        [--concurrency C] [--workers W] [--batch-size B]     override the Settings defaults
        [--sequential]                                       Phase 1 baseline runner instead
    uv run python -m ingest replay [--source S|all] [--batch-size B]   re-validate dead letters

`run all` runs every source concurrently, each with its own limits, client and run row (ADR-0030).
Ctrl-C / SIGTERM during `run`: first signal drains (finish in-flight work, flush, mark runs
'cancelled'); a second signal cancels immediately. Exit codes: 0 ok, 1 a source failed, 130
cancelled.

Explainer: docs/explain/ingest/__main__.md
"""

import argparse
import asyncio
from collections.abc import Sequence

from ingest.config import Settings, get_settings
from ingest.connectors.hn import HN_MAX_LIMIT
from ingest.connectors.registry import SOURCES
from ingest.db import create_pool
from ingest.migrate import apply_migrations, load_migrations
from ingest.pipeline import PipelineConfig
from ingest.replay import replay_dead_letters
from ingest.runner import SourceOutcome, SourcePlan, run_sources
from ingest.sequential import RunSummary
from ingest.shutdown import StopSignal
from ingest.storage import RunStatus, Store

EXIT_FAILED = 1
EXIT_CANCELLED = 130  # shell convention for "terminated by Ctrl-C" (128 + SIGINT)


def main(argv: list[str] | None = None) -> int:
    names = sorted(SOURCES)
    parser = argparse.ArgumentParser(prog="python -m ingest")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="apply pending SQL migrations")
    run = commands.add_parser("run", help="run one ingestion per source")
    run.add_argument("source", choices=[*names, "all"])
    run.add_argument("--limit", type=int, help="HN: number of top stories (default: HN_TOP_LIMIT)")
    run.add_argument("--sequential", action="store_true", help="Phase 1 one-at-a-time baseline")
    run.add_argument("--concurrency", type=int, help="fetcher pool size (<SOURCE>_CONCURRENCY)")
    run.add_argument("--workers", type=int, help="validating/flushing workers (PIPELINE_WORKERS)")
    run.add_argument("--batch-size", type=int, help="rows per upsert (BATCH_SIZE)")
    replay = commands.add_parser("replay", help="re-validate open dead letters")
    replay.add_argument("--source", choices=[*names, "all"], default="all")
    replay.add_argument("--batch-size", type=int, default=100, help="dead letters per batch")
    args = parser.parse_args(argv)

    settings = get_settings()
    if args.command == "migrate":
        applied = asyncio.run(migrate_command(settings))
        print(f"applied migrations: {applied}" if applied else "schema up to date")
        return 0
    if args.command == "replay":
        selected = names if args.source == "all" else [args.source]
        for summary in asyncio.run(replay_command(settings, selected, batch_size=args.batch_size)):
            print(summary)
        return 0

    selected = names if args.source == "all" else [args.source]
    if args.limit is not None:
        if "hn" not in selected:
            parser.error("--limit only applies to hn")
        if not 1 <= args.limit <= HN_MAX_LIMIT:
            parser.error(f"--limit must be between 1 and {HN_MAX_LIMIT}")
        settings = settings.model_copy(update={"hn_top_limit": args.limit})
    if len(selected) > 1 and (args.concurrency is not None or args.sequential):
        parser.error("--concurrency/--sequential need a single source (limits are per source)")
    try:
        plans = build_plans(
            settings,
            selected,
            sequential=args.sequential,
            concurrency=args.concurrency,
            workers=args.workers,
            batch_size=args.batch_size,
        )
    except ValueError as exc:
        parser.error(str(exc))
    try:
        outcomes = asyncio.run(run_sources(settings, plans, signals=True))
    except asyncio.CancelledError:
        print("forced shutdown: runs recorded as 'cancelled'")
        return EXIT_CANCELLED
    for outcome in outcomes:
        print(f"{outcome.source}: {outcome.summary or f'FAILED: {outcome.error!r}'}")
    return exit_code(outcomes)


def build_plans(
    settings: Settings,
    names: Sequence[str],
    *,
    sequential: bool = False,
    concurrency: int | None = None,
    workers: int | None = None,
    batch_size: int | None = None,
) -> list[SourcePlan]:
    """One plan per source: its connector, and its pipeline config (None = sequential)."""
    plans = []
    for name in names:
        spec = SOURCES[name]
        cfg = PipelineConfig.from_settings(
            settings,
            concurrency=spec.concurrency(settings) if concurrency is None else concurrency,
            workers=workers,
            batch_size=batch_size,
        )
        plans.append(SourcePlan(spec.build(settings), None if sequential else cfg))
    return plans


def exit_code(outcomes: Sequence[SourceOutcome]) -> int:
    if any(o.error is not None for o in outcomes):
        return EXIT_FAILED
    if any(o.summary is not None and o.summary.status is RunStatus.CANCELLED for o in outcomes):
        return EXIT_CANCELLED
    return 0


async def run_command(
    settings: Settings,
    names: Sequence[str],
    *,
    sequential: bool = False,
    concurrency: int | None = None,
    stop: StopSignal | None = None,
) -> list[SourceOutcome]:
    """What `run` does, minus argument parsing and signal handling (used by tests)."""
    plans = build_plans(settings, names, sequential=sequential, concurrency=concurrency)
    return await run_sources(settings, plans, stop=stop)


async def migrate_command(settings: Settings) -> list[int]:
    migrations = await asyncio.to_thread(load_migrations)  # file I/O off the event loop
    pool = await create_pool(settings)
    try:
        return await apply_migrations(
            pool, migrations, acquire_timeout=settings.db_acquire_timeout_s
        )
    finally:
        await pool.close()


async def replay_command(
    settings: Settings, names: Sequence[str] = ("hn",), *, batch_size: int = 100
) -> list[RunSummary]:
    pool = await create_pool(settings)
    try:
        store = Store(pool, acquire_timeout=settings.db_acquire_timeout_s)
        return [
            await replay_dead_letters(store, SOURCES[name].build(settings), batch_size=batch_size)
            for name in names
        ]
    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(main())
