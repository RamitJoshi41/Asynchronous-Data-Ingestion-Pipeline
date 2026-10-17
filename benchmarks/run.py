"""Benchmark harness: sequential vs concurrent HN ingestion. ADR-0012.

    uv run python -m benchmarks.run --target mock --items 200 --latency-ms 50 --mode sequential
    uv run python -m benchmarks.run --target mock --items 200 --latency-ms 50 --mode concurrent \
        --concurrency 20
    uv run python -m benchmarks.run --target real --items 30 --mode concurrent --concurrency 5

Each repeat ingests into a FRESH throwaway schema in the dev database (so every repeat measures
the same cold-insert path and mock data never mixes with real data), then drops it. Results are
written as JSON to docs/benchmarks/results/ with machine info, parameters and the commit hash.
Explainer: docs/explain/benchmarks/run.md
"""

import argparse
import asyncio
import json
import os
import platform
import socket
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import httpx
from pydantic import SecretStr

from ingest.__main__ import migrate_command
from ingest.config import Settings, get_settings
from ingest.db import create_pool
from ingest.httpclient import build_http_client
from ingest.pipeline import ConcurrentRunSummary, PipelineConfig, run_hn_concurrent, settings_for
from ingest.sequential import RunSummary, run_hn_sequential
from ingest.storage import Store

PHASE = 2  # the phase that produced new result files
REAL_MAX_ITEMS, REAL_MAX_CONCURRENCY = 30, 10  # politeness caps for the real API
RESULTS_DIR = Path(__file__).resolve().parent.parent / "docs" / "benchmarks" / "results"


@dataclass(frozen=True)
class RepeatResult:
    duration_s: float
    items_requested: int
    ingested: int  # inserted + updated + unchanged
    invalid: int
    missing: int
    failed: bool
    error: str | None
    # CPU time of THIS process during the run: tells "we're CPU-bound" from "we're waiting".
    client_cpu_s: float = 0.0
    peak_in_flight: int = 0
    blocked_puts: int = 0
    flushes: int = 0

    @property
    def items_per_s(self) -> float:
        return self.ingested / self.duration_s if self.duration_s > 0 else 0.0


def summarize(repeats: list[RepeatResult]) -> dict[str, float]:
    """Medians over repeats, plus the error rate over every requested item."""
    requested = sum(r.items_requested for r in repeats)
    not_ingested = sum(r.items_requested - r.ingested for r in repeats)
    return {
        "median_duration_s": statistics.median(r.duration_s for r in repeats),
        "median_items_per_s": statistics.median(r.items_per_s for r in repeats),
        "error_rate": not_ingested / requested if requested else 0.0,
    }


def machine_info() -> dict[str, object]:
    cpu = platform.processor() or "unknown"
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu": cpu,
        "cpu_count": os.cpu_count(),
    }


def git_commit() -> dict[str, object]:
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
    # Earlier result files in RESULTS_DIR don't change the measured code, so they don't count.
    status = subprocess.run(
        ["git", "status", "--porcelain", "--", ".", ":(exclude)docs/benchmarks/results"],
        capture_output=True,
        text=True,
    )
    return {"commit": head.stdout.strip(), "dirty": bool(status.stdout.strip())}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


async def start_mock(port: int, items: int, latency_ms: float) -> asyncio.subprocess.Process:
    # A separate PROCESS: the server must not share (and distort) the measured event loop.
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "benchmarks.mock_hn",
        "--port",
        str(port),
        "--items",
        str(items),
        "--latency-ms",
        str(latency_ms),
    )
    async with httpx.AsyncClient(timeout=1.0) as probe:
        for _ in range(100):  # up to ~10 s for the server to come up
            try:
                if (await probe.get(f"http://127.0.0.1:{port}/health")).status_code == 200:
                    return proc
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.1)
    proc.kill()
    raise RuntimeError("mock server did not become healthy")


async def stop_mock(proc: asyncio.subprocess.Process) -> None:
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except TimeoutError:
        proc.kill()
        await proc.wait()


async def one_repeat(
    settings: Settings, base_url: str, items: int, cfg: PipelineConfig | None
) -> RepeatResult:
    """One run into a fresh schema. `cfg=None` means the sequential baseline."""
    base_dsn = settings.db_dsn.get_secret_value()
    schema = f"bench_{uuid.uuid4().hex[:10]}"
    admin = await asyncpg.connect(base_dsn, timeout=settings.db_connect_timeout_s)
    try:
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        scoped = settings.model_copy(
            update={"db_dsn": SecretStr(f"{base_dsn}?search_path={schema}")}
        )
        if cfg is not None:
            scoped = settings_for(scoped, cfg)  # httpx limits + pool size follow the config
        await migrate_command(scoped)
        pool = await create_pool(scoped)
        try:
            async with build_http_client(scoped) as client:
                store = Store(pool, acquire_timeout=scoped.db_acquire_timeout_s)
                cpu_before = time.process_time()
                try:
                    s: RunSummary
                    if cfg is None:
                        s = await run_hn_sequential(store, client, base_url=base_url, limit=items)
                    else:
                        s = await run_hn_concurrent(
                            store, client, base_url=base_url, limit=items, cfg=cfg
                        )
                except Exception as exc:  # a failed run is a data point, not a crash
                    return RepeatResult(0.0, items, 0, 0, 0, True, repr(exc)[:300])
                cpu = time.process_time() - cpu_before
        finally:
            await pool.close()
        c = s.counts
        stats = s.stats if isinstance(s, ConcurrentRunSummary) else None
        return RepeatResult(
            duration_s=s.duration_s,
            items_requested=items,
            ingested=c.inserted + c.updated + c.unchanged,
            invalid=c.invalid,
            missing=c.missing,
            failed=False,
            error=None,
            client_cpu_s=cpu,
            peak_in_flight=stats.peak_in_flight if stats else 1,
            blocked_puts=stats.blocked_puts if stats else 0,
            flushes=stats.flushes if stats else c.inserted + c.updated + c.unchanged,
        )
    finally:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()


async def run_benchmark(args: argparse.Namespace, settings: Settings) -> list[RepeatResult]:
    cfg = None
    if args.mode == "concurrent":
        cfg = PipelineConfig.from_settings(
            settings, concurrency=args.concurrency, workers=args.workers, batch_size=args.batch_size
        )
    proc = None
    if args.target == "mock":
        port = free_port()
        proc = await start_mock(port, args.items, args.latency_ms)
        base_url = f"http://127.0.0.1:{port}/v0"
    else:
        base_url = settings.hn_base_url
    try:
        results = []
        for i in range(args.repeats):
            result = await one_repeat(settings, base_url, args.items, cfg)
            print(f"repeat {i + 1}/{args.repeats}: {result}")
            results.append(result)
        return results
    finally:
        if proc is not None:
            await stop_mock(proc)


def main() -> None:
    parser = argparse.ArgumentParser(description="HN ingestion benchmark (sequential/concurrent).")
    parser.add_argument("--target", choices=["mock", "real"], required=True)
    parser.add_argument("--mode", choices=["sequential", "concurrent"], default="sequential")
    parser.add_argument("--concurrency", type=int, help="concurrent mode (FETCH_CONCURRENCY)")
    parser.add_argument("--workers", type=int, help="concurrent mode (PIPELINE_WORKERS)")
    parser.add_argument("--batch-size", type=int, help="concurrent mode (BATCH_SIZE)")
    parser.add_argument("--items", type=int, default=100)
    parser.add_argument("--latency-ms", type=float, default=50.0, help="mock only")
    parser.add_argument("--repeats", type=int, default=None, help="default: 3 mock, 1 real")
    args = parser.parse_args()
    if args.repeats is None:
        args.repeats = 3 if args.target == "mock" else 1
    if args.target == "real" and (args.items > REAL_MAX_ITEMS or args.repeats > 1):
        parser.error(
            f"real-API runs are capped at --items {REAL_MAX_ITEMS} --repeats 1 (be polite)"
        )
    if args.target == "real" and (args.concurrency or 0) > REAL_MAX_CONCURRENCY:
        parser.error(f"real-API runs are capped at --concurrency {REAL_MAX_CONCURRENCY}")
    if args.mode == "sequential" and (args.concurrency or args.workers or args.batch_size):
        parser.error("--concurrency/--workers/--batch-size only apply to --mode concurrent")

    # Sync work (git, /proc reads) happens before the event loop starts.
    started_at = datetime.now(UTC)
    meta = {"machine": machine_info(), **git_commit()}
    repeats = asyncio.run(run_benchmark(args, get_settings()))

    settings = get_settings()
    params: dict[str, object] = {
        "target": args.target,
        "mode": args.mode,
        "items": args.items,
        "repeats": args.repeats,
    }
    if args.target == "mock":
        params["latency_ms"] = args.latency_ms
    if args.mode == "concurrent":
        cfg = PipelineConfig.from_settings(
            settings, concurrency=args.concurrency, workers=args.workers, batch_size=args.batch_size
        )
        params |= {"concurrency": cfg.concurrency, "workers": cfg.workers}
        params |= {"batch_size": cfg.batch_size, "flush_interval_s": cfg.flush_interval_s}
        params |= {"raw_queue_maxsize": cfg.raw_queue_maxsize}
    result = {
        "benchmark": f"hn_{args.mode}",
        "phase": PHASE,
        "started_at": started_at.isoformat(),
        **meta,
        "params": params,
        "repeats": [{**asdict(r), "items_per_s": r.items_per_s} for r in repeats],
        "summary": summarize(repeats),
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = started_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    label = "sequential" if args.mode == "sequential" else f"concurrent_c{params['concurrency']}"
    path = RESULTS_DIR / f"{stamp}_hn_{label}_{args.target}.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["summary"], indent=2))
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
