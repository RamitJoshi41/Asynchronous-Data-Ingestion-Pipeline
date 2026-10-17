"""Benchmark harness skeleton (Phase 1: sequential HN baseline). ADR-0012.

    uv run python -m benchmarks.run --target mock --items 100 --latency-ms 50
    uv run python -m benchmarks.run --target real --items 30

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
from ingest.sequential import run_hn_sequential
from ingest.storage import Store

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


async def one_repeat(settings: Settings, base_url: str, items: int) -> RepeatResult:
    base_dsn = settings.db_dsn.get_secret_value()
    schema = f"bench_{uuid.uuid4().hex[:10]}"
    admin = await asyncpg.connect(base_dsn, timeout=settings.db_connect_timeout_s)
    try:
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        scoped = settings.model_copy(
            update={"db_dsn": SecretStr(f"{base_dsn}?search_path={schema}")}
        )
        await migrate_command(scoped)
        pool = await create_pool(scoped)
        try:
            async with build_http_client(scoped) as client:
                store = Store(pool, acquire_timeout=scoped.db_acquire_timeout_s)
                try:
                    s = await run_hn_sequential(store, client, base_url=base_url, limit=items)
                except Exception as exc:  # a failed run is a data point, not a crash
                    return RepeatResult(0.0, items, 0, 0, 0, True, repr(exc)[:300])
        finally:
            await pool.close()
        c = s.counts
        ingested = c.inserted + c.updated + c.unchanged
        return RepeatResult(s.duration_s, items, ingested, c.invalid, c.missing, False, None)
    finally:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()


async def run_benchmark(args: argparse.Namespace, settings: Settings) -> list[RepeatResult]:
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
            result = await one_repeat(settings, base_url, args.items)
            print(f"repeat {i + 1}/{args.repeats}: {result}")
            results.append(result)
        return results
    finally:
        if proc is not None:
            await stop_mock(proc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1 sequential HN benchmark.")
    parser.add_argument("--target", choices=["mock", "real"], required=True)
    parser.add_argument("--items", type=int, default=100)
    parser.add_argument("--latency-ms", type=float, default=50.0, help="mock only")
    parser.add_argument("--repeats", type=int, default=None, help="default: 3 mock, 1 real")
    args = parser.parse_args()
    if args.repeats is None:
        args.repeats = 3 if args.target == "mock" else 1
    if args.target == "real" and (args.items > 30 or args.repeats > 1):
        parser.error("real-API runs are capped at --items 30 --repeats 1 (be polite to HN)")

    # Sync work (git, /proc reads) happens before the event loop starts.
    started_at = datetime.now(UTC)
    meta = {"machine": machine_info(), **git_commit()}
    repeats = asyncio.run(run_benchmark(args, get_settings()))

    params: dict[str, object] = {
        "target": args.target,
        "items": args.items,
        "repeats": args.repeats,
    }
    if args.target == "mock":
        params["latency_ms"] = args.latency_ms
    result = {
        "benchmark": "hn_sequential",
        "phase": 1,
        "started_at": started_at.isoformat(),
        **meta,
        "params": params,
        "repeats": [{**asdict(r), "items_per_s": r.items_per_s} for r in repeats],
        "summary": summarize(repeats),
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = started_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    path = RESULTS_DIR / f"{stamp}_hn_sequential_{args.target}.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["summary"], indent=2))
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
