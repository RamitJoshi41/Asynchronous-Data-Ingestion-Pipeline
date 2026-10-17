"""Pool-exhaustion experiment (Phase 2, CLAUDE.md §10). REAL Postgres, simulated flushes.

    uv run python -m benchmarks.pool_exhaustion

W concurrent "flushes" each borrow a pooled connection and hold it for `hold_s` (pg_sleep, a
stand-in for a slow batch upsert). Scenarios vary the pool size and the acquire timeout, and record
per-flush acquire waits, timeouts, and wall time, showing what an undersized pool does to workers.
Explainer: docs/explain/benchmarks/run.md   Decision: ADR-0018
"""

import asyncio
import json
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from benchmarks.run import RESULTS_DIR, git_commit, machine_info
from ingest.config import Settings, get_settings
from ingest.db import create_pool


@dataclass(frozen=True)
class ScenarioResult:
    name: str
    workers: int
    pool_size: int
    hold_s: float
    acquire_timeout_s: float
    wall_s: float
    acquire_waits_s: list[float]  # one per flush that got a connection, in completion order
    timeouts: int


async def scenario(
    settings: Settings,
    name: str,
    *,
    workers: int,
    pool_size: int,
    hold_s: float,
    acquire_timeout_s: float,
) -> ScenarioResult:
    sized = settings.model_copy(update={"db_pool_min_size": 1, "db_pool_max_size": pool_size})
    pool = await create_pool(sized)
    waits: list[float] = []
    timeouts = 0

    async def flush() -> None:
        nonlocal timeouts
        asked = time.perf_counter()
        try:
            async with pool.acquire(timeout=acquire_timeout_s) as conn:
                waits.append(round(time.perf_counter() - asked, 3))
                await conn.execute("SELECT pg_sleep($1)", hold_s)
        except TimeoutError:
            timeouts += 1

    try:
        started = time.perf_counter()
        async with asyncio.TaskGroup() as tg:
            for _ in range(workers):
                tg.create_task(flush())
        wall = round(time.perf_counter() - started, 3)
    finally:
        await pool.close()
    return ScenarioResult(
        name, workers, pool_size, hold_s, acquire_timeout_s, wall, sorted(waits), timeouts
    )


async def run_all(settings: Settings) -> list[ScenarioResult]:
    w, hold = 4, 0.2
    return [
        await scenario(
            settings, "sized (W+1)", workers=w, pool_size=w + 1, hold_s=hold, acquire_timeout_s=5
        ),
        await scenario(
            settings, "pool=1, patient", workers=w, pool_size=1, hold_s=hold, acquire_timeout_s=5
        ),
        await scenario(
            settings, "pool=1, timeout", workers=w, pool_size=1, hold_s=hold, acquire_timeout_s=0.3
        ),
    ]


def main() -> None:
    started_at = datetime.now(UTC)
    meta = {"machine": machine_info(), **git_commit()}
    results = asyncio.run(run_all(get_settings()))
    for r in results:
        print(
            f"{r.name:18} pool={r.pool_size} wall={r.wall_s:.3f}s "
            f"waits={r.acquire_waits_s} timeouts={r.timeouts}"
        )
    record = {
        "benchmark": "pool_exhaustion",
        "phase": 2,
        "started_at": started_at.isoformat(),
        **meta,
        "scenarios": [asdict(r) for r in results],
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"{started_at.strftime('%Y-%m-%dT%H-%M-%SZ')}_pool_exhaustion.json"
    path.write_text(json.dumps(record, indent=2) + "\n")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
