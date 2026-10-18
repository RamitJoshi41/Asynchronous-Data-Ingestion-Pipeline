import asyncio
import os
import signal
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import pytest

from benchmarks.run import free_port, start_mock, stop_mock
from ingest.__main__ import EXIT_CANCELLED, migrate_command
from ingest.config import Settings
from ingest.shutdown import StopSignal, handle_signals

REPO = Path(__file__).resolve().parent.parent


# --- StopSignal / handle_signals (in-process) ----------------------------------------------


async def test_stop_signal_runs_callbacks_once() -> None:
    stop, calls = StopSignal(), []
    stop.on_stop(lambda: calls.append("a"))
    stop.request("SIGINT")
    stop.request("SIGTERM")  # a second request is ignored (escalation is handled elsewhere)
    stop.on_stop(lambda: calls.append("late"))  # registered after the fact: runs immediately
    assert stop.is_set() and stop.reason == "SIGINT" and calls == ["a", "late"]


async def test_first_signal_drains_second_cancels_then_defaults_restored() -> None:
    stop = StopSignal()
    victim = asyncio.create_task(asyncio.sleep(10))  # stands in for the main task
    with handle_signals(stop, victim):
        os.kill(os.getpid(), signal.SIGINT)  # a REAL signal to this process
        await asyncio.sleep(0.05)  # the handler runs as a loop callback
        assert stop.is_set() and not victim.cancelled()
        os.kill(os.getpid(), signal.SIGTERM)
        with pytest.raises(asyncio.CancelledError):
            await victim
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


# --- end to end: a real `python -m ingest run hn` process receiving SIGINT ----------------


@asynccontextmanager
async def mock_server(latency_ms: float) -> AsyncIterator[str]:
    port = free_port()
    proc = await start_mock(port, items=300, latency_ms=latency_ms)
    try:
        yield f"http://127.0.0.1:{port}/v0"
    finally:
        await stop_mock(proc)


async def start_cli(
    settings: Settings, base_url: str, log_path: Path, *extra: str
) -> asyncio.subprocess.Process:
    env = {
        **os.environ,
        "DB_DSN": settings.db_dsn.get_secret_value(),
        "HN_BASE_URL": base_url,
        "SHUTDOWN_GRACE_S": "5",
    }
    args = ("-m", "ingest", "run", "hn", "--limit", "300", "--concurrency", "5", *extra)
    with log_path.open("wb") as log:  # a file, not a PIPE: a full pipe would block the child
        return await asyncio.create_subprocess_exec(
            sys.executable, *args, cwd=REPO, env=env, stdout=log, stderr=asyncio.subprocess.STDOUT
        )


async def wait_until(settings: Settings, query: str, minimum: int) -> None:
    conn = await asyncpg.connect(settings.db_dsn.get_secret_value())
    try:
        # Polling another PROCESS's writes: no in-process Event can signal this. Bounded: ~15 s.
        for _ in range(300):
            if await conn.fetchval(query) >= minimum:
                return
            await asyncio.sleep(0.05)
        raise AssertionError(f"never reached {minimum}: {query}")
    finally:
        await conn.close()


async def run_row(settings: Settings) -> tuple[asyncpg.Record, int]:
    conn = await asyncpg.connect(settings.db_dsn.get_secret_value())
    try:
        row = await conn.fetchrow("SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT 1")
        stored = await conn.fetchval("SELECT count(*) FROM items")
        assert row is not None
        return row, stored
    finally:
        await conn.close()


async def test_sigint_drains_and_records_cancelled_run(
    schema_settings: Settings, tmp_path: Path
) -> None:
    await migrate_command(schema_settings)
    async with mock_server(latency_ms=50) as base_url:
        proc = await start_cli(
            schema_settings, base_url, tmp_path / "out.log", "--batch-size", "200"
        )
        await wait_until(schema_settings, "SELECT count(*) FROM items", 1)  # 1st flush landed
        proc.send_signal(signal.SIGINT)
        code = await asyncio.wait_for(proc.wait(), timeout=20)
    log = (tmp_path / "out.log").read_text()

    assert code == EXIT_CANCELLED, log
    assert "shutdown_requested" in log and "drain_started" in log
    row, stored = await run_row(schema_settings)
    assert row["status"] == "cancelled" and row["finished_at"] is not None
    # Every fetched item was flushed despite batch_size=200 (buffers drained), counts exact.
    assert stored == row["items_inserted"] == row["items_fetched"] < 300


async def test_second_sigint_forces_immediate_exit(
    schema_settings: Settings, tmp_path: Path
) -> None:
    await migrate_command(schema_settings)
    # 2 s per request: after the first SIGINT the drain must wait for hanging requests, so the
    # second SIGINT reliably lands DURING the drain (with fast requests the drain can finish first).
    async with mock_server(latency_ms=2000) as base_url:
        proc = await start_cli(schema_settings, base_url, tmp_path / "out.log")
        await wait_until(schema_settings, "SELECT count(*) FROM ingestion_runs", 1)
        await asyncio.sleep(0.3)  # requests are now in flight and hanging
        proc.send_signal(signal.SIGINT)
        await asyncio.sleep(0.2)
        forced_at = time.monotonic()
        proc.send_signal(signal.SIGINT)
        code = await asyncio.wait_for(proc.wait(), timeout=20)
        exit_took = time.monotonic() - forced_at
    log = (tmp_path / "out.log").read_text()

    assert code == EXIT_CANCELLED, log
    assert "shutdown_forced" in log
    assert exit_took < 1.5  # did not wait out the 2 s requests or the 5 s drain deadline
    row, stored = await run_row(schema_settings)
    assert row["status"] == "cancelled" and row["error"] == "forced cancellation"
    assert stored == row["items_inserted"]  # no zombie 'running' row, counts still exact


async def test_stop_callbacks_can_unsubscribe() -> None:
    # The API shares one StopSignal across runs: a finished run must not stay referenced.
    stop, calls = StopSignal(), []
    unsubscribe = stop.on_stop(lambda: calls.append("gone"))
    stop.on_stop(lambda: calls.append("kept"))
    unsubscribe()
    unsubscribe()  # idempotent
    stop.request("SIGTERM")
    assert calls == ["kept"]
