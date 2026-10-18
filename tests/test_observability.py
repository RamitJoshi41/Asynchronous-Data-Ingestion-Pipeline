import asyncio
import json
from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
import respx
import structlog

from ingest.config import Settings
from ingest.connectors.hn import HackerNewsConnector
from ingest.db import DbPool
from ingest.httpclient import build_http_client
from ingest.observability import (
    Gauge,
    GaugeSampler,
    LatencyRecorder,
    LiveRuns,
    configure_logging,
    percentile,
)
from ingest.pipeline import PipelineConfig, run_concurrent
from ingest.sequential import run_sequential
from ingest.storage import Store

BASE = "https://hn.test/v0"


# --- units --------------------------------------------------------------------------------------


def test_nearest_rank_percentiles_are_exact() -> None:
    values = [float(v) for v in range(1, 101)]  # 1..100
    assert (percentile(values, 50), percentile(values, 95), percentile(values, 100)) == (
        50,
        95,
        100,
    )
    assert percentile([7.0], 95) == 7.0
    with pytest.raises(ValueError):
        percentile([], 50)


def test_latency_summary_in_milliseconds() -> None:
    recorder = LatencyRecorder()
    for ms in [30, 10, 20, 1000]:  # unsorted on purpose
        recorder.record(ms / 1000)
    summary = recorder.summary()
    assert (summary.requests, summary.p50_ms, summary.p95_ms, summary.max_ms) == (4, 20, 1000, 1000)
    assert LatencyRecorder().summary().p95_ms is None  # no requests: no percentile, not 0


def test_gauge_tracks_latest_peak_and_mean() -> None:
    gauge = Gauge()
    for value in [2, 5, 1]:
        gauge.observe(value)
    assert (gauge.latest, gauge.peak) == (1, 5) and gauge.mean == pytest.approx(2.67)
    assert Gauge().mean is None


async def test_sampler_samples_until_stopped() -> None:
    depth = iter(range(1000))
    sampler = GaugeSampler(lambda: next(depth), lambda: 3, interval_s=0.005)
    task = asyncio.create_task(sampler.run())
    await asyncio.sleep(0.05)
    sampler.stop()
    await asyncio.wait_for(task, 1)  # returns by itself: no cancellation needed
    assert sampler.queue_depth.samples >= 5 and sampler.in_flight.peak == 3


def test_live_runs_registry() -> None:
    from ingest.observability import LiveRun

    live = LiveRuns()
    live.add(LiveRun(7, "hacker_news", None))
    assert live.get(7) is not None and [r.run_id for r in live] == [7]
    live.remove(7)
    live.remove(7)  # idempotent
    assert live.get(7) is None


@pytest.fixture
def restore_logging() -> Iterator[None]:
    config = structlog.get_config()
    yield
    structlog.configure(**config)


def test_json_logging_renders_one_object_per_event(
    restore_logging: None, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(Settings(_env_file=None, log_format="json", log_level="info"))
    structlog.get_logger().bind(run_id=1).info("run_started", source="hn")
    structlog.get_logger().debug("hidden")  # below the configured level
    [line] = capsys.readouterr().out.strip().splitlines()
    event = json.loads(line)
    assert event["event"] == "run_started" and event["run_id"] == 1 and event["level"] == "info"
    assert event["timestamp"].endswith("Z")


# --- metrics end to end -------------------------------------------------------------------------


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with build_http_client(Settings(_env_file=None), concurrency=10) as c:
        yield c


def mock_hn(respx_mock: respx.MockRouter, n: int, latency: float) -> None:
    async def item(request: httpx.Request, item_id: str) -> httpx.Response:
        await asyncio.sleep(latency)
        return httpx.Response(200, json={"id": int(item_id), "type": "story"})

    respx_mock.get(f"{BASE}/topstories.json").respond(json=list(range(1, n + 1)))
    respx_mock.get(url__regex=rf"{BASE}/item/(?P<item_id>\d+)\.json").mock(side_effect=item)


async def run_row(pool: DbPool, run_id: int) -> dict[str, object]:
    row = await pool.fetchrow("SELECT * FROM ingestion_runs WHERE id = $1", run_id)
    assert row is not None
    return dict(row)


async def test_concurrent_run_stores_latency_and_gauges(
    migrated_pool: DbPool, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    mock_hn(respx_mock, 20, latency=0.03)
    cfg = PipelineConfig(
        concurrency=4,
        workers=1,
        batch_size=50,
        flush_interval_s=1.0,
        job_queue_maxsize=10,
        raw_queue_maxsize=10,
    )
    live, seen_live = LiveRuns(), []

    def on_start(source: str, run_id: int) -> None:
        seen_live.append((source, run_id))

    result = await run_concurrent(
        Store(migrated_pool, acquire_timeout=5.0),
        client,
        HackerNewsConnector(BASE, limit=20),
        cfg=cfg,
        gauge_interval_s=0.005,
        live=live,
        on_start=on_start,
    )
    row = await run_row(migrated_pool, result.run_id)
    assert seen_live == [("hacker_news", result.run_id)]
    assert live.get(result.run_id) is None  # removed when the run ended
    assert row["requests"] == 21  # topstories + 20 items
    p50, p95, top = (float(row[k]) for k in ("latency_p50_ms", "latency_p95_ms", "latency_max_ms"))  # type: ignore[arg-type]
    assert 30 <= p50 <= p95 <= top  # every item request slept 30 ms on the mock
    assert row["in_flight_peak"] == 4  # sampled: the 4 fetchers were all waiting at some sample
    assert int(row["duration_ms"]) >= 150  # type: ignore[call-overload]  # 20 items / 4 x 30 ms
    assert row["queue_depth_mean"] is not None


async def test_sequential_run_stores_latency_but_no_gauges(
    migrated_pool: DbPool, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    mock_hn(respx_mock, 3, latency=0.0)
    summary = await run_sequential(
        Store(migrated_pool, acquire_timeout=5.0), client, HackerNewsConnector(BASE, limit=3)
    )
    row = await run_row(migrated_pool, summary.run_id)
    assert row["requests"] == 4 and row["latency_p95_ms"] is not None
    assert row["in_flight_peak"] is None and row["queue_depth_peak"] is None


async def test_failed_attempts_are_timed_too(
    migrated_pool: DbPool, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    from ingest.retry import RetryPolicy

    respx_mock.get(f"{BASE}/topstories.json").respond(json=[1])
    respx_mock.get(f"{BASE}/item/1.json").mock(
        side_effect=[httpx.Response(503), httpx.Response(200, json={"id": 1})]
    )
    fast = RetryPolicy(max_attempts=3, base_delay_s=0.001, max_delay_s=0.002, max_retry_after_s=1)
    summary = await run_sequential(
        Store(migrated_pool, acquire_timeout=5.0),
        client,
        HackerNewsConnector(BASE, limit=1),
        policy=fast,
    )
    row = await run_row(migrated_pool, summary.run_id)
    assert row["requests"] == 3 and row["retries"] == 1  # topstories, 503, 200
