import asyncio
import json
from collections.abc import AsyncIterator, Sequence

import httpx
import pytest
import respx
from structlog.testing import capture_logs

from ingest.config import Settings
from ingest.db import DbPool, create_pool
from ingest.httpclient import build_http_client
from ingest.pipeline import PipelineConfig, run_hn_concurrent
from ingest.sequential import run_hn_sequential
from ingest.storage import RunStatus, Store, UpsertOutcome

BASE = "https://hn.test/v0"
ITEM_URL = r"https://hn\.test/v0/item/(?P<item_id>\d+)\.json"
TIMEOUT_S = 5.0


def story(item_id: int) -> dict[str, object]:
    return {"id": item_id, "type": "story", "by": "a", "time": 1175714200 + item_id, "title": "t"}


def cfg(**overrides: int | float) -> PipelineConfig:
    values: dict[str, int | float] = {
        "concurrency": 5,
        "workers": 2,
        "batch_size": 10,
        "flush_interval_s": 1.0,
        "id_queue_maxsize": 10,
        "raw_queue_maxsize": 10,
    }
    values.update(overrides)
    return PipelineConfig(**values)  # type: ignore[arg-type]


class MockHN:
    """respx-backed HN with optional latency, measuring in-flight requests from the server side."""

    def __init__(self, respx_mock: respx.MockRouter, bodies: dict[int, object], latency: float = 0):
        self.bodies, self.latency = bodies, latency
        self.in_flight = self.peak = 0
        respx_mock.get(f"{BASE}/topstories.json").respond(json=list(bodies))
        respx_mock.get(url__regex=ITEM_URL).mock(side_effect=self._item)

    async def _item(self, request: httpx.Request, item_id: str) -> httpx.Response:
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        try:
            await asyncio.sleep(self.latency)
        finally:
            self.in_flight -= 1
        body = self.bodies[int(item_id)]
        if isinstance(body, int):  # an int body means "respond with this HTTP status"
            return httpx.Response(body)
        return httpx.Response(200, content=json.dumps(body).encode())


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with build_http_client(Settings(_env_file=None, fetch_concurrency=50)) as c:
        yield c


@pytest.fixture
def store(migrated_pool: DbPool) -> Store:
    return Store(migrated_pool, acquire_timeout=5.0)


def other_tasks() -> set[asyncio.Task[object]]:
    return asyncio.all_tasks() - {asyncio.current_task()}


MIXED: dict[int, object] = {
    1: story(1),
    2: {**story(2), "score": "lots"},  # invalid
    3: None,  # missing
    4: {**story(4), "type": "job"},
    5: story(99),  # body for another id -> invalid
    6: story(6),
}


async def test_same_results_as_sequential_runner(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    MockHN(respx_mock, MIXED)
    seq = await run_hn_sequential(store, client, base_url=BASE, limit=6)
    seq_rows = await migrated_pool.fetch("SELECT source_id, payload FROM items ORDER BY 1")
    await migrated_pool.execute("DELETE FROM items")

    async with asyncio.timeout(TIMEOUT_S):
        con = await run_hn_concurrent(store, client, base_url=BASE, limit=6, cfg=cfg())
    con_rows = await migrated_pool.fetch("SELECT source_id, payload FROM items ORDER BY 1")

    assert con.status is RunStatus.SUCCEEDED
    assert con.counts == seq.counts  # fetched 6, inserted 3, invalid 2, missing 1
    assert [tuple(r) for r in con_rows] == [tuple(r) for r in seq_rows]


async def test_concurrency_limit_is_reached_and_never_exceeded(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    mock = MockHN(respx_mock, {i: story(i) for i in range(1, 41)}, latency=0.02)
    async with asyncio.timeout(TIMEOUT_S):
        result = await run_hn_concurrent(
            store, client, base_url=BASE, limit=40, cfg=cfg(concurrency=5)
        )
    assert mock.peak == 5  # observed by the server, independent of the pipeline's own counter
    assert result.stats.peak_in_flight == 5
    assert result.counts.inserted == 40


async def test_slow_flushes_fill_raw_queue_and_block_fetchers(
    store: Store,
    client: httpx.AsyncClient,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    MockHN(respx_mock, {i: story(i) for i in range(1, 21)})
    real_upsert = store.upsert_items

    async def slow_upsert(
        run_id: int, source: str, records: Sequence[tuple[str, str]]
    ) -> dict[str, UpsertOutcome]:
        await asyncio.sleep(0.03)  # a slow database
        return await real_upsert(run_id, source, records)

    monkeypatch.setattr(store, "upsert_items", slow_upsert)
    with capture_logs() as logs:
        async with asyncio.timeout(TIMEOUT_S):
            result = await run_hn_concurrent(
                store,
                client,
                base_url=BASE,
                limit=20,
                cfg=cfg(workers=1, batch_size=1, raw_queue_maxsize=3),
            )
    blocked = [e for e in logs if e["event"] == "producer_blocked"]
    assert result.stats.peak_raw_queue_depth == 3  # filled to the bound, never past it
    assert result.stats.blocked_puts == len(blocked) > 0
    assert all(e["queue_depth"] == 3 and e["run_id"] == result.run_id for e in blocked)
    assert result.counts.inserted == 20  # backpressure slows, never drops


async def test_batches_flush_at_size_and_final_partial_batch_on_sentinel(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    MockHN(respx_mock, {i: story(i) for i in range(1, 11)})
    async with asyncio.timeout(TIMEOUT_S):
        result = await run_hn_concurrent(
            store, client, base_url=BASE, limit=10, cfg=cfg(workers=1, batch_size=4)
        )
    assert result.stats.flush_sizes == [4, 4, 2]  # 10 % 4 != 0: the last 2 are not lost
    assert result.counts.inserted == 10


async def test_partial_batch_flushes_on_interval(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    # Items arrive every ~150 ms; the 30 ms flush interval must write each one on its own
    # instead of holding it until batch_size (100) or the end of the run.
    MockHN(respx_mock, {i: story(i) for i in range(1, 4)}, latency=0.15)
    with capture_logs() as logs:
        async with asyncio.timeout(TIMEOUT_S):
            result = await run_hn_concurrent(
                store,
                client,
                base_url=BASE,
                limit=3,
                cfg=cfg(concurrency=1, workers=1, batch_size=100, flush_interval_s=0.03),
            )
    reasons = [e["reason"] for e in logs if e["event"] == "batch_flushed"]
    assert result.stats.flush_sizes == [1, 1, 1]
    # The last item is followed at once by the stop sentinel, so it goes out as the final flush.
    assert reasons == ["interval", "interval", "final"]


async def test_http_error_aborts_run_marks_failed_and_leaks_no_tasks(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    bodies: dict[int, object] = {i: story(i) for i in range(1, 21)}
    bodies[13] = 500
    MockHN(respx_mock, bodies, latency=0.01)
    with pytest.RaisesGroup(httpx.HTTPStatusError, flatten_subgroups=True):
        async with asyncio.timeout(TIMEOUT_S):
            await run_hn_concurrent(
                store, client, base_url=BASE, limit=20, cfg=cfg(workers=2, batch_size=1)
            )
    assert other_tasks() == set()
    row = await migrated_pool.fetchrow("SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT 1")
    assert row is not None
    assert row["status"] == "failed" and "500" in row["error"]
    # Flushes that completed before the abort are committed AND counted. A flush that was
    # mid-statement when the TaskGroup cancelled its worker may have committed on the server
    # without being counted, so counts are a lower bound, off by at most workers x batch_size
    # (ADR-0020). Phase 3's graceful shutdown tightens this.
    stored = await migrated_pool.fetchval("SELECT count(*) FROM items")
    workers, batch_size = 2, 1
    assert row["items_inserted"] <= stored <= row["items_inserted"] + workers * batch_size


async def test_unexpected_worker_error_aborts_instead_of_hanging(
    store: Store,
    client: httpx.AsyncClient,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    MockHN(respx_mock, {i: story(i) for i in range(1, 31)})

    async def broken_upsert(*args: object) -> dict[str, UpsertOutcome]:
        raise RuntimeError("bug in flush")

    monkeypatch.setattr(store, "upsert_items", broken_upsert)
    # Tiny raw queue: fetchers are blocked on put() when the workers die.
    with pytest.RaisesGroup(RuntimeError, flatten_subgroups=True):
        async with asyncio.timeout(TIMEOUT_S):
            await run_hn_concurrent(
                store,
                client,
                base_url=BASE,
                limit=30,
                cfg=cfg(batch_size=1, raw_queue_maxsize=1, id_queue_maxsize=1),
            )
    assert other_tasks() == set()


async def test_rejects_pool_smaller_than_workers_plus_one(
    test_settings: Settings,
    schema: str,
    migrated_pool: DbPool,
    client: httpx.AsyncClient,
) -> None:
    small = test_settings.model_copy(update={"db_pool_min_size": 1, "db_pool_max_size": 2})
    small_pool = await create_pool(small, search_path=schema)
    try:
        store = Store(small_pool, acquire_timeout=1.0)
        with pytest.raises(ValueError, match="workers \\+ 1"):
            await run_hn_concurrent(store, client, base_url=BASE, limit=1, cfg=cfg(workers=2))
        # Rejected before any work: no run row was created.
        assert await migrated_pool.fetchval("SELECT count(*) FROM ingestion_runs") == 0
    finally:
        await small_pool.close()


async def test_empty_top_stories_succeeds(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    MockHN(respx_mock, {})
    async with asyncio.timeout(TIMEOUT_S):
        result = await run_hn_concurrent(store, client, base_url=BASE, limit=10, cfg=cfg())
    assert result.status is RunStatus.SUCCEEDED and result.counts.fetched == 0


async def test_rerun_is_unchanged(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    MockHN(respx_mock, {i: story(i) for i in range(1, 16)})
    await run_hn_concurrent(store, client, base_url=BASE, limit=15, cfg=cfg())
    again = await run_hn_concurrent(store, client, base_url=BASE, limit=15, cfg=cfg())
    assert (again.counts.inserted, again.counts.unchanged) == (0, 15)


def test_config_from_settings_and_overrides() -> None:
    s = Settings(_env_file=None, fetch_concurrency=7, pipeline_workers=3, db_pool_max_size=4)
    c = PipelineConfig.from_settings(s, batch_size=9)
    assert (c.concurrency, c.workers, c.batch_size) == (7, 3, 9)


@pytest.mark.parametrize(
    "bad",
    [{"concurrency": 0}, {"workers": 0}, {"batch_size": 0}, {"raw_queue_maxsize": 0},
     {"id_queue_maxsize": 0}, {"flush_interval_s": 0}],
)  # fmt: skip
def test_config_rejects_non_positive_values(bad: dict[str, int | float]) -> None:
    with pytest.raises(ValueError):
        cfg(**bad)
