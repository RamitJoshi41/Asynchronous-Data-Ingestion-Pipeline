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
from ingest.retry import RetryPolicy
from ingest.sequential import run_hn_sequential
from ingest.shutdown import StopSignal
from ingest.storage import DeadLetter, FlushResult, RunStatus, Store

BASE = "https://hn.test/v0"
ITEM_URL = r"https://hn\.test/v0/item/(?P<item_id>\d+)\.json"
TIMEOUT_S = 5.0
# Millisecond backoff: retry behaviour is exercised without slowing the suite down.
FAST = RetryPolicy(max_attempts=3, base_delay_s=0.001, max_delay_s=0.005, max_retry_after_s=5)


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
    real_flush = store.flush

    async def slow_flush(
        run_id: int,
        source: str,
        records: Sequence[tuple[str, str]],
        dead_letters: Sequence[DeadLetter],
    ) -> FlushResult:
        await asyncio.sleep(0.03)  # a slow database
        return await real_flush(run_id, source, records, dead_letters)

    monkeypatch.setattr(store, "flush", slow_flush)
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


async def test_persistent_500_aborts_after_retries_with_exact_counts(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    bodies: dict[int, object] = {i: story(i) for i in range(1, 21)}
    bodies[13] = 500  # every attempt fails: retries exhausted -> the run fails
    MockHN(respx_mock, bodies, latency=0.01)
    with pytest.RaisesGroup(httpx.HTTPStatusError, flatten_subgroups=True):
        async with asyncio.timeout(TIMEOUT_S):
            await run_hn_concurrent(
                store,
                client,
                base_url=BASE,
                limit=20,
                cfg=cfg(workers=2, batch_size=1),
                policy=FAST,
            )
    assert other_tasks() == set()
    row = await migrated_pool.fetchrow("SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT 1")
    assert row is not None
    assert row["status"] == "failed" and "500" in row["error"]
    assert row["retries"] >= FAST.max_attempts - 1  # item 13 was retried before giving up
    # Cancellation-safe flushes (ADR-0027): a flush that was mid-write when the TaskGroup
    # cancelled its worker still completes AND is counted before the run is recorded. Phase 2 could
    # only promise counted <= stored <= counted + W x B (ADR-0020); now it's exact.
    stored = await migrated_pool.fetchval("SELECT count(*) FROM items")
    assert stored == row["items_inserted"]


async def test_unexpected_worker_error_aborts_instead_of_hanging(
    store: Store,
    client: httpx.AsyncClient,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    MockHN(respx_mock, {i: story(i) for i in range(1, 31)})

    async def broken_flush(*args: object) -> FlushResult:
        raise RuntimeError("bug in flush")

    monkeypatch.setattr(store, "flush", broken_flush)
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


# --- Phase 3: reliability -------------------------------------------------------------------


async def test_transient_failures_are_retried_and_counted(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    respx_mock.get(f"{BASE}/topstories.json").mock(
        side_effect=[httpx.ConnectError("drop"), httpx.Response(200, json=[1, 2])]
    )
    respx_mock.get(f"{BASE}/item/1.json").mock(
        side_effect=[
            httpx.ReadTimeout("slow"),
            httpx.Response(503),
            httpx.Response(200, json=story(1)),
        ]
    )
    respx_mock.get(f"{BASE}/item/2.json").respond(json=story(2))
    async with asyncio.timeout(TIMEOUT_S):
        result = await run_hn_concurrent(
            store, client, base_url=BASE, limit=2, cfg=cfg(), policy=FAST
        )
    assert result.status is RunStatus.SUCCEEDED
    assert (result.counts.inserted, result.counts.retries) == (2, 3)  # 1 for ids + 2 for item 1
    row = await migrated_pool.fetchrow(
        "SELECT retries FROM ingestion_runs WHERE id = $1", result.run_id
    )
    assert row is not None and row["retries"] == 3


async def test_schema_change_lands_in_dead_letter_and_run_continues(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    changed = {**story(2), "score": "12"}  # upstream started sending score as a string
    MockHN(respx_mock, {1: story(1), 2: changed, 3: story(3), 4: 404})
    async with asyncio.timeout(TIMEOUT_S):
        result = await run_hn_concurrent(
            store, client, base_url=BASE, limit=4, cfg=cfg(), policy=FAST
        )
    assert result.status is RunStatus.SUCCEEDED
    assert (result.counts.inserted, result.counts.invalid) == (2, 2)
    rows = await migrated_pool.fetch(
        "SELECT source_id, run_id, reason, errors, raw_body, attempts FROM dead_letter ORDER BY 1"
    )
    assert [(r["source_id"], r["reason"]) for r in rows] == [("2", "invalid"), ("4", "http_404")]
    invalid = rows[0]
    assert json.loads(invalid["errors"]) == ["score:int_type"]
    assert json.loads(bytes(invalid["raw_body"])) == changed  # the exact rejected body, for replay
    assert invalid["run_id"] == result.run_id and invalid["attempts"] == 1


async def test_repeat_failure_updates_the_open_dead_letter(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    MockHN(respx_mock, {1: {**story(1), "score": "x"}})
    await run_hn_concurrent(store, client, base_url=BASE, limit=1, cfg=cfg(), policy=FAST)
    second = await run_hn_concurrent(store, client, base_url=BASE, limit=1, cfg=cfg(), policy=FAST)
    rows = await migrated_pool.fetch("SELECT run_id, attempts FROM dead_letter")
    assert [(r["run_id"], r["attempts"]) for r in rows] == [(second.run_id, 2)]  # one open row


async def test_successful_ingest_resolves_an_open_dead_letter(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    bodies: dict[int, object] = {1: {**story(1), "score": "x"}}
    MockHN(respx_mock, bodies)
    await run_hn_concurrent(store, client, base_url=BASE, limit=1, cfg=cfg(), policy=FAST)
    bodies[1] = story(1)  # upstream fixed the record
    fixed = await run_hn_concurrent(store, client, base_url=BASE, limit=1, cfg=cfg(), policy=FAST)
    row = await migrated_pool.fetchrow("SELECT resolved_at, resolved_run_id FROM dead_letter")
    assert row is not None and row["resolved_at"] is not None
    assert row["resolved_run_id"] == fixed.run_id


async def test_429_retry_after_pauses_all_fetchers(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    calls: list[tuple[int, float]] = []
    loop = asyncio.get_running_loop()
    first_429_sent = False

    async def item(request: httpx.Request, item_id: str) -> httpx.Response:
        nonlocal first_429_sent
        calls.append((int(item_id), loop.time()))
        if item_id == "1" and not first_429_sent:
            first_429_sent = True
            return httpx.Response(429, headers={"Retry-After": "1"})
        await asyncio.sleep(0.05)
        return httpx.Response(200, json=story(int(item_id)))

    respx_mock.get(f"{BASE}/topstories.json").respond(json=list(range(1, 11)))
    respx_mock.get(url__regex=ITEM_URL).mock(side_effect=item)
    async with asyncio.timeout(TIMEOUT_S):
        result = await run_hn_concurrent(
            store, client, base_url=BASE, limit=10, cfg=cfg(concurrency=2), policy=FAST
        )
    assert result.counts.inserted == 10
    started = calls[0][1]
    # Item 1 got 429 + Retry-After: 1 at t0. Fetcher 2 had one request in flight already; every
    # request STARTED after that must wait for the shared gate, i.e. begin >= ~1 s after t0.
    later = [t - started for _, t in calls[2:]]
    assert all(dt >= 0.95 for dt in later), later


async def test_stop_drains_in_flight_work_and_marks_cancelled(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    MockHN(respx_mock, {i: story(i) for i in range(1, 201)}, latency=0.02)
    stop = StopSignal()
    asyncio.get_running_loop().call_later(0.1, stop.request, "test")
    async with asyncio.timeout(TIMEOUT_S):
        result = await run_hn_concurrent(
            store, client, base_url=BASE, limit=200, cfg=cfg(batch_size=50), stop=stop
        )
    assert result.status is RunStatus.CANCELLED
    assert result.stats.skipped_on_stop > 0 and result.counts.fetched < 200
    # Everything that was fetched was flushed (no record left in a buffer) and counts are exact.
    stored = await migrated_pool.fetchval("SELECT count(*) FROM items")
    row = await migrated_pool.fetchrow(
        "SELECT status, items_fetched, items_inserted, finished_at FROM ingestion_runs"
    )
    assert row is not None and row["status"] == "cancelled" and row["finished_at"] is not None
    assert stored == row["items_inserted"] == row["items_fetched"] == result.counts.fetched
    assert other_tasks() == set()


async def test_drain_past_deadline_is_cut_short_but_still_recorded(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    MockHN(respx_mock, {i: story(i) for i in range(1, 11)}, latency=5.0)  # requests that hang
    stop = StopSignal()
    asyncio.get_running_loop().call_later(0.1, stop.request, "test")
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    async with asyncio.timeout(TIMEOUT_S):
        result = await run_hn_concurrent(
            store, client, base_url=BASE, limit=10, cfg=cfg(), stop=stop, drain_timeout_s=0.3
        )
    assert loop.time() - t0 < 1.5  # did not wait out the 5 s requests
    assert result.status is RunStatus.CANCELLED
    row = await migrated_pool.fetchrow("SELECT status, error FROM ingestion_runs")
    assert row is not None and row["status"] == "cancelled" and "drain exceeded" in row["error"]
    assert other_tasks() == set()


async def test_forced_cancellation_lets_the_in_progress_batch_land(
    store: Store,
    client: httpx.AsyncClient,
    respx_mock: respx.MockRouter,
    migrated_pool: DbPool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    MockHN(respx_mock, {i: story(i) for i in range(1, 101)}, latency=0.01)
    real_flush = store.flush
    write_started = asyncio.Event()

    async def slow_flush(
        run_id: int,
        source: str,
        records: Sequence[tuple[str, str]],
        dead_letters: Sequence[DeadLetter],
    ) -> FlushResult:
        write_started.set()
        await asyncio.sleep(0.1)  # a slow write that is IN PROGRESS when cancellation arrives
        return await real_flush(run_id, source, records, dead_letters)

    monkeypatch.setattr(store, "flush", slow_flush)
    task = asyncio.create_task(
        run_hn_concurrent(store, client, base_url=BASE, limit=100, cfg=cfg(workers=1, batch_size=5))
    )
    await write_started.wait()
    task.cancel()  # what a second Ctrl-C does, exactly while the first batch is being written
    with pytest.raises(asyncio.CancelledError):
        await task
    stored = await migrated_pool.fetchval("SELECT count(*) FROM items")
    row = await migrated_pool.fetchrow("SELECT status, items_inserted, error FROM ingestion_runs")
    assert (
        row is not None and row["status"] == "cancelled" and row["error"] == "forced cancellation"
    )
    # The shielded write finished instead of being cancelled and rolled back, and was counted
    # before the run was recorded (ADR-0027). Without the shield this batch would be lost: 0 rows.
    assert stored == row["items_inserted"] == 5
    assert other_tasks() == set()
