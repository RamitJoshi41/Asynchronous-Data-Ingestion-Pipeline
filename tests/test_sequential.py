import json
from collections.abc import AsyncIterator

import httpx
import pytest
import respx
from structlog.testing import capture_logs

from ingest.config import Settings
from ingest.db import DbPool
from ingest.httpclient import build_http_client
from ingest.retry import RetryPolicy
from ingest.sequential import run_hn_sequential
from ingest.shutdown import StopSignal
from ingest.storage import RunStatus, Store

FAST = RetryPolicy(max_attempts=3, base_delay_s=0.001, max_delay_s=0.005, max_retry_after_s=5)

BASE = "https://hn.test/v0"
STORY = {"id": 1, "type": "story", "by": "a", "time": 1175714200, "title": "t", "score": 5}
JOB = {"id": 4, "type": "job", "by": "b", "time": 1210981217, "title": "j"}


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with build_http_client(Settings(_env_file=None)) as c:
        yield c


@pytest.fixture
def store(migrated_pool: DbPool) -> Store:
    return Store(migrated_pool, acquire_timeout=5.0)


def mock_items(respx_mock: respx.MockRouter, items: dict[int, object]) -> None:
    respx_mock.get(f"{BASE}/topstories.json").respond(json=list(items))
    for item_id, body in items.items():
        respx_mock.get(f"{BASE}/item/{item_id}.json").respond(content=json.dumps(body).encode())


# 1 valid story, 2 invalid (string score), 3 missing (null), 4 valid job, 5 body for another id.
MIXED: dict[int, object] = {
    1: STORY,
    2: {**STORY, "id": 2, "score": "lots"},
    3: None,
    4: JOB,
    5: {**STORY, "id": 99},
}


async def run_row(pool: DbPool, run_id: int) -> dict[str, object]:
    row = await pool.fetchrow("SELECT * FROM ingestion_runs WHERE id = $1", run_id)
    assert row is not None
    return dict(row)


async def test_mixed_items_are_classified_and_valid_ones_stored(
    store: Store,
    client: httpx.AsyncClient,
    respx_mock: respx.MockRouter,
    migrated_pool: DbPool,
) -> None:
    mock_items(respx_mock, MIXED)
    summary = await run_hn_sequential(store, client, base_url=BASE, limit=5)

    c = summary.counts
    assert summary.status is RunStatus.SUCCEEDED
    assert (c.fetched, c.inserted, c.updated, c.unchanged) == (5, 2, 0, 0)
    assert (c.invalid, c.missing) == (2, 1)

    row = await run_row(migrated_pool, summary.run_id)
    assert row["status"] == "succeeded" and row["finished_at"] is not None
    assert (row["items_fetched"], row["items_inserted"], row["items_invalid"]) == (5, 2, 2)
    assert row["items_missing"] == 1

    stored = await migrated_pool.fetch("SELECT source, source_id, payload FROM items ORDER BY 2")
    assert [(r["source"], r["source_id"]) for r in stored] == [
        ("hacker_news", "1"),
        ("hacker_news", "4"),
    ]
    assert json.loads(stored[0]["payload"])["time"] == 1175714200  # validated, HN-shaped


async def test_rerun_reports_unchanged_and_writes_nothing_new(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    mock_items(respx_mock, {1: STORY, 4: JOB})
    await run_hn_sequential(store, client, base_url=BASE, limit=2)
    second = await run_hn_sequential(store, client, base_url=BASE, limit=2)
    assert (second.counts.inserted, second.counts.unchanged) == (0, 2)


async def test_limit_fetches_only_the_first_n_items(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    mock_items(respx_mock, {1: STORY, 4: JOB, 7: {**STORY, "id": 7}})
    summary = await run_hn_sequential(store, client, base_url=BASE, limit=2)
    assert summary.counts.fetched == 2
    assert not respx_mock.routes[3].called  # item 7 was never requested


async def test_http_error_mid_run_marks_run_failed_and_reraises(
    store: Store,
    client: httpx.AsyncClient,
    respx_mock: respx.MockRouter,
    migrated_pool: DbPool,
) -> None:
    respx_mock.get(f"{BASE}/topstories.json").respond(json=[1, 4, 8])
    respx_mock.get(f"{BASE}/item/1.json").respond(json=STORY)
    respx_mock.get(f"{BASE}/item/4.json").respond(json=JOB)
    respx_mock.get(f"{BASE}/item/8.json").respond(status_code=500)

    with pytest.raises(httpx.HTTPStatusError):
        await run_hn_sequential(store, client, base_url=BASE, limit=3, policy=FAST)

    row = await migrated_pool.fetchrow("SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT 1")
    assert row is not None
    assert row["status"] == "failed" and row["finished_at"] is not None
    assert "500" in row["error"]
    assert (row["items_fetched"], row["items_inserted"]) == (2, 2)
    # Rows written before the failure stay committed: each upsert is its own transaction.
    assert await migrated_pool.fetchval("SELECT count(*) FROM items") == 2


async def test_network_failure_on_top_ids_marks_run_failed(
    store: Store,
    client: httpx.AsyncClient,
    respx_mock: respx.MockRouter,
    migrated_pool: DbPool,
) -> None:
    respx_mock.get(f"{BASE}/topstories.json").mock(side_effect=httpx.ConnectError("down"))
    with pytest.raises(httpx.ConnectError):
        await run_hn_sequential(store, client, base_url=BASE, limit=3, policy=FAST)
    row = await migrated_pool.fetchrow(
        "SELECT status, items_fetched FROM ingestion_runs ORDER BY id DESC LIMIT 1"
    )
    assert row is not None
    assert (row["status"], row["items_fetched"]) == ("failed", 0)


async def test_logs_carry_run_context_but_never_payloads(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    mock_items(respx_mock, MIXED)
    with capture_logs() as logs:
        summary = await run_hn_sequential(store, client, base_url=BASE, limit=5)

    invalid = [e for e in logs if e["event"] == "item_invalid"]
    assert [e["item_id"] for e in invalid] == [2]
    assert invalid[0]["problems"] == ["score:int_type"]
    assert all(e["run_id"] == summary.run_id and e["source"] == "hacker_news" for e in logs)
    assert "lots" not in repr(logs)  # the offending raw value is not logged
    assert [e["event"] for e in logs if e["event"].startswith("run_")] == [
        "run_started",
        "run_finished",
    ]


# --- Phase 3: reliability (same shared helpers as the concurrent pipeline) ------------------


async def test_rejects_are_dead_lettered(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    mock_items(respx_mock, MIXED)
    await run_hn_sequential(store, client, base_url=BASE, limit=5, policy=FAST)
    rows = await migrated_pool.fetch("SELECT source_id, reason FROM dead_letter ORDER BY 1")
    assert [(r["source_id"], r["reason"]) for r in rows] == [("2", "invalid"), ("5", "id_mismatch")]


async def test_transient_error_is_retried(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/topstories.json").respond(json=[1])
    respx_mock.get(f"{BASE}/item/1.json").mock(
        side_effect=[httpx.ConnectError("drop"), httpx.Response(200, json=STORY)]
    )
    summary = await run_hn_sequential(store, client, base_url=BASE, limit=1, policy=FAST)
    assert (summary.counts.inserted, summary.counts.retries) == (1, 1)


async def test_stop_ends_run_after_current_item_as_cancelled(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    stop = StopSignal()
    respx_mock.get(f"{BASE}/topstories.json").respond(json=[1, 4, 7])

    def first_then_stop(request: httpx.Request) -> httpx.Response:
        stop.request("test")  # a signal arrives while item 1 is being fetched
        return httpx.Response(200, json=STORY)

    respx_mock.get(f"{BASE}/item/1.json").mock(side_effect=first_then_stop)
    summary = await run_hn_sequential(store, client, base_url=BASE, limit=3, stop=stop)
    assert summary.status is RunStatus.CANCELLED
    assert summary.counts.fetched == 1  # item 1 finished; 4 and 7 never requested
    row = await migrated_pool.fetchrow("SELECT status, items_inserted FROM ingestion_runs")
    assert row is not None and (row["status"], row["items_inserted"]) == ("cancelled", 1)
