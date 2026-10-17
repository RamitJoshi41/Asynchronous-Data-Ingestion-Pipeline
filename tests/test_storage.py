import json

import asyncpg
import pytest

from ingest.db import DbPool
from ingest.storage import MAX_ERROR_CHARS, RunCounts, RunStatus, Store, UpsertOutcome

T = 5.0


@pytest.fixture
def store(migrated_pool: DbPool) -> Store:
    return Store(migrated_pool, acquire_timeout=T)


async def item_row(pool: DbPool, source: str, source_id: str) -> asyncpg.Record:
    # xmin = id of the transaction that wrote this row version: it changes on every real write.
    row = await pool.fetchrow(
        "SELECT payload, updated_at, last_changed_run_id, xmin::text AS xmin "
        "FROM items WHERE source = $1 AND source_id = $2",
        source,
        source_id,
    )
    assert row is not None
    return row


async def test_start_run_creates_running_row(store: Store, migrated_pool: DbPool) -> None:
    run_id = await store.start_run("hacker_news")
    row = await migrated_pool.fetchrow("SELECT * FROM ingestion_runs WHERE id = $1", run_id)
    assert row is not None
    assert row["status"] == "running" and row["finished_at"] is None and row["items_fetched"] == 0


async def test_finish_run_records_counts_and_status(store: Store, migrated_pool: DbPool) -> None:
    run_id = await store.start_run("hacker_news")
    counts = RunCounts(fetched=6, inserted=2, updated=1, unchanged=1, invalid=1, missing=1)
    await store.finish_run(run_id, RunStatus.SUCCEEDED, counts)
    row = await migrated_pool.fetchrow("SELECT * FROM ingestion_runs WHERE id = $1", run_id)
    assert row is not None
    assert row["status"] == "succeeded" and row["finished_at"] is not None
    assert [row[f"items_{k}"] for k in ("fetched", "inserted", "updated")] == [6, 2, 1]
    assert [row[f"items_{k}"] for k in ("unchanged", "invalid", "missing")] == [1, 1, 1]
    assert row["error"] is None


async def test_failed_run_error_is_truncated(store: Store, migrated_pool: DbPool) -> None:
    run_id = await store.start_run("hacker_news")
    await store.finish_run(run_id, RunStatus.FAILED, RunCounts(), error="x" * 5000)
    error = await migrated_pool.fetchval("SELECT error FROM ingestion_runs WHERE id = $1", run_id)
    assert error == "x" * MAX_ERROR_CHARS


async def test_finishing_twice_or_unknown_run_raises(store: Store) -> None:
    run_id = await store.start_run("hacker_news")
    await store.finish_run(run_id, RunStatus.SUCCEEDED, RunCounts())
    with pytest.raises(RuntimeError, match="not an unfinished run"):
        await store.finish_run(run_id, RunStatus.FAILED, RunCounts())
    with pytest.raises(RuntimeError):
        await store.finish_run(999_999, RunStatus.FAILED, RunCounts())


async def test_finish_run_rejects_running_status(store: Store) -> None:
    run_id = await store.start_run("hacker_news")
    with pytest.raises(ValueError, match="terminal"):
        await store.finish_run(run_id, RunStatus.RUNNING, RunCounts())


async def test_upsert_insert_then_unchanged_then_update(
    store: Store, migrated_pool: DbPool
) -> None:
    run1 = await store.start_run("hacker_news")
    run2 = await store.start_run("hacker_news")
    v1 = json.dumps({"id": 1, "score": 10})

    assert await store.upsert_item(run1, "hacker_news", "1", v1) is UpsertOutcome.INSERTED
    first = await item_row(migrated_pool, "hacker_news", "1")
    assert json.loads(first["payload"]) == {"id": 1, "score": 10}
    assert first["last_changed_run_id"] == run1

    # Same payload from a later run: nothing is written. Same row version (xmin), same timestamps.
    assert await store.upsert_item(run2, "hacker_news", "1", v1) is UpsertOutcome.UNCHANGED
    same = await item_row(migrated_pool, "hacker_news", "1")
    assert same["xmin"] == first["xmin"]
    assert same["updated_at"] == first["updated_at"]
    assert same["last_changed_run_id"] == run1

    v2 = json.dumps({"id": 1, "score": 11})
    assert await store.upsert_item(run2, "hacker_news", "1", v2) is UpsertOutcome.UPDATED
    changed = await item_row(migrated_pool, "hacker_news", "1")
    assert changed["xmin"] != first["xmin"]
    assert changed["updated_at"] > first["updated_at"]
    assert changed["last_changed_run_id"] == run2


async def test_jsonb_comparison_ignores_key_order_and_whitespace(store: Store) -> None:
    run_id = await store.start_run("hacker_news")
    await store.upsert_item(run_id, "hacker_news", "7", '{"a": 1, "b": 2}')
    outcome = await store.upsert_item(run_id, "hacker_news", "7", '{ "b":2,"a":1 }')
    assert outcome is UpsertOutcome.UNCHANGED


async def test_same_id_in_different_sources_are_separate_rows(
    store: Store, migrated_pool: DbPool
) -> None:
    run_id = await store.start_run("hacker_news")
    assert await store.upsert_item(run_id, "hacker_news", "1", "{}") is UpsertOutcome.INSERTED
    assert await store.upsert_item(run_id, "other", "1", "{}") is UpsertOutcome.INSERTED
    assert await migrated_pool.fetchval("SELECT count(*) FROM items") == 2


async def test_upsert_requires_existing_run(store: Store) -> None:
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await store.upsert_item(424242, "hacker_news", "1", "{}")


def test_run_counts_record_outcomes() -> None:
    counts = RunCounts()
    for outcome in (UpsertOutcome.INSERTED, UpsertOutcome.INSERTED, UpsertOutcome.UNCHANGED):
        counts.record(outcome)
    counts.record(UpsertOutcome.UPDATED)
    assert (counts.inserted, counts.updated, counts.unchanged) == (2, 1, 1)
