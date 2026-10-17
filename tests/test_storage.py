import json
import time

import asyncpg
import pytest

from ingest.config import Settings
from ingest.db import DbPool, create_pool
from ingest.storage import (
    MAX_ERROR_CHARS,
    DeadLetter,
    RunCounts,
    RunKind,
    RunStatus,
    Store,
    UpsertOutcome,
)

T = 5.0


@pytest.fixture
def store(migrated_pool: DbPool) -> Store:
    return Store(migrated_pool, acquire_timeout=T)


async def upsert_one(
    store: Store, run_id: int, source: str, source_id: str, payload: str
) -> UpsertOutcome:
    return (await store.upsert_items(run_id, source, [(source_id, payload)]))[source_id]


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

    assert await upsert_one(store, run1, "hacker_news", "1", v1) is UpsertOutcome.INSERTED
    first = await item_row(migrated_pool, "hacker_news", "1")
    assert json.loads(first["payload"]) == {"id": 1, "score": 10}
    assert first["last_changed_run_id"] == run1

    # Same payload from a later run: nothing is written. Same row version (xmin), same timestamps.
    assert await upsert_one(store, run2, "hacker_news", "1", v1) is UpsertOutcome.UNCHANGED
    same = await item_row(migrated_pool, "hacker_news", "1")
    assert same["xmin"] == first["xmin"]
    assert same["updated_at"] == first["updated_at"]
    assert same["last_changed_run_id"] == run1

    v2 = json.dumps({"id": 1, "score": 11})
    assert await upsert_one(store, run2, "hacker_news", "1", v2) is UpsertOutcome.UPDATED
    changed = await item_row(migrated_pool, "hacker_news", "1")
    assert changed["xmin"] != first["xmin"]
    assert changed["updated_at"] > first["updated_at"]
    assert changed["last_changed_run_id"] == run2


async def test_jsonb_comparison_ignores_key_order_and_whitespace(store: Store) -> None:
    run_id = await store.start_run("hacker_news")
    await upsert_one(store, run_id, "hacker_news", "7", '{"a": 1, "b": 2}')
    outcome = await upsert_one(store, run_id, "hacker_news", "7", '{ "b":2,"a":1 }')
    assert outcome is UpsertOutcome.UNCHANGED


async def test_same_id_in_different_sources_are_separate_rows(
    store: Store, migrated_pool: DbPool
) -> None:
    run_id = await store.start_run("hacker_news")
    assert await upsert_one(store, run_id, "hacker_news", "1", "{}") is UpsertOutcome.INSERTED
    assert await upsert_one(store, run_id, "other", "1", "{}") is UpsertOutcome.INSERTED
    assert await migrated_pool.fetchval("SELECT count(*) FROM items") == 2


async def test_upsert_requires_existing_run(store: Store) -> None:
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await upsert_one(store, 424242, "hacker_news", "1", "{}")


def test_run_counts_record_outcomes() -> None:
    counts = RunCounts()
    for outcome in (UpsertOutcome.INSERTED, UpsertOutcome.INSERTED, UpsertOutcome.UNCHANGED):
        counts.record(outcome)
    counts.record(UpsertOutcome.UPDATED)
    assert (counts.inserted, counts.updated, counts.unchanged) == (2, 1, 1)


async def test_batch_upsert_reports_outcome_per_key(store: Store, migrated_pool: DbPool) -> None:
    run1 = await store.start_run("hacker_news")
    first = await store.upsert_items(run1, "hacker_news", [("1", '{"v": 1}'), ("2", '{"v": 2}')])
    assert first == {"1": UpsertOutcome.INSERTED, "2": UpsertOutcome.INSERTED}
    before = await item_row(migrated_pool, "hacker_news", "1")

    run2 = await store.start_run("hacker_news")
    second = await store.upsert_items(
        run2, "hacker_news", [("1", '{"v": 1}'), ("2", '{"v": 22}'), ("3", "{}")]
    )
    assert second == {
        "1": UpsertOutcome.UNCHANGED,
        "2": UpsertOutcome.UPDATED,
        "3": UpsertOutcome.INSERTED,
    }
    # The unchanged row inside a batch is also not rewritten.
    assert (await item_row(migrated_pool, "hacker_news", "1"))["xmin"] == before["xmin"]
    assert (await item_row(migrated_pool, "hacker_news", "2"))["last_changed_run_id"] == run2


async def test_batch_duplicates_are_collapsed_last_wins(
    store: Store, migrated_pool: DbPool
) -> None:
    # Without de-duplication Postgres rejects the whole statement (CardinalityViolationError).
    run_id = await store.start_run("hacker_news")
    outcome = await store.upsert_items(
        run_id, "hacker_news", [("9", '{"v": "old"}'), ("9", '{"v": "new"}')]
    )
    assert outcome == {"9": UpsertOutcome.INSERTED}
    row = await item_row(migrated_pool, "hacker_news", "9")
    assert json.loads(row["payload"]) == {"v": "new"}


async def test_empty_batch_is_a_no_op(store: Store) -> None:
    run_id = await store.start_run("hacker_news")
    assert await store.upsert_items(run_id, "hacker_news", []) == {}


async def test_undersized_pool_flush_times_out_instead_of_hanging(
    test_settings: Settings, schema: str, migrated_pool: DbPool
) -> None:
    # Deliberate pool exhaustion (ADR-0018): a pool of ONE connection, already borrowed by
    # "another worker". The next flush must fail within the acquire timeout, not wait forever.
    tiny = test_settings.model_copy(update={"db_pool_min_size": 1, "db_pool_max_size": 1})
    tiny_pool = await create_pool(tiny, search_path=schema)
    try:
        store = Store(tiny_pool, acquire_timeout=0.2)
        async with tiny_pool.acquire() as busy_worker_conn:
            assert busy_worker_conn is not None
            started = time.perf_counter()
            with pytest.raises(TimeoutError):
                await store.upsert_items(1, "hacker_news", [("1", "{}")])
            assert time.perf_counter() - started < 1.0
    finally:
        await tiny_pool.close()


def test_store_reports_pool_max_size(store: Store, test_settings: Settings) -> None:
    assert store.pool_max_size == test_settings.db_pool_max_size


# --- Phase 3: dead letters, transactional flush, run kinds --------------------------------


async def open_dead_letters(pool: DbPool) -> list[asyncpg.Record]:
    return await pool.fetch(
        "SELECT source_id, reason, attempts, run_id FROM dead_letter "
        "WHERE resolved_at IS NULL ORDER BY source_id"
    )


async def test_flush_writes_items_and_dead_letters_together(
    store: Store, migrated_pool: DbPool
) -> None:
    run_id = await store.start_run("hacker_news")
    reject = DeadLetter("2", "invalid", ["score:int_type"], b'{"id": 2, "score": "x"}')
    result = await store.flush(run_id, "hacker_news", [("1", '{"id": 1}')], [reject])
    assert result.outcomes == {"1": UpsertOutcome.INSERTED}
    assert (result.dead_lettered, result.resolved) == (1, 0)
    row = await migrated_pool.fetchrow("SELECT * FROM dead_letter")
    assert row is not None
    assert (row["source_id"], row["reason"], row["run_id"]) == ("2", "invalid", run_id)
    assert json.loads(row["errors"]) == ["score:int_type"]
    assert bytes(row["raw_body"]) == b'{"id": 2, "score": "x"}'


async def test_flush_is_atomic(store: Store, migrated_pool: DbPool) -> None:
    run_id = await store.start_run("hacker_news")
    broken = DeadLetter("2", None)  # type: ignore[arg-type]  # violates reason NOT NULL
    with pytest.raises(asyncpg.NotNullViolationError):
        await store.flush(run_id, "hacker_news", [("1", '{"id": 1}')], [broken])
    # The item upsert ran first in the same transaction and was rolled back with it.
    assert await migrated_pool.fetchval("SELECT count(*) FROM items") == 0


async def test_non_utf8_body_is_stored_byte_for_byte(store: Store, migrated_pool: DbPool) -> None:
    run_id = await store.start_run("hacker_news")
    garbage = b"\xff\xfe not utf-8 \x00"
    await store.flush(run_id, "hacker_news", [], [DeadLetter("9", "invalid", [], garbage)])
    stored = await migrated_pool.fetchval("SELECT raw_body FROM dead_letter")
    assert bytes(stored) == garbage  # why the column is bytea, not text/jsonb


async def test_repeat_failure_keeps_one_open_row(store: Store, migrated_pool: DbPool) -> None:
    run1 = await store.start_run("hacker_news")
    run2 = await store.start_run("hacker_news")
    await store.flush(run1, "hacker_news", [], [DeadLetter("7", "invalid", ["a:x"], b"v1")])
    await store.flush(run2, "hacker_news", [], [DeadLetter("7", "id_mismatch", ["id:8"], b"v2")])
    rows = await open_dead_letters(migrated_pool)
    assert [(r["reason"], r["attempts"], r["run_id"]) for r in rows] == [("id_mismatch", 2, run2)]


async def test_clean_ingest_resolves_open_dead_letter_and_later_failure_opens_new_one(
    store: Store, migrated_pool: DbPool
) -> None:
    run1, run2, run3 = [await store.start_run("hacker_news") for _ in range(3)]
    await store.flush(run1, "hacker_news", [], [DeadLetter("5", "invalid", [], b"bad")])
    fixed = await store.flush(run2, "hacker_news", [("5", '{"id": 5}')], [])
    assert fixed.resolved == 1 and await open_dead_letters(migrated_pool) == []
    resolved = await migrated_pool.fetchrow("SELECT resolved_run_id FROM dead_letter")
    assert resolved is not None and resolved["resolved_run_id"] == run2

    # History is kept: a new failure later opens a fresh row instead of reopening the old one.
    await store.flush(run3, "hacker_news", [], [DeadLetter("5", "invalid", [], b"bad again")])
    assert await migrated_pool.fetchval("SELECT count(*) FROM dead_letter") == 2
    assert [r["attempts"] for r in await open_dead_letters(migrated_pool)] == [1]


async def test_open_dead_letters_pages_and_skips_http_and_resolved(
    store: Store, migrated_pool: DbPool
) -> None:
    run_id = await store.start_run("hacker_news")
    rejects = [DeadLetter(str(i), "invalid", [], b"x") for i in range(1, 6)]
    rejects.append(DeadLetter("404", "http_404", [], b"Not Found"))
    await store.flush(run_id, "hacker_news", [("3", '{"id": 3}')], [])  # resolves nothing yet
    await store.flush(run_id, "hacker_news", [], rejects)
    await store.flush(run_id, "hacker_news", [("3", '{"id": 3}')], [])  # resolves "3"

    first = await store.open_dead_letters("hacker_news", after_id=0, limit=2)
    rest = await store.open_dead_letters("hacker_news", after_id=first[-1].id, limit=10)
    assert [d.source_id for d in first + rest] == ["1", "2", "4", "5"]  # no "3", no http_404


async def test_run_kind_retries_and_cancelled_status_are_recorded(
    store: Store, migrated_pool: DbPool
) -> None:
    run_id = await store.start_run("hacker_news", RunKind.REPLAY)
    await store.finish_run(run_id, RunStatus.CANCELLED, RunCounts(retries=4))
    row = await migrated_pool.fetchrow("SELECT kind, status, retries FROM ingestion_runs")
    assert row is not None and tuple(row) == ("replay", "cancelled", 4)
