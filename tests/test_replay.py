import json

import pytest

from ingest.db import DbPool
from ingest.replay import replay_dead_letters
from ingest.storage import DeadLetter, RunKind, RunStatus, Store

STORY_9 = {"id": 9, "type": "story", "by": "a", "time": 1175714200, "title": "fixed"}


@pytest.fixture
def store(migrated_pool: DbPool) -> Store:
    return Store(migrated_pool, acquire_timeout=5.0)


async def seed(store: Store) -> int:
    """Dead letters as an older, stricter model might have left them."""
    run_id = await store.start_run("hacker_news")
    await store.flush(
        run_id,
        "hacker_news",
        [],
        [
            # Valid under the CURRENT model: imagine it was rejected before a model fix.
            DeadLetter("9", "invalid", ["title:string_too_long"], json.dumps(STORY_9).encode()),
            DeadLetter("10", "invalid", ["score:int_type"], b'{"id": 10, "score": "x"}'),
            DeadLetter("11", "id_mismatch", ["id:12"], b'{"id": 12}'),
            DeadLetter("13", "http_404", ["http:404"], b"Not Found"),  # not replayable
        ],
    )
    return run_id


async def open_rows(pool: DbPool) -> dict[str, tuple[str, int]]:
    rows = await pool.fetch(
        "SELECT source_id, reason, attempts FROM dead_letter WHERE resolved_at IS NULL"
    )
    return {r["source_id"]: (r["reason"], r["attempts"]) for r in rows}


async def test_replay_resolves_now_valid_records_and_keeps_the_rest(
    store: Store, migrated_pool: DbPool
) -> None:
    await seed(store)
    summary = await replay_dead_letters(store, batch_size=2)  # 2 pages of body-level rejects

    assert summary.status is RunStatus.SUCCEEDED
    c = summary.counts
    assert (c.fetched, c.inserted, c.invalid) == (3, 1, 2)  # examined 3; http_404 skipped

    payload = await migrated_pool.fetchval("SELECT payload FROM items WHERE source_id = '9'")
    assert json.loads(payload)["title"] == "fixed"
    resolved = await migrated_pool.fetchrow(
        "SELECT resolved_run_id FROM dead_letter WHERE source_id = '9'"
    )
    assert resolved is not None and resolved["resolved_run_id"] == summary.run_id
    # Still-invalid rows stay open with one more attempt; the HTTP reject is untouched.
    assert await open_rows(migrated_pool) == {
        "10": ("invalid", 2),
        "11": ("id_mismatch", 2),
        "13": ("http_404", 1),
    }
    run = await migrated_pool.fetchrow(
        "SELECT kind, items_fetched, items_inserted FROM ingestion_runs WHERE id = $1",
        summary.run_id,
    )
    assert run is not None and tuple(run) == (RunKind.REPLAY.value, 3, 1)


async def test_second_replay_changes_nothing(store: Store, migrated_pool: DbPool) -> None:
    await seed(store)
    await replay_dead_letters(store)
    items_before = await migrated_pool.fetchval("SELECT count(*) FROM items")
    again = await replay_dead_letters(store)
    assert (again.counts.fetched, again.counts.inserted, again.counts.invalid) == (2, 0, 2)
    assert await migrated_pool.fetchval("SELECT count(*) FROM items") == items_before


async def test_replay_with_nothing_open_is_a_successful_no_op(store: Store) -> None:
    summary = await replay_dead_letters(store)
    assert summary.status is RunStatus.SUCCEEDED and summary.counts.fetched == 0
