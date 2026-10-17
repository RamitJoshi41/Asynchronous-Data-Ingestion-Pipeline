"""Postgres writes: the generic `items` upsert and `ingestion_runs` bookkeeping.

Source-agnostic on purpose (ADR-0006): it stores `(source, source_id, payload_json)` and never
imports a source model. Every connection is acquired with an explicit timeout (asyncpg's
`Pool.execute` shortcuts acquire without one). Explainer: docs/explain/ingest/storage.md
"""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from ingest.db import DbPool

# Error text stored per run is capped: it's for humans, and must never become a payload dump.
MAX_ERROR_CHARS = 1000

# Skip-unchanged upsert (ADR-0007). The WHERE turns "same payload" into a no-op: no new row
# version, no WAL, and RETURNING yields no row. `xmax = 0` is true only for a freshly inserted
# row version, which distinguishes insert from update.
_UPSERT = """
INSERT INTO items (source, source_id, payload, last_changed_run_id)
VALUES ($1, $2, $3::jsonb, $4)
ON CONFLICT (source, source_id) DO UPDATE
    SET payload = EXCLUDED.payload,
        updated_at = now(),
        last_changed_run_id = EXCLUDED.last_changed_run_id
    WHERE items.payload IS DISTINCT FROM EXCLUDED.payload
RETURNING (xmax = 0) AS inserted
"""

# Batch form of _UPSERT (ADR-0017): one round trip per batch. unnest() turns the two parallel
# arrays into rows. RETURNING yields only inserted/updated rows, so a key that is absent from the
# result was unchanged. Keys must be unique within one batch (see upsert_items).
_UPSERT_BATCH = """
INSERT INTO items (source, source_id, payload, last_changed_run_id)
SELECT $1, batch.source_id, batch.payload, $4
FROM unnest($2::text[], $3::jsonb[]) AS batch (source_id, payload)
ON CONFLICT (source, source_id) DO UPDATE
    SET payload = EXCLUDED.payload,
        updated_at = now(),
        last_changed_run_id = EXCLUDED.last_changed_run_id
    WHERE items.payload IS DISTINCT FROM EXCLUDED.payload
RETURNING source_id, (xmax = 0) AS inserted
"""

_START_RUN = "INSERT INTO ingestion_runs (source, status) VALUES ($1, 'running') RETURNING id"

_FINISH_RUN = """
UPDATE ingestion_runs
SET status = $2, finished_at = now(),
    items_fetched = $3, items_inserted = $4, items_updated = $5,
    items_unchanged = $6, items_invalid = $7, items_missing = $8, error = $9
WHERE id = $1 AND status = 'running'
"""


class UpsertOutcome(StrEnum):
    INSERTED = "inserted"
    UPDATED = "updated"
    UNCHANGED = "unchanged"


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class RunCounts:
    """What happened to every item a run fetched. Mutable: the runner increments it."""

    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    invalid: int = 0
    missing: int = 0

    def record(self, outcome: UpsertOutcome) -> None:
        match outcome:
            case UpsertOutcome.INSERTED:
                self.inserted += 1
            case UpsertOutcome.UPDATED:
                self.updated += 1
            case UpsertOutcome.UNCHANGED:
                self.unchanged += 1


class Store:
    def __init__(self, pool: DbPool, *, acquire_timeout: float) -> None:
        self._pool = pool
        self._acquire_timeout = acquire_timeout

    @property
    def pool_max_size(self) -> int:
        return self._pool.get_max_size()

    async def start_run(self, source: str) -> int:
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            run_id = await conn.fetchval(_START_RUN, source)
        assert isinstance(run_id, int)
        return run_id

    async def finish_run(
        self, run_id: int, status: RunStatus, counts: RunCounts, error: str | None = None
    ) -> None:
        if status is RunStatus.RUNNING:
            raise ValueError("finish_run needs a terminal status")
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            result = await conn.execute(
                _FINISH_RUN,
                run_id,
                status.value,
                counts.fetched,
                counts.inserted,
                counts.updated,
                counts.unchanged,
                counts.invalid,
                counts.missing,
                None if error is None else error[:MAX_ERROR_CHARS],
            )
        # "UPDATE 0" means the run doesn't exist or was already finished: a logic bug upstream.
        if result != "UPDATE 1":
            raise RuntimeError(f"run {run_id} is not an unfinished run ({result})")

    async def upsert_item(
        self, run_id: int, source: str, source_id: str, payload_json: str
    ) -> UpsertOutcome:
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            row = await conn.fetchrow(_UPSERT, source, source_id, payload_json, run_id)
        if row is None:
            return UpsertOutcome.UNCHANGED
        return UpsertOutcome.INSERTED if row["inserted"] else UpsertOutcome.UPDATED

    async def upsert_items(
        self, run_id: int, source: str, records: Sequence[tuple[str, str]]
    ) -> dict[str, UpsertOutcome]:
        """Upsert `(source_id, payload_json)` pairs in ONE statement; return the outcome per key.

        Duplicate keys in one batch would make Postgres reject the whole statement
        (CardinalityViolationError), so they are collapsed first: the last payload wins.
        """
        latest = dict(records)  # keeps first-seen key order, last-seen value
        if not latest:
            return {}
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            rows = await conn.fetch(
                _UPSERT_BATCH, source, list(latest), list(latest.values()), run_id
            )
        written = {
            r["source_id"]: UpsertOutcome.INSERTED if r["inserted"] else UpsertOutcome.UPDATED
            for r in rows
        }
        return {key: written.get(key, UpsertOutcome.UNCHANGED) for key in latest}
