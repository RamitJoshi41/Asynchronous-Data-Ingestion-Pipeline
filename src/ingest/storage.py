"""Postgres writes: the generic `items` upsert, dead letters, and `ingestion_runs` bookkeeping.

Source-agnostic on purpose (ADR-0006): it stores `(source, source_id, payload_json)` and never
imports a source model. Every connection is acquired with an explicit timeout (asyncpg's
`Pool.execute` shortcuts acquire without one). Explainer: docs/explain/ingest/storage.md
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from ingest.db import DbPool

# Error text stored per run is capped: it's for humans, and must never become a payload dump.
MAX_ERROR_CHARS = 1000

# Skip-unchanged batch upsert (ADR-0007, ADR-0017): one round trip per batch. unnest() turns the
# two parallel arrays into rows. The WHERE turns "same payload" into a no-op: no new row version,
# no WAL, and no RETURNING row, so a key absent from the result was unchanged. `xmax = 0` is true
# only for a freshly inserted row version (insert vs update). Keys must be unique per statement.
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

# A record that now ingests cleanly no longer needs its open dead letter (ADR-0024).
_RESOLVE_DEAD_LETTERS = """
UPDATE dead_letter SET resolved_at = now(), resolved_run_id = $3
WHERE source = $1 AND source_id = ANY($2::text[]) AND resolved_at IS NULL
"""

# One OPEN dead letter per record (partial unique index): a repeat failure updates it in place.
_UPSERT_DEAD_LETTERS = """
INSERT INTO dead_letter (source, source_id, run_id, reason, errors, raw_body)
SELECT $1, d.source_id, $2, d.reason, d.errors, d.raw_body
FROM unnest($3::text[], $4::text[], $5::jsonb[], $6::bytea[])
    AS d (source_id, reason, errors, raw_body)
ON CONFLICT (source, source_id) WHERE resolved_at IS NULL DO UPDATE
    SET run_id = EXCLUDED.run_id,
        reason = EXCLUDED.reason,
        errors = EXCLUDED.errors,
        raw_body = EXCLUDED.raw_body,
        attempts = dead_letter.attempts + 1,
        last_failed_at = now()
"""

# Keyset pagination: each replay pass visits every open row exactly once, even as rows change.
# Only body-level rejects are replayable: an http_<status> row holds an error page, not a record
# (those resolve when a later run fetches the record successfully).
_OPEN_DEAD_LETTERS = """
SELECT id, source_id, raw_body FROM dead_letter
WHERE source = $1 AND resolved_at IS NULL AND reason IN ('invalid', 'id_mismatch') AND id > $2
ORDER BY id LIMIT $3
"""

_START_RUN = """
INSERT INTO ingestion_runs (source, status, kind) VALUES ($1, 'running', $2) RETURNING id
"""

_FINISH_RUN = """
UPDATE ingestion_runs
SET status = $2, finished_at = now(),
    items_fetched = $3, items_inserted = $4, items_updated = $5,
    items_unchanged = $6, items_invalid = $7, items_missing = $8, retries = $9, error = $10
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
    CANCELLED = "cancelled"  # graceful shutdown (Phase 3)


class RunKind(StrEnum):
    INGEST = "ingest"
    REPLAY = "replay"


@dataclass(frozen=True)
class DeadLetter:
    """A record that could not be ingested, with everything needed to inspect or replay it."""

    source_id: str
    reason: str  # invalid | id_mismatch | http_<status>
    errors: list[str] = field(default_factory=list)  # "loc:type" strings, never raw values
    raw_body: bytes = b""


@dataclass(frozen=True)
class OpenDeadLetter:
    id: int
    source_id: str
    raw_body: bytes


@dataclass(frozen=True)
class FlushResult:
    outcomes: dict[str, UpsertOutcome]
    dead_lettered: int
    resolved: int  # open dead letters closed because their record now ingested cleanly


@dataclass
class RunCounts:
    """What happened to every item a run fetched. Mutable: the runner increments it."""

    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    invalid: int = 0  # rejected records; each one is dead-lettered (Phase 3)
    missing: int = 0
    retries: int = 0

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

    async def start_run(self, source: str, kind: RunKind = RunKind.INGEST) -> int:
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            run_id = await conn.fetchval(_START_RUN, source, kind.value)
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
                counts.retries,
                None if error is None else error[:MAX_ERROR_CHARS],
            )
        # "UPDATE 0" means the run doesn't exist or was already finished: a logic bug upstream.
        if result != "UPDATE 1":
            raise RuntimeError(f"run {run_id} is not an unfinished run ({result})")

    async def upsert_items(
        self, run_id: int, source: str, records: Sequence[tuple[str, str]]
    ) -> dict[str, UpsertOutcome]:
        """Upsert `(source_id, payload_json)` pairs in ONE statement; return the outcome per key."""
        result = await self.flush(run_id, source, records, [])
        return result.outcomes

    async def flush(
        self,
        run_id: int,
        source: str,
        records: Sequence[tuple[str, str]],
        dead_letters: Sequence[DeadLetter],
    ) -> FlushResult:
        """Write one batch atomically: upsert items, resolve their open dead letters, dead-letter
        the rejects. One transaction, so a batch is never half-written (ADR-0017, ADR-0024).

        Duplicate keys in one statement would make Postgres reject it (CardinalityViolationError),
        so both lists are collapsed first: the last entry per key wins.
        """
        latest = dict(records)  # keeps first-seen key order, last-seen value
        rejects = {d.source_id: d for d in dead_letters}
        if not latest and not rejects:
            return FlushResult({}, 0, 0)
        written: dict[str, UpsertOutcome] = {}
        resolved = 0
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            async with conn.transaction():
                if latest:
                    keys = list(latest)
                    rows = await conn.fetch(
                        _UPSERT_BATCH, source, keys, list(latest.values()), run_id
                    )
                    written = {
                        r["source_id"]: (
                            UpsertOutcome.INSERTED if r["inserted"] else UpsertOutcome.UPDATED
                        )
                        for r in rows
                    }
                    status = await conn.execute(_RESOLVE_DEAD_LETTERS, source, keys, run_id)
                    resolved = int(status.split()[-1])  # "UPDATE <n>"
                if rejects:
                    await conn.execute(
                        _UPSERT_DEAD_LETTERS,
                        source,
                        run_id,
                        list(rejects),
                        [d.reason for d in rejects.values()],
                        [json.dumps(d.errors) for d in rejects.values()],
                        [d.raw_body for d in rejects.values()],
                    )
        outcomes = {key: written.get(key, UpsertOutcome.UNCHANGED) for key in latest}
        return FlushResult(outcomes, len(rejects), resolved)

    async def open_dead_letters(
        self, source: str, *, after_id: int, limit: int
    ) -> list[OpenDeadLetter]:
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            rows = await conn.fetch(_OPEN_DEAD_LETTERS, source, after_id, limit)
        return [OpenDeadLetter(r["id"], r["source_id"], bytes(r["raw_body"])) for r in rows]
