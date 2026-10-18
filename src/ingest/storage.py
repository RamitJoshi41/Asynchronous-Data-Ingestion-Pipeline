"""Postgres writes: the generic `items` upsert, dead letters, `ingestion_runs`, and watermarks.

Source-agnostic on purpose (ADR-0006): it stores `(source, source_id, payload_json)` and never
imports a source model. Every connection is acquired with an explicit timeout (asyncpg's
`Pool.execute` shortcuts acquire without one). Explainer: docs/explain/ingest/storage.md
"""

import json
from collections.abc import Mapping, Sequence
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
    items_unchanged = $6, items_invalid = $7, items_missing = $8, retries = $9, error = $10,
    duration_ms = $11, requests = $12, latency_p50_ms = $13, latency_p95_ms = $14,
    latency_max_ms = $15, queue_depth_peak = $16, queue_depth_mean = $17,
    in_flight_peak = $18, in_flight_mean = $19
WHERE id = $1 AND status = 'running'
"""

# Only called while holding the source's advisory lock (runner.SourceLocks): any other 'running' row
# for the source then belongs to a process that died without finishing it (ADR-0036).
_ABANDON_STALE = """
UPDATE ingestion_runs SET status = 'failed', finished_at = now(), error = 'abandoned: ' || $2
WHERE source = $1 AND status = 'running'
RETURNING id
"""

_GET_RUN = "SELECT * FROM ingestion_runs WHERE id = $1"

# Keyset pagination, newest first: `before_id` is the last id of the previous page.
_LIST_RUNS = """
SELECT * FROM ingestion_runs
WHERE ($1::text IS NULL OR source = $1) AND ($2::bigint IS NULL OR id < $2)
ORDER BY id DESC LIMIT $3
"""


# Watermarks only move forward (ADR-0031): a slow, older run finishing late can't rewind them.
# The source comes from the run row itself, so a run can only write its own source's marks.
_ADVANCE_WATERMARKS = """
INSERT INTO source_watermarks (source, key, value, run_id)
SELECT r.source, w.key, w.value, r.id
FROM ingestion_runs AS r, unnest($2::text[], $3::text[]) AS w (key, value)
WHERE r.id = $1
ON CONFLICT (source, key) DO UPDATE
    SET value = EXCLUDED.value, run_id = EXCLUDED.run_id, updated_at = now()
    WHERE source_watermarks.value < EXCLUDED.value
"""

_WATERMARKS = "SELECT key, value FROM source_watermarks WHERE source = $1"


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
    reason: str  # invalid | id_mismatch | invalid_page | http_<status>
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


@dataclass(frozen=True)
class RunMetrics:
    """Phase 5 run metrics (ADR-0038). Gauge fields are None for the sequential runner."""

    duration_ms: int
    requests: int
    latency_p50_ms: float | None = None
    latency_p95_ms: float | None = None
    latency_max_ms: float | None = None
    queue_depth_peak: int | None = None
    queue_depth_mean: float | None = None
    in_flight_peak: int | None = None
    in_flight_mean: float | None = None


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
        self,
        run_id: int,
        status: RunStatus,
        counts: RunCounts,
        error: str | None = None,
        *,
        watermarks: Mapping[str, str] | None = None,
        metrics: RunMetrics | None = None,
    ) -> None:
        """Finalise the run; a succeeded run also advances its watermarks, atomically (ADR-0031)."""
        if status is RunStatus.RUNNING:
            raise ValueError("finish_run needs a terminal status")
        if watermarks and status is not RunStatus.SUCCEEDED:
            raise ValueError("only a succeeded run may advance watermarks")
        async with (
            self._pool.acquire(timeout=self._acquire_timeout) as conn,
            conn.transaction(),
        ):
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
                *(_metric_values(metrics)),
            )
            # "UPDATE 0": the run doesn't exist or was already finished, a logic bug upstream.
            # Raising inside the transaction also rolls back (and so skips) the watermarks.
            if result != "UPDATE 1":
                raise RuntimeError(f"run {run_id} is not an unfinished run ({result})")
            if watermarks:
                await conn.execute(
                    _ADVANCE_WATERMARKS, run_id, list(watermarks), list(watermarks.values())
                )

    async def abandon_stale_runs(self, source: str, reason: str) -> list[int]:
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            rows = await conn.fetch(_ABANDON_STALE, source, reason)
        return [r["id"] for r in rows]

    async def get_run(self, run_id: int) -> dict[str, object] | None:
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            row = await conn.fetchrow(_GET_RUN, run_id)
        return None if row is None else dict(row)

    async def list_runs(
        self, *, source: str | None, before_id: int | None, limit: int
    ) -> list[dict[str, object]]:
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            rows = await conn.fetch(_LIST_RUNS, source, before_id, limit)
        return [dict(r) for r in rows]

    async def watermarks(self, source: str) -> dict[str, str]:
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            rows = await conn.fetch(_WATERMARKS, source)
        return {r["key"]: r["value"] for r in rows}

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


def _metric_values(metrics: RunMetrics | None) -> tuple[object, ...]:
    if metrics is None:
        return (None,) * 9
    return (
        metrics.duration_ms,
        metrics.requests,
        metrics.latency_p50_ms,
        metrics.latency_p95_ms,
        metrics.latency_max_ms,
        metrics.queue_depth_peak,
        metrics.queue_depth_mean,
        metrics.in_flight_peak,
        metrics.in_flight_mean,
    )
