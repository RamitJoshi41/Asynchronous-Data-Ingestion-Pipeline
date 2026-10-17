"""Dead-letter replay: re-validate stored raw bodies against the CURRENT model (ADR-0025).

The "fix the model, then replay" flow. No network: the body that was rejected is exactly the body
that is re-checked. Now-valid records are upserted and their dead letters resolved in the same
transaction (Store.flush); still-invalid ones stay open with attempts + 1. Idempotent: a second
replay with no model change finds nothing to resolve. Per source: the connector's `revalidate`
knows how to read its own stored bodies (Phase 4).
Explainer: docs/explain/ingest/replay.md
"""

import time

import structlog

from ingest.connector import Connector, Record
from ingest.records import account, finish_safely
from ingest.sequential import RunSummary
from ingest.storage import DeadLetter, RunCounts, RunKind, RunStatus, Store

log = structlog.get_logger()


async def replay_dead_letters(
    store: Store, connector: Connector, *, batch_size: int = 100
) -> RunSummary:
    source = connector.source
    run_id = await store.start_run(source, RunKind.REPLAY)
    run_log = log.bind(run_id=run_id, source=source, kind="replay")
    counts = RunCounts()
    started = time.perf_counter()
    resolved = 0
    after_id = 0
    run_log.info("replay_started", batch_size=batch_size)
    try:
        while True:
            rows = await store.open_dead_letters(source, after_id=after_id, limit=batch_size)
            if not rows:
                break
            after_id = rows[-1].id
            items: list[tuple[str, str]] = []
            rejects: list[DeadLetter] = []
            for row in rows:
                counts.fetched += 1  # "fetched" = dead letters examined in a replay run
                result = account(connector.revalidate(row.source_id, row.raw_body), counts, run_log)
                if isinstance(result, DeadLetter):
                    rejects.append(result)
                elif isinstance(result, Record):
                    items.append((result.source_id, result.payload_json))
            flushed = await store.flush(run_id, source, items, rejects)
            for outcome in flushed.outcomes.values():
                counts.record(outcome)
            resolved += flushed.resolved
    except Exception as exc:
        await finish_safely(store, run_id, RunStatus.FAILED, counts, repr(exc))
        run_log.error("replay_failed", error=repr(exc))
        raise
    await finish_safely(store, run_id, RunStatus.SUCCEEDED, counts)
    run_log.info(
        "replay_finished",
        examined=counts.fetched,
        resolved=resolved,
        still_invalid=counts.invalid,
        missing=counts.missing,
    )
    return RunSummary(run_id, RunStatus.SUCCEEDED, counts, time.perf_counter() - started)
