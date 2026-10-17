"""Per-record classification shared by the sequential and concurrent runners.

One place decides what a fetched body *is* (valid / invalid / missing), so both runners count and
log identically. Explainer: docs/explain/ingest/records.md
"""

from pydantic import ValidationError
from structlog.typing import FilteringBoundLogger

from ingest.models.hn import HNItem, parse_hn_item
from ingest.storage import RunCounts


def classify_hn_body(
    body: bytes, item_id: int, counts: RunCounts, run_log: FilteringBoundLogger
) -> HNItem | None:
    """Return the validated item, or None after counting and logging why it was skipped.

    Never raises for bad data: invalid, missing and mismatched records are per-record outcomes.
    """
    try:
        item = parse_hn_item(body)
    except ValidationError as exc:
        counts.invalid += 1
        # Field locations and error types only: never the raw payload at INFO/WARNING.
        problems = [f"{'.'.join(map(str, e['loc']))}:{e['type']}" for e in exc.errors()]
        run_log.warning("item_invalid", item_id=item_id, problems=problems)
        return None
    if item is None:
        counts.missing += 1
        run_log.info("item_missing", item_id=item_id)
        return None
    if item.id != item_id:
        # A body for a different record than requested is not trustworthy data for this key.
        counts.invalid += 1
        run_log.warning("item_id_mismatch", item_id=item_id, body_id=item.id)
        return None
    return item
