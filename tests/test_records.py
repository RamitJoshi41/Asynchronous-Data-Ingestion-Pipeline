import structlog
from structlog.testing import capture_logs

from ingest.records import classify_hn_body
from ingest.storage import RunCounts

LOG = structlog.get_logger().bind(run_id=1, source="hacker_news")


def test_valid_body_returns_item_without_counting() -> None:
    counts = RunCounts()
    item = classify_hn_body(b'{"id": 5, "type": "story"}', 5, counts, LOG)
    assert item is not None and item.id == 5
    assert counts == RunCounts()  # the caller counts the upsert outcome, not the classifier


def test_invalid_body_is_counted_and_logged_without_the_value() -> None:
    counts = RunCounts()
    with capture_logs() as logs:
        item = classify_hn_body(b'{"id": 5, "score": "secret-ish"}', 5, counts, LOG)
    assert item is None and counts.invalid == 1
    assert logs[0]["event"] == "item_invalid" and logs[0]["problems"] == ["score:int_type"]
    assert "secret-ish" not in repr(logs)


def test_null_body_is_counted_missing() -> None:
    counts = RunCounts()
    assert classify_hn_body(b"null", 5, counts, LOG) is None
    assert counts.missing == 1


def test_body_for_another_id_is_counted_invalid() -> None:
    counts = RunCounts()
    with capture_logs() as logs:
        assert classify_hn_body(b'{"id": 6}', 5, counts, LOG) is None
    assert counts.invalid == 1
    assert logs[0]["event"] == "item_id_mismatch" and logs[0]["body_id"] == 6


def test_malformed_json_never_raises() -> None:
    counts = RunCounts()
    assert classify_hn_body(b"{not json", 5, counts, LOG) is None
    assert counts.invalid == 1
