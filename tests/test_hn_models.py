import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ingest.models.hn import HNItem, parse_hn_item, parse_top_ids

# Shapes follow the examples in the HN API README (https://github.com/HackerNews/API).
STORY = {
    "by": "dhouston",
    "descendants": 71,
    "id": 8863,
    "kids": [8952, 9224],
    "score": 111,
    "time": 1175714200,
    "title": "My YC app: Dropbox - Throw away your USB drive",
    "type": "story",
    "url": "http://www.getdropbox.com/u/2/screencast.html",
}
JOB = {
    "by": "justin",
    "id": 192327,
    "score": 6,
    "text": "Justin.tv is the biggest live video site online.",
    "time": 1210981217,
    "title": "Justin.tv is looking for a Lead Flash Engineer!",
    "type": "job",
}


def body(obj: object) -> bytes:
    return json.dumps(obj).encode()


def errors_of(raw: object) -> list[str]:
    with pytest.raises(ValidationError) as exc_info:
        parse_hn_item(body(raw))
    return [e["type"] for e in exc_info.value.errors()]


def test_story_is_parsed_with_time_as_aware_utc_datetime() -> None:
    item = parse_hn_item(body(STORY))
    assert isinstance(item, HNItem)
    assert item.id == 8863 and item.type == "story" and item.kids == (8952, 9224)
    assert item.time == datetime(2007, 4, 4, 19, 16, 40, tzinfo=UTC)


def test_job_is_parsed() -> None:
    item = parse_hn_item(body(JOB))
    assert item is not None and item.type == "job" and item.url is None


def test_deleted_item_with_only_id_is_valid() -> None:
    item = parse_hn_item(body({"id": 1, "deleted": True}))
    assert item is not None and item.deleted is True and item.title is None


def test_null_body_means_no_such_item() -> None:
    assert parse_hn_item(b"null") is None


def test_unknown_fields_are_ignored() -> None:
    item = parse_hn_item(body({**STORY, "brand_new_field": {"x": 1}}))
    assert item is not None
    assert "brand_new_field" not in item.model_dump()


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        ("score", "12", "int_type"),  # lax mode would coerce this; strict rejects it
        ("score", 12.5, "int_type"),
        ("id", "8863", "int_type"),
        ("deleted", 1, "bool_type"),
        ("kids", [1, "2"], "int_type"),
        ("type", "article", "literal_error"),
        ("descendants", -1, "greater_than_equal"),
        ("time", "2007-04-04T19:16:40Z", "value_error"),  # only Unix-seconds ints accepted
        ("time", True, "value_error"),  # bool is an int subclass; must not become 1970
        ("time", 10**20, "value_error"),  # out of datetime range: ValidationError, not a crash
    ],
)
def test_wrong_types_are_rejected_not_coerced(field: str, value: object, error_type: str) -> None:
    assert error_type in errors_of({**STORY, field: value})


def test_missing_or_non_positive_id_is_rejected() -> None:
    assert "missing" in errors_of({"type": "story"})
    assert "greater_than" in errors_of({"id": 0})


def test_malformed_json_is_a_validation_error() -> None:
    with pytest.raises(ValidationError):
        parse_hn_item(b'{"id": 1,')


def test_model_is_frozen() -> None:
    item = parse_hn_item(body(STORY))
    assert item is not None
    with pytest.raises(ValidationError):
        item.score = 1  # type: ignore[misc]


def test_payload_json_round_trip_is_stable() -> None:
    item = parse_hn_item(body(STORY))
    assert item is not None
    dumped = item.model_dump_json()
    assert json.loads(dumped)["time"] == STORY["time"]  # stored in HN's own format
    assert HNItem.model_validate_json(dumped) == item  # what we store can be read back


def test_top_ids_are_parsed() -> None:
    assert parse_top_ids(b"[3, 2, 1]") == [3, 2, 1]


@pytest.mark.parametrize("raw", [b'[1, "2"]', b"[0]", b"null", b'{"ids": [1]}'])
def test_bad_top_ids_are_rejected(raw: bytes) -> None:
    with pytest.raises(ValidationError):
        parse_top_ids(raw)
