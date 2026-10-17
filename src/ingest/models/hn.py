"""Hacker News record schemas, validated in STRICT mode (ADR-0008).

Strict means wrong JSON types are rejected instead of coerced: "12" is not an int, 1 is not a
bool. The one deliberate conversion is `time` (Unix seconds -> aware UTC datetime), done
explicitly in a validator. Field list from https://github.com/HackerNews/API ("Items").
Explainer: docs/explain/ingest/models/hn.md
"""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_serializer, field_validator

SOURCE = "hacker_news"

ItemType = Literal["job", "story", "comment", "poll", "pollopt"]


class HNItem(BaseModel):
    # extra="ignore": the HN README tells clients to ignore fields they don't expect.
    # frozen=True: a validated record is a value; nothing should mutate it on the way to the DB.
    model_config = ConfigDict(strict=True, extra="ignore", frozen=True)

    id: int = Field(gt=0)  # the only field the API documents as required
    type: ItemType | None = None
    by: str | None = None
    time: datetime | None = None
    title: str | None = None
    url: str | None = None
    text: str | None = None
    score: int | None = None
    descendants: int | None = Field(default=None, ge=0)
    kids: tuple[int, ...] = ()
    parts: tuple[int, ...] = ()
    parent: int | None = None
    poll: int | None = None
    deleted: bool = False
    dead: bool = False

    @field_validator("time", mode="before")
    @classmethod
    def _unix_seconds_to_utc(cls, value: object) -> object:
        # Strict mode refuses int -> datetime, so convert here, and ONLY from a real int.
        if value is None or isinstance(value, datetime):
            return value
        # bool is a subclass of int; True must not become 1970-01-01T00:00:01.
        if isinstance(value, int) and not isinstance(value, bool):
            try:
                return datetime.fromtimestamp(value, tz=UTC)
            except (OverflowError, OSError, ValueError) as exc:
                # Only ValueError becomes a ValidationError; the others would escape as crashes.
                raise ValueError(f"time {value} is outside the representable range") from exc
        raise ValueError("time must be Unix seconds as an integer")

    @field_serializer("time")
    def _utc_to_unix_seconds(self, value: datetime | None) -> int | None:
        # Serialise back to HN's wire format so a stored payload re-validates (round-trip safe).
        return None if value is None else int(value.timestamp())


_ITEM_OR_NULL: TypeAdapter[HNItem | None] = TypeAdapter(HNItem | None)
_TOP_IDS: TypeAdapter[list[int]] = TypeAdapter(
    list[Annotated[int, Field(gt=0)]], config=ConfigDict(strict=True)
)


def parse_hn_item(body: bytes) -> HNItem | None:
    """Validate one `/item/<id>.json` body. JSON `null` (no such item) -> None.

    Raises pydantic.ValidationError for malformed JSON or schema violations.
    """
    return _ITEM_OR_NULL.validate_json(body)


def parse_top_ids(body: bytes) -> list[int]:
    """Validate a `/topstories.json` body: a JSON array of positive ints."""
    return _TOP_IDS.validate_json(body)
