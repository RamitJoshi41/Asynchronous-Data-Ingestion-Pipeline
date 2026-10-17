"""GitHub issue / pull-request schema, validated in STRICT mode (ADR-0008, ADR-0033).

From `GET /repos/{owner}/{repo}/issues` (https://docs.github.com/en/rest/issues/issues). The API
returns both issues and PRs; a PR is recognised by its `pull_request` key. Only the fields we keep
are declared; the rest (dozens of URLs, reactions, ...) is ignored, so upstream additions don't
break us. In strict JSON mode Pydantic still parses ISO 8601 strings into datetimes (verified with
pydantic 2.13.5) but refuses "12" for an int.
Explainer: docs/explain/ingest/models/github.md
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SOURCE = "github"

_STRICT = ConfigDict(strict=True, extra="ignore", frozen=True)


class GitHubUser(BaseModel):
    model_config = _STRICT
    login: str


class GitHubLabel(BaseModel):
    model_config = _STRICT
    name: str


class PullRequestRef(BaseModel):
    model_config = _STRICT
    merged_at: datetime | None = None


class GitHubIssue(BaseModel):
    model_config = _STRICT

    id: int = Field(gt=0)
    number: int = Field(gt=0)
    title: str
    state: Literal["open", "closed"]
    state_reason: str | None = None
    user: GitHubUser | None = None  # null for some deleted accounts
    labels: tuple[GitHubLabel, ...] = ()
    comments: int = Field(ge=0)
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None = None
    html_url: str
    pull_request: PullRequestRef | None = None  # present <=> this "issue" is a pull request

    @field_validator("created_at", "updated_at", "closed_at")
    @classmethod
    def _must_be_aware(cls, value: datetime | None) -> datetime | None:
        # updated_at becomes the watermark: a naive timestamp would compare wrongly.
        if value is not None and value.utcoffset() is None:
            raise ValueError("timestamp must include a UTC offset")
        return value
