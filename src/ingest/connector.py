"""The connector contract: what a source must provide so the core can ingest it (ADR-0028).

The unit of work is job -> pages -> records:
    plan()   -> jobs       one per stream: an HN item, a GitHub repo, an Open-Meteo city
    pages()  -> pages      HTTP responses for one job, in order (GitHub follows `Link`)
    parse()  -> records    split + strictly validate one page; bad records become DeadLetters

Connectors never retry, count, buffer or write: the core does that for every source alike. This
module is part of the core; connector implementations (ingest/connectors/) import it, never the
other way round (tests/test_architecture.py).
Explainer: docs/explain/ingest/connector.md
"""

from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import NamedTuple, Protocol

import httpx
from pydantic import ValidationError
from structlog.typing import FilteringBoundLogger

from ingest.storage import DeadLetter


@dataclass(frozen=True)
class Job:
    """One unit of work, owned by one fetcher from start to finish."""

    key: str  # names the stream; also its watermark key and its HTTP-reject dead-letter key
    url: str
    params: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Page:
    job: Job
    number: int  # 1-based position within the job
    body: bytes


class Record(NamedTuple):
    """A validated record, ready for storage."""

    source_id: str
    payload_json: str  # the VALIDATED model serialised, never the raw HTTP body
    # Watermark candidate. Must sort as a string in time order (e.g. fixed-width UTC ISO 8601);
    # None for sources without incremental fetching.
    cursor: str | None = None


class Missing(NamedTuple):
    """The source says this record doesn't exist (HN's `null`): counted, nothing stored."""

    source_id: str


type Parsed = Record | DeadLetter | Missing


class InvalidPage(Exception):
    """The page as a whole is unreadable (not the expected envelope): no record can be trusted."""

    def __init__(self, errors: list[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = errors


type HttpGet = Callable[[str, Mapping[str, str] | None], Awaitable[httpx.Response]]


@dataclass(frozen=True)
class FetchContext:
    """What the core hands a connector for I/O.

    `get(url, params)` already retries (with this connector's rate-limit rules) and raises
    httpx.HTTPStatusError for any non-2xx status that is left after retrying.
    """

    get: HttpGet
    log: FilteringBoundLogger


class Connector(Protocol):
    @property
    def source(self) -> str:
        """Stable name stored in every row (`items.source`, `ingestion_runs.source`, ...)."""
        ...

    @property
    def headers(self) -> Mapping[str, str]:
        """Default headers for THIS source's HTTP client only (e.g. auth)."""
        ...

    async def plan(self, ctx: FetchContext, watermarks: Mapping[str, str]) -> list[Job]:
        """The jobs for one run. `watermarks` maps job key -> last committed cursor."""
        ...

    def pages(self, ctx: FetchContext, job: Job) -> AsyncGenerator[Page]:
        """Fetch one job's pages in order (an async generator)."""
        ...

    def parse(self, page: Page) -> list[Parsed]:
        """Validate every record on the page. Raise InvalidPage if the page itself is unusable."""
        ...

    def revalidate(self, source_id: str, raw: bytes) -> Parsed:
        """Re-check one dead letter's stored body against the current model (replay; no network)."""
        ...

    def rate_limit_delay(self, response: httpx.Response) -> float | None:
        """Seconds to pause ALL this source's fetchers if `response` is a rate limit, else None."""
        ...


def validation_problems(exc: ValidationError) -> list[str]:
    """`loc:type` strings for a dead letter: where and what kind, never the offending values."""
    return [f"{'.'.join(map(str, e['loc']))}:{e['type']}" for e in exc.errors()]
