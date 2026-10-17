"""Hacker News connector (https://github.com/HackerNews/API): top stories, one item per request.

plan() fetches /topstories.json (with the core's retries) and makes one job per item id; each job
is a single page. HN publishes no rate-limit headers, so the standard 429 + Retry-After rule
applies. No watermark: top stories is a ranking, not a time-ordered feed.
Explainer: docs/explain/ingest/connectors/hn.md
"""

from collections.abc import AsyncGenerator, Mapping

import httpx
from pydantic import ValidationError

from ingest.connector import FetchContext, Job, Missing, Page, Parsed, Record, validation_problems
from ingest.models.hn import SOURCE, parse_hn_item, parse_top_ids
from ingest.retry import default_rate_limit
from ingest.storage import DeadLetter

HN_MAX_LIMIT = 500  # /topstories returns at most 500 ids


class HackerNewsConnector:
    def __init__(self, base_url: str, *, limit: int) -> None:
        if not 1 <= limit <= HN_MAX_LIMIT:
            raise ValueError(f"limit must be between 1 and {HN_MAX_LIMIT}")
        self._base_url = base_url.rstrip("/")
        self._limit = limit

    @property
    def source(self) -> str:
        return SOURCE

    @property
    def headers(self) -> Mapping[str, str]:
        return {}  # public API, no auth

    async def plan(self, ctx: FetchContext, watermarks: Mapping[str, str]) -> list[Job]:
        response = await ctx.get(f"{self._base_url}/topstories.json", None)
        ids = parse_top_ids(response.content)[: self._limit]
        return [Job(str(item_id), f"{self._base_url}/item/{item_id}.json") for item_id in ids]

    async def pages(self, ctx: FetchContext, job: Job) -> AsyncGenerator[Page]:
        response = await ctx.get(job.url, job.params)
        yield Page(job, 1, response.content)

    def parse(self, page: Page) -> list[Parsed]:
        return [self.revalidate(page.job.key, page.body)]

    def revalidate(self, source_id: str, raw: bytes) -> Parsed:
        """Strict validation of one /item/<id>.json body requested as `source_id`."""
        try:
            item = parse_hn_item(raw)
        except ValidationError as exc:
            return DeadLetter(source_id, "invalid", validation_problems(exc), raw)
        if item is None:  # JSON null: HN has no such item
            return Missing(source_id)
        if str(item.id) != source_id:
            # A body for a different record than requested is not trustworthy data for this key.
            return DeadLetter(source_id, "id_mismatch", [f"id:{item.id}"], raw)
        return Record(source_id, item.model_dump_json())

    def rate_limit_delay(self, response: httpx.Response) -> float | None:
        return default_rate_limit(response)
