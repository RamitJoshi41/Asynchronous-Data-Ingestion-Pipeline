"""GitHub connector: issues + pull requests of configured repos, incrementally (ADR-0033).

One job per repo: GET /repos/{owner}/{repo}/issues?state=all&sort=updated&direction=asc
&per_page=100[&since=<watermark>], then follow `Link: rel="next"` page by page. Each element of a
page is validated on its own, so one bad issue dead-letters alone. Rate limits are GitHub's dialect
(403/429 + x-ratelimit-* / retry-after, ADR-0032). Docs read 2026-10-17:
https://docs.github.com/en/rest/issues/issues#list-repository-issues
https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api
https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
Explainer: docs/explain/ingest/connectors/github.md
"""

import json
import time
from collections.abc import AsyncGenerator, Callable, Mapping
from datetime import UTC
from typing import Self

import httpx
from pydantic import SecretStr, ValidationError

from ingest.config import Settings
from ingest.connector import (
    FetchContext,
    InvalidPage,
    Job,
    Page,
    Parsed,
    Record,
    validation_problems,
)
from ingest.models.github import SOURCE, GitHubIssue
from ingest.retry import retry_after_seconds
from ingest.storage import DeadLetter

PER_PAGE = "100"  # the documented maximum; larger values are silently reduced
# The docs: with no retry-after and no exhausted quota, "wait for at least one minute".
SECONDARY_LIMIT_WAIT_S = 60.0


class GitHubConnector:
    def __init__(
        self,
        base_url: str,
        repos: list[str],
        *,
        token: SecretStr | None,
        api_version: str,
        max_pages: int,
        clock: Callable[[], float] = time.time,  # epoch seconds, like x-ratelimit-reset
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._base = httpx.URL(self._base_url)
        self._repos, self._token = repos, token
        self._api_version, self._max_pages, self._clock = api_version, max_pages, clock

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        return cls(
            settings.github_base_url,
            settings.github_repos,
            token=settings.github_token,
            api_version=settings.github_api_version,
            max_pages=settings.github_max_pages,
        )

    @property
    def source(self) -> str:
        return SOURCE

    @property
    def headers(self) -> Mapping[str, str]:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": self._api_version,
        }
        if self._token is not None:
            # Only ever set on GitHub's own client (ADR-0029).
            headers["Authorization"] = f"Bearer {self._token.get_secret_value()}"
        return headers

    async def plan(self, ctx: FetchContext, watermarks: Mapping[str, str]) -> list[Job]:
        jobs = []
        for repo in self._repos:
            params = {"state": "all", "sort": "updated", "direction": "asc", "per_page": PER_PAGE}
            if (since := watermarks.get(repo)) is not None:
                params["since"] = since  # incremental: only issues updated since the last success
            jobs.append(Job(repo, f"{self._base_url}/repos/{repo}/issues", params))
        if self._token is None:
            ctx.log.warning("github_unauthenticated", hourly_limit=60)
        return jobs

    async def pages(self, ctx: FetchContext, job: Job) -> AsyncGenerator[Page]:
        url: str = job.url
        params: Mapping[str, str] | None = job.params
        for number in range(1, self._max_pages + 1):
            response = await ctx.get(url, params)
            ctx.log.info(
                "github_page_fetched",
                repo=job.key,
                page=number,
                ratelimit_remaining=response.headers.get("x-ratelimit-remaining"),
            )
            yield Page(job, number, response.content)
            next_url = response.links.get("next", {}).get("url")
            if next_url is None:
                return
            # Follow GitHub's URL as given (it carries an opaque cursor), but never to another
            # host: this client sends the token with every request.
            self._check_same_origin(next_url)
            url, params = next_url, None
        ctx.log.info("github_page_cap_reached", repo=job.key, max_pages=self._max_pages)

    def _check_same_origin(self, url: str) -> None:
        target = httpx.URL(url)
        if (target.scheme, target.host, target.port) != (
            self._base.scheme,
            self._base.host,
            self._base.port,
        ):
            raise ValueError(f"refusing to follow a Link to another origin: {target.host}")

    def parse(self, page: Page) -> list[Parsed]:
        try:
            elements = json.loads(page.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidPage(["page:json_invalid"]) from exc
        if not isinstance(elements, list):
            raise InvalidPage([f"page:expected_list_got_{type(elements).__name__}"])
        repo = page.job.key
        parsed: list[Parsed] = []
        for index, element in enumerate(elements):
            raw = json.dumps(element, separators=(",", ":")).encode()
            number = element.get("number") if isinstance(element, dict) else None
            if isinstance(number, int) and not isinstance(number, bool):
                parsed.append(self.revalidate(f"{repo}#{number}", raw))
            else:
                # No usable number: we can't even name the record, so key it by its position.
                key = f"{repo}@page{page.number}[{index}]"
                parsed.append(DeadLetter(key, "invalid", ["number:missing"], raw))
        return parsed

    def revalidate(self, source_id: str, raw: bytes) -> Parsed:
        """Validate one issue element stored under `owner/repo#number`."""
        try:
            issue = GitHubIssue.model_validate_json(raw)
        except ValidationError as exc:
            return DeadLetter(source_id, "invalid", validation_problems(exc), raw)
        if not source_id.endswith(f"#{issue.number}"):
            return DeadLetter(source_id, "id_mismatch", [f"number:{issue.number}"], raw)
        cursor = issue.updated_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return Record(source_id, issue.model_dump_json(), cursor)

    def rate_limit_delay(self, response: httpx.Response) -> float | None:
        """GitHub signals limits with 403 OR 429 (ADR-0032). None means "not a rate limit"."""
        if response.status_code not in (403, 429):
            return None
        retry_after = retry_after_seconds(response.headers.get("retry-after"))
        if retry_after is not None:
            return retry_after
        if response.headers.get("x-ratelimit-remaining") == "0":
            reset = response.headers.get("x-ratelimit-reset", "")
            if reset.isdigit():
                return max(0.0, int(reset) - self._clock())
            return SECONDARY_LIMIT_WAIT_S
        # Secondary limits come without headers; GitHub's message says so. A plain 403
        # (permissions) has no such text and stays non-retryable -> an http_403 dead letter.
        if response.status_code == 429 or "rate limit" in response.text.lower():
            return SECONDARY_LIMIT_WAIT_S
        return None
