import asyncio
import json
from collections.abc import AsyncIterator, Mapping

import httpx
import pytest
import respx
import structlog
from pydantic import SecretStr
from structlog.testing import capture_logs

from ingest.config import Settings
from ingest.connector import FetchContext, InvalidPage, Job, Page, Record
from ingest.connectors.github import SECONDARY_LIMIT_WAIT_S, GitHubConnector
from ingest.db import DbPool
from ingest.httpclient import build_http_client
from ingest.pipeline import PipelineConfig, run_concurrent
from ingest.retry import RetryAfterTooLongError, RetryPolicy
from ingest.shutdown import StopSignal
from ingest.storage import DeadLetter, RunStatus, Store

BASE = "https://api.github.test"  # never the real API: respx fails any unmocked request
ISSUES = r"https://api\.github\.test/repos/(?P<owner>[^/]+)/(?P<name>[^/]+)/issues"
FAST = RetryPolicy(max_attempts=3, base_delay_s=0.001, max_delay_s=0.005, max_retry_after_s=5)
TOKEN = SecretStr("ghp_test_token_not_real")
TIMEOUT_S = 5.0


def issue(number: int, minute: int = 0, **extra: object) -> dict[str, object]:
    return {
        "id": 1000 + number,
        "number": number,
        "title": f"issue {number}",
        "state": "open",
        "user": {"login": "octocat", "id": 1},
        "labels": [{"name": "bug", "color": "f00"}],
        "comments": 0,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": f"2026-01-01T00:{minute:02d}:00Z",
        "closed_at": None,
        "html_url": f"https://github.com/o/r/issues/{number}",
        "reactions": {"+1": 3},  # one of the many fields we ignore
        **extra,
    }


def connector(repos: list[str] | None = None, **kwargs: object) -> GitHubConnector:
    options: dict[str, object] = {"token": TOKEN, "api_version": "2022-11-28", "max_pages": 10}
    options.update(kwargs)
    return GitHubConnector(BASE, repos or ["o/r"], **options)  # type: ignore[arg-type]


def cfg(**overrides: int) -> PipelineConfig:
    values = {"concurrency": 2, "workers": 2, "batch_size": 50, "job_queue_maxsize": 10}
    values.update(overrides)
    return PipelineConfig(flush_interval_s=1.0, raw_queue_maxsize=10, **values)


class MockGitHub:
    """respx GitHub: per repo, a list of pages (lists of issues, a status code, or raw bytes).

    Page n links to page n+1 with an opaque cursor, the way the real API did when probed.
    """

    def __init__(self, respx_mock: respx.MockRouter, repos: dict[str, list[object]]) -> None:
        self.repos = repos
        self.requests: list[httpx.Request] = []
        self.responses: dict[tuple[str, int], httpx.Response] = {}
        respx_mock.get(url__regex=ISSUES).mock(side_effect=self._issues)

    def _issues(self, request: httpx.Request, owner: str, name: str) -> httpx.Response:
        self.requests.append(request)
        repo = f"{owner}/{name}"
        number = int(request.url.params.get("page", "1"))
        override = self.responses.get((repo, number))
        if override is not None:
            return override
        page = self.repos[repo][number - 1]
        if isinstance(page, int):
            return httpx.Response(page)
        headers = {"x-ratelimit-remaining": "4999"}
        if number < len(self.repos[repo]):
            nxt = f"{BASE}/repos/{repo}/issues?per_page=100&after=cur{number}&page={number + 1}"
            headers["link"] = (
                f'<{nxt}>; rel="next", <{BASE}/repos/{repo}/issues?page=99>; rel="last"'
            )
        content = page if isinstance(page, bytes) else json.dumps(page).encode()
        return httpx.Response(200, content=content, headers=headers)


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    headers = connector().headers
    async with build_http_client(Settings(_env_file=None), concurrency=4, headers=headers) as c:
        yield c


@pytest.fixture
async def ctx(client: httpx.AsyncClient) -> FetchContext:
    async def get(url: str, params: Mapping[str, str] | None) -> httpx.Response:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response

    return FetchContext(get=get, log=structlog.get_logger())


@pytest.fixture
def store(migrated_pool: DbPool) -> Store:
    return Store(migrated_pool, acquire_timeout=5.0)


def page(body: object, number: int = 1, repo: str = "o/r") -> Page:
    raw = body if isinstance(body, bytes) else json.dumps(body).encode()
    return Page(Job(repo, f"{BASE}/repos/{repo}/issues"), number, raw)


# --- connector: request shape ------------------------------------------------------------------


def test_headers_follow_the_docs_and_carry_the_token_only_when_set() -> None:
    assert connector().headers == {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Authorization": "Bearer ghp_test_token_not_real",
    }
    assert "Authorization" not in connector(token=None).headers


async def test_plan_is_one_job_per_repo_with_since_from_its_watermark(ctx: FetchContext) -> None:
    jobs = await connector(["a/x", "b/y"]).plan(ctx, {"b/y": "2026-01-01T00:00:00Z"})
    assert [(j.key, j.url) for j in jobs] == [
        ("a/x", f"{BASE}/repos/a/x/issues"),
        ("b/y", f"{BASE}/repos/b/y/issues"),
    ]
    base = {"state": "all", "sort": "updated", "direction": "asc", "per_page": "100"}
    assert jobs[0].params == base
    assert jobs[1].params == {**base, "since": "2026-01-01T00:00:00Z"}


async def test_plan_warns_when_unauthenticated(ctx: FetchContext) -> None:
    with capture_logs() as logs:
        await connector(token=None).plan(ctx, {})
    assert [e["event"] for e in logs] == ["github_unauthenticated"]


# --- connector: pagination ---------------------------------------------------------------------


async def test_pages_follow_link_next_until_it_is_absent(
    ctx: FetchContext, respx_mock: respx.MockRouter
) -> None:
    mock = MockGitHub(respx_mock, {"o/r": [[issue(1)], [issue(2)], [issue(3)]]})
    [job] = await connector().plan(ctx, {})
    pages = [p async for p in connector().pages(ctx, job)]
    assert [p.number for p in pages] == [1, 2, 3]
    # Page 2+ use GitHub's URL verbatim (with its opaque cursor), not a page number we built.
    assert mock.requests[1].url.params["after"] == "cur1"
    assert mock.requests[0].url.params["sort"] == "updated"


async def test_pages_stop_at_the_cap(ctx: FetchContext, respx_mock: respx.MockRouter) -> None:
    MockGitHub(respx_mock, {"o/r": [[issue(n)] for n in range(1, 6)]})
    gh = connector(max_pages=2)
    [job] = await gh.plan(ctx, {})
    with capture_logs() as logs:
        pages = [p async for p in gh.pages(ctx, job)]
    assert len(pages) == 2
    assert any(e["event"] == "github_page_cap_reached" for e in logs)


async def test_a_link_to_another_host_is_refused(
    ctx: FetchContext, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/repos/o/r/issues").respond(
        json=[issue(1)], headers={"link": '<https://evil.test/steal?page=2>; rel="next"'}
    )
    gh = connector()
    [job] = await gh.plan(ctx, {})
    with pytest.raises(ValueError, match="another origin"):
        [p async for p in gh.pages(ctx, job)]


# --- connector: parsing ------------------------------------------------------------------------


def test_each_issue_becomes_a_record_with_its_updated_at_as_cursor() -> None:
    pr = issue(2, minute=5, pull_request={"merged_at": None, "url": "u"})
    first, second = connector().parse(page([issue(1, minute=3), pr]))
    assert isinstance(first, Record) and isinstance(second, Record)
    assert (first.source_id, first.cursor) == ("o/r#1", "2026-01-01T00:03:00Z")
    payload = json.loads(second.payload_json)
    assert payload["pull_request"] == {"merged_at": None}  # PRs are recognisable
    assert "reactions" not in payload and payload["labels"] == [{"name": "bug"}]


def test_one_bad_issue_dead_letters_alone_with_its_own_json() -> None:
    bad = issue(2, comments="many")
    good, rejected = connector().parse(page([issue(1), bad]))
    assert isinstance(good, Record)
    assert isinstance(rejected, DeadLetter)
    assert (rejected.source_id, rejected.reason, rejected.errors) == (
        "o/r#2",
        "invalid",
        ["comments:int_type"],
    )
    assert json.loads(rejected.raw_body) == bad  # replayable on its own


def test_an_element_without_a_number_is_keyed_by_position() -> None:
    [rejected] = connector().parse(page([{"title": "?"}], number=3))
    assert rejected == DeadLetter("o/r@page3[0]", "invalid", ["number:missing"], b'{"title":"?"}')


@pytest.mark.parametrize("body", [b"<html>oops</html>", {"message": "Not a list"}])
def test_a_page_that_is_not_a_json_list_is_invalid(body: object) -> None:
    with pytest.raises(InvalidPage):
        connector().parse(page(body))


def test_naive_timestamps_are_rejected() -> None:
    [rejected] = connector().parse(page([issue(1, updated_at="2026-01-01T00:00:00")]))
    assert isinstance(rejected, DeadLetter) and rejected.errors == ["updated_at:value_error"]


def test_revalidate_checks_the_issue_belongs_to_its_key() -> None:
    raw = json.dumps(issue(7)).encode()
    assert isinstance(connector().revalidate("o/r#7", raw), Record)
    assert connector().revalidate("o/r#17", raw) == DeadLetter(
        "o/r#17", "id_mismatch", ["number:7"], raw
    )


# --- connector: rate limits (ADR-0032) -----------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "headers", "body", "expected"),
    [(403, {"retry-after": "30"}, "", 30.0),
     (429, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1012"}, "", 12.0),
     (403, {"x-ratelimit-remaining": "0"}, "", SECONDARY_LIMIT_WAIT_S),
     (429, {}, "", SECONDARY_LIMIT_WAIT_S),
     (403, {}, '{"message": "You have exceeded a secondary rate limit"}', SECONDARY_LIMIT_WAIT_S),
     (403, {"x-ratelimit-remaining": "17"}, '{"message": "Resource not accessible"}', None),
     (500, {"retry-after": "30"}, "", None)],
)  # fmt: skip
def test_rate_limit_delay_speaks_githubs_dialect(
    status: int, headers: dict[str, str], body: str, expected: float | None
) -> None:
    gh = connector(clock=lambda: 1000.0)
    assert gh.rate_limit_delay(httpx.Response(status, headers=headers, text=body)) == expected


# --- end to end through the generic pipeline ---------------------------------------------------


async def test_run_ingests_all_pages_and_stores_a_watermark_per_repo(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    mock = MockGitHub(
        respx_mock,
        {"o/r": [[issue(1, 1), issue(2, 2)], [issue(3, 9)], [issue(4, 4)]], "o/s": [[issue(1, 7)]]},
    )
    async with asyncio.timeout(TIMEOUT_S):
        result = await run_concurrent(store, client, connector(["o/r", "o/s"]), cfg=cfg())
    assert result.status is RunStatus.SUCCEEDED
    assert (result.counts.fetched, result.counts.inserted) == (5, 5)
    ids = await migrated_pool.fetch("SELECT source_id FROM items WHERE source = 'github'")
    assert sorted(r["source_id"] for r in ids) == ["o/r#1", "o/r#2", "o/r#3", "o/r#4", "o/s#1"]
    # Max updated_at per repo, not "the last one seen" (page 3 holds an older issue).
    assert await store.watermarks("github") == {
        "o/r": "2026-01-01T00:09:00Z",
        "o/s": "2026-01-01T00:07:00Z",
    }
    auth = {r.headers.get("Authorization") for r in mock.requests}
    assert auth == {"Bearer ghp_test_token_not_real"}


async def test_next_run_asks_only_for_what_changed_since_the_watermark(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    mock = MockGitHub(respx_mock, {"o/r": [[issue(1, 1), issue(2, 2)]]})
    await run_concurrent(store, client, connector(), cfg=cfg())
    second = await run_concurrent(store, client, connector(), cfg=cfg())
    assert mock.requests[-1].url.params["since"] == "2026-01-01T00:02:00Z"
    # since may be inclusive: re-read records are free thanks to skip-unchanged upserts.
    assert (second.counts.inserted, second.counts.unchanged) == (0, 2)


async def test_failed_run_leaves_every_watermark_unchanged(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    mock = MockGitHub(respx_mock, {"o/r": [[issue(1, 1)]], "o/s": [[issue(1, 1)]]})
    await run_concurrent(store, client, connector(["o/r", "o/s"]), cfg=cfg())
    mock.repos = {"o/r": [[issue(1, 5)]], "o/s": [500]}  # o/s is down past the retries
    with pytest.RaisesGroup(httpx.HTTPStatusError, flatten_subgroups=True):
        await run_concurrent(store, client, connector(["o/r", "o/s"]), cfg=cfg(), policy=FAST)
    marks = await store.watermarks("github")
    assert marks == {"o/r": "2026-01-01T00:01:00Z", "o/s": "2026-01-01T00:01:00Z"}


async def test_cancelled_run_leaves_watermarks_unchanged(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    MockGitHub(respx_mock, {"o/r": [[issue(1, 1)], [issue(2, 2)]]})
    stop = StopSignal()
    stop.request("test")  # drain from the start: nothing should advance
    result = await run_concurrent(store, client, connector(), cfg=cfg(), stop=stop)
    assert result.status is RunStatus.CANCELLED
    assert await store.watermarks("github") == {}


async def test_primary_rate_limit_waits_until_reset_then_succeeds(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    mock = MockGitHub(respx_mock, {"o/r": [[issue(1)]]})
    limited = {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1000"}
    mock.responses[("o/r", 1)] = httpx.Response(403, headers=limited)
    loop = asyncio.get_running_loop()
    loop.call_later(0.05, mock.responses.clear)  # the window resets
    t0 = loop.time()
    gh = connector(clock=lambda: 999.8)  # reset is 0.2 s away
    result = await run_concurrent(store, client, gh, cfg=cfg(), policy=FAST)
    assert result.status is RunStatus.SUCCEEDED and result.counts.retries == 1
    assert loop.time() - t0 >= 0.2  # waited for x-ratelimit-reset, not a jittered millisecond


async def test_primary_rate_limit_reset_far_away_fails_fast(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    mock = MockGitHub(respx_mock, {"o/r": [[issue(1)]]})
    limited = {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "4600"}
    mock.responses[("o/r", 1)] = httpx.Response(403, headers=limited)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    with pytest.RaisesGroup(RetryAfterTooLongError, flatten_subgroups=True):
        await run_concurrent(store, client, connector(clock=lambda: 1000.0), cfg=cfg(), policy=FAST)
    assert loop.time() - t0 < 1.0  # did not sleep for an hour
    error = await migrated_pool.fetchval("SELECT error FROM ingestion_runs")
    assert "3600s" in error and await store.watermarks("github") == {}


async def test_plain_403_dead_letters_that_repo_and_holds_its_watermark(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    MockGitHub(respx_mock, {"o/private": [403], "o/r": [[issue(1, 1)]]})
    result = await run_concurrent(store, client, connector(["o/private", "o/r"]), cfg=cfg())
    assert result.status is RunStatus.SUCCEEDED
    rows = await migrated_pool.fetch("SELECT source_id, reason FROM dead_letter")
    assert [(r["source_id"], r["reason"]) for r in rows] == [("o/private", "http_403")]
    assert await store.watermarks("github") == {"o/r": "2026-01-01T00:01:00Z"}


async def test_unreadable_page_dead_letters_and_holds_that_repos_watermark(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter, migrated_pool: DbPool
) -> None:
    MockGitHub(respx_mock, {"o/r": [[issue(1, 1)], b"<html>maintenance</html>"]})
    result = await run_concurrent(store, client, connector(), cfg=cfg())
    assert result.status is RunStatus.SUCCEEDED and result.counts.inserted == 1
    row = await migrated_pool.fetchrow("SELECT source_id, reason FROM dead_letter")
    assert row is not None and (row["source_id"], row["reason"]) == ("o/r@page2", "invalid_page")
    assert await store.watermarks("github") == {}  # page 2's records were never seen


async def test_a_full_page_is_written_in_exact_batch_size_chunks(
    store: Store, client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    MockGitHub(respx_mock, {"o/r": [[issue(n) for n in range(1, 101)], [issue(101)]]})
    result = await run_concurrent(store, client, connector(), cfg=cfg(workers=1, batch_size=40))
    assert result.counts.inserted == 101
    assert result.stats.flush_sizes == [40, 40, 21]  # 100 + 1 records, never a 100-row batch
