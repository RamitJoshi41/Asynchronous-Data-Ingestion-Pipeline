from collections.abc import AsyncIterator, Mapping

import httpx
import pytest
import respx
import structlog
from pydantic import ValidationError

from ingest.config import Settings
from ingest.connector import FetchContext, Job, Missing, Page, Record
from ingest.connectors.hn import HN_MAX_LIMIT, HackerNewsConnector
from ingest.httpclient import build_http_client
from ingest.storage import DeadLetter

BASE = "https://hn.test/v0"  # never the real API: respx fails any unmocked request
HN = HackerNewsConnector(BASE, limit=3)


@pytest.fixture
async def ctx() -> AsyncIterator[FetchContext]:
    async with build_http_client(Settings(_env_file=None), concurrency=1) as client:

        async def get(url: str, params: Mapping[str, str] | None) -> httpx.Response:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response

        yield FetchContext(get=get, log=structlog.get_logger())


def page(item_id: int, body: bytes) -> Page:
    return Page(Job(str(item_id), f"{BASE}/item/{item_id}.json"), 1, body)


async def test_plan_makes_one_job_per_top_id_in_order(
    ctx: FetchContext, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/topstories.json").respond(json=[30, 20, 10, 5])
    jobs = await HN.plan(ctx, {})
    assert [(j.key, j.url) for j in jobs] == [
        ("30", f"{BASE}/item/30.json"),
        ("20", f"{BASE}/item/20.json"),
        ("10", f"{BASE}/item/10.json"),
    ]


async def test_malformed_top_ids_raise_validation_error(
    ctx: FetchContext, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/topstories.json").respond(json={"not": "a list"})
    with pytest.raises(ValidationError):
        await HN.plan(ctx, {})


async def test_each_job_is_one_unparsed_page(
    ctx: FetchContext, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/item/8863.json").respond(content=b'{"id": 8863}')
    job = Job("8863", f"{BASE}/item/8863.json")
    pages = [p async for p in HN.pages(ctx, job)]
    assert pages == [Page(job, 1, b'{"id": 8863}')]


async def test_http_errors_propagate_to_the_core(
    ctx: FetchContext, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/item/1.json").respond(status_code=404)
    with pytest.raises(httpx.HTTPStatusError):
        [p async for p in HN.pages(ctx, Job("1", f"{BASE}/item/1.json"))]


def test_valid_body_becomes_a_record_keyed_by_id() -> None:
    [record] = HN.parse(page(5, b'{"id": 5, "type": "story", "time": 1175714200}'))
    assert isinstance(record, Record) and record.source_id == "5" and record.cursor is None
    assert '"time":1175714200' in record.payload_json  # round-trips to HN's wire format


def test_invalid_body_is_a_dead_letter_with_its_raw_bytes() -> None:
    body = b'{"id": 5, "score": "secret-ish"}'
    assert HN.parse(page(5, body)) == [DeadLetter("5", "invalid", ["score:int_type"], body)]


def test_malformed_json_is_a_dead_letter_not_a_crash() -> None:
    [result] = HN.parse(page(5, b"{not json"))
    assert isinstance(result, DeadLetter) and result.raw_body == b"{not json"


def test_null_is_missing() -> None:
    assert HN.parse(page(5, b"null")) == [Missing("5")]


def test_body_for_another_id_is_an_id_mismatch() -> None:
    assert HN.parse(page(5, b'{"id": 6}')) == [
        DeadLetter("5", "id_mismatch", ["id:6"], b'{"id": 6}')
    ]


def test_revalidate_uses_the_same_rules_as_parse() -> None:
    assert HN.revalidate("5", b'{"id": 6}') == HN.parse(page(5, b'{"id": 6}'))[0]


def test_rate_limit_is_the_standard_429_retry_after() -> None:
    assert HN.rate_limit_delay(httpx.Response(429, headers={"Retry-After": "3"})) == 3.0
    assert HN.rate_limit_delay(httpx.Response(403, headers={"Retry-After": "3"})) is None


@pytest.mark.parametrize("limit", [0, HN_MAX_LIMIT + 1])
def test_limit_must_be_within_what_topstories_returns(limit: int) -> None:
    with pytest.raises(ValueError):
        HackerNewsConnector(BASE, limit=limit)


def test_no_auth_headers() -> None:
    assert HN.headers == {} and HN.source == "hacker_news"
