from collections.abc import AsyncIterator

import httpx
import pytest
import respx
from pydantic import ValidationError

from ingest.config import Settings
from ingest.httpclient import USER_AGENT, build_http_client, http_limits
from ingest.sources.hn import fetch_item, fetch_top_ids

BASE = "https://hn.test/v0"  # never the real API: respx fails any unmocked request


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with build_http_client(Settings(_env_file=None)) as c:
        yield c


async def test_top_ids_respects_limit_and_order(
    client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/topstories.json").respond(json=[30, 20, 10, 5])
    assert await fetch_top_ids(client, BASE, limit=3) == [30, 20, 10]


async def test_item_body_is_returned_unparsed(
    client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/item/8863.json").respond(content=b'{"id": 8863}')
    respx_mock.get(f"{BASE}/item/1.json").respond(content=b"null")
    assert await fetch_item(client, BASE, 8863) == b'{"id": 8863}'
    assert await fetch_item(client, BASE, 1) == b"null"


@pytest.mark.parametrize("status", [404, 429, 500, 503, 301])
async def test_non_2xx_status_raises(
    client: httpx.AsyncClient, respx_mock: respx.MockRouter, status: int
) -> None:
    headers = {"Location": f"{BASE}/elsewhere"} if status == 301 else {}
    respx_mock.get(f"{BASE}/item/1.json").respond(status_code=status, headers=headers)
    with pytest.raises(httpx.HTTPStatusError):
        await fetch_item(client, BASE, 1)


async def test_transport_error_propagates(
    client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/item/1.json").mock(side_effect=httpx.ConnectError("network down"))
    with pytest.raises(httpx.ConnectError):
        await fetch_item(client, BASE, 1)


async def test_malformed_top_ids_raise_validation_error(
    client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    respx_mock.get(f"{BASE}/topstories.json").respond(json={"not": "a list"})
    with pytest.raises(ValidationError):
        await fetch_top_ids(client, BASE, limit=5)


async def test_requests_carry_our_user_agent(
    client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    route = respx_mock.get(f"{BASE}/item/1.json").respond(content=b"null")
    await fetch_item(client, BASE, 1)
    assert route.calls.last.request.headers["User-Agent"] == USER_AGENT


async def test_client_has_all_four_timeouts_from_settings() -> None:
    settings = Settings(
        _env_file=None,
        http_connect_timeout_s=1.5,
        http_read_timeout_s=2.5,
        http_write_timeout_s=3.5,
        http_pool_timeout_s=4.5,
    )
    async with build_http_client(settings) as c:
        assert c.timeout == httpx.Timeout(connect=1.5, read=2.5, write=3.5, pool=4.5)
        assert c.follow_redirects is False


def test_http_limits_match_fetch_concurrency() -> None:
    limits = http_limits(Settings(_env_file=None, fetch_concurrency=37))
    # Keep-alive equal to max connections: no reconnect churn at full concurrency.
    assert limits == httpx.Limits(max_connections=37, max_keepalive_connections=37)
