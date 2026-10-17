import httpx
import pytest
import respx

from ingest.config import Settings
from ingest.httpclient import USER_AGENT, build_http_client, http_limits

BASE = "https://api.test"  # never a real API: respx fails any unmocked request


async def test_requests_carry_our_user_agent(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(f"{BASE}/x").respond(content=b"null")
    async with build_http_client(Settings(_env_file=None), concurrency=1) as client:
        await client.get(f"{BASE}/x")
    assert route.calls.last.request.headers["User-Agent"] == USER_AGENT


async def test_client_has_all_four_timeouts_from_settings() -> None:
    settings = Settings(
        _env_file=None,
        http_connect_timeout_s=1.5,
        http_read_timeout_s=2.5,
        http_write_timeout_s=3.5,
        http_pool_timeout_s=4.5,
    )
    async with build_http_client(settings, concurrency=1) as c:
        assert c.timeout == httpx.Timeout(connect=1.5, read=2.5, write=3.5, pool=4.5)
        assert c.follow_redirects is False


def test_http_limits_match_concurrency() -> None:
    # Keep-alive equal to max connections: no reconnect churn at full concurrency.
    assert http_limits(37) == httpx.Limits(max_connections=37, max_keepalive_connections=37)


async def test_source_headers_are_added_to_that_client_only(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(f"{BASE}/x").respond(json={})
    settings = Settings(_env_file=None)
    async with build_http_client(settings, concurrency=1, headers={"Authorization": "t"}) as a:
        await a.get(f"{BASE}/x")
    assert route.calls.last.request.headers["Authorization"] == "t"
    assert route.calls.last.request.headers["User-Agent"] == USER_AGENT  # defaults kept
    async with build_http_client(settings, concurrency=1) as b:
        await b.get(f"{BASE}/x")
    assert "Authorization" not in route.calls.last.request.headers


@pytest.mark.parametrize("status", [301, 302])
async def test_redirects_are_not_followed(respx_mock: respx.MockRouter, status: int) -> None:
    respx_mock.get(f"{BASE}/x").respond(status_code=status, headers={"Location": f"{BASE}/y"})
    async with build_http_client(Settings(_env_file=None), concurrency=1) as client:
        response = await client.get(f"{BASE}/x")
    assert response.status_code == status
