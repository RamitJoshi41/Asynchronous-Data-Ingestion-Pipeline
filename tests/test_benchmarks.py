import time

import httpx
import pytest

from benchmarks.mock_hn import create_app, synthetic_item
from benchmarks.pool_exhaustion import scenario
from benchmarks.run import RepeatResult, summarize
from ingest.config import Settings
from ingest.models.hn import HNItem, parse_hn_item, parse_top_ids


def asgi_client(n_items: int, latency_s: float = 0.0) -> httpx.AsyncClient:
    # In-process ASGI transport: exercises the real app without opening a socket.
    transport = httpx.ASGITransport(app=create_app(n_items=n_items, latency_s=latency_s))
    return httpx.AsyncClient(transport=transport, base_url="http://mock")


async def test_mock_serves_top_ids_and_valid_items() -> None:
    async with asgi_client(5) as client:
        top = await client.get("/v0/topstories.json")
        assert parse_top_ids(top.content) == [1, 2, 3, 4, 5]
        item = parse_hn_item((await client.get("/v0/item/3.json")).content)
        assert isinstance(item, HNItem) and item.id == 3 and item.type == "story"


async def test_mock_returns_null_for_unknown_ids() -> None:
    async with asgi_client(5) as client:
        assert (await client.get("/v0/item/6.json")).content == b"null"
        assert (await client.get("/health")).status_code == 200


async def test_mock_applies_latency_per_request() -> None:
    async with asgi_client(1, latency_s=0.05) as client:
        started = time.perf_counter()
        await client.get("/v0/item/1.json")
        assert time.perf_counter() - started >= 0.05


def test_synthetic_items_are_deterministic() -> None:
    assert synthetic_item(7) == synthetic_item(7)
    assert synthetic_item(7) != synthetic_item(8)


def test_summarize_medians_and_error_rate() -> None:
    ok = RepeatResult(2.0, 100, 100, 0, 0, False, None)
    slow = RepeatResult(4.0, 100, 100, 0, 0, False, None)
    partial = RepeatResult(3.0, 100, 90, 6, 4, False, None)
    summary = summarize([ok, slow, partial])
    assert summary["median_duration_s"] == 3.0
    assert summary["median_items_per_s"] == 30.0  # 50, 25, 30 -> median 30
    assert summary["error_rate"] == pytest.approx(10 / 300)


def test_failed_repeat_counts_every_item_as_an_error() -> None:
    failed = RepeatResult(0.0, 50, 0, 0, 0, True, "ConnectError('down')")
    assert failed.items_per_s == 0.0
    assert summarize([failed])["error_rate"] == 1.0


async def test_pool_exhaustion_scenarios_serialise_or_time_out(test_settings: Settings) -> None:
    sized = await scenario(
        test_settings, "sized", workers=2, pool_size=3, hold_s=0.1, acquire_timeout_s=2
    )
    assert sized.timeouts == 0 and max(sized.acquire_waits_s) < 0.05  # nobody waits

    serial = await scenario(
        test_settings, "pool=1", workers=2, pool_size=1, hold_s=0.1, acquire_timeout_s=2
    )
    assert serial.timeouts == 0
    assert serial.acquire_waits_s[1] >= 0.09  # the 2nd flush waited for the 1st to finish

    starved = await scenario(
        test_settings, "timeout", workers=3, pool_size=1, hold_s=0.3, acquire_timeout_s=0.1
    )
    assert starved.timeouts == 2 and len(starved.acquire_waits_s) == 1
