import json

import httpx
import pytest
import respx

from ingest.config import Settings
from ingest.connector import InvalidPage, Job, Page, Parsed, Record
from ingest.connectors.open_meteo import City, OpenMeteoConnector
from ingest.db import DbPool
from ingest.httpclient import build_http_client
from ingest.pipeline import PipelineConfig, run_concurrent
from ingest.storage import DeadLetter, RunStatus, Store

BASE = "https://api.open-meteo.test/v1"  # never the real API: respx fails any unmocked request
PATIALA = City("patiala", 30.34, 76.39)
DAYS = ["2026-10-17", "2026-10-18", "2026-10-19", "2026-10-20", "2026-10-21", "2026-10-22",
        "2026-10-23"]  # fmt: skip


def forecast(**daily_overrides: object) -> dict[str, object]:
    """Shaped like a real response (probed 2026-10-17), including fields we drop."""
    daily: dict[str, object] = {
        "time": DAYS,
        "temperature_2m_max": [29.1, 29.3, 29.4, 30.0, 30.1, 30.5, 30.9],
        "temperature_2m_min": [18.4, 17.9, 17.7, 18.0, 18.0, 18.1, 18.2],
        "precipitation_sum": [0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00],
        "weather_code": [0, 0, 0, 2, 0, 0, 0],
    }
    daily.update(daily_overrides)
    return {
        "latitude": 30.333918,
        "longitude": 76.38921,
        "generationtime_ms": 631.5361261367798,  # changes on every call
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": 255.0,
        "daily_units": {"time": "iso8601", "temperature_2m_max": "°C"},
        "daily": daily,
    }


OM = OpenMeteoConnector(BASE, [PATIALA])


def page(body: object) -> Page:
    raw = body if isinstance(body, bytes) else json.dumps(body).encode()
    return Page(Job("patiala", f"{BASE}/forecast"), 1, raw)


def only_records(parsed: list[Parsed]) -> list[Record]:
    records = [r for r in parsed if isinstance(r, Record)]
    assert len(records) == len(parsed), parsed
    return records


def cfg() -> PipelineConfig:
    return PipelineConfig(
        concurrency=2,
        workers=1,
        batch_size=50,
        flush_interval_s=1.0,
        job_queue_maxsize=10,
        raw_queue_maxsize=10,
    )


def test_seven_columns_become_seven_rows_keyed_city_date() -> None:
    records = only_records(OM.parse(page(forecast())))
    assert [r.source_id for r in records] == [f"patiala:{d}" for d in DAYS]
    first = json.loads(records[0].payload_json)
    assert first == {
        "city": "patiala",
        "latitude": 30.333918,
        "longitude": 76.38921,
        "date": "2026-10-17",
        "temperature_2m_max": 29.1,
        "temperature_2m_min": 18.4,
        "precipitation_sum": 0.0,
        "weather_code": 0,
    }
    # Volatile, so it would make every run look like an update: deliberately not stored.
    assert "generationtime_ms" not in records[0].payload_json


def test_no_cursor_open_meteo_has_no_watermark() -> None:
    assert {r.cursor for r in only_records(OM.parse(page(forecast())))} == {None}


def test_one_bad_value_dead_letters_only_that_day() -> None:
    codes = [0, 0, "sunny", 2, 0, 0, 0]
    records = OM.parse(page(forecast(weather_code=codes)))
    rejected = [r for r in records if isinstance(r, DeadLetter)]
    assert [(r.source_id, r.errors) for r in rejected] == [
        ("patiala:2026-10-19", ["weather_code:int_type"])
    ]
    assert sum(isinstance(r, Record) for r in records) == 6


def test_nulls_are_allowed_for_measurements() -> None:
    assert len(only_records(OM.parse(page(forecast(temperature_2m_max=[None] * 7))))) == 7


@pytest.mark.parametrize(
    "body",
    [b"not json", {"error": True, "reason": "x"},
     forecast(weather_code=[0, 0]),  # columns of different lengths
     {**forecast(), "daily": {"time": DAYS}}],  # a requested column is missing
)  # fmt: skip
def test_unusable_envelopes_are_invalid_pages(body: object) -> None:
    with pytest.raises(InvalidPage):
        OM.parse(page(body))


def test_revalidate_checks_the_row_belongs_to_its_key() -> None:
    [first, *_] = only_records(OM.parse(page(forecast())))
    raw = json.dumps({**json.loads(first.payload_json), "city": "delhi"}).encode()
    result = OM.revalidate("patiala:2026-10-17", raw)
    assert isinstance(result, DeadLetter) and result.reason == "id_mismatch"


def test_cities_come_from_settings_and_are_validated() -> None:
    om = OpenMeteoConnector.from_settings(
        Settings(_env_file=None, open_meteo_cities=["a:1:2", "b:-3.5:4"])
    )
    assert om.source == "open_meteo" and om.headers == {}
    with pytest.raises(ValueError, match="name:lat:lon"):
        City.parse("patiala:30.34")
    with pytest.raises(ValueError, match="unique"):
        OpenMeteoConnector(BASE, [PATIALA, PATIALA])


async def test_run_requests_the_documented_parameters_and_reruns_unchanged(
    migrated_pool: DbPool, respx_mock: respx.MockRouter
) -> None:
    route = respx_mock.get(f"{BASE}/forecast").respond(json=forecast())
    store = Store(migrated_pool, acquire_timeout=5.0)
    async with build_http_client(Settings(_env_file=None), concurrency=2) as client:
        first = await run_concurrent(store, client, OM, cfg=cfg())
        again = await run_concurrent(store, client, OM, cfg=cfg())
    params = route.calls.last.request.url.params
    assert dict(params) == {
        "latitude": "30.34",
        "longitude": "76.39",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code",
        "timezone": "GMT",
        "forecast_days": "7",
    }
    assert first.counts.inserted == 7
    assert (again.counts.inserted, again.counts.unchanged) == (0, 7)


async def test_a_400_error_dead_letters_the_city(
    migrated_pool: DbPool, respx_mock: respx.MockRouter
) -> None:
    body = {"error": True, "reason": "Cannot initialize WeatherVariable from invalid String value"}
    respx_mock.get(f"{BASE}/forecast").mock(return_value=httpx.Response(400, json=body))
    store = Store(migrated_pool, acquire_timeout=5.0)
    async with build_http_client(Settings(_env_file=None), concurrency=2) as client:
        result = await run_concurrent(store, client, OM, cfg=cfg())
    assert result.status is RunStatus.SUCCEEDED and result.counts.invalid == 1
    row = await migrated_pool.fetchrow("SELECT source_id, reason FROM dead_letter")
    assert row is not None and (row["source_id"], row["reason"]) == ("patiala", "http_400")
