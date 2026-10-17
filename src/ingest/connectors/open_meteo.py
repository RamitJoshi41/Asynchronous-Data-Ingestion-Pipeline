"""Open-Meteo connector: a 7-day daily forecast per configured city (ADR-0034).

One job per city, one page per job. The response holds parallel arrays; parse() turns column i
into one row record keyed `city:date`. No watermark: a forecast is a snapshot that changes on every
run, and skip-unchanged upserts store only the days that actually changed. Free, keyless,
non-commercial tier; data CC BY 4.0 (https://open-meteo.com/en/terms, read 2026-10-17).
Explainer: docs/explain/ingest/connectors/open_meteo.md
"""

import json
from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass
from typing import Self

import httpx
from pydantic import BaseModel, ConfigDict, JsonValue, ValidationError

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
from ingest.models.open_meteo import DAILY_VARIABLES, SOURCE, DailyForecast
from ingest.retry import default_rate_limit
from ingest.storage import DeadLetter

FORECAST_DAYS = "7"


@dataclass(frozen=True)
class City:
    name: str
    latitude: float
    longitude: float

    @classmethod
    def parse(cls, spec: str) -> Self:
        """`name:lat:lon`, e.g. `patiala:30.34:76.39`. ValueError on anything else."""
        parts = spec.split(":")
        if len(parts) != 3 or not parts[0]:
            raise ValueError(f"open_meteo_cities entries must be 'name:lat:lon', got {spec!r}")
        return cls(parts[0], float(parts[1]), float(parts[2]))


class _Envelope(BaseModel):
    """Just enough structure to find the columns; each row is validated strictly on its own."""

    model_config = ConfigDict(strict=True, extra="ignore")
    latitude: float
    longitude: float
    daily: dict[str, list[JsonValue]]


class OpenMeteoConnector:
    def __init__(self, base_url: str, cities: list[City]) -> None:
        self._base_url = base_url.rstrip("/")
        self._cities = {city.name: city for city in cities}
        if len(self._cities) != len(cities):
            raise ValueError("open_meteo_cities names must be unique (they key the records)")

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        cities = [City.parse(spec) for spec in settings.open_meteo_cities]
        return cls(settings.open_meteo_base_url, cities)

    @property
    def source(self) -> str:
        return SOURCE

    @property
    def headers(self) -> Mapping[str, str]:
        return {}  # keyless free tier

    async def plan(self, ctx: FetchContext, watermarks: Mapping[str, str]) -> list[Job]:
        return [
            Job(
                city.name,
                f"{self._base_url}/forecast",
                {
                    "latitude": str(city.latitude),
                    "longitude": str(city.longitude),
                    "daily": ",".join(DAILY_VARIABLES),
                    "timezone": "GMT",  # required with `daily`; GMT is the documented default
                    "forecast_days": FORECAST_DAYS,
                },
            )
            for city in self._cities.values()
        ]

    async def pages(self, ctx: FetchContext, job: Job) -> AsyncGenerator[Page]:
        response = await ctx.get(job.url, job.params)
        yield Page(job, 1, response.content)

    def parse(self, page: Page) -> list[Parsed]:
        try:
            envelope = _Envelope.model_validate_json(page.body)
        except ValidationError as exc:
            raise InvalidPage(validation_problems(exc)) from exc
        columns = envelope.daily
        times = columns.get("time")
        if times is None or any(v not in columns for v in DAILY_VARIABLES):
            raise InvalidPage(["daily:missing_column"])
        if any(len(columns[v]) != len(times) for v in DAILY_VARIABLES):
            raise InvalidPage(["daily:length_mismatch"])
        city = page.job.key
        parsed: list[Parsed] = []
        for i, day in enumerate(times):
            row = {
                "city": city,
                "latitude": envelope.latitude,
                "longitude": envelope.longitude,
                "date": day,
                **{v: columns[v][i] for v in DAILY_VARIABLES},
            }
            raw = json.dumps(row, separators=(",", ":")).encode()
            parsed.append(self.revalidate(f"{city}:{day}", raw))
        return parsed

    def revalidate(self, source_id: str, raw: bytes) -> Parsed:
        """Validate one row stored under `city:YYYY-MM-DD`."""
        try:
            day = DailyForecast.model_validate_json(raw)
        except ValidationError as exc:
            return DeadLetter(source_id, "invalid", validation_problems(exc), raw)
        if f"{day.city}:{day.date.isoformat()}" != source_id:
            return DeadLetter(source_id, "id_mismatch", [f"key:{day.city}:{day.date}"], raw)
        return Record(source_id, day.model_dump_json())

    def rate_limit_delay(self, response: httpx.Response) -> float | None:
        # Open-Meteo documents no rate-limit response format: use the HTTP standard.
        return default_rate_limit(response)
