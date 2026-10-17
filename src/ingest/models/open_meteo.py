"""Open-Meteo daily forecast row, validated in STRICT mode (ADR-0008, ADR-0034).

The API answers with parallel arrays (`daily.time[i]`, `daily.temperature_2m_max[i]`, ...); the
connector turns column i into one of these rows. Field meanings and units from
https://open-meteo.com/en/docs (read 2026-10-17). Volatile response fields such as
`generationtime_ms` are deliberately absent: they'd make every run look like an update.
Explainer: docs/explain/ingest/models/open_meteo.md
"""

from datetime import date

from pydantic import BaseModel, ConfigDict, Field

SOURCE = "open_meteo"


class DailyForecast(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    city: str
    latitude: float = Field(ge=-90, le=90)  # the model grid cell actually used, not the request
    longitude: float = Field(ge=-180, le=180)
    date: date  # in the requested timezone (GMT)
    temperature_2m_max: float | None  # °C
    temperature_2m_min: float | None  # °C
    precipitation_sum: float | None = Field(ge=0)  # mm
    weather_code: int | None = Field(ge=0)  # WMO code


# Must match DailyForecast's measurement fields: these are what the connector requests.
DAILY_VARIABLES = ("temperature_2m_max", "temperature_2m_min", "precipitation_sum", "weather_code")
