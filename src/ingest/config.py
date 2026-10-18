"""Application settings, read from environment variables and an optional local `.env` file.

This is the only module that reads the environment. Fields are added in the phase whose code
first needs them (ADR-0004), so every setting here is one that something actually uses or that
CLAUDE.md already fixes the shape of.
"""

from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # Shells and CI runners export many unrelated variables; don't fail on them.
        extra="ignore",
    )

    # --- Database -------------------------------------------------------------------------
    # SecretStr because a DSN embeds the DB password: repr/logging shows '**********'.
    # Default points at the compose `db` service (host port 5433, see compose.yaml).
    db_dsn: SecretStr = Field(
        default=SecretStr("postgresql://ingest:ingest@localhost:5433/ingest"),
        description="PostgreSQL connection string (local-dev default; override via DB_DSN).",
    )
    # Must cover one concurrent flush per worker plus one for run bookkeeping (ADR-0018).
    db_pool_min_size: int = Field(default=1, ge=1)
    db_pool_max_size: int = Field(default=3, ge=1)
    db_connect_timeout_s: float = Field(default=5.0, gt=0)
    db_acquire_timeout_s: float = Field(default=5.0, gt=0)
    db_command_timeout_s: float = Field(default=10.0, gt=0)

    # --- HTTP (applies to every source; each source gets its own client, ADR-0029) ----------
    http_connect_timeout_s: float = Field(default=5.0, gt=0)
    http_read_timeout_s: float = Field(default=10.0, gt=0)
    http_write_timeout_s: float = Field(default=10.0, gt=0)
    http_pool_timeout_s: float = Field(default=5.0, gt=0)

    # --- Concurrent pipeline (Phase 2; ADR-0014..0016) ---------------------------------------
    # Fetcher-pool sizes are per source (`<source>_concurrency`, ADR-0030); these apply to each.
    pipeline_workers: int = Field(default=2, ge=1, le=32)
    batch_size: int = Field(default=50, ge=1, le=1000)
    flush_interval_s: float = Field(default=1.0, gt=0)
    job_queue_maxsize: int = Field(default=100, ge=1)  # 0 would mean unbounded in asyncio
    raw_queue_maxsize: int = Field(default=100, ge=1)

    # --- Reliability (Phase 3; ADR-0021..0026) -----------------------------------------------
    retry_max_attempts: int = Field(default=4, ge=1, le=10)  # total tries, including the first
    retry_base_delay_s: float = Field(default=0.5, gt=0)
    retry_max_delay_s: float = Field(default=10.0, gt=0)
    retry_after_max_s: float = Field(default=60.0, gt=0)  # longer Retry-After => give up
    shutdown_grace_s: float = Field(default=10.0, gt=0)  # drain deadline after SIGINT/SIGTERM

    # --- Observability + API (Phase 5; ADR-0038, ADR-0039) ----------------------------------
    log_format: Literal["console", "json"] = "console"  # json for machines, console for humans
    log_level: Literal["debug", "info", "warning", "error"] = "info"
    gauge_interval_s: float = Field(default=0.25, gt=0)  # queue depth / in-flight sampling period
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65535)

    # --- Hacker News -----------------------------------------------------------------------
    hn_base_url: str = Field(default="https://hacker-news.firebaseio.com/v0")
    hn_concurrency: int = Field(default=10, ge=1, le=200)  # fixed fetcher pool size
    # /topstories returns up to 500 ids (HN API README), so larger limits are meaningless.
    hn_top_limit: int = Field(default=30, ge=1, le=500)

    # --- GitHub (Phase 4; ADR-0033) ---------------------------------------------------------
    github_token: SecretStr | None = Field(
        default=None,
        description="GitHub personal access token (GITHUB_TOKEN). Never commit it.",
    )
    github_base_url: str = Field(default="https://api.github.com")
    # The version GitHub actually selected for /issues when probed on 2026-10-17.
    github_api_version: str = Field(default="2022-11-28")
    # JSON list in the environment: GITHUB_REPOS='["encode/httpx", "MagicStack/asyncpg"]'.
    github_repos: list[str] = Field(default=["encode/httpx", "MagicStack/asyncpg"], min_length=1)
    # Unauthenticated GitHub allows 60 requests/hour: one page = one request. With sort=updated asc
    # a capped run fetches the oldest part, so the watermark resumes from there (ADR-0031).
    github_max_pages: int = Field(default=3, ge=1, le=100)
    github_concurrency: int = Field(default=2, ge=1, le=20)

    # --- Open-Meteo (Phase 4; ADR-0034) -----------------------------------------------------
    open_meteo_base_url: str = Field(default="https://api.open-meteo.com/v1")
    # JSON list of "name:latitude:longitude"; one request per city per run.
    open_meteo_cities: list[str] = Field(
        default=["patiala:30.34:76.39", "delhi:28.61:77.21", "london:51.51:-0.13"], min_length=1
    )
    open_meteo_concurrency: int = Field(default=2, ge=1, le=20)

    @model_validator(mode="after")
    def _pool_fits_workers(self) -> Self:
        # Fail at startup, not as acquire timeouts mid-run (pool exhaustion, ADR-0018).
        if self.db_pool_max_size < self.pipeline_workers + 1:
            raise ValueError(
                f"db_pool_max_size ({self.db_pool_max_size}) must be >= pipeline_workers + 1 "
                f"({self.pipeline_workers + 1}): one connection per flushing worker + bookkeeping"
            )
        for repo in self.github_repos:
            owner, _, name = repo.partition("/")
            if not owner or not name or "/" in name:
                raise ValueError(f"github_repos entries must look like 'owner/repo', got {repo!r}")
        if self.retry_base_delay_s > self.retry_max_delay_s:
            raise ValueError("retry_base_delay_s must be <= retry_max_delay_s")
        return self


@lru_cache
def get_settings() -> Settings:
    """Build Settings once per process; later calls return the same object."""
    return Settings()
