"""Application settings, read from environment variables and an optional local `.env` file.

This is the only module that reads the environment. Fields are added in the phase whose code
first needs them (ADR-0004), so every setting here is one that something actually uses or that
CLAUDE.md already fixes the shape of.
"""

from functools import lru_cache
from typing import Self

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

    # --- HTTP (applies to every source) ----------------------------------------------------
    http_connect_timeout_s: float = Field(default=5.0, gt=0)
    http_read_timeout_s: float = Field(default=10.0, gt=0)
    http_write_timeout_s: float = Field(default=10.0, gt=0)
    http_pool_timeout_s: float = Field(default=5.0, gt=0)

    # --- Concurrent pipeline (Phase 2; ADR-0014..0016) ---------------------------------------
    fetch_concurrency: int = Field(default=10, ge=1, le=200)  # fixed fetcher pool size
    pipeline_workers: int = Field(default=2, ge=1, le=32)
    batch_size: int = Field(default=50, ge=1, le=1000)
    flush_interval_s: float = Field(default=1.0, gt=0)
    id_queue_maxsize: int = Field(default=100, ge=1)  # 0 would mean unbounded in asyncio
    raw_queue_maxsize: int = Field(default=100, ge=1)

    # --- Reliability (Phase 3; ADR-0021..0026) -----------------------------------------------
    retry_max_attempts: int = Field(default=4, ge=1, le=10)  # total tries, including the first
    retry_base_delay_s: float = Field(default=0.5, gt=0)
    retry_max_delay_s: float = Field(default=10.0, gt=0)
    retry_after_max_s: float = Field(default=60.0, gt=0)  # longer Retry-After => give up
    shutdown_grace_s: float = Field(default=10.0, gt=0)  # drain deadline after SIGINT/SIGTERM

    # --- Hacker News -----------------------------------------------------------------------
    hn_base_url: str = Field(default="https://hacker-news.firebaseio.com/v0")
    # /topstories returns up to 500 ids (HN API README), so larger limits are meaningless.
    hn_top_limit: int = Field(default=30, ge=1, le=500)

    # --- GitHub (Phase 4) ------------------------------------------------------------------
    github_token: SecretStr | None = Field(
        default=None,
        description="GitHub personal access token (GITHUB_TOKEN). Never commit it.",
    )

    @model_validator(mode="after")
    def _pool_fits_workers(self) -> Self:
        # Fail at startup, not as acquire timeouts mid-run (pool exhaustion, ADR-0018).
        if self.db_pool_max_size < self.pipeline_workers + 1:
            raise ValueError(
                f"db_pool_max_size ({self.db_pool_max_size}) must be >= pipeline_workers + 1 "
                f"({self.pipeline_workers + 1}): one connection per flushing worker + bookkeeping"
            )
        if self.retry_base_delay_s > self.retry_max_delay_s:
            raise ValueError("retry_base_delay_s must be <= retry_max_delay_s")
        return self


@lru_cache
def get_settings() -> Settings:
    """Build Settings once per process; later calls return the same object."""
    return Settings()
