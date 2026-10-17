"""Application settings, read from environment variables and an optional local `.env` file.

This is the only module that reads the environment. Fields are added in the phase whose code
first needs them (ADR-0004), so every setting here is one that something actually uses or that
CLAUDE.md already fixes the shape of.
"""

from functools import lru_cache

from pydantic import Field, SecretStr
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
    # Sequential Phase 1 uses one connection at a time; sizing is revisited in Phase 2's ADR.
    db_pool_min_size: int = Field(default=1, ge=1)
    db_pool_max_size: int = Field(default=2, ge=1)
    db_connect_timeout_s: float = Field(default=5.0, gt=0)
    db_acquire_timeout_s: float = Field(default=5.0, gt=0)
    db_command_timeout_s: float = Field(default=10.0, gt=0)

    # --- HTTP (applies to every source) ----------------------------------------------------
    http_connect_timeout_s: float = Field(default=5.0, gt=0)
    http_read_timeout_s: float = Field(default=10.0, gt=0)
    http_write_timeout_s: float = Field(default=10.0, gt=0)
    http_pool_timeout_s: float = Field(default=5.0, gt=0)

    # --- Hacker News -----------------------------------------------------------------------
    hn_base_url: str = Field(default="https://hacker-news.firebaseio.com/v0")
    # /topstories returns up to 500 ids (HN API README), so larger limits are meaningless.
    hn_top_limit: int = Field(default=30, ge=1, le=500)

    # --- GitHub (Phase 4) ------------------------------------------------------------------
    github_token: SecretStr | None = Field(
        default=None,
        description="GitHub personal access token (GITHUB_TOKEN). Never commit it.",
    )


@lru_cache
def get_settings() -> Settings:
    """Build Settings once per process; later calls return the same object."""
    return Settings()
