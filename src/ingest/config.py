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

    # SecretStr because a DSN embeds the DB password: repr/logging shows '**********'.
    db_dsn: SecretStr = Field(
        default=SecretStr("postgresql://postgres:postgres@localhost:5432/ingest"),
        description="PostgreSQL connection string (local-dev default; override via DB_DSN).",
    )
    github_token: SecretStr | None = Field(
        default=None,
        description="GitHub personal access token (GITHUB_TOKEN). Never commit it.",
    )


@lru_cache
def get_settings() -> Settings:
    """Build Settings once per process; later calls return the same object."""
    return Settings()
