from collections.abc import Iterator

import pytest

from ingest.config import Settings, get_settings


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    # A developer's real shell may export these; tests must not depend on it.
    monkeypatch.delenv("DB_DSN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_defaults_without_env() -> None:
    # _env_file=None ignores any local .env so the test sees only code defaults.
    settings = Settings(_env_file=None)
    assert settings.github_token is None
    assert settings.db_dsn.get_secret_value().startswith("postgresql://")


def test_env_vars_override_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_DSN", "postgresql://u:p@db:5432/x")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_example")
    settings = Settings(_env_file=None)
    assert settings.db_dsn.get_secret_value() == "postgresql://u:p@db:5432/x"
    assert settings.github_token is not None
    assert settings.github_token.get_secret_value() == "ghp_example"


def test_env_var_names_are_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("github_token", "lowercase-works")
    settings = Settings(_env_file=None)
    assert settings.github_token is not None
    assert settings.github_token.get_secret_value() == "lowercase-works"


def test_secrets_are_masked_in_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_DSN", "postgresql://u:hunter2@db/x")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_supersecret")
    text = repr(Settings(_env_file=None))
    assert "hunter2" not in text
    assert "ghp_supersecret" not in text


def test_get_settings_is_cached() -> None:
    assert get_settings() is get_settings()
