from collections.abc import Iterator

import pytest
from pydantic import ValidationError

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


def test_default_dsn_targets_compose_db_port() -> None:
    assert ":5433/" in Settings(_env_file=None).db_dsn.get_secret_value()


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("HN_TOP_LIMIT", "0"),
        ("HN_TOP_LIMIT", "501"),  # /topstories returns at most 500 ids
        ("DB_POOL_MAX_SIZE", "0"),
        ("HTTP_READ_TIMEOUT_S", "0"),
        ("DB_COMMAND_TIMEOUT_S", "-1"),
    ],
)
def test_out_of_range_values_fail_at_startup(
    monkeypatch: pytest.MonkeyPatch, field: str, bad_value: str
) -> None:
    monkeypatch.setenv(field, bad_value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_numeric_env_strings_are_converted(monkeypatch: pytest.MonkeyPatch) -> None:
    # Env vars are always strings; settings fields are lax so "45" becomes 45.
    monkeypatch.setenv("HN_TOP_LIMIT", "45")
    assert Settings(_env_file=None).hn_top_limit == 45
