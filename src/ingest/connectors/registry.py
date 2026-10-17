"""The list of sources the CLI knows. Adding a source = a connector + model + one entry here.

Each entry builds its connector from Settings and names its own concurrency setting (ADR-0030).
Explainer: docs/explain/ingest/connectors/registry.md
"""

from collections.abc import Callable
from dataclasses import dataclass

from ingest.config import Settings
from ingest.connector import Connector
from ingest.connectors.github import GitHubConnector
from ingest.connectors.hn import HackerNewsConnector
from ingest.connectors.open_meteo import OpenMeteoConnector


@dataclass(frozen=True)
class SourceSpec:
    build: Callable[[Settings], Connector]
    concurrency: Callable[[Settings], int]


SOURCES: dict[str, SourceSpec] = {
    "hn": SourceSpec(
        build=lambda s: HackerNewsConnector(s.hn_base_url, limit=s.hn_top_limit),
        concurrency=lambda s: s.hn_concurrency,
    ),
    "github": SourceSpec(
        build=GitHubConnector.from_settings,
        concurrency=lambda s: s.github_concurrency,
    ),
    "open-meteo": SourceSpec(
        build=OpenMeteoConnector.from_settings,
        concurrency=lambda s: s.open_meteo_concurrency,
    ),
}
