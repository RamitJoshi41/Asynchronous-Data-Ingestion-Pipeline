"""The list of sources the CLI knows. Adding a source = a connector + model + one entry here.

Each entry builds its connector from Settings and names its own concurrency setting (ADR-0030).
Explainer: docs/explain/ingest/connectors/registry.md
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from ingest.config import Settings
from ingest.connector import Connector
from ingest.connectors.github import GitHubConnector
from ingest.connectors.hn import HackerNewsConnector
from ingest.connectors.open_meteo import OpenMeteoConnector
from ingest.pipeline import PipelineConfig
from ingest.runner import SourcePlan


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


def build_plans(
    settings: Settings,
    names: Sequence[str],
    *,
    sequential: bool = False,
    concurrency: int | None = None,
    workers: int | None = None,
    batch_size: int | None = None,
) -> list[SourcePlan]:
    """One plan per source: its connector, and its pipeline config (None = sequential)."""
    plans = []
    for name in names:
        spec = SOURCES[name]
        cfg = PipelineConfig.from_settings(
            settings,
            concurrency=spec.concurrency(settings) if concurrency is None else concurrency,
            workers=workers,
            batch_size=batch_size,
        )
        plans.append(SourcePlan(spec.build(settings), None if sequential else cfg))
    return plans
