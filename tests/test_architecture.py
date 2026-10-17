"""The core must not know which sources exist (CLAUDE.md §3, ADR-0028, ADR-0035).

Parses each core module's imports (no execution) and fails if one reaches into a connector or a
source model. Adding a source can then never require editing the queue, worker or storage code.
"""

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src" / "ingest"
CORE = [
    "connector.py",
    "pipeline.py",
    "sequential.py",
    "records.py",
    "replay.py",
    "runner.py",
    "storage.py",
    "retry.py",
    "shutdown.py",
    "httpclient.py",
    "db.py",
    "migrate.py",
]
FORBIDDEN = ("ingest.connectors", "ingest.models")


def imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            found.add(node.module)
    return found


@pytest.mark.parametrize("module", CORE)
def test_core_module_imports_no_connector_or_model(module: str) -> None:
    leaks = {m for m in imported_modules(SRC / module) if m.startswith(FORBIDDEN)}
    assert leaks == set(), f"{module} depends on a specific source: {sorted(leaks)}"


def test_every_core_module_is_checked() -> None:
    # A new top-level module must be classified: core (listed above) or wiring (below).
    wiring = {"__init__.py", "__main__.py", "config.py", "toy_pipeline.py"}
    assert {p.name for p in SRC.glob("*.py")} == set(CORE) | wiring


def test_the_check_actually_detects_a_leak(tmp_path: Path) -> None:
    leaky = tmp_path / "leaky.py"
    leaky.write_text("from ingest.models.hn import SOURCE\nimport ingest.connectors.hn\n")
    assert imported_modules(leaky) >= {"ingest.models.hn", "ingest.connectors.hn"}
