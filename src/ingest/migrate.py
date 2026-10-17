"""Versioned SQL migration runner (ADR-0005).

Migrations are `NNNN_name.sql` files in `src/ingest/migrations/`. Each is applied once, in
version order, inside its own transaction, and recorded in `schema_migrations` with a checksum.
Explainer: docs/explain/ingest/migrate.md
"""

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

import structlog

from ingest.db import DbPool

log = structlog.get_logger()

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
_FILENAME = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")

# Arbitrary but fixed key for pg_advisory_lock: every migrator must use the same number.
_LOCK_KEY = 74_201_001

# The runner's own bookkeeping table is the one CREATE TABLE that cannot live in a migration
# (it records which migrations ran). Every migration tool has this bootstrap step.
_BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    integer     PRIMARY KEY,
    name       text        NOT NULL,
    checksum   text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)
"""


_RECORD = "INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)"


class MigrationError(Exception):
    """The migration history is inconsistent; refuse to touch the schema."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()


def load_migrations(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    """Read migration files. Synchronous file I/O: from async code, use asyncio.to_thread."""
    migrations: list[Migration] = []
    for path in sorted(directory.glob("*.sql")):
        match = _FILENAME.match(path.name)
        if match is None:
            raise MigrationError(f"bad migration filename {path.name!r}; expected NNNN_name.sql")
        migrations.append(Migration(int(match[1]), match[2], path.read_text(encoding="utf-8")))
    versions = [m.version for m in migrations]
    if len(versions) != len(set(versions)):
        raise MigrationError(f"duplicate migration versions in {directory}")
    return migrations


async def apply_migrations(
    pool: DbPool, migrations: list[Migration], *, acquire_timeout: float
) -> list[int]:
    """Apply pending migrations; return the versions applied (empty if already up to date)."""
    async with pool.acquire(timeout=acquire_timeout) as conn:
        # Session-level lock: a second migrator blocks here until we unlock (or disconnect).
        await conn.execute("SELECT pg_advisory_lock($1)", _LOCK_KEY)
        try:
            await conn.execute(_BOOTSTRAP)
            rows = await conn.fetch("SELECT version, checksum FROM schema_migrations")
            applied: dict[int, str] = {r["version"]: r["checksum"] for r in rows}
            pending = _pending(migrations, applied)
            for m in pending:
                # One transaction per file: a failing migration leaves no partial schema and is
                # not recorded, so fixing the file and re-running is safe.
                async with conn.transaction():
                    await conn.execute(m.sql)
                    await conn.execute(_RECORD, m.version, m.name, m.checksum)
                log.info("migration_applied", version=m.version, name=m.name)
            return [m.version for m in pending]
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _LOCK_KEY)


def _pending(migrations: list[Migration], applied: dict[int, str]) -> list[Migration]:
    by_version = {m.version: m for m in migrations}
    for version, checksum in applied.items():
        known = by_version.get(version)
        if known is None:
            raise MigrationError(f"database has migration {version} but no file for it")
        if known.checksum != checksum:
            raise MigrationError(
                f"migration {version} ({known.name}) was edited after being applied; "
                "add a new migration instead"
            )
    pending = sorted((m for m in migrations if m.version not in applied), key=lambda m: m.version)
    newest_applied = max(applied, default=0)
    if pending and pending[0].version < newest_applied:
        raise MigrationError(
            f"migration {pending[0].version} is older than applied migration {newest_applied}"
        )
    return pending
