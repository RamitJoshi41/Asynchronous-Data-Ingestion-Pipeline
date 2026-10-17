# PipeLine: async data ingestion pipeline

A headless Python 3.12 service that ingests data from a fixed set of public APIs (Hacker News, and
later GitHub and Open-Meteo). It validates every record with strict Pydantic v2 schemas,
upserts clean rows into PostgreSQL (skipping unchanged rows), records every run, and is being built
up phase by phase into a concurrent, failure-tolerant pipeline with a small FastAPI status layer.

> **Status: Phase 1 of 6.** Hacker News works end to end, **sequentially** (this is the measured
> baseline). Concurrency, retries, the dead-letter table, more sources, and the API are *planned*.
> Progress: [docs/STATUS.md](docs/STATUS.md).

## Architecture

**Now (Phase 1):**

```mermaid
flowchart LR
    CLI["python -m ingest run hn --limit N"] --> R["run_hn_sequential()"]
    R -- "1 start_run" --> RUNS[("ingestion_runs<br/>status=running")]
    R -- "2 GET /topstories.json" --> HN[Hacker News API]
    R -- "3 for each id, ONE AT A TIME:<br/>GET /item/id.json" --> HN
    HN -- bytes --> V{"parse_hn_item<br/>strict Pydantic"}
    V -- "HNItem" --> U["upsert_item<br/>ON CONFLICT DO UPDATE<br/>WHERE payload IS DISTINCT FROM"]
    V -- "ValidationError / id mismatch" --> INV["count invalid + log<br/>(dead-letter: Phase 3)"]
    V -- "null" --> MISS["count missing"]
    U --> ITEMS[("items<br/>(source, source_id) → payload jsonb")]
    R -- "4 finish_run<br/>succeeded / failed + counts" --> RUNS
```

**Target (planned):**

```mermaid
flowchart LR
    subgraph SRC["Sources"]
        HN[Hacker News API]
        GH[GitHub REST API]
        OM[Open-Meteo API]
    end
    F["fetchers<br/>httpx AsyncClient · Semaphore · retries"]
    Q[("bounded asyncio.Queue")]
    W["worker tasks<br/>parse + Pydantic validate"]
    B["batch buffer"]
    DB[("PostgreSQL<br/>items · dead_letter · ingestion_runs")]
    API["FastAPI<br/>GET /runs · /runs/{id} · /health<br/>POST /trigger (overlap-guarded)"]
    HN & GH & OM --> F --> Q --> W
    W -- valid --> B -- "bulk upsert<br/>skip unchanged" --> DB
    W -- invalid --> DB
    F & W -. "counts, retries, p95" .-> DB
    API --> DB
```

Details: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Features

| Feature | Status |
|---|---|
| Concurrency skeleton: Semaphore, bounded queue with visible backpressure, sentinel shutdown, no task leaks | Done as a **simulation** (Phase 0) |
| Typed config (pydantic-settings), secrets masked, ranges validated at startup | Done |
| Versioned SQL migrations (checksums, advisory lock, transaction per file) | Done (Phase 1) |
| HN top stories → strict validation → Postgres, `ingestion_runs` per run | Done, sequential (Phase 1) |
| Upserts that skip unchanged rows (proven: no new row version) | Done (Phase 1) |
| Benchmark harness (local mock server + one small real run) | Skeleton done, baseline recorded (Phase 1) |
| Concurrent fetch + batched writes | Planned: Phase 2 |
| Retries with jitter, `Retry-After`, dead-letter + replay, graceful shutdown | Planned: Phase 3 |
| GitHub + Open-Meteo, per-source limits, watermarks | Planned: Phase 4 |
| Metrics, FastAPI endpoints, overlap guard | Planned: Phase 5 |
| One-command Docker Compose run, CI | Planned: Phase 6 |

## Quickstart

Requires [uv](https://docs.astral.sh/uv/) and Docker with the compose plugin.

```bash
uv sync                                   # .venv with runtime + dev deps
docker compose up -d --wait db            # Postgres 16 on localhost:5433
uv run python -m ingest migrate           # create/upgrade tables
uv run python -m ingest run hn --limit 30 # ingest the top 30 HN stories (real API)
uv run python -m ingest.toy_pipeline      # Phase 0 concurrency simulation
```

A one-command `docker compose up` for the whole app is planned for Phase 6.

## Configuration

Environment variables (or a git-ignored `.env` in the working directory), defined in
`src/ingest/config.py`. Out-of-range values fail at startup.

| Variable | Default | Meaning |
|---|---|---|
| `DB_DSN` | `postgresql://ingest:ingest@localhost:5433/ingest` | Postgres DSN (secret, masked in logs) |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | `1` / `2` | asyncpg pool size |
| `DB_CONNECT_TIMEOUT_S` | `5` | opening a DB connection |
| `DB_ACQUIRE_TIMEOUT_S` | `5` | waiting for a free pooled connection |
| `DB_COMMAND_TIMEOUT_S` | `10` | per SQL statement |
| `HTTP_CONNECT_TIMEOUT_S` / `HTTP_READ_TIMEOUT_S` / `HTTP_WRITE_TIMEOUT_S` / `HTTP_POOL_TIMEOUT_S` | `5` / `10` / `10` / `5` | httpx timeouts |
| `HN_BASE_URL` | `https://hacker-news.firebaseio.com/v0` | HN API base |
| `HN_TOP_LIMIT` | `30` (1–500) | default number of top stories |
| `GITHUB_TOKEN` | unset | GitHub token (Phase 4; secret) |

## API endpoints

Planned for Phase 5: `GET /runs`, `GET /runs/{id}`, `GET /health`, `POST /trigger` (409 if a run is
already active). Until then, inspect runs in SQL:
`SELECT * FROM ingestion_runs ORDER BY id DESC;`

## Adding a source

Planned for Phase 4. The storage layer is already source-agnostic (`items(source, source_id,
payload jsonb)`), so the goal is that a new source is a connector plus a Pydantic model, with no
changes to storage, queue, or worker code.

## Testing

```bash
docker compose up -d --wait db            # tests need the real Postgres
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run pytest -q
```

Tests never hit real public APIs (respx fails any unmocked request). DB tests use database
`ingest_test` with a fresh schema per test.

## Benchmarks

Sequential baseline, measured 2026-10-17 on an i5-11400H laptop
([methodology and raw JSON](docs/BENCHMARKS.md)):

| Target | Items | Median duration | Throughput | Error rate |
|---|---|---|---|---|
| Local mock, 50 ms injected latency (3 repeats) | 100 | 5.454 s | 18.3 items/s | 0 |
| Real HN API (1 run) | 30 | 12.762 s | 2.35 items/s | 0 |

The mock number reflects this laptop with a fixed artificial delay. The real number is one run at
one moment over one home network. About 93% of the mock run is waiting on responses, which is what
Phase 2's concurrency targets.

## Failure modes

| Failure | Detection | Behavior | Test |
|---|---|---|---|
| Record fails strict validation (e.g. string `score`) | `ValidationError` | counted `invalid`, logged (location and type only), run continues | `test_mixed_items_are_classified_and_valid_ones_stored` |
| Item body is `null` | parser returns `None` | counted `missing`, run continues | same |
| Body id ≠ requested id | id check | counted `invalid` | same |
| Same payload re-ingested | `ON CONFLICT ... WHERE IS DISTINCT FROM` | no write (row version unchanged), counted `unchanged` | `test_upsert_insert_then_unchanged_then_update` |
| HTTP 5xx / 4xx mid-run | `HTTPStatusError` | run `failed` with error, earlier rows kept, exception re-raised | `test_http_error_mid_run_marks_run_failed_and_reraises` |
| Network down | `httpx.ConnectError` | run `failed`, re-raised | `test_network_failure_on_top_ids_marks_run_failed` |
| Slow SQL statement | `command_timeout` | `TimeoutError` | `test_pool_applies_default_command_timeout` |
| Edited applied migration | checksum mismatch | `migrate` refuses | `test_edited_applied_migration_is_refused` |
| Two migrators at once | advisory lock | applied exactly once | `test_concurrent_migrators_apply_each_migration_exactly_once` |
| Ctrl-C mid-run | `CancelledError` | client/pool closed, **run row stays `running`** (known gap) | not yet (Phase 3) |

Retries, 429/`Retry-After`, and the dead-letter table arrive in Phase 3.

## Limitations / non-goals

- Sequential only: no retries, invalid records aren't stored yet, and Ctrl-C leaves a `running` run row.
- Benchmarks so far describe one laptop (mock) and one moment of the internet (real).
- Non-goals: a universal "any API" connector, scraping, anti-bot evasion, a UI, auth/multi-tenancy,
  Kafka/Celery/Airflow.

## Project layout

```
src/ingest/
  __main__.py        CLI (migrate, run hn)
  config.py          settings from env
  db.py              asyncpg pool
  migrate.py         migration runner;  migrations/*.sql  the schema
  httpclient.py      shared httpx client
  sources/hn.py      HN HTTP calls
  models/hn.py       strict HNItem schema
  storage.py         items upsert + ingestion_runs
  sequential.py      Phase 1 runner
  toy_pipeline.py    Phase 0 simulation
benchmarks/          mock HN server + benchmark harness (results in docs/benchmarks/results/)
tests/               pytest (asyncio auto mode, respx, real Postgres)
compose.yaml         Postgres 16 for dev/tests
docs/                start at docs/WALKTHROUGH.md
```
Full list: [docs/FILE_MAP.md](docs/FILE_MAP.md).
