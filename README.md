# PipeLine: async data ingestion pipeline

A headless Python 3.12 service that will concurrently pull data from a fixed set of public APIs
(Hacker News, GitHub, Open-Meteo), validate every record with strict Pydantic v2 schemas, batch-upsert
clean rows into PostgreSQL, send invalid records to a dead-letter table instead of crashing, and
expose run status through a thin FastAPI layer.

> **Status: Phase 0 of 6.** What exists today is the concurrency skeleton as a **simulation**
> (`asyncio.sleep` in place of HTTP and DB) plus typed configuration. Everything else below that's
> marked *planned* isn't built yet. Progress: [docs/STATUS.md](docs/STATUS.md).

## Architecture

**Now (Phase 0, simulation):**

```mermaid
flowchart LR
    items[["items (ints)"]] --> P
    subgraph RUN["run(): outer TaskGroup"]
        subgraph P["producer: inner TaskGroup"]
            F1["fetch(item)"]
            F2["fetch(item)"]
            FN["fetch(item) × N"]
        end
        S{{"Semaphore(C)<br/>≤ C fake fetches in flight"}}
        Q[("bounded Queue(maxsize=Q)<br/>full ⇒ put() waits<br/>log producer_blocked")]
        W1["worker 0"]
        W2["worker W-1"]
    end
    F1 & F2 & FN -.acquire.-> S
    F1 & F2 & FN -- "put() after releasing slot" --> Q
    P -- "then W × None (sentinels)" --> Q
    Q --> W1 & W2
    W1 & W2 --> R["RunReport<br/>processed / failed / peaks / blocked_puts"]
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
| Semaphore-bounded concurrency, bounded queue with visible backpressure, N workers, sentinel shutdown, no task leaks on failure | Done (simulation, Phase 0) |
| Typed config via pydantic-settings, secrets masked | Done (Phase 0) |
| HN source, Postgres, migrations, `ingestion_runs` | Planned: Phase 1 |
| Real concurrent fetch + batched writes | Planned: Phase 2 |
| Retries with jitter, `Retry-After`, dead-letter + replay, idempotent upserts, graceful shutdown | Planned: Phase 3 |
| GitHub + Open-Meteo connectors, per-source limits, watermarks | Planned: Phase 4 |
| structlog run context, metrics, FastAPI endpoints, overlap guard | Planned: Phase 5 |
| Benchmarks, Docker Compose, CI | Planned: Phase 6 |

## Quickstart

Requires [uv](https://docs.astral.sh/uv/). Python 3.12 is installed by uv if missing.

```bash
uv sync                                   # creates .venv with runtime + dev deps
uv run python -m ingest.toy_pipeline      # run the Phase 0 simulation
uv run python -m ingest.toy_pipeline --help
```

The default demo makes workers about 20× slower than fetchers, so you'll see `producer_blocked` /
`producer_unblocked` lines (backpressure), one `item_failed` (simulated bad record 7), and a final
`RunReport`.

`docker compose up` is planned for Phase 6.

## Configuration

Read from environment variables or a `.env` file in the working directory (`.env` is git-ignored).

| Variable | Type | Default | Used by |
|---|---|---|---|
| `DB_DSN` | secret string | `postgresql://postgres:postgres@localhost:5432/ingest` | nothing yet (Phase 1) |
| `GITHUB_TOKEN` | secret string, optional | unset | nothing yet (Phase 4) |

## API endpoints

Planned for Phase 5: `GET /runs`, `GET /runs/{id}`, `GET /health`, `POST /trigger` (409 if a run is
already active).

## Adding a source

Planned for Phase 4. The goal is that a new source is a small config entry plus a Pydantic model,
with **no** changes to queue, worker, or storage code.

## Testing

```bash
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run pytest -q
```

Tests never hit real public APIs.

## Benchmarks

Not yet measured. Methodology and results will live in [docs/BENCHMARKS.md](docs/BENCHMARKS.md) (Phase 6).

## Failure modes

| Failure | Detection | Behavior | Test |
|---|---|---|---|
| Consumers slower than producers | queue full on `put()` | producer suspends (backpressure), `producer_blocked` logged; memory bounded by Q + blocked putters | `test_slow_consumer_fills_queue_and_blocks_producers` |
| Bad record (simulated) | `InvalidRecordError` in worker | recorded in `failed`, run continues | `test_invalid_record_is_recorded_and_run_continues` |
| Fatal fetch error (simulated) | `FetchError` in a fetch task | `TaskGroup` cancels all tasks, `ExceptionGroup` raised, no tasks leaked | `test_fetch_failure_aborts_run_and_leaks_no_tasks` |
| Bug in a worker while producers are blocked | unexpected exception in worker | run aborts instead of hanging, no tasks leaked | `test_unexpected_worker_error_aborts_run_even_with_blocked_producers` |

Network drops, 429s, schema changes, and DB outages are covered from Phase 3 onward.

## Limitations / non-goals

- Today it's a simulation only. Its timings describe asyncio scheduling on one laptop, not the network.
- Non-goals: a universal "any API" connector, scraping, anti-bot evasion, a UI, auth/multi-tenancy,
  Kafka/Celery/Airflow.

## Project layout

```
src/ingest/config.py         settings from env (pydantic-settings)
src/ingest/toy_pipeline.py   Phase 0 simulation
tests/                       pytest (asyncio auto mode)
docs/                        architecture, ADRs, per-file explainers, journal; start at docs/WALKTHROUGH.md
```
Full list: [docs/FILE_MAP.md](docs/FILE_MAP.md).
