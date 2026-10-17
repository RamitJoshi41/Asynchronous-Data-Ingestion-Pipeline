# PipeLine: async data ingestion pipeline

A headless Python 3.12 service that ingests data from a fixed set of public APIs (Hacker News, and
later GitHub and Open-Meteo). It validates every record with strict Pydantic v2 schemas,
upserts clean rows into PostgreSQL (skipping unchanged rows), records every run, and is being built
up phase by phase into a concurrent, failure-tolerant pipeline with a small FastAPI status layer.

> **Status: Phase 3 of 6.** Hacker News works end to end, concurrently and **reliably**: retries with
> jitter and Retry-After, a dead-letter table with replay, and graceful Ctrl-C/SIGTERM drain. More
> sources (GitHub, Open-Meteo), metrics and the API are *planned*.

## Architecture

**Now (Phase 3):**

```mermaid
flowchart LR
    CLI["python -m ingest run hn<br/>Ctrl-C: 1st drain, 2nd cancel"] --> R["run_hn_concurrent()"]
    R -- "start / finish (shielded)" --> RUNS[("ingestion_runs<br/>succeeded · failed · cancelled<br/>retries, kind")]
    R --> FEED["feeder<br/>(stops on SIGINT/SIGTERM)"] --> IDQ[("id_queue<br/>bounded")]
    IDQ --> F["C fetchers<br/>Retrier: full jitter + Retry-After<br/>shared 429 gate"]
    F -- "GET /item/id.json" --> HN[Hacker News API]
    F -- "bytes | http_4xx reject" --> RAWQ[("raw_queue<br/>bounded")]
    RAWQ --> W["W workers<br/>classify_hn_body (strict)"]
    W -- "flush: ONE transaction<br/>(shielded, awaited on shutdown)" --> TX{{"upsert items (skip unchanged)<br/>resolve their dead letters<br/>upsert rejects"}}
    TX --> ITEMS[("items")]
    TX --> DLQ[("dead_letter<br/>raw bytea + errors<br/>one open row per record")]
    REPLAY["python -m ingest replay"] -- "re-validate stored bodies" --> TX
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


## Features

| Feature | Status |
|---|---|
| Concurrency skeleton: Semaphore, bounded queue with visible backpressure, sentinel shutdown, no task leaks | Done as a **simulation** (Phase 0) |
| Typed config (pydantic-settings), secrets masked, ranges validated at startup | Done |
| Versioned SQL migrations (checksums, advisory lock, transaction per file) | Done (Phase 1) |
| HN top stories → strict validation → Postgres, `ingestion_runs` per run | Done, sequential (Phase 1) |
| Upserts that skip unchanged rows (proven: no new row version) | Done (Phase 1) |
| Benchmark harness (local mock server in its own process + one small real run, CPU time) | Done (Phases 1–2) |
| Concurrent fetch (fixed pool of C), bounded queues with visible backpressure, batched `unnest()` upserts | Done (Phase 2) |
| DB pool sized to workers (validated at startup), httpx limits aligned with concurrency | Done (Phase 2) |
| Retries: full-jitter exponential backoff, `Retry-After` (seconds + HTTP-date), shared 429 gate, retry budget | Done (Phase 3) |
| Dead-letter table (raw bytes + errors, one open row per record) written atomically with each batch; auto-resolve on clean re-ingest | Done (Phase 3) |
| `replay`: re-validate dead letters against the current model | Done (Phase 3) |
| Graceful shutdown: Ctrl-C/SIGTERM drains and records `cancelled` (exit 130); 2nd signal forces; cancellation-safe flushes | Done (Phase 3) |
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
uv run python -m ingest run hn --limit 30 --concurrency 5   # concurrent (default mode)
uv run python -m ingest run hn --limit 30 --sequential      # Phase 1 baseline runner
uv run python -m ingest replay                              # re-validate dead letters after a model fix
uv run python -m ingest.toy_pipeline      # Phase 0 concurrency simulation
```

A one-command `docker compose up` for the whole app is planned for Phase 6.

## Configuration

Environment variables (or a git-ignored `.env` in the working directory), defined in
`src/ingest/config.py`. Out-of-range values fail at startup.

| Variable | Default | Meaning |
|---|---|---|
| `DB_DSN` | `postgresql://ingest:ingest@localhost:5433/ingest` | Postgres DSN (secret, masked in logs) |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | `1` / `3` | asyncpg pool size (max must be ≥ `PIPELINE_WORKERS` + 1) |
| `DB_CONNECT_TIMEOUT_S` | `5` | opening a DB connection |
| `DB_ACQUIRE_TIMEOUT_S` | `5` | waiting for a free pooled connection |
| `DB_COMMAND_TIMEOUT_S` | `10` | per SQL statement |
| `HTTP_CONNECT_TIMEOUT_S` / `HTTP_READ_TIMEOUT_S` / `HTTP_WRITE_TIMEOUT_S` / `HTTP_POOL_TIMEOUT_S` | `5` / `10` / `10` / `5` | httpx timeouts |
| `FETCH_CONCURRENCY` | `10` (1–200) | fixed fetcher pool size; httpx connection limits follow it |
| `PIPELINE_WORKERS` | `2` (1–32) | validating/flushing workers |
| `BATCH_SIZE` | `50` (1–1000) | rows per upsert statement |
| `FLUSH_INTERVAL_S` | `1.0` | max time a buffered record waits before being written |
| `ID_QUEUE_MAXSIZE` / `RAW_QUEUE_MAXSIZE` | `100` / `100` (≥ 1) | queue bounds (backpressure) |
| `RETRY_MAX_ATTEMPTS` | `4` (1–10) | total tries per request (1 = no retries) |
| `RETRY_BASE_DELAY_S` / `RETRY_MAX_DELAY_S` | `0.5` / `10` | full-jitter backoff window |
| `RETRY_AFTER_MAX_S` | `60` | a longer `Retry-After` gives up instead of stalling |
| `SHUTDOWN_GRACE_S` | `10` | drain deadline after SIGINT/SIGTERM |
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

Measured 2026-10-17 on an i5-11400H laptop ([methodology, raw JSON, caveats](docs/BENCHMARKS.md)).
Local mock with 50 ms injected latency, 200 items, 3 repeats, medians:

| Mode | Duration | Throughput | Speedup | Error rate |
|---|---|---|---|---|
| sequential (Phase 1 runner) | 11.577 s | 17.3 items/s | 1.0× | 0 |
| concurrent, C = 5 | 2.279 s | 87.8 items/s | 5.1× | 0 |
| concurrent, C = 20 | 0.658 s | 303.9 items/s | 17.6× | 0 |
| concurrent, C = 50 | 0.826 s | 242.1 items/s | 14.0× | 0 |
| concurrent, C = 20, Phase 3 reliability layer | 0.630 s | 317.7 items/s | 18.4× | 0 |

C = 50 is *slower* than C = 20: the client becomes CPU-bound in httpcore's connection-pool
bookkeeping (O(connections²) per request event, found by profiling).

**Under injected faults** (10% 500s, 1% 429 with `Retry-After: 1`, C = 20): without retries every
run failed; with a 4-attempt budget, 2 of 3 runs ingested all 200 items (24–27 retries, 2.3–4.2 s),
and one failed when a single item hit four 500s in a row (≈3% expected per run at this fault rate).

Real HN API, one run each: sequential 30 items in 12.762 s vs concurrent C = 5 in 3.931 s (different
times of day, so indicative only). Mock numbers describe this laptop with a fixed artificial delay.

## Failure modes

| Failure | Detection | Behavior | Test |
|---|---|---|---|
| Record fails strict validation (e.g. string `score`) | `ValidationError` | counted `invalid`, logged (location and type only), run continues | `test_mixed_items_are_classified_and_valid_ones_stored` |
| Item body is `null` | parser returns `None` | counted `missing`, run continues | same |
| Body id ≠ requested id | id check | counted `invalid` | same |
| Same payload re-ingested | `ON CONFLICT ... WHERE IS DISTINCT FROM` | no write (row version unchanged), counted `unchanged` | `test_upsert_insert_then_unchanged_then_update` |
| Network drop / timeout / 5xx | httpx error | retried with full-jitter backoff (4 attempts); counted in `retries` | `test_transient_failures_are_retried_and_counted` |
| 429 with `Retry-After` | status + header | the server's delay is honoured; **all** fetchers pause (shared gate); > 60 s → give up | `test_429_retry_after_pauses_all_fetchers`, `test_retry.py` |
| Errors persist after retries | budget exhausted | run `failed`, ExceptionGroup re-raised, counts == committed rows | `test_persistent_500_aborts_after_retries_with_exact_counts` |
| Schema change / invalid record | strict validation | **dead letter** (raw body + error), run continues; `replay` after a model fix | `test_schema_change_lands_in_dead_letter_and_run_continues`, `test_replay.py` |
| 404 for one item | non-retryable status | dead letter `http_404`, run continues | same |
| Record fixed upstream | clean re-ingest | its open dead letter is resolved in the same transaction | `test_successful_ingest_resolves_an_open_dead_letter` |
| Slow SQL statement | `command_timeout` | `TimeoutError` | `test_pool_applies_default_command_timeout` |
| Edited applied migration | checksum mismatch | `migrate` refuses | `test_edited_applied_migration_is_refused` |
| Two migrators at once | advisory lock | applied exactly once | `test_concurrent_migrators_apply_each_migration_exactly_once` |
| Slow DB / workers | raw queue full | fetchers suspend (`producer_blocked`), memory ≤ C + Q + W×B, nothing dropped | `test_slow_flushes_fill_raw_queue_and_block_fetchers` |
| Bug in a flush while fetchers are blocked | exception in a worker | aborts instead of hanging | `test_unexpected_worker_error_aborts_instead_of_hanging` |
| DB pool too small for workers | config / runtime check | rejected at startup (Settings) or before the run starts | `test_pool_must_fit_workers_plus_bookkeeping`, `test_rejects_pool_smaller_than_workers_plus_one` |
| Pool exhausted anyway | `acquire(timeout=...)` | `TimeoutError` instead of a hang | `test_undersized_pool_flush_times_out_instead_of_hanging` |
| Duplicate key within one batch | de-duplication | last payload wins (Postgres would reject the whole statement) | `test_batch_duplicates_are_collapsed_last_wins` |
| Ctrl-C / SIGTERM | signal handler | **drain**: stop feeding, finish in-flight, flush all buffers, run `cancelled`, exit 130 | `test_sigint_drains_and_records_cancelled_run` (real subprocess + SIGINT) |
| Drain too slow | deadline (`SHUTDOWN_GRACE_S`) | remaining work cancelled, run `cancelled` with reason | `test_drain_past_deadline_is_cut_short_but_still_recorded` |
| Second Ctrl-C | signal handler | immediate cancel; the in-progress batch still lands and is counted; `cancelled` | `test_second_sigint_forces_immediate_exit`, `test_forced_cancellation_lets_the_in_progress_batch_land` |


## Limitations / non-goals

- A record whose retries are exhausted fails the whole run (by design: the source is unhealthy). An
  idempotent re-run converges, but there's no per-run dead-letter cap or circuit breaker yet.
- On a *forced* stop, records buffered in workers but not yet being written are lost (re-fetched on
  the next run). A graceful drain flushes them. A killed process (SIGKILL) or a dead DB can still leave a
  `running` row.
- Signal handling uses `loop.add_signal_handler`, which is Unix-only (Linux/macOS/Docker).
- A single `httpx.AsyncClient` stops scaling at about 20 connections on this laptop (httpcore pool overhead).
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
  records.py         shared per-record rules: fetch with retries, classify, dead letters
  retry.py           backoff with full jitter, Retry-After, rate-limit gate
  shutdown.py        SIGINT/SIGTERM → drain / cancel
  replay.py          dead-letter replay
  pipeline.py        Phase 2 concurrent runner
  sequential.py      Phase 1 runner (baseline)
  toy_pipeline.py    Phase 0 simulation
benchmarks/          mock HN server (with fault injection), benchmark harness, pool-exhaustion experiment
tests/               pytest (asyncio auto mode, respx, real Postgres)
compose.yaml         Postgres 16 for dev/tests
docs/                BENCHMARKS.md (methodology) + benchmarks/results/*.json (raw results)
```
