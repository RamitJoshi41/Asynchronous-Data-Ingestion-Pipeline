# PipeLine: async data ingestion pipeline

A headless Python 3.12 service that ingests data from a fixed set of public APIs (Hacker News, GitHub
and Open-Meteo). It validates every record with strict Pydantic v2 schemas,
upserts clean rows into PostgreSQL (skipping unchanged rows), records every run, and is being built
up phase by phase into a concurrent, failure-tolerant pipeline with a small FastAPI status layer.

> **Status: Phase 5 of 6.** Three sources (Hacker News, GitHub issues/PRs, Open-Meteo forecasts) run
> through one source-agnostic, concurrent, reliable pipeline, each with its own limits; GitHub is
> incremental via watermarks. Every run records latency percentiles and queue/in-flight gauges, and a
> FastAPI layer lists runs and triggers new ones (409 if a source is already running anywhere).
> One-command Docker and CI are *planned* (Phase 6).

## Architecture

**Now (Phase 4):**

```mermaid
flowchart LR
    CLI["python -m ingest run hn|github|open-meteo|all<br/>Ctrl-C: 1st drain all, 2nd cancel"] --> RS["runner.run_sources()<br/>one DB pool Σ(W+1), one StopSignal"]
    RS --> P1["run_concurrent(HN connector)<br/>own httpx client, C=10"]
    RS --> P2["run_concurrent(GitHub connector)<br/>own client + token, C=2"]
    RS --> P3["run_concurrent(Open-Meteo connector)<br/>own client, C=2"]
    subgraph PIPE["each pipeline instance (same core code)"]
        PLAN["connector.plan(watermarks)"] --> JQ[("job_queue<br/>bounded")]
        JQ --> F["C fetchers<br/>job_pages: pages in order<br/>Retrier + connector rate-limit hook"]
        F --> RQ[("raw_queue<br/>pages, bounded")]
        RQ --> W["W workers<br/>classify: connector.parse<br/>exact batch_size chunks"]
    end
    P2 -.-> PIPE
    F -- "GET (Link next, same-origin only)" --> API["HN · GitHub · Open-Meteo"]
    W -- "flush: ONE transaction" --> TX{{"upsert items (skip unchanged)<br/>resolve dead letters<br/>upsert rejects"}}
    TX --> ITEMS[("items")]
    TX --> DLQ[("dead_letter")]
    PIPE -- "succeeded only, same txn as status" --> WM[("source_watermarks<br/>per (source, key)")]
    WM -. "since=" .-> PLAN
    PIPE --> RUNS[("ingestion_runs<br/>one row per source per run")]
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
| Connector protocol: the core (queues, workers, storage, retries) never imports a source; enforced by a test | Done (Phase 4) |
| GitHub: issues + PRs per repo, token auth, `Link` pagination, `x-ratelimit-*` handling | Done (Phase 4) |
| Open-Meteo: daily forecast per city (added without touching core code) | Done (Phase 4) |
| Watermarks: advanced only by a succeeded run, in the same transaction; GitHub resumes with `since` | Done (Phase 4) |
| Per-source concurrency + HTTP client; `run all` with failure isolation | Done (Phase 4) |
| Run metrics: duration, attempts, exact p50/p95/max request latency, queue-depth and in-flight peak/mean | Done (Phase 5) |
| FastAPI: `/health`, `/runs`, `/runs/{id}` (live gauges), `POST /trigger`; runs drain on API shutdown | Done (Phase 5) |
| Overlap guard: per-source Postgres advisory lock (API 409, CLI exit 75); abandoned `running` rows cleaned up | Done (Phase 5) |
| Structured logs: console or JSON (`LOG_FORMAT`), `run_id`/`source` on every run event | Done (Phase 5) |
| One-command Docker Compose run, CI | Planned: Phase 6 |

## Quickstart

Requires [uv](https://docs.astral.sh/uv/) and Docker with the compose plugin.

```bash
uv sync                                   # .venv with runtime + dev deps
docker compose up -d --wait db            # Postgres 16 on localhost:5433
uv run python -m ingest migrate           # create/upgrade tables
uv run python -m ingest run hn --limit 30 --concurrency 5   # concurrent (default mode)
uv run python -m ingest run github                          # GITHUB_TOKEN optional (60 req/h without)
uv run python -m ingest run open-meteo
uv run python -m ingest run all                             # every source at once, each with its own limits
uv run python -m ingest run hn --limit 30 --sequential      # Phase 1 baseline runner
uv run python -m ingest replay                              # re-validate dead letters (all sources)
uv run python -m ingest serve                               # API on http://127.0.0.1:8000 (docs at /docs)
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
| `PIPELINE_WORKERS` | `2` (1–32) | validating/flushing workers (per source) |
| `BATCH_SIZE` | `50` (1–1000) | rows per upsert statement |
| `FLUSH_INTERVAL_S` | `1.0` | max time a buffered record waits before being written |
| `JOB_QUEUE_MAXSIZE` / `RAW_QUEUE_MAXSIZE` | `100` / `100` (≥ 1) | queue bounds (backpressure) |
| `RETRY_MAX_ATTEMPTS` | `4` (1–10) | total tries per request (1 = no retries) |
| `RETRY_BASE_DELAY_S` / `RETRY_MAX_DELAY_S` | `0.5` / `10` | full-jitter backoff window |
| `RETRY_AFTER_MAX_S` | `60` | a longer `Retry-After` gives up instead of stalling |
| `SHUTDOWN_GRACE_S` | `10` | drain deadline after SIGINT/SIGTERM |
| `LOG_FORMAT` / `LOG_LEVEL` | `console` / `info` | `json` for machine-readable logs |
| `GAUGE_INTERVAL_S` | `0.25` | queue-depth / in-flight sampling period |
| `API_HOST` / `API_PORT` | `127.0.0.1` / `8000` | `serve` bind address |
| `HN_BASE_URL` | `https://hacker-news.firebaseio.com/v0` | HN API base |
| `HN_TOP_LIMIT` | `30` (1–500) | default number of top stories |
| `HN_CONCURRENCY` | `10` (1–200) | HN fetcher pool size (httpx limits follow it) |
| `GITHUB_TOKEN` | unset | GitHub token (secret; sent only to GitHub) |
| `GITHUB_REPOS` | `["encode/httpx", "MagicStack/asyncpg"]` | JSON list of `owner/repo` |
| `GITHUB_MAX_PAGES` | `3` (1–100) | pages (×100 issues) per repo per run |
| `GITHUB_CONCURRENCY` | `2` (1–20) | GitHub fetcher pool size |
| `GITHUB_API_VERSION` / `GITHUB_BASE_URL` | `2022-11-28` / `https://api.github.com` | |
| `OPEN_METEO_CITIES` | `["patiala:30.34:76.39", "delhi:28.61:77.21", "london:51.51:-0.13"]` | JSON list of `name:lat:lon` |
| `OPEN_METEO_CONCURRENCY` / `OPEN_METEO_BASE_URL` | `2` / `https://api.open-meteo.com/v1` | |

## API endpoints

`uv run python -m ingest serve` (interactive docs at `/docs`). Runs triggered here execute in the API's
own event loop and DB pool; stopping the API drains them (recorded `cancelled`).

| Endpoint | Returns |
|---|---|
| `GET /health` | `{"status": "ok", "database": "ok", "active_runs": 0}`; 503 if the DB is unreachable |
| `GET /runs?source=&limit=20&before_id=` | runs, newest first (keyset paging with `before_id`) |
| `GET /runs/{id}` | one run with counts and metrics, plus `live` gauges while it's running; 404 if unknown |
| `POST /trigger` `{"sources": ["hn", "open-meteo"]}` (or `["all"]`) | **202** `{"runs": {"hacker_news": 15, "open_meteo": 16}}`; **409** if a source is already running (here or in a CLI process); 422 for unknown sources |

Real example (2026-10-18), `GET /runs/15` after a triggered HN run:
`status=succeeded, items_fetched=30, requests=31, duration_ms=3515, latency_p50_ms≈373,
latency_p95_ms≈1054, in_flight_peak=10`.

## Adding a source

A source is a connector (a class satisfying the small `Connector` protocol in
`src/ingest/connector.py`: `plan`, `pages`, `parse`, `revalidate`, `rate_limit_delay`), a strict
Pydantic model, a settings block and one registry entry. Queue, worker, storage, retry and shutdown code
doesn't change: `tests/test_architecture.py` fails if core code imports a connector, and Open-Meteo was
added with a diff of 5 files, 344 insertions, 0 deletions.

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
| concurrent, C = 20, Phase 4 connector refactor (two sets) | 0.651 s / 0.627 s | 307.4 / 319.2 items/s | 17.8× / 18.5× | 0 |
| concurrent, C = 20, Phase 5 metrics + gauges | 0.637 s | 313.9 items/s | 18.2× | 0 |

C = 50 is *slower* than C = 20: the client becomes CPU-bound in httpcore's connection-pool
bookkeeping (O(connections²) per request event, found by profiling).

**Under injected faults** (10% 500s, 1% 429 with `Retry-After: 1`, C = 20): without retries every
run failed; with a 4-attempt budget, 2 of 3 runs ingested all 200 items (24–27 retries, 2.3–4.2 s),
and one failed when a single item hit four 500s in a row (≈3% expected per run at this fault rate).

Real HN API, one run each: sequential 30 items in 12.762 s vs concurrent C = 5 in 3.931 s (different
times of day, so indicative only). Mock numbers describe this laptop with a fixed artificial delay.

After the Phase 4 refactor the sequential baseline measured 11.304 s (17.7 items/s): no measurable cost
from the connector abstraction. The new sources aren't benchmarked yet (Phase 6).

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
| GitHub rate limit (403/429, `x-ratelimit-remaining: 0`) | connector hook | wait until reset (≤ 60 s) with all GitHub fetchers paused; longer → run fails fast, watermark unchanged | `test_primary_rate_limit_waits_until_reset_then_succeeds`, `test_primary_rate_limit_reset_far_away_fails_fast` |
| Repo private / gone (plain 403/404) | non-retryable status | dead letter keyed `owner/repo`, that repo's watermark held, others continue | `test_plain_403_dead_letters_that_repo_and_holds_its_watermark` |
| Page isn't the expected envelope (HTML, error object) | `InvalidPage` | `invalid_page` dead letter, watermark held | `test_unreadable_page_dead_letters_and_holds_that_repos_watermark` |
| Run crashes / fails / is cancelled | no succeeded status | watermark unchanged; next run re-reads the overlap, skip-unchanged makes it free | `test_failed_run_leaves_every_watermark_unchanged`, `test_watermark_and_succeeded_status_commit_together` |
| One source fails during `run all` | per-source isolation | other sources finish; exit code 1 | `test_a_failing_source_does_not_cancel_the_others` |
| `Link` header points to another host | same-origin check | refused (token never leaves GitHub) | `test_a_link_to_another_host_is_refused`, `test_the_token_is_never_sent_to_another_host` |
| Same source triggered twice / CLI + API at once | advisory lock per source | 409 from the API, exit 75 from the CLI; nothing starts | `test_live_gauges_while_running_then_409_for_a_second_trigger`, `test_409_when_another_process_holds_the_source`, `test_cli_run_refuses_a_busy_source` |
| Process killed mid-run (SIGKILL) | lock is free but row says `running` | next start of that source marks it `failed` ("abandoned") | `test_a_dead_sessions_lock_is_released_by_postgres`, `test_stale_running_rows_are_abandoned_when_the_lock_is_free` |
| API stopped with runs active | lifespan exit | runs drain and are recorded `cancelled` | `test_shutdown_drains_active_runs` |
| Second Ctrl-C | signal handler | immediate cancel; the in-progress batch still lands and is counted; `cancelled` | `test_second_sigint_forces_immediate_exit`, `test_forced_cancellation_lets_the_in_progress_batch_land` |


## Limitations / non-goals

- A record whose retries are exhausted fails the whole run (by design: the source is unhealthy). An
  idempotent re-run converges, but there's no per-run dead-letter cap or circuit breaker yet.
- On a *forced* stop, records buffered in workers but not yet being written are lost (re-fetched on
  the next run). A graceful drain flushes them. A killed process (SIGKILL) leaves a `running`
  row until the next run of that source marks it abandoned.
- API-triggered runs share the API's event loop: a CPU-heavy run would slow API responses (fine for this
  I/O-bound workload). Gauges are sampled every 0.25 s, so sub-interval spikes can be missed.
- Signal handling uses `loop.add_signal_handler`, which is Unix-only (Linux/macOS/Docker).
- A single `httpx.AsyncClient` stops scaling at about 20 connections on this laptop (httpcore pool overhead).
- GitHub backfills oldest-first at `GITHUB_MAX_PAGES` × 100 issues per repo per run. A primary rate-limit
  reset more than 60 s away fails the run rather than waiting. A drained run advances no watermark.
- Benchmarks so far describe one laptop (mock) and one moment of the internet (real).
- Weather data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0), used on its free, non-commercial tier.
- Non-goals: a universal "any API" connector, scraping, anti-bot evasion, a UI, auth/multi-tenancy,
  Kafka/Celery/Airflow.

## Project layout

```
src/ingest/
  __main__.py        CLI (migrate, run hn|github|open-meteo|all, replay, serve)
  config.py          settings from env (one block per source)
  connector.py       the connector contract (Job, Page, Record, Connector protocol)
  connectors/        hn.py, github.py, open_meteo.py, registry.py
  models/            strict schemas: hn.py, github.py, open_meteo.py
  runner.py          runs sources: per-source clients, sized DB pool, signals, advisory locks
  pipeline.py        concurrent runner (any connector)
  sequential.py      one-at-a-time runner (baseline)
  records.py         shared per-record rules: retries, rejects, classify, watermarks
  retry.py           backoff with full jitter, Retry-After, rate-limit gate + hook
  shutdown.py        SIGINT/SIGTERM → drain / cancel
  replay.py          dead-letter replay (per source)
  api.py             FastAPI status/trigger layer
  observability.py   logging setup, latency percentiles, gauges, live-run registry
  storage.py         items upsert, dead letters, ingestion_runs, watermarks
  httpclient.py      per-source httpx client factory
  db.py, migrate.py  asyncpg pool; migration runner;  migrations/*.sql  the schema
  toy_pipeline.py    Phase 0 simulation
benchmarks/          mock HN server (with fault injection), benchmark harness, pool-exhaustion experiment
tests/               pytest (asyncio auto mode, respx, real Postgres)
compose.yaml         Postgres 16 for dev/tests
docs/                BENCHMARKS.md (methodology) + benchmarks/results/*.json (raw results)
```
