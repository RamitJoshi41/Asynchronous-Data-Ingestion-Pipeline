# Benchmarks

Only numbers produced by `benchmarks/run.py` appear here. Each one links to its raw JSON in
`docs/benchmarks/results/`. Harness design: [ADR-0012](adr/0012-benchmark-harness.md), explainer:
[explain/benchmarks/run.md](explain/benchmarks/run.md).

## Methodology

- **Workload:** Hacker News top stories. Fetch the id list, then for each id: GET the item →
  strict Pydantic validation → skip-unchanged upsert into Postgres, one row per statement.
- **Timing:** wall time of `run_hn_sequential` (`time.perf_counter`), covering fetching,
  validating, and writing. It excludes pool creation and migrations.
- **Starting state:** every repeat writes into a fresh, empty schema, so every item takes the insert path.
- **Mock target:** `benchmarks/mock_hn.py` in a separate process on `127.0.0.1`, synthetic valid
  stories, a fixed injected latency per request (`asyncio.sleep`), pre-encoded JSON.
- **Real target:** `https://hacker-news.firebaseio.com/v0`, capped at 30 items and 1 repeat to be polite.
- **Database:** compose `postgres:16` (16.15) on the same laptop.
- **Metrics:** items/s = ingested / duration. Error rate = (requested − ingested) / requested.
  Medians over repeats.

## Phase 1: sequential baseline (2026-10-17)

Machine: 11th Gen Intel Core i5-11400H @ 2.70GHz, 12 logical CPUs, Linux 6.8.0-138-generic,
Python 3.12.13. Commit `ef7bab9`.

| Target | Items | Latency | Repeats | Median duration | Median throughput | Error rate | Raw |
|---|---|---|---|---|---|---|---|
| mock | 100 | 50 ms injected | 3 | **5.454 s** (5.441 / 5.454 / 5.457) | **18.3 items/s** | 0.0 | [json](benchmarks/results/2026-10-17T08-03-12Z_hn_sequential_mock.json) |
| real HN API | 30 | (internet) | 1 | **12.762 s** | **2.35 items/s** | 0.0 | [json](benchmarks/results/2026-10-17T08-03-35Z_hn_sequential_real.json) |

**Reading the numbers**
- Mock: 101 requests (1 id list + 100 items) × 50 ms = 5.05 s of pure injected latency. The measured
  5.45 s leaves about 0.4 s, roughly 4 ms per item, for local HTTP, validation, and the DB upsert.
  So **about 93% of the sequential run is waiting**. That's the time Phase 2's concurrency can overlap.
- Real: about 0.41 s per item from this laptop at that moment. Real per-request latency is about 8×
  the mock's 50 ms, so the real API is even more wait-dominated.

**Caveats (read before quoting)**
- The mock number describes **this laptop with a fixed 50 ms delay**, not the internet. There's no
  latency variance, throttling, or TLS.
- The real number is **one run, at one moment** (2026-10-17 08:03 UTC), over one home network. It
  isn't a stable property of HN or of the code.
- The real-run JSON records `"dirty": true`. The only uncommitted file at that time was the mock
  result JSON written 23 s earlier, and the measured code was exactly `ef7bab9`. The harness now
  ignores its own results directory when computing the dirty flag.
- 3 repeats is a small sample. It's fine for a baseline that later phases are expected to beat by
  multiples, not by percents.

## Planned

- Phase 2: the same harness with a concurrent runner, before and after, against this baseline.
- Phase 6: sequential vs concurrency 5 / 20 / 50, throughput and error rate, mock plus one small real run.
