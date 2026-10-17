# Benchmarks

Only numbers produced by `benchmarks/run.py` appear here. Each one links to its raw JSON in
`docs/benchmarks/results/`. Harness: `benchmarks/run.py`, `benchmarks/mock_hn.py`,
`benchmarks/pool_exhaustion.py`.

**Commit provenance.** Each JSON records the commit it was measured at. Local history was later
rewritten (2026-10-17) to keep design notes out of the published repo. Code was unchanged, so the
recorded hashes map to these published commits:

| Hash in JSON | Published commit | Contents |
|---|---|---|
| `ef7bab9` | `d02f9c4` | benchmark harness skeleton (Phase 1 results) |
| `97f0bc9` | `ad43183` | concurrent mode + pool experiment (Phase 2 results) |

## Methodology

- **Workload:** Hacker News top stories. Fetch the id list, then for each id: GET the item →
  strict Pydantic validation → skip-unchanged upsert into Postgres. Sequential mode writes one row
  per statement. Concurrent mode (Phase 2) uses C fetchers, W workers, and batched `unnest()` upserts.
- **Timing:** wall time of `run_hn_sequential` / `run_hn_concurrent` (`time.perf_counter`),
  covering fetching, validating, and writing. It excludes pool creation and migrations. Phase 2 also
  records the **client process CPU time** (`time.process_time`) to tell waiting from working.
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

## Phase 2: concurrency, before vs after (2026-10-17)

Same machine as Phase 1. Commit `97f0bc9` (all files `dirty: false`). Mock latency 50 ms, **N = 200**,
3 repeats each. Concurrent config: W = 2 workers, B = 50, flush interval 1 s, raw queue 100, httpx
limits = C, DB pool = W + 1.

| Mode | Median duration | Median throughput | Speedup vs sequential | Client CPU (median repeat) | Error rate | Raw |
|---|---|---|---|---|---|---|
| sequential ("before") | **11.577 s** | **17.3 items/s** | 1.0× | 0.79 s (7% of wall) | 0.0 | [json](benchmarks/results/2026-10-17T11-17-58Z_hn_sequential_mock.json) |
| concurrent C = 5 | **2.279 s** | **87.8 items/s** | **5.1×** | 0.46 s | 0.0 | [json](benchmarks/results/2026-10-17T11-18-41Z_hn_concurrent_c5_mock.json) |
| concurrent C = 20 | **0.658 s** | **303.9 items/s** | **17.6×** | 0.26 s (40%) | 0.0 | [json](benchmarks/results/2026-10-17T11-18-49Z_hn_concurrent_c20_mock.json) |
| concurrent C = 50 | **0.826 s** | **242.1 items/s** | 14.0× (**slower than C = 20**) | 0.71 s (86%) | 0.0 | [json](benchmarks/results/2026-10-17T11-18-52Z_hn_concurrent_c50_mock.json) |

Every run hit its concurrency limit exactly (`peak_in_flight == C`), and no fetcher ever blocked on
the raw queue (`blocked_puts = 0`), so the workers and DB kept up at all three levels.

**Reading the numbers**
- **C = 5 and C = 20 are close to the latency ideal.** With N/C rounds of 50 ms plus the id list, the
  ideal is 2.05 s (C=5) and 0.55 s (C=20). Measured: 2.28 s (90% of ideal) and 0.66 s (84%).
- **C = 50 got slower, and the client is why.** The ideal would be 0.25 s, but the run took 0.83 s
  with the client's CPU busy 86% of the time. It used ~2.7× more CPU than at C = 20 for the *same* 200
  requests. A profiling run (not a benchmark: cProfile inflates times) showed:
  - At C = 50, httpcore's `AsyncConnectionPool._assign_requests_to_connections` accounted for most of
    the client CPU, calling `connection.is_idle()` **≈2.5 million times** (≈52 thousand at C = 20).
  - The mock server process used only **0.08 s of CPU**, so it wasn't the bottleneck.
  - In httpcore 1.0.9 (`_async/connection_pool.py`), that method runs on every request
    arrival/completion, and for *each* connection rebuilds `[c.is_idle() for c in self._connections]`.
    That's O(connections²) work per request event, so pool bookkeeping grows quadratically with C.
  - **Conclusion for this laptop: about C = 20 is the sweet spot.** Past it, the HTTP client's own
    pool management costs more than the extra overlap saves.
- **Latency still dominates at C ≤ 20:** client CPU stays well below wall time.

**Real API (one polite run)**

| Run | Items | Duration | Throughput | Error rate | Raw |
|---|---|---|---|---|---|
| Phase 1 sequential (08:03 UTC) | 30 | 12.762 s | 2.35 items/s | 0.0 | [json](benchmarks/results/2026-10-17T08-03-35Z_hn_sequential_real.json) |
| Phase 2 concurrent C = 5 (11:19 UTC) | 30 | 3.931 s | 7.63 items/s | 0.0 | [json](benchmarks/results/2026-10-17T11-19-50Z_hn_concurrent_c5_real.json) |

About 3.2× faster. **Caveat:** two single runs about 3 hours apart over a home network, so network
conditions differed. Indicative only, not a controlled comparison. A supporting observation, not a
benchmark file: a CLI `run hn --limit 30 --sequential` about 4 minutes after the concurrent run logged
`duration_s=12.773` (the same timer as the harness), consistent with the morning baseline.

**Pool-exhaustion experiment** (`benchmarks/pool_exhaustion.py`, 4 simulated flushes each holding a
connection for 0.2 s via `pg_sleep`, [json](benchmarks/results/2026-10-17T11-19-55Z_pool_exhaustion.json)):

| Scenario | Pool size | Acquire timeout | Wall time | Acquire waits (s) | Timeouts |
|---|---|---|---|---|---|
| sized (W + 1) | 5 | 5 s | 0.209 s | 0.000, 0.004, 0.005, 0.005 | 0 |
| undersized, patient | 1 | 5 s | 0.811 s | 0.000, 0.202, 0.404, 0.608 | 0 |
| undersized, short timeout | 1 | 0.3 s | 0.406 s | 0.000, 0.203 | **2** |

An undersized pool either **serialises** flushes (wall time ≈ W × hold, so concurrency is gone) or,
with a bounded acquire timeout, **fails** them. In the pipeline, a worker stuck in `acquire()`
isn't consuming, the raw queue fills, and fetchers block. Hence ADR-0018: pool ≥ W + 1, validated at startup.

**Caveats:** mock numbers describe this laptop (one client process, one mock server process, local
Postgres) with a fixed 50 ms delay. The C = 50 result is specific to httpcore 1.0.9's pool algorithm
and this CPU. 3 repeats per configuration.

## Planned

- Phase 6: the full matrix (sequential vs 5 / 20 / 50) re-run at the end, plus one small real run,
  and possibly the httpx default-limits vs aligned-limits experiment (ADR-0019).
