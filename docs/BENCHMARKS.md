# Benchmarks

Only numbers produced by `benchmarks/run.py` appear here. Each one links to its raw JSON in
`docs/benchmarks/results/`. Harness: `benchmarks/run.py`, `benchmarks/mock_hn.py`,
`benchmarks/pool_exhaustion.py`.

**Commit provenance.** Each JSON records the commit it was measured at. Since Phase 3 it also records
`trees` (content hashes of `src/`, `benchmarks/`, `pyproject.toml`, `uv.lock`), which survive amends
and rebases. Local history was later
rewritten (2026-10-17) to keep design notes out of the published repo. Code was unchanged, so the
recorded hashes map to these published commits:

| Hash in JSON | Published commit | Contents |
|---|---|---|
| `ef7bab9` | `d02f9c4` | benchmark harness skeleton (Phase 1 results) |
| `97f0bc9` | `ad43183` | concurrent mode + pool experiment (Phase 2 results) |
| `95847d9` | the Phase 4 commit (amended to add these results; trees identical: `src` 78dbf77, `benchmarks` a5d1f21) | Phase 4 regression results |
| `bf844cd` | the Phase 5 commit (amended to add results; `src` tree e924aab identical) | Phase 5 regression result |
| `1f14b12` | the Phase 6 commit (amended to add results and README; `src`/`benchmarks` trees identical) | Phase 6 final matrix |

## Methodology

- **Workload:** Hacker News top stories. Fetch the id list, then for each id: GET the item →
  strict Pydantic validation → skip-unchanged upsert into Postgres. Sequential mode writes one row
  per statement. Concurrent mode (Phase 2) uses C fetchers, W workers, and batched `unnest()` upserts.
- **Timing:** wall time of `run_hn_sequential` / `run_hn_concurrent` (Phase 4: `run_sequential` /
  `run_concurrent` with the HN connector) (`time.perf_counter`),
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

## Phase 3: reliability under injected faults (2026-10-17)

Same machine. Measured at commit `30a038f`, before that commit was amended to include these results.
Each result also records content hashes (`trees.src = 935cf7b374…`), which stay valid across amends:
verify with `git rev-parse <commit>:src`. Mock latency 50 ms, N = 200, concurrency 20, 3 repeats.
Retry policy: 4 attempts, full jitter (base 0.5 s, cap 10 s), Retry-After honoured (≤ 60 s).

**1. No regression on a healthy source**
([json](benchmarks/results/2026-10-17T12-39-46Z_hn_concurrent_c20_mock.json))

| Run | Median duration | Throughput | Error rate |
|---|---|---|---|
| Phase 2, C = 20 | 0.658 s | 303.9 items/s | 0.0 |
| Phase 3, C = 20 (retries, dead letters, transactional flush, safe flush) | **0.630 s** (0.629 / 0.630 / 0.631) | **317.7 items/s** | 0.0 |

The reliability layer costs nothing measurable when nothing fails. The small improvement is within
run-to-run noise between sessions.

**2. Injected faults: 10% of item requests answer 500, 1% answer 429 with `Retry-After: 1`** (seed 7)

| Retry budget | Repeat 1 | Repeat 2 | Repeat 3 | Error rate | Raw |
|---|---|---|---|---|---|
| 1 attempt (no retries) | failed | failed | failed | **1.0** | [json](benchmarks/results/2026-10-17T12-40-07Z_hn_concurrent_c20_faults_a1_mock.json) |
| 4 attempts | failed (item 4: 500 ×4) | **2.285 s, 24 retries** | **4.216 s, 27 retries** | 0.333 | [json](benchmarks/results/2026-10-17T12-39-57Z_hn_concurrent_c20_faults_a4_mock.json) |

**Reading the numbers**
- **Without retries the pipeline is useless under this fault rate:** with ~200 requests at 11% failure,
  the first non-dead-letterable failure aborts the run (fail-fast, ADR-0022). Every run failed.
- **With retries, 2 of 3 runs ingested all 200 items**, retrying 24–27 requests (≈ 11–12% of
  requests, matching the injected rate). It took **3.6–6.7× longer** than the healthy 0.63 s. Most of
  that is the shared 429 gate: each `Retry-After: 1` pauses *all* fetchers for a second.
- **The failed repeat is the retry *budget* working as designed:** one item got four 500s in a row.
  The expected chance per 200-item run is about 200 × 0.11⁴ ≈ 3%. In an ad-hoc follow-up (a scratch
  script, not stored as a result file) 1 of 15 runs failed the same way, with up to 3 retries logged
  for the failing item. A run that exhausts retries fails loudly instead of writing a partial
  dataset, and an idempotent re-run converges (ADR-0022 discusses the dead-letter-on-exhaustion alternative).
- The harness summary takes medians over all repeats, with a failed repeat counted as 0 s / 0 items/s.
  That's why the per-repeat columns are shown here.

**Caveats:** 3 repeats per configuration, and the fault pattern is seeded but interleaving-dependent.
The mock's faults are independent per request, while real outages are bursty and correlated.

## Phase 4: regression check after the connector refactor (2026-10-17)

Phase 4 made the pipeline generic (HN now runs through `HackerNewsConnector`: jobs → async-generator
pages → records). Question: did the abstraction cost throughput? Same mock, 200 items, 50 ms latency,
measured at commit `95847d9` (clean tree), with content hashes recorded in the JSON.

| Run | Repeats (s) | Median | Throughput | Client CPU (s) | Raw |
|---|---|---|---|---|---|
| Phase 3, C = 20 | 0.630 / 0.629 / 0.631 | 0.630 s | 317.7 items/s | 0.20–0.22 | (Phase 3 file) |
| Phase 4, C = 20, set 1 | 0.651 / 0.656 / 0.630 | **0.651 s** | 307.4 items/s | 0.23–0.27 | [json](benchmarks/results/2026-10-17T13-43-26Z_hn_concurrent_c20_mock.json) |
| Phase 4, C = 20, set 2 | 0.627 / 0.673 / 0.621 | **0.627 s** | 319.2 items/s | 0.19–0.26 | [json](benchmarks/results/2026-10-17T13-44-30Z_hn_concurrent_c20_mock.json) |
| Phase 2, sequential | 11.577 / 11.564 / 11.641 | 11.577 s | 17.3 items/s | 0.79–0.83 | (Phase 2 file) |
| Phase 4, sequential | 11.479 / 11.304 / 11.235 | **11.304 s** | 17.7 items/s | 0.57–0.70 | [json](benchmarks/results/2026-10-17T13-43-33Z_hn_sequential_mock.json) |

**Reading the numbers:** the two Phase 4 C = 20 sets (0.651 and 0.627 s) bracket Phase 3's 0.630 s, and
their spread (0.621–0.673 s) is larger than the difference, so there's **no measurable cost** at this
resolution. Client CPU may be marginally higher (async-generator steps per page), within noise. The
sequential baseline now builds its client with `concurrency=1` (it was the default 10 before), which
doesn't matter for one request at a time. GitHub and Open-Meteo aren't benchmarked: their runs are
quota-bound (GitHub 60 req/h unauthenticated), not throughput-bound. Their real smoke runs (600 issues,
21 forecast rows, JOURNAL) are functional checks, not measurements.

## Phase 5: regression check with metrics and gauges on (2026-10-18)

Every HTTP attempt is now timed and a sampler task reads queue depth and in-flight every 250 ms. Same
mock workload, C = 20: repeats 0.637 / 0.655 / 0.620 s, **median 0.637 s** (313.9 items/s), client CPU
0.21–0.22 s, [json](benchmarks/results/2026-10-18T08-19-07Z_hn_concurrent_c20_mock.json). That's inside
Phase 4's spread (0.621–0.673 s), so there's no measurable cost.

## Phase 6: final matrix (2026-10-18)

Measured at commit `1f14b12` (clean tree), mock with 50 ms latency, 200 items, 3 repeats, medians; plus one
small real-API run. **Machine state: on battery, `powersave` governor, CPU at ~2.7 of 4.5 GHz** (read from
`/sys` right after the run). Earlier phases didn't record power state.

| Mode | Repeats (s) | Median | Throughput | Speedup | Client CPU (s) | Error rate | Raw |
|---|---|---|---|---|---|---|---|
| sequential | 11.616 / 11.423 / 11.721 | **11.616 s** | 17.2 items/s | 1.0× | 0.68–0.84 | 0.0 | [json](benchmarks/results/2026-10-18T09-05-45Z_hn_sequential_mock.json) |
| concurrent C = 5 | 2.332 / 2.360 / 2.350 | **2.350 s** | 85.1 items/s | **4.9×** | 0.51–0.60 | 0.0 | [json](benchmarks/results/2026-10-18T09-06-21Z_hn_concurrent_c5_mock.json) |
| concurrent C = 20 | 0.700 / 0.708 / 0.681 | **0.700 s** | 285.7 items/s | **16.6×** | 0.36–0.38 | 0.0 | [json](benchmarks/results/2026-10-18T09-06-29Z_hn_concurrent_c20_mock.json) |
| concurrent C = 50 | 1.427 / 1.409 / 1.336 | **1.409 s** | 141.9 items/s | 8.2× | 1.23–1.32 | 0.0 | [json](benchmarks/results/2026-10-18T09-06-33Z_hn_concurrent_c50_mock.json) |
| real HN API, C = 5 | 4.229 (1 run) | 4.229 s | 7.09 items/s | | 0.15 | 0.0 | [json](benchmarks/results/2026-10-18T09-06-38Z_hn_concurrent_c5_real.json) |

**Reading the numbers**
- The shape matches Phase 2: near-linear speedup to C = 20, then a *slowdown* at C = 50, where the client
  becomes CPU-bound in httpcore's O(connections²) pool bookkeeping (profiled in Phase 2).
- Absolute numbers are worse than Phase 5's same-code C = 20 (0.637 s, 0.21 s CPU): the only source change
  since then is a config validator (`git diff c849fc2 1f14b12 -- src`). Client CPU *seconds* roughly doubled
  for the same work, which fits a throttled CPU. C = 50, the CPU-bound case, suffers most (8.2× vs Phase 2's
  14.0×). Treat this table as "the same laptop on battery". Re-run on AC power with the commands in
  Methodology to compare.
- Error rate is 0 everywhere. The fault-injection results (Phase 3) are the reliability evidence.
- Real run: 30 items at C = 5 in 4.23 s (Phase 2: 3.93 s; Phase 1 sequential: 12.76 s). Internet timing on
  different days, so indicative only.

## Planned

- Phase 6: the full matrix (sequential vs 5 / 20 / 50) re-run at the end, plus one small real run,
  and possibly the httpx default-limits vs aligned-limits experiment (ADR-0019).
- Possible: summary statistics that exclude failed repeats (success-only medians + failure count).
