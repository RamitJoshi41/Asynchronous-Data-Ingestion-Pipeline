# Benchmarks

**Not yet measured.**

Planned (CLAUDE.md §10, Phase 1 baseline, Phase 6 full run): sequential vs async at concurrency
5 / 20 / 50, measuring throughput (records/s) and error rate, against a local mock server plus one
small real-API run. Raw results will be stored as JSON under `docs/benchmarks/results/` with date,
machine info, parameters, and commit hash.

The Phase 0 toy pipeline is a simulation (`asyncio.sleep`). Its timings aren't benchmarks and aren't
recorded here.
