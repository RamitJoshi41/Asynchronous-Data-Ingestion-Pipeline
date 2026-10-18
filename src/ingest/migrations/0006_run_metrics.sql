-- Full run metrics (ADR-0038). Nullable: rows from earlier phases (and sequential runs, for gauges)
-- simply don't have them. Explainer: docs/explain/ingest/migrations.md
ALTER TABLE ingestion_runs
    ADD COLUMN duration_ms      integer CHECK (duration_ms >= 0),
    ADD COLUMN requests         integer CHECK (requests >= 0),  -- HTTP attempts, retries included
    ADD COLUMN latency_p50_ms   double precision CHECK (latency_p50_ms >= 0),
    ADD COLUMN latency_p95_ms   double precision CHECK (latency_p95_ms >= 0),
    ADD COLUMN latency_max_ms   double precision CHECK (latency_max_ms >= 0),
    ADD COLUMN queue_depth_peak integer CHECK (queue_depth_peak >= 0),
    ADD COLUMN queue_depth_mean double precision CHECK (queue_depth_mean >= 0),
    ADD COLUMN in_flight_peak   integer CHECK (in_flight_peak >= 0),
    ADD COLUMN in_flight_mean   double precision CHECK (in_flight_mean >= 0);
