-- Phase 3: runs can be cancelled (graceful shutdown), count retries, and be ingestions or replays.
-- Explainer: docs/explain/ingest/migrations.md   Decisions: ADR-0026, ADR-0021, ADR-0025
-- Constraint name verified in the live schema (Postgres auto-named the inline CHECK).
ALTER TABLE ingestion_runs DROP CONSTRAINT ingestion_runs_status_check;
ALTER TABLE ingestion_runs ADD CONSTRAINT ingestion_runs_status_check
    CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled'));

ALTER TABLE ingestion_runs ADD COLUMN retries integer NOT NULL DEFAULT 0 CHECK (retries >= 0);
ALTER TABLE ingestion_runs ADD COLUMN kind text NOT NULL DEFAULT 'ingest'
    CHECK (kind IN ('ingest', 'replay'));
