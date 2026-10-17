-- One row per pipeline run: written at start (status 'running') and finalised at the end.
-- Explainer: docs/explain/ingest/migrations.md   Decision: ADR-0011
CREATE TABLE ingestion_runs (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source          text        NOT NULL,
    status          text        NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    items_fetched   integer     NOT NULL DEFAULT 0 CHECK (items_fetched >= 0),
    items_inserted  integer     NOT NULL DEFAULT 0 CHECK (items_inserted >= 0),
    items_updated   integer     NOT NULL DEFAULT 0 CHECK (items_updated >= 0),
    items_unchanged integer     NOT NULL DEFAULT 0 CHECK (items_unchanged >= 0),
    items_invalid   integer     NOT NULL DEFAULT 0 CHECK (items_invalid >= 0),
    items_missing   integer     NOT NULL DEFAULT 0 CHECK (items_missing >= 0),
    error           text,
    -- A run is finished exactly when it is no longer 'running'.
    CONSTRAINT finished_iff_not_running CHECK ((status = 'running') = (finished_at IS NULL))
);

-- "Latest runs for a source" is the main read pattern (GET /runs in Phase 5).
CREATE INDEX ingestion_runs_source_started_idx ON ingestion_runs (source, started_at DESC);
