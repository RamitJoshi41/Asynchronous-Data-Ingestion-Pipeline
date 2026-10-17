-- Phase 3: records that could not be ingested, kept with their raw body for inspection and replay.
-- Explainer: docs/explain/ingest/migrations.md   Decision: ADR-0024
CREATE TABLE dead_letter (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source          text        NOT NULL,
    source_id       text        NOT NULL,
    run_id          bigint      NOT NULL REFERENCES ingestion_runs (id),  -- run of the LATEST failure
    reason          text        NOT NULL,  -- invalid | id_mismatch | http_<status>
    errors          jsonb       NOT NULL,  -- ["loc:type", ...] - never raw values
    -- bytea, not jsonb/text: a rejected body may be malformed JSON or not even valid UTF-8.
    raw_body        bytea       NOT NULL,
    attempts        integer     NOT NULL DEFAULT 1 CHECK (attempts >= 1),
    first_failed_at timestamptz NOT NULL DEFAULT now(),
    last_failed_at  timestamptz NOT NULL DEFAULT now(),
    resolved_at     timestamptz,
    resolved_run_id bigint      REFERENCES ingestion_runs (id),
    CONSTRAINT resolved_together CHECK ((resolved_at IS NULL) = (resolved_run_id IS NULL))
);

-- At most ONE open (unresolved) dead letter per record; a repeat failure updates it.
-- Resolved rows are kept as history and don't block a new failure later.
CREATE UNIQUE INDEX dead_letter_open_uq ON dead_letter (source, source_id) WHERE resolved_at IS NULL;
