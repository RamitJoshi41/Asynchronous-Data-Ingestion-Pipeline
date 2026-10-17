-- Validated records from every source, one generic table (ADR-0006).
-- Explainer: docs/explain/ingest/migrations.md
CREATE TABLE items (
    source              text        NOT NULL,
    source_id           text        NOT NULL,
    -- The VALIDATED model serialised to JSON, not the raw HTTP body.
    payload             jsonb       NOT NULL,
    first_seen_at       timestamptz NOT NULL DEFAULT now(),
    -- Moves only when the payload actually changes (skip-unchanged upsert, ADR-0007).
    updated_at          timestamptz NOT NULL DEFAULT now(),
    last_changed_run_id bigint      NOT NULL REFERENCES ingestion_runs (id),
    PRIMARY KEY (source, source_id)
);
