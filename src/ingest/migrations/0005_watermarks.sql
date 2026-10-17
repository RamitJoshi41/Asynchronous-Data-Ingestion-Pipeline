-- Per-source incremental-ingestion progress (ADR-0031). One row per stream (e.g. GitHub repo).
-- Written only by a SUCCEEDED run, in the same transaction that marks it succeeded.
-- Explainer: docs/explain/ingest/migrations.md
CREATE TABLE source_watermarks (
    source     text        NOT NULL,
    key        text        NOT NULL,
    -- Opaque to the database; connectors emit values that sort as text in time order.
    value      text        NOT NULL,
    run_id     bigint      NOT NULL REFERENCES ingestion_runs (id),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, key)
);
