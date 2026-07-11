-- Audited operator controls for source admission, replay, and reviewed manual inputs.

CREATE TABLE source_dataflow_controls (
    dataflow_id TEXT PRIMARY KEY REFERENCES dataflows(id) ON DELETE CASCADE,
    paused      BOOLEAN NOT NULL DEFAULT FALSE,
    actor       TEXT NOT NULL CHECK (char_length(btrim(actor)) > 0),
    reason      TEXT NOT NULL CHECK (char_length(btrim(reason)) > 0),
    paused_at   TIMESTAMPTZ,
    resumed_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((paused AND paused_at IS NOT NULL) OR NOT paused)
);

CREATE INDEX source_dataflow_controls_paused_idx
ON source_dataflow_controls (dataflow_id)
WHERE paused;

CREATE TABLE operator_audit_log (
    id          BIGSERIAL PRIMARY KEY,
    action      TEXT NOT NULL CHECK (char_length(btrim(action)) > 0),
    target_type TEXT NOT NULL CHECK (char_length(btrim(target_type)) > 0),
    target_id   TEXT NOT NULL CHECK (char_length(btrim(target_id)) > 0),
    actor       TEXT NOT NULL CHECK (char_length(btrim(actor)) > 0),
    reason      TEXT NOT NULL CHECK (char_length(btrim(reason)) > 0),
    details     JSONB NOT NULL DEFAULT '{}'::JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX operator_audit_log_target_idx
ON operator_audit_log (target_type, target_id, occurred_at DESC);

CREATE TABLE manual_input_reviews (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    generation_id   UUID NOT NULL UNIQUE
                    REFERENCES ingestion_generations(id) ON DELETE RESTRICT,
    artifact_id     BYTEA NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT
                    CHECK (octet_length(artifact_id) = 32),
    dataflow_id     TEXT NOT NULL REFERENCES dataflows(id) ON DELETE RESTRICT,
    source_url      TEXT NOT NULL,
    license         TEXT NOT NULL CHECK (char_length(btrim(license)) > 0),
    retrieved_at    DATE NOT NULL,
    reviewer_role   TEXT NOT NULL CHECK (char_length(btrim(reviewer_role)) > 0),
    reviewed_at     DATE NOT NULL,
    evidence_notes  TEXT NOT NULL CHECK (char_length(btrim(evidence_notes)) > 0),
    actor           TEXT NOT NULL CHECK (char_length(btrim(actor)) > 0),
    reason          TEXT NOT NULL CHECK (char_length(btrim(reason)) > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX manual_input_reviews_dataflow_idx
ON manual_input_reviews (dataflow_id, reviewed_at DESC);
