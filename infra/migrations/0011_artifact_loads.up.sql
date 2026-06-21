CREATE TABLE artifact_loads (
    artifact_id          BYTEA       NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE
                         CHECK (octet_length(artifact_id) = 32),
    source_id            TEXT        NOT NULL REFERENCES sources(id),
    dataflow_id          TEXT        NOT NULL REFERENCES dataflows(id),
    observations_parsed  BIGINT      NOT NULL CHECK (observations_parsed >= 0),
    observations_loaded  BIGINT      NOT NULL CHECK (observations_loaded >= 0),
    job_id               TEXT,
    trace_parent         TEXT,
    completed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (artifact_id, dataflow_id)
);

CREATE INDEX artifact_loads_source_dataflow_idx
ON artifact_loads (source_id, dataflow_id, completed_at DESC);
