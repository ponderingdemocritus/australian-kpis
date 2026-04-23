-- Initial schema for australian-kpis.
-- See `Spec.md § Data model` and `Spec.md § Database schema`.
--
-- This migration is reversible (`0001_init.down.sql`) and idempotent
-- under `revert → run`: the down script drops every object created
-- here, so re-running this script after a revert yields the same
-- schema as the first application.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- Reference / metadata tables (vanilla Postgres).
-- ---------------------------------------------------------------------------

CREATE TABLE sources (
    id          TEXT PRIMARY KEY
                CHECK (char_length(id) BETWEEN 1 AND 128),
    name        TEXT NOT NULL,
    homepage    TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE codelists (
    id          TEXT PRIMARY KEY
                CHECK (char_length(id) BETWEEN 1 AND 128),
    name        TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE codes (
    codelist_id TEXT NOT NULL REFERENCES codelists(id) ON DELETE CASCADE,
    id          TEXT NOT NULL
                CHECK (char_length(id) BETWEEN 1 AND 128),
    name        TEXT NOT NULL,
    description TEXT,
    parent_id   TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (codelist_id, id),
    FOREIGN KEY (codelist_id, parent_id) REFERENCES codes(codelist_id, id)
);
CREATE INDEX codes_codelist_idx ON codes (codelist_id);

CREATE TABLE measures (
    id          TEXT PRIMARY KEY
                CHECK (char_length(id) BETWEEN 1 AND 128),
    name        TEXT NOT NULL,
    description TEXT,
    unit        TEXT NOT NULL,
    scale       DOUBLE PRECISION,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dataflows (
    id           TEXT PRIMARY KEY
                 CHECK (char_length(id) BETWEEN 1 AND 128),
    source_id    TEXT NOT NULL REFERENCES sources(id),
    name         TEXT NOT NULL,
    description  TEXT,
    dimensions   TEXT[] NOT NULL,
    measures     TEXT[] NOT NULL,
    frequency    TEXT NOT NULL
                 CHECK (frequency IN ('daily','weekly','monthly','quarterly','annual','irregular')),
    license      TEXT NOT NULL,
    attribution  TEXT NOT NULL,
    source_url   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX dataflows_source_idx ON dataflows (source_id);

CREATE TABLE dimensions (
    dataflow_id TEXT NOT NULL REFERENCES dataflows(id) ON DELETE CASCADE,
    id          TEXT NOT NULL
                CHECK (char_length(id) BETWEEN 1 AND 128),
    name        TEXT NOT NULL,
    description TEXT,
    codelist_id TEXT NOT NULL REFERENCES codelists(id),
    position    SMALLINT NOT NULL CHECK (position >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataflow_id, id),
    UNIQUE (dataflow_id, position)
);

-- ---------------------------------------------------------------------------
-- Series — metadata per time series; JSONB dimensions with GIN index.
-- ---------------------------------------------------------------------------

CREATE TABLE series (
    series_key     BYTEA PRIMARY KEY
                   CHECK (octet_length(series_key) = 32),
    dataflow_id    TEXT NOT NULL REFERENCES dataflows(id),
    measure_id     TEXT NOT NULL REFERENCES measures(id),
    dimensions     JSONB NOT NULL,
    unit           TEXT NOT NULL,
    first_observed TIMESTAMPTZ,
    last_observed  TIMESTAMPTZ,
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX series_dataflow_idx ON series (dataflow_id);
CREATE INDEX series_dimensions_gin ON series USING GIN (dimensions jsonb_path_ops);
CREATE INDEX series_active_idx ON series (dataflow_id) WHERE active;

-- ---------------------------------------------------------------------------
-- Artifacts — raw upstream files, content-addressed (sha256).
-- ---------------------------------------------------------------------------

CREATE TABLE artifacts (
    id           BYTEA PRIMARY KEY
                 CHECK (octet_length(id) = 32),
    source_id    TEXT NOT NULL REFERENCES sources(id),
    source_url   TEXT NOT NULL,
    content_type TEXT NOT NULL,
    size_bytes   BIGINT NOT NULL CHECK (size_bytes >= 0),
    storage_key  TEXT NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX artifacts_source_idx ON artifacts (source_id);

-- ---------------------------------------------------------------------------
-- Observations — narrow hot table, Timescale hypertable.
-- PK matches the composite `ObservationId` natural key.
-- ---------------------------------------------------------------------------

CREATE TABLE observations (
    series_key         BYTEA       NOT NULL REFERENCES series(series_key),
    time               TIMESTAMPTZ NOT NULL,
    revision_no        INTEGER     NOT NULL DEFAULT 0 CHECK (revision_no >= 0),
    time_precision     TEXT        NOT NULL
                       CHECK (time_precision IN ('day','week','month','quarter','year')),
    value              DOUBLE PRECISION,
    status             TEXT        NOT NULL
                       CHECK (status IN ('normal','estimated','forecast','imputed',
                                         'missing','provisional','revised','break')),
    attributes         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_artifact_id BYTEA       NOT NULL REFERENCES artifacts(id),
    PRIMARY KEY (series_key, time, revision_no)
);

-- Convert to hypertable: chunk on `time` every 1 month,
-- plus a hash space partition on `series_key` so heavy series
-- don't hotspot a single chunk.
SELECT create_hypertable(
    'observations',
    'time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists       => TRUE
);
SELECT add_dimension(
    'observations',
    'series_key',
    number_partitions => 4,
    if_not_exists     => TRUE
);

-- Latest-revision view: the default query target for reads.
-- Full `observations` retains the revision chain for audit/history.
CREATE VIEW observations_latest AS
SELECT DISTINCT ON (series_key, time)
       series_key,
       time,
       revision_no,
       time_precision,
       value,
       status,
       attributes,
       ingested_at,
       source_artifact_id
FROM   observations
ORDER BY series_key, time, revision_no DESC;

-- Compression: keep recent chunks hot; compress everything >7 days old.
ALTER TABLE observations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_key',
    timescaledb.compress_orderby   = 'time DESC, revision_no DESC'
);
SELECT add_compression_policy(
    'observations',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------------
-- Parse errors — rows that failed validation, linked back to the artifact.
-- ---------------------------------------------------------------------------

CREATE TABLE parse_errors (
    id            BIGSERIAL PRIMARY KEY,
    artifact_id   BYTEA       NOT NULL REFERENCES artifacts(id),
    error_kind    TEXT        NOT NULL,
    error_message TEXT        NOT NULL,
    row_context   JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX parse_errors_artifact_idx ON parse_errors (artifact_id);

-- ---------------------------------------------------------------------------
-- API keys — argon2id-hashed; scopes + tier drive authz + rate limits.
-- ---------------------------------------------------------------------------

CREATE TABLE api_keys (
    id              UUID PRIMARY KEY,
    key_hash        TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    scopes          TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    rate_limit_tier TEXT NOT NULL DEFAULT 'free',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at    TIMESTAMPTZ,
    revoked_at      TIMESTAMPTZ
);
