-- Durable production-v1 scheduling and ingestion stage state.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE queue_cron_schedules
ADD COLUMN timezone TEXT NOT NULL DEFAULT 'UTC'
    CHECK (char_length(timezone) BETWEEN 1 AND 128),
ADD COLUMN next_run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
ADD COLUMN last_enqueued_at TIMESTAMPTZ;

ALTER TABLE queue_jobs
ADD COLUMN dedupe_key TEXT;

CREATE UNIQUE INDEX queue_jobs_active_dedupe_idx
ON queue_jobs (dedupe_key)
WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'running');

CREATE INDEX queue_cron_schedules_due_idx
ON queue_cron_schedules (next_run_at, id)
WHERE enabled;

CREATE TABLE queue_schedule_occurrences (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schedule_id   TEXT NOT NULL
                  REFERENCES queue_cron_schedules(id) ON DELETE CASCADE,
    scheduled_for TIMESTAMPTZ NOT NULL,
    job_id        BIGINT NOT NULL UNIQUE
                  REFERENCES queue_jobs(id) ON DELETE RESTRICT,
    status        TEXT NOT NULL DEFAULT 'enqueued'
                  CHECK (status IN ('enqueued', 'completed', 'failed')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (schedule_id, scheduled_for)
);

CREATE INDEX queue_schedule_occurrences_schedule_idx
ON queue_schedule_occurrences (schedule_id, scheduled_for DESC);

CREATE TABLE discovered_work (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    occurrence_id      UUID
                       REFERENCES queue_schedule_occurrences(id) ON DELETE SET NULL,
    source_id          TEXT NOT NULL REFERENCES sources(id),
    dataflow_id        TEXT NOT NULL REFERENCES dataflows(id),
    source_url         TEXT NOT NULL,
    upstream_revision  TEXT NOT NULL,
    identity_key       BYTEA NOT NULL UNIQUE
                       CHECK (octet_length(identity_key) = 32),
    status             TEXT NOT NULL DEFAULT 'pending_fetch'
                       CHECK (status IN (
                           'pending_fetch', 'fetching', 'fetched', 'handled',
                           'rejected'
                       )),
    discovery_metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    discovered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    fetched_at         TIMESTAMPTZ,
    handled_at         TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, source_id, dataflow_id)
);

CREATE INDEX discovered_work_status_idx
ON discovered_work (status, discovered_at, id)
WHERE status IN ('pending_fetch', 'fetching', 'fetched');

CREATE INDEX discovered_work_dataflow_idx
ON discovered_work (dataflow_id, discovered_at DESC);

CREATE TABLE ingestion_generations (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    discovered_work_id   UUID NOT NULL,
    artifact_fetch_id    BIGINT NOT NULL REFERENCES artifact_fetches(id),
    source_id            TEXT NOT NULL REFERENCES sources(id),
    dataflow_id          TEXT NOT NULL REFERENCES dataflows(id),
    parser_version       TEXT NOT NULL,
    transform_version    TEXT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'pending_parse'
                         CHECK (status IN (
                             'pending_parse', 'parsing', 'parsed_clean',
                             'parsed_partial', 'rejected', 'pending_load',
                             'loading', 'published', 'failed'
                         )),
    parsed_count         BIGINT NOT NULL DEFAULT 0 CHECK (parsed_count >= 0),
    loaded_count         BIGINT NOT NULL DEFAULT 0 CHECK (loaded_count >= 0),
    error_count          BIGINT NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    stage_digest         BYTEA CHECK (
                             stage_digest IS NULL OR octet_length(stage_digest) = 32
                         ),
    job_id               BIGINT,
    trace_parent         TEXT,
    actor                TEXT NOT NULL DEFAULT 'system',
    reason               TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    parsing_started_at   TIMESTAMPTZ,
    parsed_at            TIMESTAMPTZ,
    loading_started_at   TIMESTAMPTZ,
    published_at         TIMESTAMPTZ,
    failed_at            TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (artifact_fetch_id, dataflow_id, parser_version, transform_version),
    FOREIGN KEY (discovered_work_id, source_id, dataflow_id)
        REFERENCES discovered_work(id, source_id, dataflow_id)
);

ALTER TABLE artifact_fetches
ADD CONSTRAINT artifact_fetches_id_source_key UNIQUE (id, source_id);

ALTER TABLE ingestion_generations
ADD CONSTRAINT ingestion_generations_fetch_source_fkey
FOREIGN KEY (artifact_fetch_id, source_id)
REFERENCES artifact_fetches(id, source_id) ON DELETE RESTRICT;

CREATE INDEX ingestion_generations_status_idx
ON ingestion_generations (status, updated_at, id)
WHERE status NOT IN ('published', 'rejected', 'failed');

CREATE INDEX ingestion_generations_dataflow_idx
ON ingestion_generations (dataflow_id, created_at DESC);

CREATE UNLOGGED TABLE observation_stage (
    generation_id     UUID NOT NULL
                      REFERENCES ingestion_generations(id) ON DELETE CASCADE,
    row_no            BIGINT NOT NULL CHECK (row_no >= 0),
    series_key        BYTEA NOT NULL CHECK (octet_length(series_key) = 32),
    dataflow_id       TEXT NOT NULL REFERENCES dataflows(id),
    measure_id        TEXT NOT NULL REFERENCES measures(id),
    dimensions        JSONB NOT NULL,
    unit              TEXT NOT NULL,
    time              TIMESTAMPTZ NOT NULL,
    time_precision    TEXT NOT NULL
                      CHECK (time_precision IN (
                          'minute', 'day', 'week', 'month', 'quarter', 'year'
                      )),
    revision_no       INTEGER NOT NULL DEFAULT 0 CHECK (revision_no >= 0),
    value             DOUBLE PRECISION,
    status            TEXT NOT NULL
                      CHECK (status IN (
                          'normal', 'estimated', 'forecast', 'imputed',
                          'missing', 'provisional', 'revised', 'break'
                      )),
    attributes        JSONB NOT NULL DEFAULT '{}'::JSONB,
    ingested_at       TIMESTAMPTZ NOT NULL,
    source_artifact_id BYTEA NOT NULL REFERENCES artifacts(id)
                       CHECK (octet_length(source_artifact_id) = 32),
    PRIMARY KEY (generation_id, row_no)
);

CREATE INDEX observation_stage_series_time_idx
ON observation_stage (generation_id, series_key, time, row_no);

ALTER TABLE observations
ADD COLUMN ingestion_generation_id UUID;

ALTER TABLE observations
ADD CONSTRAINT observations_ingestion_generation_id_fkey
FOREIGN KEY (ingestion_generation_id)
REFERENCES ingestion_generations(id) ON DELETE RESTRICT;

CREATE INDEX observations_generation_idx
ON observations (ingestion_generation_id, time DESC)
WHERE ingestion_generation_id IS NOT NULL;

ALTER TABLE artifact_loads
ADD COLUMN ingestion_generation_id UUID
REFERENCES ingestion_generations(id) ON DELETE RESTRICT;

ALTER TABLE parse_errors
ADD COLUMN ingestion_generation_id UUID
REFERENCES ingestion_generations(id) ON DELETE SET NULL;
