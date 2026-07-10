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

-- TimescaleDB 2.17 cannot alter a hypertable while compression is enabled.
-- Existing production chunks must be decompressed before the generation
-- ownership column is added, then the original policy is restored below.
SELECT remove_compression_policy('observations', if_exists => TRUE);

DO $$
DECLARE
    compressed_chunk REGCLASS;
BEGIN
    FOR compressed_chunk IN
        SELECT format('%I.%I', chunk_schema, chunk_name)::REGCLASS
        FROM timescaledb_information.chunks
        WHERE hypertable_schema = 'public'
          AND hypertable_name = 'observations'
          AND is_compressed
    LOOP
        PERFORM decompress_chunk(compressed_chunk, if_compressed => TRUE);
    END LOOP;
END
$$;

ALTER TABLE observations SET (timescaledb.compress = FALSE);

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

-- Every pre-durable observation is assigned to an explicit immutable legacy
-- generation. This preserves provenance without pretending that the original
-- parser/transform versions are known.
INSERT INTO artifact_fetches (
    artifact_id,
    source_id,
    source_url,
    content_type,
    response_headers,
    size_bytes,
    storage_key,
    fetched_at,
    created_at
)
SELECT artifacts.id,
       artifacts.source_id,
       artifacts.source_url,
       artifacts.content_type,
       artifacts.response_headers,
       artifacts.size_bytes,
       artifacts.storage_key,
       artifacts.fetched_at,
       artifacts.created_at
FROM artifacts
WHERE NOT EXISTS (
    SELECT 1
    FROM artifact_fetches
    WHERE artifact_fetches.artifact_id = artifacts.id
      AND artifact_fetches.source_id = artifacts.source_id
)
AND EXISTS (
    SELECT 1
    FROM observations
    WHERE observations.source_artifact_id = artifacts.id
);

CREATE TEMPORARY TABLE migration_0014_legacy_units ON COMMIT DROP AS
SELECT
    observations.source_artifact_id AS artifact_id,
    series.dataflow_id,
    fetches.id AS artifact_fetch_id,
    fetches.source_id,
    fetches.source_url,
    count(*)::BIGINT AS observation_count,
    max(observations.ingested_at) AS published_at
FROM observations
JOIN series ON series.series_key = observations.series_key
JOIN LATERAL (
    SELECT artifact_fetches.id,
           artifact_fetches.source_id,
           artifact_fetches.source_url
    FROM artifact_fetches
    WHERE artifact_fetches.artifact_id = observations.source_artifact_id
    ORDER BY artifact_fetches.id
    LIMIT 1
) AS fetches ON TRUE
GROUP BY observations.source_artifact_id,
         series.dataflow_id,
         fetches.id,
         fetches.source_id,
         fetches.source_url;

INSERT INTO discovered_work (
    source_id,
    dataflow_id,
    source_url,
    upstream_revision,
    identity_key,
    status,
    discovery_metadata,
    discovered_at,
    fetched_at,
    handled_at,
    updated_at
)
SELECT
    source_id,
    dataflow_id,
    source_url,
    'legacy:' || encode(artifact_id, 'hex'),
    digest(
        convert_to(
            'legacy-v1' || E'\n' || source_id || E'\n' || dataflow_id ||
            E'\n' || source_url || E'\n' || encode(artifact_id, 'hex'),
            'UTF8'
        ),
        'sha256'
    ),
    'handled',
    jsonb_build_object(
        'legacy', TRUE,
        'artifact_id', encode(artifact_id, 'hex')
    ),
    published_at,
    published_at,
    published_at,
    published_at
FROM migration_0014_legacy_units
ON CONFLICT (identity_key) DO NOTHING;

WITH legacy_work AS (
    SELECT
        migration_0014_legacy_units.*,
        discovered_work.id AS discovered_work_id
    FROM migration_0014_legacy_units
    JOIN discovered_work ON discovered_work.identity_key = digest(
        convert_to(
            'legacy-v1' || E'\n' || migration_0014_legacy_units.source_id || E'\n' ||
            migration_0014_legacy_units.dataflow_id || E'\n' ||
            migration_0014_legacy_units.source_url || E'\n' ||
            encode(migration_0014_legacy_units.artifact_id, 'hex'),
            'UTF8'
        ),
        'sha256'
    )
)
INSERT INTO ingestion_generations (
    discovered_work_id,
    artifact_fetch_id,
    source_id,
    dataflow_id,
    parser_version,
    transform_version,
    status,
    parsed_count,
    loaded_count,
    error_count,
    actor,
    reason,
    created_at,
    parsed_at,
    published_at,
    updated_at
)
SELECT
    discovered_work_id,
    artifact_fetch_id,
    source_id,
    dataflow_id,
    'legacy-pre-durable-v1',
    'legacy-pre-durable-v1',
    'published',
    observation_count,
    observation_count,
    0,
    'migration-0014',
    'backfill pre-durable observations',
    published_at,
    published_at,
    published_at,
    published_at
FROM legacy_work
ON CONFLICT (artifact_fetch_id, dataflow_id, parser_version, transform_version)
DO NOTHING;

UPDATE observations
SET ingestion_generation_id = ingestion_generations.id
FROM series, ingestion_generations, artifact_fetches
WHERE series.series_key = observations.series_key
  AND artifact_fetches.id = ingestion_generations.artifact_fetch_id
  AND artifact_fetches.artifact_id = observations.source_artifact_id
  AND ingestion_generations.dataflow_id = series.dataflow_id
  AND ingestion_generations.parser_version = 'legacy-pre-durable-v1'
  AND ingestion_generations.transform_version = 'legacy-pre-durable-v1';

DO $$
DECLARE
    unmapped TEXT;
BEGIN
    SELECT string_agg(
        encode(observations.source_artifact_id, 'hex') || ':' || series.dataflow_id,
        ', '
        ORDER BY encode(observations.source_artifact_id, 'hex'), series.dataflow_id
    )
    INTO unmapped
    FROM observations
    JOIN series ON series.series_key = observations.series_key
    WHERE observations.ingestion_generation_id IS NULL;

    IF unmapped IS NOT NULL THEN
        RAISE EXCEPTION
            'migration 0014 could not assign existing observations to legacy generations: %',
            unmapped;
    END IF;
END
$$;

UPDATE artifact_loads
SET ingestion_generation_id = ingestion_generations.id
FROM ingestion_generations
WHERE ingestion_generations.artifact_fetch_id = artifact_loads.artifact_fetch_id
  AND ingestion_generations.dataflow_id = artifact_loads.dataflow_id
  AND ingestion_generations.parser_version = 'legacy-pre-durable-v1'
  AND ingestion_generations.transform_version = 'legacy-pre-durable-v1';

ALTER TABLE observations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_key',
    timescaledb.compress_orderby = 'time DESC, revision_no DESC'
);

SELECT add_compression_policy(
    'observations',
    INTERVAL '7 days',
    if_not_exists => TRUE
);
