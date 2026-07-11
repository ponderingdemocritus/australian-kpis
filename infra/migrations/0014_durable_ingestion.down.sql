ALTER TABLE parse_errors
DROP COLUMN IF EXISTS ingestion_generation_id;

ALTER TABLE artifact_loads
DROP COLUMN IF EXISTS ingestion_generation_id;

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

DROP INDEX IF EXISTS observations_generation_idx;

ALTER TABLE observations
DROP COLUMN IF EXISTS ingestion_generation_id;

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

DROP TABLE IF EXISTS observation_stage;
DROP TABLE IF EXISTS ingestion_generations;

ALTER TABLE artifact_fetches
DROP CONSTRAINT IF EXISTS artifact_fetches_id_source_key;

DROP TABLE IF EXISTS discovered_work;
DROP TABLE IF EXISTS queue_schedule_occurrences;

DROP INDEX IF EXISTS queue_cron_schedules_due_idx;

ALTER TABLE queue_cron_schedules
DROP COLUMN IF EXISTS last_enqueued_at,
DROP COLUMN IF EXISTS next_run_at,
DROP COLUMN IF EXISTS timezone;

DROP INDEX IF EXISTS queue_jobs_active_dedupe_idx;

ALTER TABLE queue_jobs
DROP COLUMN IF EXISTS dedupe_key;
