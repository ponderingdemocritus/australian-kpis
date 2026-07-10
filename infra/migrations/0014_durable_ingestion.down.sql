ALTER TABLE parse_errors
DROP COLUMN IF EXISTS ingestion_generation_id;

ALTER TABLE artifact_loads
DROP COLUMN IF EXISTS ingestion_generation_id;

DROP INDEX IF EXISTS observations_generation_idx;

ALTER TABLE observations
DROP COLUMN IF EXISTS ingestion_generation_id;

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
