-- Rebuild observation rollups so revised observations contribute only their
-- latest revision to weekly/monthly/quarterly aggregates.

SELECT remove_continuous_aggregate_policy('observations_rollup_quarterly', if_exists => TRUE);
SELECT remove_continuous_aggregate_policy('observations_rollup_monthly', if_exists => TRUE);
SELECT remove_continuous_aggregate_policy('observations_rollup_weekly', if_exists => TRUE);

DROP MATERIALIZED VIEW IF EXISTS observations_rollup_quarterly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS observations_rollup_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS observations_rollup_weekly CASCADE;

CREATE MATERIALIZED VIEW observations_rollup_weekly_points
WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = false
) AS
SELECT series_key,
       time_bucket(INTERVAL '1 week', time) AS bucket_time,
       time                                  AS observation_time,
       last(value, revision_no)              AS value,
       max(revision_no)::INTEGER             AS revision_no,
       last(ingested_at, revision_no)        AS ingested_at,
       last(source_artifact_id, revision_no) AS source_artifact_id
FROM   observations
GROUP  BY series_key, time_bucket(INTERVAL '1 week', time), time
WITH NO DATA;

CREATE VIEW observations_rollup_weekly AS
SELECT series_key,
       bucket_time                           AS time,
       avg(value)                            AS value,
       min(value)                            AS min_value,
       max(value)                            AS max_value,
       count(value)::BIGINT                  AS observations_count,
       max(revision_no)::INTEGER             AS revision_no,
       max(ingested_at)                      AS ingested_at,
       last(source_artifact_id, ingested_at) AS source_artifact_id
FROM   observations_rollup_weekly_points
WHERE  value IS NOT NULL
GROUP  BY series_key, bucket_time;

CREATE MATERIALIZED VIEW observations_rollup_monthly_points
WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = false
) AS
SELECT series_key,
       time_bucket(INTERVAL '1 month', time) AS bucket_time,
       time                                   AS observation_time,
       last(value, revision_no)               AS value,
       max(revision_no)::INTEGER              AS revision_no,
       last(ingested_at, revision_no)         AS ingested_at,
       last(source_artifact_id, revision_no)  AS source_artifact_id
FROM   observations
GROUP  BY series_key, time_bucket(INTERVAL '1 month', time), time
WITH NO DATA;

CREATE VIEW observations_rollup_monthly AS
SELECT series_key,
       bucket_time                           AS time,
       avg(value)                            AS value,
       min(value)                            AS min_value,
       max(value)                            AS max_value,
       count(value)::BIGINT                  AS observations_count,
       max(revision_no)::INTEGER             AS revision_no,
       max(ingested_at)                      AS ingested_at,
       last(source_artifact_id, ingested_at) AS source_artifact_id
FROM   observations_rollup_monthly_points
WHERE  value IS NOT NULL
GROUP  BY series_key, bucket_time;

CREATE MATERIALIZED VIEW observations_rollup_quarterly_points
WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = false
) AS
SELECT series_key,
       time_bucket(INTERVAL '3 months', time) AS bucket_time,
       time                                    AS observation_time,
       last(value, revision_no)                AS value,
       max(revision_no)::INTEGER               AS revision_no,
       last(ingested_at, revision_no)          AS ingested_at,
       last(source_artifact_id, revision_no)   AS source_artifact_id
FROM   observations
GROUP  BY series_key, time_bucket(INTERVAL '3 months', time), time
WITH NO DATA;

CREATE VIEW observations_rollup_quarterly AS
SELECT series_key,
       bucket_time                           AS time,
       avg(value)                            AS value,
       min(value)                            AS min_value,
       max(value)                            AS max_value,
       count(value)::BIGINT                  AS observations_count,
       max(revision_no)::INTEGER             AS revision_no,
       max(ingested_at)                      AS ingested_at,
       last(source_artifact_id, ingested_at) AS source_artifact_id
FROM   observations_rollup_quarterly_points
WHERE  value IS NOT NULL
GROUP  BY series_key, bucket_time;

SELECT add_continuous_aggregate_policy(
    'observations_rollup_weekly_points',
    start_offset      => NULL,
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

SELECT add_continuous_aggregate_policy(
    'observations_rollup_monthly_points',
    start_offset      => NULL,
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

SELECT add_continuous_aggregate_policy(
    'observations_rollup_quarterly_points',
    start_offset      => NULL,
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

SELECT alter_job(job_id, next_start => now() + INTERVAL '1 hour')
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_refresh_continuous_aggregate'
AND hypertable_name IN (
    'observations_rollup_weekly_points',
    'observations_rollup_monthly_points',
    'observations_rollup_quarterly_points'
)
AND scheduled;
