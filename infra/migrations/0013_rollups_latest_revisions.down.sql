-- Restore the original direct-observations continuous aggregates.

SELECT remove_continuous_aggregate_policy('observations_rollup_quarterly_points', if_exists => TRUE);
SELECT remove_continuous_aggregate_policy('observations_rollup_monthly_points', if_exists => TRUE);
SELECT remove_continuous_aggregate_policy('observations_rollup_weekly_points', if_exists => TRUE);

DROP VIEW IF EXISTS observations_rollup_quarterly CASCADE;
DROP VIEW IF EXISTS observations_rollup_monthly CASCADE;
DROP VIEW IF EXISTS observations_rollup_weekly CASCADE;

DROP MATERIALIZED VIEW IF EXISTS observations_rollup_quarterly_points CASCADE;
DROP MATERIALIZED VIEW IF EXISTS observations_rollup_monthly_points CASCADE;
DROP MATERIALIZED VIEW IF EXISTS observations_rollup_weekly_points CASCADE;

CREATE MATERIALIZED VIEW observations_rollup_weekly
WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = false
) AS
SELECT series_key,
       time_bucket(INTERVAL '1 week', time) AS time,
       avg(value)                           AS value,
       min(value)                           AS min_value,
       max(value)                           AS max_value,
       count(value)::BIGINT                 AS observations_count,
       max(revision_no)::INTEGER            AS revision_no,
       max(ingested_at)                     AS ingested_at,
       last(source_artifact_id, ingested_at) AS source_artifact_id
FROM   observations
WHERE  value IS NOT NULL
GROUP  BY series_key, time_bucket(INTERVAL '1 week', time)
WITH NO DATA;

CREATE MATERIALIZED VIEW observations_rollup_monthly
WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = false
) AS
SELECT series_key,
       time_bucket(INTERVAL '1 month', time) AS time,
       avg(value)                            AS value,
       min(value)                            AS min_value,
       max(value)                            AS max_value,
       count(value)::BIGINT                  AS observations_count,
       max(revision_no)::INTEGER             AS revision_no,
       max(ingested_at)                      AS ingested_at,
       last(source_artifact_id, ingested_at) AS source_artifact_id
FROM   observations
WHERE  value IS NOT NULL
GROUP  BY series_key, time_bucket(INTERVAL '1 month', time)
WITH NO DATA;

CREATE MATERIALIZED VIEW observations_rollup_quarterly
WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = false
) AS
SELECT series_key,
       time_bucket(INTERVAL '3 months', time) AS time,
       avg(value)                             AS value,
       min(value)                             AS min_value,
       max(value)                             AS max_value,
       count(value)::BIGINT                   AS observations_count,
       max(revision_no)::INTEGER              AS revision_no,
       max(ingested_at)                       AS ingested_at,
       last(source_artifact_id, ingested_at)  AS source_artifact_id
FROM   observations
WHERE  value IS NOT NULL
GROUP  BY series_key, time_bucket(INTERVAL '3 months', time)
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'observations_rollup_weekly',
    start_offset      => NULL,
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

SELECT add_continuous_aggregate_policy(
    'observations_rollup_monthly',
    start_offset      => NULL,
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

SELECT add_continuous_aggregate_policy(
    'observations_rollup_quarterly',
    start_offset      => NULL,
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

SELECT alter_job(job_id, next_start => now() + INTERVAL '1 hour')
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_refresh_continuous_aggregate'
AND hypertable_name IN (
    'observations_rollup_weekly',
    'observations_rollup_monthly',
    'observations_rollup_quarterly'
)
AND scheduled;
