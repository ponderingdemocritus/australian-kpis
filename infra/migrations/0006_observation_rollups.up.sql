-- Continuous aggregates for common observation rollups.
--
-- These views aggregate numeric, non-missing observations per series. The API
-- enriches rows with series metadata and rollup attributes at read time.

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
