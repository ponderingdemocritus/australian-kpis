-- Drop rollup continuous aggregates. Timescale removes associated refresh
-- jobs when the continuous aggregate views are dropped.

SELECT remove_continuous_aggregate_policy('observations_rollup_quarterly', if_exists => TRUE);
SELECT remove_continuous_aggregate_policy('observations_rollup_monthly', if_exists => TRUE);
SELECT remove_continuous_aggregate_policy('observations_rollup_weekly', if_exists => TRUE);

DROP MATERIALIZED VIEW IF EXISTS observations_rollup_quarterly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS observations_rollup_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS observations_rollup_weekly CASCADE;
