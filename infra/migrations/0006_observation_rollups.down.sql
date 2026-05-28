-- Drop rollup continuous aggregates. Timescale removes associated refresh
-- jobs when the continuous aggregate views are dropped.

DROP MATERIALIZED VIEW IF EXISTS observations_rollup_quarterly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS observations_rollup_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS observations_rollup_weekly CASCADE;
