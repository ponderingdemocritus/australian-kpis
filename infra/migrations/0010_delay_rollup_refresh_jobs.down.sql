-- Intentionally no-op: restoring immediate refresh scheduling would
-- reintroduce the rollback race this migration prevents. Migration 0006
-- removes these jobs when rollup views are dropped.
SELECT 1;
