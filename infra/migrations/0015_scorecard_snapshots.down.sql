DROP VIEW IF EXISTS scorecard_snapshots_latest;
DROP VIEW IF EXISTS scorecard_snapshots_as_published;
DROP TABLE IF EXISTS scorecard_snapshot_generations;
DROP TABLE IF EXISTS scorecard_snapshot_contributions;
DROP TABLE IF EXISTS scorecard_snapshots;
DROP FUNCTION IF EXISTS reject_scorecard_snapshot_mutation();
DROP TRIGGER IF EXISTS scorecard_configs_immutable ON scorecard_configs;
DROP FUNCTION IF EXISTS reject_scorecard_config_mutation();
DROP TABLE IF EXISTS scorecard_configs;
