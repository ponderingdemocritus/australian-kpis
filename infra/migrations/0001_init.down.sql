-- Reverse of 0001_init.up.sql. Drops every object in reverse FK order
-- so `sqlx migrate revert` → `sqlx migrate run` is idempotent.

DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS parse_errors;

-- Compression policy + hypertable metadata live in timescaledb's own
-- catalogs and are cleaned up automatically when the table is dropped.
DROP VIEW  IF EXISTS observations_latest;
DROP TABLE IF EXISTS observations;

DROP TABLE IF EXISTS artifacts;

DROP TABLE IF EXISTS series;

DROP TABLE IF EXISTS dimensions;
DROP TABLE IF EXISTS dataflows;
DROP TABLE IF EXISTS measures;

DROP TABLE IF EXISTS codes;
DROP TABLE IF EXISTS codelists;

DROP TABLE IF EXISTS sources;

-- Intentionally **do not** `DROP EXTENSION timescaledb` — the extension
-- is an instance-level resource and may be shared with other schemas or
-- databases we don't own.
