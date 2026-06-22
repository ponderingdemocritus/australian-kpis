SELECT remove_compression_policy('observations', if_exists => TRUE);

ALTER TABLE observations SET (timescaledb.compress = false);

ALTER TABLE observations DROP CONSTRAINT observations_time_precision_check;

ALTER TABLE observations
  ADD CONSTRAINT observations_time_precision_check
  CHECK (time_precision IN ('day', 'week', 'month', 'quarter', 'year')) NOT VALID;

ALTER TABLE observations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_key',
    timescaledb.compress_orderby   = 'time DESC, revision_no DESC'
);

SELECT add_compression_policy('observations', INTERVAL '7 days', if_not_exists => TRUE);
