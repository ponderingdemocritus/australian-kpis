CREATE INDEX IF NOT EXISTS series_dataflow_first_observed_series_key_idx
ON series (dataflow_id, first_observed, series_key)
WHERE first_observed IS NOT NULL AND last_observed IS NOT NULL;
