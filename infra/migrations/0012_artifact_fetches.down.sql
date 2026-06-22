DROP INDEX IF EXISTS artifact_loads_artifact_fetch_idx;

ALTER TABLE artifact_loads
DROP CONSTRAINT IF EXISTS artifact_loads_artifact_fetch_id_fkey;

ALTER TABLE artifact_loads
DROP CONSTRAINT artifact_loads_pkey;

DELETE FROM artifact_loads AS loads
USING (
    SELECT ctid,
           row_number() OVER (
               PARTITION BY artifact_id, dataflow_id
               ORDER BY completed_at DESC, source_id DESC
           ) AS row_number
    FROM artifact_loads
) AS ranked
WHERE loads.ctid = ranked.ctid
  AND ranked.row_number > 1;

ALTER TABLE artifact_loads
ADD PRIMARY KEY (artifact_id, dataflow_id);

ALTER TABLE artifact_loads
DROP COLUMN IF EXISTS artifact_fetch_id;

DROP TABLE IF EXISTS artifact_fetches;
