CREATE TABLE artifact_fetches (
    id               BIGSERIAL PRIMARY KEY,
    artifact_id      BYTEA       NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE
                     CHECK (octet_length(artifact_id) = 32),
    source_id        TEXT        NOT NULL REFERENCES sources(id),
    source_url       TEXT        NOT NULL,
    content_type     TEXT        NOT NULL,
    response_headers JSONB       NOT NULL,
    size_bytes       BIGINT      NOT NULL CHECK (size_bytes >= 0),
    storage_key      TEXT        NOT NULL,
    fetched_at       TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, artifact_id)
);

CREATE INDEX artifact_fetches_artifact_idx
ON artifact_fetches (artifact_id, id);

CREATE INDEX artifact_fetches_source_idx
ON artifact_fetches (source_id, fetched_at DESC);

CREATE INDEX artifact_fetches_source_url_idx
ON artifact_fetches (source_url);

INSERT INTO artifact_fetches (
    artifact_id, source_id, source_url, content_type, response_headers,
    size_bytes, storage_key, fetched_at, created_at
)
SELECT id, source_id, source_url, content_type, response_headers,
       size_bytes, storage_key, fetched_at, created_at
FROM artifacts;

ALTER TABLE artifact_loads
ADD COLUMN artifact_fetch_id BIGINT;

ALTER TABLE artifact_loads
DROP CONSTRAINT artifact_loads_pkey;

ALTER TABLE artifact_loads
ADD PRIMARY KEY (artifact_id, source_id, dataflow_id);

UPDATE artifact_loads AS loads
SET artifact_fetch_id = (
    SELECT fetches.id
    FROM artifact_fetches AS fetches
    WHERE fetches.artifact_id = loads.artifact_id
      AND fetches.source_id = loads.source_id
    ORDER BY fetches.id
    LIMIT 1
);

ALTER TABLE artifact_loads
ADD CONSTRAINT artifact_loads_artifact_fetch_id_fkey
FOREIGN KEY (artifact_fetch_id, artifact_id)
REFERENCES artifact_fetches(id, artifact_id);

CREATE INDEX artifact_loads_artifact_fetch_idx
ON artifact_loads (artifact_fetch_id)
WHERE artifact_fetch_id IS NOT NULL;
