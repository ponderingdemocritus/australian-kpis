-- Catalog search indexes for `/v1/search`.
-- Full-text indexes handle ranked token search; trigram indexes keep fuzzy
-- name/description matches fast without coupling the API to Postgres forever.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX dataflows_search_tsv_gin
ON dataflows
USING GIN (to_tsvector('english', name || ' ' || COALESCE(description, '')));

CREATE INDEX dataflows_name_trgm_gin
ON dataflows
USING GIN (name gin_trgm_ops);

CREATE INDEX dataflows_description_trgm_gin
ON dataflows
USING GIN (description gin_trgm_ops);

CREATE INDEX measures_search_tsv_gin
ON measures
USING GIN (to_tsvector('english', name || ' ' || COALESCE(description, '')));

CREATE INDEX measures_name_trgm_gin
ON measures
USING GIN (name gin_trgm_ops);

CREATE INDEX measures_description_trgm_gin
ON measures
USING GIN (description gin_trgm_ops);
