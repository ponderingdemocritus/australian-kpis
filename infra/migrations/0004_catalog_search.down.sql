DROP INDEX IF EXISTS measures_description_trgm_gin;
DROP INDEX IF EXISTS measures_name_trgm_gin;
DROP INDEX IF EXISTS measures_search_tsv_gin;
DROP INDEX IF EXISTS dataflows_description_trgm_gin;
DROP INDEX IF EXISTS dataflows_name_trgm_gin;
DROP INDEX IF EXISTS dataflows_search_tsv_gin;

-- Intentionally keep `pg_trgm`: extensions are database-level resources and may
-- be shared by other schemas or later migrations.
