ALTER TABLE artifacts
    ADD COLUMN response_headers JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE artifacts
    ALTER COLUMN response_headers DROP DEFAULT;
