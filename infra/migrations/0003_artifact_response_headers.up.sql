ALTER TABLE artifacts
    ADD COLUMN response_headers JSONB NOT NULL DEFAULT '{}'::jsonb;
