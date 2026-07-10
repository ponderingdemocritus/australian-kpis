-- Immutable APS configuration and published daily snapshot history.

CREATE TABLE scorecard_configs (
    scorecard_id   TEXT NOT NULL,
    version        TEXT NOT NULL,
    digest         BYTEA NOT NULL CHECK (octet_length(digest) = 32),
    config         JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scorecard_id, version),
    UNIQUE (scorecard_id, digest)
);

CREATE FUNCTION reject_scorecard_config_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'scorecard config rows are immutable';
END
$$;

CREATE TRIGGER scorecard_configs_immutable
BEFORE UPDATE OR DELETE ON scorecard_configs
FOR EACH ROW EXECUTE FUNCTION reject_scorecard_config_mutation();

CREATE TABLE scorecard_snapshots (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scorecard_id             TEXT NOT NULL,
    config_version           TEXT NOT NULL,
    config_digest            BYTEA NOT NULL CHECK (octet_length(config_digest) = 32),
    snapshot_date            DATE NOT NULL,
    revision                 INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    supersedes_snapshot_id   UUID REFERENCES scorecard_snapshots(id) ON DELETE RESTRICT,
    correction_reason        TEXT,
    as_of                    TIMESTAMPTZ NOT NULL,
    published_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    publication_state        TEXT NOT NULL
                             CHECK (publication_state IN (
                                 'published', 'insufficient_coverage'
                             )),
    score                    DOUBLE PRECISION CHECK (score BETWEEN 0 AND 100),
    zone                     TEXT CHECK (zone IN ('scarcity', 'mixed', 'abundance')),
    overall_coverage_pct     DOUBLE PRECISION NOT NULL
                             CHECK (overall_coverage_pct BETWEEN 0 AND 100),
    throughput_coverage_pct  DOUBLE PRECISION NOT NULL
                             CHECK (throughput_coverage_pct BETWEEN 0 AND 100),
    orientation_coverage_pct DOUBLE PRECISION NOT NULL
                             CHECK (orientation_coverage_pct BETWEEN 0 AND 100),
    summary_payload          JSONB NOT NULL,
    snapshot_payload         JSONB NOT NULL,
    FOREIGN KEY (scorecard_id, config_version)
        REFERENCES scorecard_configs(scorecard_id, version) ON DELETE RESTRICT,
    UNIQUE (scorecard_id, config_version, snapshot_date, revision),
    CHECK (
        (revision = 0 AND supersedes_snapshot_id IS NULL AND correction_reason IS NULL)
        OR
        (revision > 0 AND supersedes_snapshot_id IS NOT NULL
         AND char_length(trim(correction_reason)) > 0)
    ),
    CHECK (
        (publication_state = 'published' AND score IS NOT NULL AND zone IS NOT NULL)
        OR
        (publication_state = 'insufficient_coverage' AND score IS NULL AND zone IS NULL)
    )
);

CREATE INDEX scorecard_snapshots_latest_idx
ON scorecard_snapshots (scorecard_id, snapshot_date DESC, revision DESC, published_at DESC);

CREATE FUNCTION reject_scorecard_snapshot_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'published scorecard snapshot rows are immutable';
END
$$;

CREATE TRIGGER scorecard_snapshots_immutable
BEFORE UPDATE OR DELETE ON scorecard_snapshots
FOR EACH ROW EXECUTE FUNCTION reject_scorecard_snapshot_mutation();

CREATE TABLE scorecard_snapshot_contributions (
    snapshot_id             UUID NOT NULL
                            REFERENCES scorecard_snapshots(id) ON DELETE CASCADE,
    indicator_id            TEXT NOT NULL,
    series_key              BYTEA CHECK (
                                series_key IS NULL OR octet_length(series_key) = 32
                            ),
    source_artifact_id      BYTEA REFERENCES artifacts(id) ON DELETE RESTRICT
                            CHECK (
                                source_artifact_id IS NULL
                                OR octet_length(source_artifact_id) = 32
                            ),
    ingestion_generation_id UUID
                            REFERENCES ingestion_generations(id) ON DELETE RESTRICT,
    contribution            JSONB NOT NULL,
    PRIMARY KEY (snapshot_id, indicator_id)
);

CREATE INDEX scorecard_snapshot_contributions_generation_idx
ON scorecard_snapshot_contributions (ingestion_generation_id)
WHERE ingestion_generation_id IS NOT NULL;

CREATE TABLE scorecard_snapshot_generations (
    snapshot_id   UUID NOT NULL
                  REFERENCES scorecard_snapshots(id) ON DELETE CASCADE,
    generation_id UUID NOT NULL
                  REFERENCES ingestion_generations(id) ON DELETE RESTRICT,
    PRIMARY KEY (snapshot_id, generation_id)
);

CREATE TRIGGER scorecard_snapshot_contributions_immutable
BEFORE UPDATE OR DELETE ON scorecard_snapshot_contributions
FOR EACH ROW EXECUTE FUNCTION reject_scorecard_snapshot_mutation();

CREATE TRIGGER scorecard_snapshot_generations_immutable
BEFORE UPDATE OR DELETE ON scorecard_snapshot_generations
FOR EACH ROW EXECUTE FUNCTION reject_scorecard_snapshot_mutation();

CREATE VIEW scorecard_snapshots_as_published AS
SELECT DISTINCT ON (scorecard_id, config_version, snapshot_date) *
FROM scorecard_snapshots
ORDER BY scorecard_id, config_version, snapshot_date, revision ASC;

CREATE VIEW scorecard_snapshots_latest AS
SELECT DISTINCT ON (scorecard_id, config_version, snapshot_date) *
FROM scorecard_snapshots
ORDER BY scorecard_id, config_version, snapshot_date, revision DESC;
