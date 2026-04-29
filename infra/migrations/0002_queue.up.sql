-- Queue storage for issue #22.
-- Postgres-backed jobs use transactional leasing with SKIP LOCKED,
-- retry metadata, dead-letter records, cron registrations, and group
-- keys for serialising loads per dataflow.

CREATE TABLE queue_jobs (
    id            BIGSERIAL PRIMARY KEY,
    stage         TEXT NOT NULL
                  CHECK (stage IN ('discover','fetch','parse','load','backfill')),
    payload       JSONB NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','running','completed','dead')),
    priority      INTEGER NOT NULL DEFAULT 0,
    attempts      INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts  INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by     TEXT,
    locked_at     TIMESTAMPTZ,
    lease_version BIGINT NOT NULL DEFAULT 0,
    trace_parent  TEXT,
    job_group_key TEXT,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX queue_jobs_ready_idx
    ON queue_jobs (stage, status, run_at, priority DESC, id)
    WHERE status = 'pending';

CREATE UNIQUE INDEX queue_jobs_one_running_group_idx
    ON queue_jobs (job_group_key)
    WHERE status = 'running' AND job_group_key IS NOT NULL;

CREATE TABLE queue_dead_letters (
    id            BIGSERIAL PRIMARY KEY,
    job_id        BIGINT NOT NULL UNIQUE,
    stage         TEXT NOT NULL,
    payload       JSONB NOT NULL,
    attempts      INTEGER NOT NULL CHECK (attempts > 0),
    error_class   TEXT NOT NULL,
    error_message TEXT NOT NULL,
    trace_parent  TEXT,
    failed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE queue_cron_schedules (
    id              TEXT PRIMARY KEY,
    stage           TEXT NOT NULL
                    CHECK (stage IN ('discover','fetch','parse','load','backfill')),
    cron_expression TEXT NOT NULL,
    payload         JSONB NOT NULL,
    trace_parent    TEXT,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
