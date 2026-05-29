-- Webhook subscriptions and durable delivery attempts for phase-5 consumer
-- notifications when new observations are loaded.

CREATE TABLE webhook_subscriptions (
    id             UUID PRIMARY KEY,
    api_key_id     UUID NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    target_url     TEXT NOT NULL CHECK (target_url ~ '^https?://'),
    dataflow_ids   TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    signing_secret TEXT NOT NULL CHECK (char_length(signing_secret) >= 32),
    status         TEXT NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'paused', 'revoked')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX webhook_subscriptions_api_key_idx
ON webhook_subscriptions (api_key_id, created_at DESC);

CREATE INDEX webhook_subscriptions_dataflow_ids_gin
ON webhook_subscriptions USING GIN (dataflow_ids);

CREATE INDEX webhook_subscriptions_active_idx
ON webhook_subscriptions (status)
WHERE status = 'active';

CREATE TABLE webhook_deliveries (
    id               BIGSERIAL PRIMARY KEY,
    subscription_id  UUID NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
    event_type       TEXT NOT NULL CHECK (event_type IN ('data.updated')),
    dataflow_id      TEXT NOT NULL REFERENCES dataflows(id),
    artifact_id      BYTEA REFERENCES artifacts(id)
                     CHECK (artifact_id IS NULL OR octet_length(artifact_id) = 32),
    payload          JSONB NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'delivering', 'delivered', 'failed')),
    attempts         INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts     INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    next_attempt_at  TIMESTAMPTZ,
    delivered_at     TIMESTAMPTZ,
    last_status_code INTEGER CHECK (
                         last_status_code IS NULL
                         OR last_status_code BETWEEN 100 AND 599
                     ),
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX webhook_deliveries_due_idx
ON webhook_deliveries (next_attempt_at, id)
WHERE status = 'pending';

CREATE INDEX webhook_deliveries_subscription_idx
ON webhook_deliveries (subscription_id, created_at DESC);

CREATE INDEX webhook_deliveries_dataflow_idx
ON webhook_deliveries (dataflow_id, created_at DESC);

CREATE TABLE webhook_delivery_attempts (
    id             BIGSERIAL PRIMARY KEY,
    delivery_id    BIGINT NOT NULL REFERENCES webhook_deliveries(id) ON DELETE CASCADE,
    attempt_no     INTEGER NOT NULL CHECK (attempt_no > 0),
    success        BOOLEAN NOT NULL,
    status_code    INTEGER CHECK (
                       status_code IS NULL OR status_code BETWEEN 100 AND 599
                   ),
    error_message  TEXT,
    attempted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    latency_ms     BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
    UNIQUE (delivery_id, attempt_no)
);

CREATE INDEX webhook_delivery_attempts_delivery_idx
ON webhook_delivery_attempts (delivery_id, attempt_no);
