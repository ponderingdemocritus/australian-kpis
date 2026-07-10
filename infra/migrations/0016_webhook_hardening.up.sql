-- Verified subscriptions, encrypted signing secrets, and fenced delivery leases.

ALTER TABLE webhook_subscriptions
DROP CONSTRAINT webhook_subscriptions_status_check;

ALTER TABLE webhook_subscriptions
DROP COLUMN signing_secret,
ADD COLUMN secret_ciphertext BYTEA,
ADD COLUMN secret_nonce BYTEA CHECK (secret_nonce IS NULL OR octet_length(secret_nonce) = 12),
ADD COLUMN secret_key_version INTEGER CHECK (secret_key_version IS NULL OR secret_key_version > 0),
ADD COLUMN previous_secret_ciphertext BYTEA,
ADD COLUMN previous_secret_nonce BYTEA CHECK (
    previous_secret_nonce IS NULL OR octet_length(previous_secret_nonce) = 12
),
ADD COLUMN previous_secret_key_version INTEGER CHECK (
    previous_secret_key_version IS NULL OR previous_secret_key_version > 0
),
ADD COLUMN previous_secret_expires_at TIMESTAMPTZ,
ADD COLUMN verified_at TIMESTAMPTZ,
ADD COLUMN verification_digest BYTEA CHECK (
    verification_digest IS NULL OR octet_length(verification_digest) = 32
),
ADD COLUMN verification_expires_at TIMESTAMPTZ,
ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0
    CHECK (consecutive_failures >= 0),
ADD COLUMN last_success_at TIMESTAMPTZ,
ADD COLUMN paused_at TIMESTAMPTZ,
ADD COLUMN revoked_at TIMESTAMPTZ;

ALTER TABLE webhook_subscriptions
ALTER COLUMN status SET DEFAULT 'pending_verification';

UPDATE webhook_subscriptions
SET status = 'revoked',
    revoked_at = now(),
    target_url = regexp_replace(target_url, '^http://', 'https://');

ALTER TABLE webhook_subscriptions
ADD CONSTRAINT webhook_subscriptions_status_check CHECK (
    status IN ('pending_verification', 'active', 'paused', 'revoked')
),
ADD CONSTRAINT webhook_subscriptions_encrypted_secret_check CHECK (
    (status = 'revoked') OR
    (secret_ciphertext IS NOT NULL AND secret_nonce IS NOT NULL AND secret_key_version IS NOT NULL)
),
ADD CONSTRAINT webhook_subscriptions_https_target_check CHECK (
    target_url ~ '^https://'
);

DROP INDEX webhook_subscriptions_active_idx;
CREATE INDEX webhook_subscriptions_deliverable_idx
ON webhook_subscriptions (status, id)
WHERE status = 'active';

ALTER TABLE webhook_deliveries
DROP CONSTRAINT webhook_deliveries_status_check;

ALTER TABLE webhook_deliveries
ADD COLUMN event_id UUID NOT NULL DEFAULT gen_random_uuid(),
ADD COLUMN generation_id UUID REFERENCES ingestion_generations(id) ON DELETE RESTRICT,
ADD COLUMN lease_owner UUID,
ADD COLUMN lease_version BIGINT NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
ADD COLUMN leased_until TIMESTAMPTZ,
ADD COLUMN first_attempt_at TIMESTAMPTZ,
ADD COLUMN expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '24 hours');

ALTER TABLE webhook_deliveries
ALTER COLUMN max_attempts SET DEFAULT 12;

UPDATE webhook_deliveries SET max_attempts = 12 WHERE max_attempts < 12;

ALTER TABLE webhook_deliveries
ADD CONSTRAINT webhook_deliveries_status_check CHECK (
    status IN ('pending', 'delivering', 'delivered', 'dead_letter', 'failed')
),
ADD CONSTRAINT webhook_deliveries_lease_check CHECK (
    (status = 'delivering' AND lease_owner IS NOT NULL AND leased_until IS NOT NULL)
    OR
    (status <> 'delivering')
),
ADD CONSTRAINT webhook_deliveries_event_subscription_key UNIQUE (event_id, subscription_id);

DROP INDEX webhook_deliveries_due_idx;
CREATE INDEX webhook_deliveries_due_idx
ON webhook_deliveries (next_attempt_at, id)
WHERE status = 'pending' OR status = 'delivering';

ALTER TABLE webhook_delivery_attempts
ADD COLUMN lease_version BIGINT CHECK (lease_version IS NULL OR lease_version > 0);

CREATE INDEX webhook_deliveries_terminal_retention_idx
ON webhook_deliveries (updated_at, id)
WHERE status IN ('delivered', 'dead_letter', 'failed');
