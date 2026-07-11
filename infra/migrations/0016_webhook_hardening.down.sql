DROP INDEX IF EXISTS webhook_deliveries_terminal_retention_idx;

ALTER TABLE webhook_delivery_attempts
DROP COLUMN IF EXISTS lease_version;

DROP INDEX IF EXISTS webhook_deliveries_due_idx;
ALTER TABLE webhook_deliveries
DROP CONSTRAINT IF EXISTS webhook_deliveries_event_subscription_key,
DROP CONSTRAINT IF EXISTS webhook_deliveries_lease_check,
DROP CONSTRAINT IF EXISTS webhook_deliveries_status_check,
DROP COLUMN IF EXISTS expires_at,
DROP COLUMN IF EXISTS first_attempt_at,
DROP COLUMN IF EXISTS leased_until,
DROP COLUMN IF EXISTS lease_version,
DROP COLUMN IF EXISTS lease_owner,
DROP COLUMN IF EXISTS generation_id,
DROP COLUMN IF EXISTS event_id;
ALTER TABLE webhook_deliveries
ALTER COLUMN max_attempts SET DEFAULT 5;
UPDATE webhook_deliveries SET status = 'failed' WHERE status = 'dead_letter';
ALTER TABLE webhook_deliveries
ADD CONSTRAINT webhook_deliveries_status_check CHECK (
    status IN ('pending', 'delivering', 'delivered', 'failed')
);
CREATE INDEX webhook_deliveries_due_idx
ON webhook_deliveries (next_attempt_at, id)
WHERE status = 'pending';

DROP INDEX IF EXISTS webhook_subscriptions_deliverable_idx;
ALTER TABLE webhook_subscriptions
DROP CONSTRAINT IF EXISTS webhook_subscriptions_https_target_check,
DROP CONSTRAINT IF EXISTS webhook_subscriptions_encrypted_secret_check,
DROP CONSTRAINT IF EXISTS webhook_subscriptions_status_check;
UPDATE webhook_subscriptions SET status = 'paused' WHERE status = 'pending_verification';
ALTER TABLE webhook_subscriptions
ADD COLUMN signing_secret TEXT NOT NULL DEFAULT repeat('x', 32),
DROP COLUMN IF EXISTS revoked_at,
DROP COLUMN IF EXISTS paused_at,
DROP COLUMN IF EXISTS last_success_at,
DROP COLUMN IF EXISTS consecutive_failures,
DROP COLUMN IF EXISTS verification_expires_at,
DROP COLUMN IF EXISTS verification_digest,
DROP COLUMN IF EXISTS verified_at,
DROP COLUMN IF EXISTS previous_secret_expires_at,
DROP COLUMN IF EXISTS previous_secret_key_version,
DROP COLUMN IF EXISTS previous_secret_nonce,
DROP COLUMN IF EXISTS previous_secret_ciphertext,
DROP COLUMN IF EXISTS secret_key_version,
DROP COLUMN IF EXISTS secret_nonce,
DROP COLUMN IF EXISTS secret_ciphertext;
ALTER TABLE webhook_subscriptions
ALTER COLUMN signing_secret DROP DEFAULT,
ADD CONSTRAINT webhook_subscriptions_status_check CHECK (
    status IN ('active', 'paused', 'revoked')
);
CREATE INDEX webhook_subscriptions_active_idx
ON webhook_subscriptions (status)
WHERE status = 'active';
