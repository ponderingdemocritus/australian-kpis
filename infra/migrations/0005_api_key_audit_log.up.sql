-- API-key issuance/revocation audit log.
-- Retention follows `Spec.md § Security posture`: keep lifecycle events for
-- one year so leaked or abused keys can be investigated without storing key
-- plaintext.

CREATE TABLE api_key_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    api_key_id      UUID NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    action          TEXT NOT NULL CHECK (action IN ('created', 'revoked')),
    actor           TEXT NOT NULL CHECK (char_length(actor) BETWEEN 1 AND 320),
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    retention_until TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '1 year')
);

CREATE INDEX api_key_audit_log_key_idx
ON api_key_audit_log (api_key_id, occurred_at DESC);

CREATE INDEX api_key_audit_log_retention_idx
ON api_key_audit_log (retention_until);
