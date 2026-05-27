# Security Operations

## API Key Rotation

API keys are created and revoked through `au-kpis-cli`, which uses the
configured Postgres and Redis connections. The plaintext key is printed only by
the create command and is never stored.

1. Create a replacement key with the scopes and tier the client needs:

   ```bash
   au-kpis-cli api-keys create \
     --name "client name" \
     --scope observations:read \
     --rate-limit-tier free \
     --actor admin@example.com
   ```

2. Copy the returned `api_key` value into the client's secret store and deploy
   the client change.

3. Confirm the client can call protected routes with the new `X-API-Key`.

4. Revoke the old key by id:

   ```bash
   au-kpis-cli api-keys revoke \
     --id 00000000-0000-0000-0000-000000000000 \
     --actor admin@example.com
   ```

5. Watch for any remaining requests using the revoked key. Issuance and
   revocation events are retained in `api_key_audit_log` for one year.
