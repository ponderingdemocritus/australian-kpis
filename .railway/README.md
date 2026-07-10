# Railway infrastructure graph

`.railway/railway.ts` defines Singapore stateless compute plus singleton
disposable Redis for staging and production. It intentionally does not create
TimescaleDB or object storage; those durable dependencies are managed in
Timescale Cloud and Cloudflare R2.

```bash
pnpm install
railway login
railway link
pnpm railway:plan
pnpm railway:apply
```

Every code service is created without a source. Do not connect the services to
the repository or enable Railway auto-deploy. `.github/workflows/deploy.yml`
connects an immutable signed GHCR digest after release checks and protected
environment approval.

Secrets and external endpoints use `preserve()` and must be set out-of-band in
each Railway environment. See `docs/deploy/railway.md` for the full bootstrap,
topology, variables, and migration procedure.
