# Railway Infrastructure as Code

This directory defines the Railway project graph for new `australian-kpis`
deployments.

Use it from a Railway-linked checkout:

```bash
pnpm install
railway login
railway --version  # must be 5.2.0 or newer for IaC commands
railway init        # only when creating a new Railway project
# or: railway link  # when attaching this checkout to an existing project
pnpm railway:plan
pnpm railway:apply
```

`railway.ts` creates the stateful TimescaleDB service, Redis, artifact bucket,
PDF extractor, ingestion worker, scheduler, API, and web dashboard. The existing
`infra/railway/*.toml` files still document the per-service build/deploy shape,
but new projects should use this project-level definition first.

Do not point an IaC-managed Railway service at `infra/railway/*.toml` as a
custom config file. Railway treats per-service Config-as-Code and project-level
IaC as separate sources of truth and blocks services managed by both.

`package.json` keeps this directory in ESM mode so Railway loads the default
TypeScript export correctly without changing the root package type.

After apply, generate Railway public domains for `web` and optionally `api`.
Railway-generated domains are not currently stored in `.railway/railway.ts`.
