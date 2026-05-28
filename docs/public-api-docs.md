# Public API Docs

Generated from `openapi.json` with Scalar, the public API reference is built
from the committed API contract. Do not
hand-edit endpoint pages; change the Rust handlers and OpenAPI annotations,
regenerate `openapi.json`, and rebuild the docs app.

## Local Build

```bash
pnpm install
pnpm --filter @au-kpis/docs test
```

The docs package builds `apps/docs/dist` and verifies that the generated bundle
contains the current `openapi.json` paths, including `/v1/openapi.json` and
`/v1/observations`.

## Deployment

`.github/workflows/docs.yml` publishes `apps/docs/dist` to GitHub Pages on every
push to `main` that changes the docs app, `openapi.json`, or workspace package
metadata. The intended public URL is
`https://ponderingdemocritus.github.io/australian-kpis/`.

The workflow sets `DOCS_BASE_PATH=/australian-kpis/` for GitHub Pages project
hosting. For a custom domain or root-hosted static deployment, set
`DOCS_BASE_PATH=/` and publish the same `apps/docs/dist` directory.

## Regeneration

The reference is deterministic because the app imports the committed
`openapi.json` artifact at build time. API-doc drift is therefore reviewable as
a normal PR diff: handler/schema changes update `openapi.json`, and the docs app
rebuilds from that exact artifact without endpoint-by-endpoint editing.
