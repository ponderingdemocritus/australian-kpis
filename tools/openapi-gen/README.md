# OpenAPI TypeScript Generation

`pnpm sdk:generate` refreshes the committed API contract and TypeScript client scaffold:

1. `cargo run -p au-kpis-openapi` writes the canonical `openapi.json` document served by `/v1/openapi.json`.
2. `openapi-typescript` writes typed OpenAPI paths and schemas to `packages/sdk-generated/src/types.ts`.
3. `orval` writes the generated fetch client to `packages/sdk-generated/src/client.ts`.

The generated files are committed so API and SDK drift is reviewable.
