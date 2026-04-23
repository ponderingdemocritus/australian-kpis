## API, OpenAPI, And SDK Review Guide

Applies to:
- `crates/au-kpis-api-http/**`
- `crates/au-kpis-openapi/**`
- `crates/bins/au-kpis-api/**`
- `packages/sdk/**`
- `packages/sdk-generated/**`

Primary review goal:
- protect the public contract exposed to API and SDK consumers

Focus on:
- OpenAPI parity
- additive vs breaking API changes
- SDK regeneration expectations
- attribution and metadata in outward-facing responses
- contract and integration test coverage

Ask:
- does the handler/schema diff require updated OpenAPI artifacts or SDK generation?
- is this a breaking change that should be versioned?
- do response-shape changes have contract coverage?
- is outward-facing attribution or license metadata now missing or inconsistent?
