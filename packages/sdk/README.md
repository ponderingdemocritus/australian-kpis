# @au-kpis/sdk

Typed TypeScript client for the Australian KPIs API.

## Install

```bash
npm install @au-kpis/sdk
```

The package ships ESM JavaScript and TypeScript declarations. It uses only
`fetch`, `URL`, and standard web APIs, so the same build works in Node 20+, Bun,
Deno with npm imports, and modern browsers.

## Quickstart

```ts
import { createClient } from '@au-kpis/sdk'

const client = createClient({
  apiKey: process.env.AU_KPIS_KEY,
  baseUrl: 'https://api.au-kpis.example',
})

const dataflows = await client.dataflows.list({ source: 'abs' })
const apsLatest = await client.scorecards.aps.latest()

const cpi = await client.observations.list({
  dataflow: 'abs.cpi',
  dimensions: {
    measure: 'All groups CPI',
    region: 'AUS',
  },
  since: '2010-01-01',
})

console.log(dataflows.dataflows.length)
console.log(apsLatest.score, apsLatest.zone)
console.log(cpi.observations[0])
```

```ts
const apsConfig = await client.scorecards.aps.config()
const apsHistory = await client.scorecards.aps.history({
  since: '2024-01-01',
  until: '2024-12-31',
})

console.log(apsConfig.version)
console.log(apsHistory.map((snapshot) => snapshot.score))
```

## Bulk Parquet

JSON and CSV observation requests are page-sized and capped at 10,000 rows.
For larger analytical pulls, request Parquet as a raw streaming response:

```ts
const response = await client.observations.parquet({
  dataflow: 'abs.cpi',
  dimensions: { region: 'AUS' },
  since: '2010-01-01',
  limit: 50_000,
})

const bytes = new Uint8Array(await response.arrayBuffer())
```

Parquet exports use `Cache-Control: no-store`, omit ETags, and are capped by
the API bulk-export row limit.

## Runtime targets

- Node 20+ through the global `fetch` implementation.
- Bun through native ESM and global `fetch`.
- Deno through npm package imports.
- Modern browsers through native ESM and global `fetch`.

## Validation

Runtime response validation is off by default. Enable it when crossing trust
boundaries or debugging API drift:

```ts
const client = createClient({
  baseUrl: 'https://api.au-kpis.example',
  validate: true,
})
```

Validation uses the generated OpenAPI-derived Zod schemas from
`@au-kpis/sdk-generated`.
