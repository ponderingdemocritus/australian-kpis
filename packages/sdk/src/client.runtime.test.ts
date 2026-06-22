import type {
  ObservationsResponse,
  ScorecardConfig,
  ScorecardSnapshot,
} from '@au-kpis/sdk-generated/client'
import { ApiRequestError, ApiValidationError, createClient } from './index.js'

type FetchCall = {
  init?: RequestInit
  url: string
}

type MockFetch = {
  calls: FetchCall[]
  fetch: typeof fetch
}

const seriesKey = 'a'.repeat(64)
const artifactId = 'b'.repeat(64)

await run('dataflow methods call catalog endpoints and attach api key', async () => {
  const mock = mockFetch([
    jsonResponse({
      dataflows: [dataflow()],
    }),
    jsonResponse({
      dataflow: dataflow(),
      dimensions: [dimension()],
    }),
    jsonResponse({
      codelist: {
        codes: [{ codelist_id: 'CL_REGION', id: 'AUS', name: 'Australia' }],
        id: 'CL_REGION',
        name: 'Regions',
      },
      dataflow_id: 'abs.cpi',
      dimension_id: 'region',
    }),
  ])
  const client = createClient({
    apiKey: 'test-key',
    baseUrl: 'https://api.example.test/',
    fetch: mock.fetch,
  })

  await client.dataflows.list({ frequency: 'quarterly', source: 'abs' })
  await client.dataflows.get('abs.cpi')
  await client.dataflows.codelists('abs.cpi', 'region')

  assertPath(mock.calls[0], '/v1/dataflows')
  assertSearch(mock.calls[0], 'source', 'abs')
  assertSearch(mock.calls[0], 'frequency', 'quarterly')
  assertPath(mock.calls[1], '/v1/dataflows/abs.cpi')
  assertPath(mock.calls[2], '/v1/dataflows/abs.cpi/codelists/region')
  assertEqual(new Headers(mock.calls[0]?.init?.headers).get('x-api-key'), 'test-key')
})

await run('search.catalog calls the search endpoint', async () => {
  const response = {
    query: 'cpi',
    results: [
      {
        dataflow_ids: ['abs.cpi'],
        description: 'Quarterly Consumer Price Index observations.',
        id: 'abs.cpi',
        kind: 'dataflow',
        name: 'Consumer Price Index',
        score: 1,
        source_id: 'abs',
      },
    ],
  }
  const mock = mockFetch([jsonResponse(response)])
  const client = createClient({ baseUrl: 'https://api.example.test', fetch: mock.fetch })

  const result = await client.search.catalog({ limit: 5, q: 'cpi' })

  assertEqual(result, response)
  assertPath(mock.calls[0], '/v1/search')
  assertSearch(mock.calls[0], 'q', 'cpi')
  assertSearch(mock.calls[0], 'limit', '5')
})

await run('default global fetch is bound before requests are made', async () => {
  const originalFetch = globalThis.fetch
  try {
    globalThis.fetch = function defaultFetchGuard(this: typeof globalThis) {
      if (this !== globalThis) {
        throw new Error('expected default fetch to be bound to globalThis')
      }
      return Promise.resolve(jsonResponse({ dataflows: [] }))
    } as typeof fetch

    const client = createClient({
      baseUrl: 'https://api.example.test',
      retry: { sleep: async () => undefined },
    })

    await client.dataflows.list()
  } finally {
    globalThis.fetch = originalFetch
  }
})

await run('relative browser base URLs resolve against window location', async () => {
  const originalLocation = Reflect.get(globalThis, 'location') as Location | undefined
  const mock = mockFetch([jsonResponse({ status: 'ok' })])

  Object.defineProperty(globalThis, 'location', {
    configurable: true,
    value: { origin: 'http://app.example.test' },
  })

  try {
    const client = createClient({ baseUrl: '/api/au-kpis', fetch: mock.fetch })

    await client.health()

    assertEqual(mock.calls[0]?.url, 'http://app.example.test/api/au-kpis/v1/health')
  } finally {
    if (originalLocation === undefined) {
      Reflect.deleteProperty(globalThis, 'location')
    } else {
      Object.defineProperty(globalThis, 'location', {
        configurable: true,
        value: originalLocation,
      })
    }
  }
})

await run('observations.list encodes dimensions and returns the response envelope', async () => {
  const page = observationsPage({ nextCursor: 'cursor-2' })
  const mock = mockFetch([jsonResponse(page)])
  const client = createClient({ baseUrl: 'https://api.example.test', fetch: mock.fetch })

  const result = await client.observations.list({
    dataflow: 'abs.cpi',
    dimensions: {
      measure: 'All groups CPI',
      region: 'AUS',
    },
    limit: 100,
    since: '2024-01-01',
  })

  assertEqual(result, page)
  assertPath(mock.calls[0], '/v1/observations')
  assertSearch(mock.calls[0], 'dataflow', 'abs.cpi')
  assertSearch(mock.calls[0], 'since', '2024-01-01')
  assertSearch(mock.calls[0], 'limit', '100')
  assertSearchValues(mock.calls[0], 'dimensions[]', ['measure=All groups CPI', 'region=AUS'])
})

await run('observations.stream follows pagination cursors', async () => {
  const first = observationsPage({ nextCursor: 'cursor-2', value: 136.2 })
  const second = observationsPage({ nextCursor: null, value: 136.9 })
  const mock = mockFetch([jsonResponse(first), jsonResponse(second)])
  const client = createClient({ baseUrl: 'https://api.example.test', fetch: mock.fetch })

  const values: number[] = []
  for await (const observation of client.observations.stream({
    dataflow: 'abs.cpi',
    dimensions: { region: 'AUS' },
    limit: 1,
  })) {
    values.push(observation.value ?? Number.NaN)
  }

  assertEqual(values, [136.2, 136.9])
  assertSearch(mock.calls[1], 'cursor', 'cursor-2')
  assertSearchValues(mock.calls[1], 'dimensions[]', ['region=AUS'])
})

await run('observations.latest calls the series lookup endpoint', async () => {
  const response = {
    latest_observation: observation(),
    revision: {
      ingested_at: '2024-07-24T00:00:00Z',
      is_revision: true,
      revision_no: 1,
      source_artifact_id: artifactId,
    },
    series: {
      active: true,
      dataflow_id: 'abs.cpi',
      dimensions: { region: 'AUS' },
      first_observed: '2024-03-01T00:00:00Z',
      last_observed: '2024-06-01T00:00:00Z',
      measure_id: 'index',
      series_key: seriesKey,
      unit: 'index',
    },
  }
  const mock = mockFetch([jsonResponse(response)])
  const client = createClient({ baseUrl: 'https://api.example.test', fetch: mock.fetch })

  const result = await client.observations.latest({
    dataflow: 'abs.cpi',
    seriesKey,
  })

  assertEqual(result, response)
  assertPath(mock.calls[0], `/v1/series/abs.cpi/${seriesKey}`)
})

await run('scorecard helpers call APS endpoints and serialize history bounds', async () => {
  const config = scorecardConfig()
  const latest = scorecardSnapshot()
  const history = [scorecardSnapshot({ asOf: '2024-01-01', score: 40 })]
  const mock = mockFetch([jsonResponse(config), jsonResponse(latest), jsonResponse(history)])
  const client = createClient({ baseUrl: 'https://api.example.test', fetch: mock.fetch })

  assertEqual(await client.scorecards.aps.config(), config)
  assertEqual(await client.scorecards.aps.latest(), latest)
  assertEqual(
    await client.scorecards.aps.history({
      since: '2024-01-01',
      until: '2024-12-31',
    }),
    history,
  )

  assertPath(mock.calls[0], '/v1/scorecards/aps/config')
  assertPath(mock.calls[1], '/v1/scorecards/aps/latest')
  assertPath(mock.calls[2], '/v1/scorecards/aps/history')
  assertSearch(mock.calls[2], 'since', '2024-01-01')
  assertSearch(mock.calls[2], 'until', '2024-12-31')
})

await run('validate true accepts scorecard config latest and history responses', async () => {
  const mock = mockFetch([
    jsonResponse(scorecardConfig()),
    jsonResponse(scorecardSnapshot()),
    jsonResponse([scorecardSnapshot()]),
  ])
  const client = createClient({
    baseUrl: 'https://api.example.test',
    fetch: mock.fetch,
    validate: true,
  })

  await client.scorecards.aps.config()
  await client.scorecards.aps.latest()
  await client.scorecards.aps.history()
})

await run('search.catalog calls the catalog search endpoint', async () => {
  const response = {
    query: 'index',
    results: [
      {
        dataflow_ids: ['abs.cpi'],
        description: 'Quarterly Consumer Price Index across Australian regions.',
        id: 'abs.cpi',
        kind: 'dataflow',
        name: 'Consumer Price Index',
        score: 1.2,
        source_id: 'abs',
      },
    ],
  }
  const mock = mockFetch([jsonResponse(response)])
  const client = createClient({ baseUrl: 'https://api.example.test', fetch: mock.fetch })

  const result = await client.search.catalog({ limit: 5, q: 'index' })

  assertEqual(result, response)
  assertPath(mock.calls[0], '/v1/search')
  assertSearch(mock.calls[0], 'q', 'index')
  assertSearch(mock.calls[0], 'limit', '5')
})

await run('request retries respect Retry-After before succeeding', async () => {
  const slept: number[] = []
  const mock = mockFetch([
    jsonResponse({ status: 429, title: 'Too Many Requests', type: 'about:blank' }, 429, {
      'Retry-After': '2',
    }),
    jsonResponse({ dataflows: [dataflow()] }),
  ])
  const client = createClient({
    baseUrl: 'https://api.example.test',
    fetch: mock.fetch,
    retry: {
      maxAttempts: 2,
      sleep: async (ms) => {
        slept.push(ms)
      },
    },
  })

  const result = await client.dataflows.list()

  assertEqual(result.dataflows.length, 1)
  assertEqual(mock.calls.length, 2)
  assertEqual(slept, [2000])
})

await run('request errors include status and body after retries are exhausted', async () => {
  const mock = mockFetch([
    jsonResponse({ status: 503, title: 'Unavailable', type: 'about:blank' }, 503),
  ])
  const client = createClient({
    baseUrl: 'https://api.example.test',
    fetch: mock.fetch,
    retry: { maxAttempts: 1 },
  })

  await assertRejects(client.dataflows.list(), ApiRequestError)
})

await run('validate true rejects responses that do not match generated schemas', async () => {
  const mock = mockFetch([jsonResponse({ not_dataflows: [] })])
  const client = createClient({
    baseUrl: 'https://api.example.test',
    fetch: mock.fetch,
    validate: true,
  })

  await assertRejects(client.dataflows.list(), ApiValidationError)
})

await run('validation is off by default', async () => {
  const malformed = { not_dataflows: [] }
  const mock = mockFetch([jsonResponse(malformed)])
  const client = createClient({ baseUrl: 'https://api.example.test', fetch: mock.fetch })

  const result = await client.dataflows.list()

  assertEqual(result, malformed)
})

function mockFetch(responses: Response[]): MockFetch {
  const calls: FetchCall[] = []

  return {
    calls,
    fetch: async (input, init) => {
      calls.push({
        init,
        url: input instanceof Request ? input.url : input.toString(),
      })
      const response = responses.shift()
      if (response === undefined) {
        throw new Error('unexpected fetch call')
      }
      return response
    },
  }
}

function jsonResponse(body: unknown, status = 200, headers: HeadersInit = {}): Response {
  const responseHeaders = new Headers(headers)
  responseHeaders.set('content-type', 'application/json')
  return new Response(JSON.stringify(body), { headers: responseHeaders, status })
}

function observationsPage(options: {
  nextCursor: string | null
  value?: number
}): ObservationsResponse {
  return {
    metadata: {
      attribution: 'Source: Australian Bureau of Statistics',
      dataflow: 'abs.cpi',
      license: 'CC-BY-4.0',
      source_url: 'https://www.abs.gov.au/',
    },
    observations: [observation(options.value)],
    pagination: {
      next_cursor: options.nextCursor,
    },
  }
}

function observation(value = 136.9) {
  return {
    attributes: {},
    dimensions: { region: 'AUS' },
    ingested_at: '2024-07-24T00:00:00Z',
    measure_id: 'index',
    revision_no: 1,
    series_key: seriesKey,
    source_artifact_id: artifactId,
    status: 'normal',
    time: '2024-06-01T00:00:00Z',
    time_precision: 'quarter',
    unit: 'index',
    value,
  } as const
}

function dataflow() {
  return {
    attribution: 'Source: Australian Bureau of Statistics',
    description: null,
    dimensions: ['region'],
    frequency: 'quarterly',
    id: 'abs.cpi',
    license: 'CC-BY-4.0',
    measures: ['index'],
    name: 'Consumer Price Index',
    source_id: 'abs',
    source_url: 'https://www.abs.gov.au/',
  } as const
}

function scorecardConfig(): ScorecardConfig {
  return {
    attribution: 'Derived scorecard configuration maintained by australian-kpis.',
    description: 'National Australia scorecard.',
    formula: 'APS = 100 * T * (0.5 + 0.5 * O)',
    id: 'aps',
    indicators: [
      {
        axis: 'throughput',
        cadence: 'monthly',
        component: 'housing',
        confidence: 'high',
        coverage_status: 'missing_expected',
        description: 'Monthly dwelling approvals.',
        dimension_selector: { region: 'AUS' },
        direction: 'higher_is_better',
        display_label: 'Housing approvals',
        indicator_id: 'housing.approvals',
        measure_id: 'dwellings_approved',
        normalization: { best: 25000, worst: 10000 },
        provenance: {
          attribution: 'Source: Australian Bureau of Statistics',
          license: 'CC-BY-4.0',
          source_url: 'https://www.abs.gov.au/',
        },
        source_dataflow_id: 'abs.building_approvals',
        unit: 'dwellings',
        weight: 0.35,
      },
    ],
    label: 'Abundance Position Score',
    license: 'Apache-2.0',
    version: 'aps.v1',
  }
}

function scorecardSnapshot(options: { asOf?: string; score?: number } = {}): ScorecardSnapshot {
  return {
    as_of: options.asOf ?? '2024-02-01',
    confidence: 'medium',
    confidence_band: { high: 100, low: 0 },
    config_version: 'aps.v1',
    contributions: [
      {
        attribution: 'Source: Australian Bureau of Statistics',
        axis: 'throughput',
        component: 'housing',
        confidence: 'high',
        coverage_status: 'resolved',
        dimensions: { region: 'AUS' },
        direction: 'higher_is_better',
        indicator_id: 'housing.approvals',
        label: 'Housing approvals',
        latest_period: '2024-02-01',
        license: 'CC-BY-4.0',
        measure_id: 'dwellings_approved',
        normalized_value: 1,
        raw_value: 25000,
        series_key: seriesKey,
        source_artifact_id: artifactId,
        source_dataflow_id: 'abs.building_approvals',
        source_url: 'https://www.abs.gov.au/',
        unit: 'dwellings',
        weight: 0.35,
      },
    ],
    coverage_pct: 40,
    latest_period: '2024-02-01',
    score: options.score ?? 100,
    scorecard_id: 'aps',
    sub_indexes: [
      {
        axis: 'throughput',
        components: [
          {
            component: 'housing',
            coverage_pct: 100,
            score: 1,
            weight: 0.35,
          },
        ],
        confidence_band: { high: 1, low: 0 },
        coverage_pct: 35,
        score: 1,
        weight: 1,
      },
    ],
    trend: 'up',
    zone: 'green',
  }
}

function dimension() {
  return {
    codelist_id: 'CL_REGION',
    description: null,
    id: 'region',
    name: 'Region',
    position: 0,
  }
}

function assertPath(call: FetchCall | undefined, expected: string): void {
  assertEqual(new URL(required(call).url).pathname, expected)
}

function assertSearch(call: FetchCall | undefined, key: string, expected: string): void {
  assertEqual(new URL(required(call).url).searchParams.get(key), expected)
}

function assertSearchValues(call: FetchCall | undefined, key: string, expected: string[]): void {
  assertEqual(new URL(required(call).url).searchParams.getAll(key), expected)
}

async function assertRejects(
  promise: Promise<unknown>,
  errorType: new (...args: never[]) => Error,
): Promise<void> {
  try {
    await promise
  } catch (error) {
    if (error instanceof errorType) {
      return
    }
    throw new Error(`expected ${errorType.name}, got ${String(error)}`)
  }
  throw new Error(`expected ${errorType.name}`)
}

function assertEqual(actual: unknown, expected: unknown): void {
  const actualJson = JSON.stringify(actual)
  const expectedJson = JSON.stringify(expected)
  if (actualJson !== expectedJson) {
    throw new Error(`expected ${expectedJson}, got ${actualJson}`)
  }
}

function required<T>(value: T | undefined): T {
  if (value === undefined) {
    throw new Error('missing value')
  }
  return value
}

async function run(name: string, fn: () => Promise<void>): Promise<void> {
  try {
    await fn()
    console.log(`ok - ${name}`)
  } catch (error) {
    console.error(`not ok - ${name}`)
    throw error
  }
}
