import { check } from 'k6'
import { Rate } from 'k6/metrics'
import http from 'k6/http'

const baseUrl = __ENV.AU_KPIS_BASE_URL || 'http://127.0.0.1:3000'
const apiKey = __ENV.AU_KPIS_API_KEY
const dataflow = __ENV.AU_KPIS_SCALE_DATAFLOW || 'abs.cpi'
const targetRate = Number(__ENV.AU_KPIS_FULL_LOAD_RPS || '1000')
const duration = __ENV.AU_KPIS_FULL_LOAD_DURATION || '30m'
const preAllocatedVUs = Number(__ENV.AU_KPIS_FULL_LOAD_PREALLOCATED_VUS || '400')
const maxVUs = Number(__ENV.AU_KPIS_FULL_LOAD_MAX_VUS || '2500')
const apiReplicas = Number(__ENV.AU_KPIS_API_REPLICAS || '2')

const headers = {
  Accept: 'application/json',
  'Accept-Encoding': 'identity',
  ...(apiKey ? { 'X-API-Key': apiKey } : {}),
}
const noCacheHeaders = { ...headers, 'Cache-Control': 'no-cache' }
const expectedReadStatuses = http.expectedStatuses(200)
const expectedValidationStatuses = http.expectedStatuses(400, 429)
const serverErrorRate = new Rate('server_error_rate')

function workload(rate, endpoint, exec) {
  return {
    executor: 'constant-arrival-rate',
    rate: Math.max(1, Math.round(targetRate * rate)),
    timeUnit: '1s',
    duration,
    preAllocatedVUs: Math.max(1, Math.round(preAllocatedVUs * rate)),
    maxVUs: Math.max(1, Math.round(maxVUs * rate)),
    exec,
    tags: { scenario: 'production-v1-certification', endpoint },
  }
}

export const options = {
  scenarios: {
    warm_single_series_json: workload(0.45, 'warm-single-series', 'warmSingleSeries'),
    cold_range_json: workload(0.15, 'cold-range-json', 'coldRangeJson'),
    rollups: workload(0.1, 'rollups', 'rollupRequest'),
    catalog_source_search: workload(0.1, 'catalog-source-search', 'catalogSourceSearch'),
    aps_latest: workload(0.1, 'aps-latest', 'apsLatest'),
    aps_history: workload(0.05, 'aps-history', 'apsHistory'),
    validation_rate_limit: workload(0.05, 'validation-rate-limit', 'validationRateLimit'),
    bulk_exports: {
      executor: 'constant-vus',
      vus: apiReplicas * 4,
      duration,
      gracefulStop: '2m',
      exec: 'bulkExport',
      tags: { scenario: 'production-v1-certification', endpoint: 'bulk-parquet' },
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    'http_req_duration{endpoint:warm-single-series}': ['p(95)<200'],
    'http_req_duration{endpoint:cold-range-json}': ['p(95)<500'],
    'http_req_duration{endpoint:rollups}': ['p(95)<500'],
    'http_req_duration{endpoint:aps-latest}': ['p(95)<200'],
    'http_req_duration{endpoint:aps-history}': ['p(95)<500'],
    'http_req_duration{endpoint:bulk-parquet}': ['p(95)<30000'],
    server_error_rate: ['rate<0.001'],
    checks: ['rate>0.999'],
    dropped_iterations: ['count==0'],
  },
}

export function setup() {
  const probes = [
    [`/v1/observations?dataflow=${dataflow}&dimensions[benchmark_series]=0&limit=5`, 'observations'],
    ['/v1/sources', 'sources'],
    ['/v1/scorecards/aps/latest', 'snapshot_id'],
    ['/v1/scorecards/aps/history?limit=10', null],
  ]
  for (const [path, field] of probes) {
    const response = request(path, 'setup', expectedReadStatuses)
    check(response, {
      [`setup ${path} is certification-ready`]: (res) => {
        if (res.status !== 200) return false
        if (field === null) return Array.isArray(res.json())
        if (field === 'observations' || field === 'sources') return Array.isArray(res.json(field))
        return res.json(field) !== undefined
      },
    })
  }
}

export function warmSingleSeries() {
  const response = request(
    `/v1/observations?dataflow=${dataflow}&dimensions[benchmark_series]=0&limit=100`,
    'warm-single-series',
    expectedReadStatuses,
  )
  record(response, [200], 'warm single-series JSON returns 200')
}

export function coldRangeJson() {
  const year = 1950 + ((__VU + __ITER) % 60)
  const response = request(
    `/v1/observations?dataflow=${dataflow}&dimensions[benchmark_series]=${(__VU + __ITER) % 100}&since=${year}-01-01&until=${year}-12-31&limit=1000`,
    'cold-range-json',
    expectedReadStatuses,
    noCacheHeaders,
  )
  record(response, [200], 'cold range JSON returns 200')
}

export function rollupRequest() {
  const grains = ['weekly', 'monthly', 'quarterly']
  const frequency = grains[(__VU + __ITER) % grains.length]
  const response = request(
    `/v1/observations?dataflow=${dataflow}&frequency=${frequency}&limit=1000`,
    'rollups',
    expectedReadStatuses,
  )
  record(response, [200], 'rollup returns 200')
}

export function catalogSourceSearch() {
  const paths = [
    '/v1/dataflows?limit=100',
    `/v1/dataflows/${dataflow}`,
    '/v1/sources',
    '/v1/sources/abs',
    '/v1/search?q=certification&limit=20',
  ]
  const response = request(
    paths[(__VU + __ITER) % paths.length],
    'catalog-source-search',
    expectedReadStatuses,
  )
  record(response, [200], 'catalog/source/search returns 200')
}

export function apsLatest() {
  const response = request('/v1/scorecards/aps/latest', 'aps-latest', expectedReadStatuses)
  record(response, [200], 'APS latest returns 200')
}

export function apsHistory() {
  const response = request(
    '/v1/scorecards/aps/history?since=2016-01-01&limit=1000',
    'aps-history',
    expectedReadStatuses,
  )
  record(response, [200], 'APS history returns 200')
}

export function validationRateLimit() {
  const paths = [
    `/v1/observations?dataflow=${dataflow}&unknown=1`,
    `/v1/observations?dataflow=${dataflow}&limit=1&limit=2`,
  ]
  const response = request(
    paths[(__VU + __ITER) % paths.length],
    'validation-rate-limit',
    expectedValidationStatuses,
  )
  record(response, [400, 429], 'validation/rate-limit response is bounded')
}

export function bulkExport() {
  const series = (__VU - 1) % 100
  const response = http.get(
    `${baseUrl}/v1/observations?dataflow=${dataflow}&dimensions[benchmark_series]=${series}&format=parquet&limit=1000000`,
    {
      headers,
      responseType: 'none',
      responseCallback: expectedReadStatuses,
      tags: { endpoint: 'bulk-parquet', mix: 'bulk-export' },
      timeout: '125s',
    },
  )
  record(response, [200], 'bulk Parquet export returns 200')
}

function request(path, endpoint, responseCallback, requestHeaders = headers) {
  return http.get(`${baseUrl}${path}`, {
    headers: requestHeaders,
    responseCallback,
    tags: { endpoint, mix: 'certified' },
    timeout: '125s',
  })
}

function record(response, expectedStatuses, label) {
  serverErrorRate.add(response.status >= 500)
  check(response, { [label]: (res) => expectedStatuses.includes(res.status) })
}

export function handleSummary(data) {
  return {
    'target/release-scale-report/k6-raw-summary.json': JSON.stringify(data, null, 2),
    stdout: `production-v1 scale thresholds: ${JSON.stringify(data.root_group.checks)}\n`,
  }
}
