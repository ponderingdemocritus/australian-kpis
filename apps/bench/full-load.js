import { check } from 'k6'
import http from 'k6/http'

const baseUrl = __ENV.AU_KPIS_BASE_URL || 'http://127.0.0.1:3000'
const headers = {
  Accept: 'application/json',
  'Accept-Encoding': 'identity',
  ...(__ENV.AU_KPIS_API_KEY ? { 'X-API-Key': __ENV.AU_KPIS_API_KEY } : {}),
}
const targetRate = Number(__ENV.AU_KPIS_FULL_LOAD_RPS || '1000')
const duration = __ENV.AU_KPIS_FULL_LOAD_DURATION || '10m'
const preAllocatedVUs = Number(__ENV.AU_KPIS_FULL_LOAD_PREALLOCATED_VUS || '250')
const maxVUs = Number(__ENV.AU_KPIS_FULL_LOAD_MAX_VUS || '2000')
const observationsUrl = `${baseUrl}/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&limit=5`

export const options = {
  scenarios: {
    observations_1000_rps: {
      executor: 'constant-arrival-rate',
      rate: targetRate,
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs,
      maxVUs,
      tags: { scenario: 'full-load' },
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    'http_req_duration{endpoint:observations}': ['p(99)<1000'],
    http_req_failed: ['rate<0.001'],
    checks: ['rate>0.999'],
    dropped_iterations: ['count==0'],
  },
}

if (duration !== '10m') {
  options.scenarios.observations_1000_rps.duration = duration
}

export function setup() {
  const response = http.get(observationsUrl, {
    headers,
    tags: { endpoint: 'observations', mix: 'setup' },
  })
  check(response, {
    'observations returns JSON page': (res) =>
      res.status === 200 && Array.isArray(res.json('observations')),
  })
}

export default function () {
  const response = http.get(observationsUrl, {
    headers,
    responseType: 'none',
    tags: { endpoint: 'observations', mix: 'full-load' },
  })
  check(response, {
    'observations returns 200': (res) => res.status === 200,
  })
}
