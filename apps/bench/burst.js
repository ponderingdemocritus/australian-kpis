import { check } from 'k6'
import http from 'k6/http'
import { Counter } from 'k6/metrics'
import { Rate } from 'k6/metrics'

const baseUrl = __ENV.AU_KPIS_BASE_URL || 'http://127.0.0.1:3000'
const headers = __ENV.AU_KPIS_API_KEY ? { 'X-API-Key': __ENV.AU_KPIS_API_KEY } : {}

export const rateLimitResponses = new Counter('rate_limit_responses')
export const serverErrorResponses = new Counter('server_error_responses')
export const rateLimitRatio = new Rate('rate_limit_ratio')
export const serverErrorRatio = new Rate('server_error_ratio')
export const rateLimitSeen = new Rate('rate_limit_seen')

const endpoints = [
  {
    name: 'observations all regions',
    path: '/v1/observations?dataflow=abs.cpi&limit=1000',
  },
  {
    name: 'observations aus',
    path: '/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&limit=100',
  },
  {
    name: 'dataflow detail',
    path: '/v1/dataflows/abs.cpi',
  },
  {
    name: 'search',
    path: '/v1/search?q=price%20index',
  },
]

export const options = {
  scenarios: {
    burst: {
      executor: 'ramping-vus',
      stages: [
        { duration: '2m', target: 2000 },
        { duration: '2m', target: 2000 },
        { duration: '2m', target: 0 },
      ],
      gracefulRampDown: '30s',
      tags: { scenario: 'burst' },
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    rate_limit_ratio: ['rate<0.30'],
    server_error_ratio: ['rate<0.005'],
    rate_limit_seen: ['rate>0'],
    checks: ['rate>0.99'],
  },
}

export default function () {
  const endpoint = endpoints[Math.floor(Math.random() * endpoints.length)]
  const response = http.get(`${baseUrl}${endpoint.path}`, {
    headers,
    tags: { endpoint: endpoint.name, mix: 'burst' },
  })

  recordBurstResponse(response)
  check(response, {
    'request succeeds or is rate limited': (r) => r.status === 200 || r.status === 429,
    'no server error response': (r) => r.status < 500,
  })
}

function recordBurstResponse(response) {
  const rateLimited = response.status === 429
  const serverError = response.status >= 500

  if (rateLimited) {
    rateLimitResponses.add(1)
  }
  if (serverError) {
    serverErrorResponses.add(1)
  }

  rateLimitRatio.add(rateLimited)
  serverErrorRatio.add(serverError)
  rateLimitSeen.add(rateLimited)
}
