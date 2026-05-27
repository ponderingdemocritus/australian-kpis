import { check } from 'k6'
import { sleep } from 'k6'
import http from 'k6/http'

const baseUrl = __ENV.AU_KPIS_BASE_URL || 'http://127.0.0.1:3000'
const headers = __ENV.AU_KPIS_API_KEY ? { 'X-API-Key': __ENV.AU_KPIS_API_KEY } : {}

const series = [
  {
    region: 'AUS',
    key: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  },
  {
    region: 'NSW',
    key: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
  },
  {
    region: 'VIC',
    key: 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
  },
  {
    region: 'QLD',
    key: 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
  },
]

export const options = {
  scenarios: {
    sustained: {
      executor: 'constant-vus',
      vus: 100,
      duration: '10m',
      gracefulStop: '30s',
      tags: { scenario: 'sustained' },
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    http_req_duration: ['p(95)<500', 'p(99)<1500'],
    http_req_failed: ['rate<0.001'],
    checks: ['rate>0.99'],
  },
}

export default function () {
  const roll = Math.random()

  if (roll < 0.7) {
    singleSeriesRequest()
  } else if (roll < 0.9) {
    bulkObservationsRequest()
  } else {
    catalogRequest()
  }

  sleep(0.25 + Math.random() * 1.75)
}

export function singleSeriesRequest() {
  const item = pick(series)
  const endpoint =
    Math.random() < 0.75
      ? {
          name: 'single-series observations',
          path: `/v1/observations?dataflow=abs.cpi&dimensions[region]=${item.region}&limit=100`,
          validate: (response) => hasJsonArray(response, 'observations'),
        }
      : {
          name: 'series detail',
          path: `/v1/series/abs.cpi/${item.key}`,
          validate: (response) =>
            response.status === 200 && response.json('series.series_key') === item.key,
        }

  const response = request(endpoint.path, endpoint.name, { mix: 'single-series' })
  check(response, {
    [`${endpoint.name} returns expected response`]: endpoint.validate,
  })
}

export function bulkObservationsRequest() {
  const endpoints = [
    {
      name: 'bulk observations all regions',
      path: '/v1/observations?dataflow=abs.cpi&limit=1000',
      validate: (response) => hasJsonArray(response, 'observations'),
    },
    {
      name: 'bulk observations date window',
      path: '/v1/observations?dataflow=abs.cpi&since=2023-09-01&until=2024-06-01&limit=1000',
      validate: (response) => hasJsonArray(response, 'observations'),
    },
    {
      name: 'bulk observations csv',
      path: '/v1/observations?dataflow=abs.cpi&format=csv&limit=1000',
      validate: (response) =>
        response.status === 200 &&
        response.headers['Content-Type'] !== undefined &&
        response.headers['Content-Type'].includes('text/csv'),
    },
  ]
  const endpoint = pick(endpoints)
  const response = request(endpoint.path, endpoint.name, { mix: 'bulk' })
  check(response, {
    [`${endpoint.name} returns expected response`]: endpoint.validate,
  })
}

export function catalogRequest() {
  const endpoints = [
    {
      name: 'catalog dataflows',
      path: '/v1/dataflows?source=abs&frequency=quarterly',
      validate: (response) => hasJsonArray(response, 'dataflows'),
    },
    {
      name: 'catalog dataflow detail',
      path: '/v1/dataflows/abs.cpi',
      validate: (response) => response.status === 200 && response.json('dataflow.id') === 'abs.cpi',
    },
    {
      name: 'catalog codelist',
      path: '/v1/dataflows/abs.cpi/codelists/region',
      validate: (response) => hasJsonArray(response, 'codelist.codes'),
    },
    {
      name: 'catalog search',
      path: '/v1/search?q=price%20index',
      validate: (response) => hasJsonArray(response, 'results'),
    },
  ]
  const endpoint = pick(endpoints)
  const response = request(endpoint.path, endpoint.name, { mix: 'catalog' })
  check(response, {
    [`${endpoint.name} returns expected response`]: endpoint.validate,
  })
}

function request(path, endpoint, tags) {
  return http.get(`${baseUrl}${path}`, {
    headers,
    tags: { endpoint, ...tags },
  })
}

function hasJsonArray(response, field) {
  return response.status === 200 && Array.isArray(response.json(field))
}

function pick(items) {
  return items[Math.floor(Math.random() * items.length)]
}
