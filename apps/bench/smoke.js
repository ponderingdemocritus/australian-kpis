import { check } from 'k6'
import { sleep } from 'k6'
import http from 'k6/http'

const baseUrl = __ENV.AU_KPIS_BASE_URL || 'http://127.0.0.1:3000'
const headers = __ENV.AU_KPIS_API_KEY ? { 'X-API-Key': __ENV.AU_KPIS_API_KEY } : {}
const fixtureSeriesKey = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'

const endpoints = [
  {
    name: 'health',
    path: '/v1/health',
    check: (response) => response.status === 200 && response.json('status') === 'ok',
  },
  {
    name: 'openapi',
    path: '/v1/openapi.json',
    check: (response) => response.status === 200 && response.json('openapi') !== undefined,
  },
  {
    name: 'dataflows list',
    path: '/v1/dataflows?source=abs&frequency=quarterly',
    check: (response) => response.status === 200 && Array.isArray(response.json('dataflows')),
  },
  {
    name: 'dataflow detail',
    path: '/v1/dataflows/abs.cpi',
    check: (response) => response.status === 200 && response.json('dataflow.id') === 'abs.cpi',
  },
  {
    name: 'dataflow codelist',
    path: '/v1/dataflows/abs.cpi/codelists/region',
    check: (response) => response.status === 200 && Array.isArray(response.json('codelist.codes')),
  },
  {
    name: 'observations',
    path: '/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&limit=5',
    check: (response) => response.status === 200 && Array.isArray(response.json('observations')),
  },
  {
    name: 'series detail',
    path: '/v1/series/abs.cpi/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    check: (response) =>
      response.status === 200 && response.json('series.series_key') === fixtureSeriesKey,
  },
  {
    name: 'search',
    path: '/v1/search?q=price%20index',
    check: (response) => response.status === 200 && Array.isArray(response.json('results')),
  },
]

export const options = {
  vus: 1,
  duration: '30s',
  thresholds: {
    http_req_duration: ['p(95)<200'],
    http_req_failed: ['rate<0.01'],
  },
}

export default function () {
  for (const endpoint of endpoints) {
    const response = http.get(`${baseUrl}${endpoint.path}`, {
      headers,
      tags: { endpoint: endpoint.name },
    })
    check(response, {
      [`${endpoint.name} returns expected response`]: endpoint.check,
    })
  }
  sleep(2)
}
