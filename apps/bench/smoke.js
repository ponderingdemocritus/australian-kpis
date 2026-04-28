import { check } from 'k6'
import http from 'k6/http'

const baseUrl = __ENV.AU_KPIS_BASE_URL || 'http://127.0.0.1:3000'

export const options = {
  vus: 1,
  duration: '30s',
  thresholds: {
    http_req_duration: ['p(95)<200'],
    http_req_failed: ['rate<0.01'],
  },
}

export default function () {
  const health = http.get(`${baseUrl}/v1/health`)
  check(health, {
    'health returns 200': (response) => response.status === 200,
  })

  const openapi = http.get(`${baseUrl}/v1/openapi.json`)
  check(openapi, {
    'openapi returns 200': (response) => response.status === 200,
  })
}
