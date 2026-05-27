import { check } from 'k6'
import http from 'k6/http'

const baseUrl = __ENV.AU_KPIS_PDF_BASE_URL || 'http://127.0.0.1:8010'
const s3Key = __ENV.AU_KPIS_PDF_S3_KEY || 'artifacts/fixtures/bp4-agency-resourcing.pdf'
const sourceId = __ENV.AU_KPIS_PDF_SOURCE_ID || 'treasury'

export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    http_req_duration: ['p(95)<60000'],
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
  },
}

export default function () {
  const health = http.get(`${baseUrl}/health`, {
    tags: { endpoint: 'pdf health' },
  })
  check(health, {
    'pdf health returns ok': (response) =>
      response.status === 200 && response.json('status') === 'ok',
  })

  const extract = http.post(
    `${baseUrl}/extract`,
    JSON.stringify({
      s3_key: s3Key,
      source_id: sourceId,
      artifact_date: '2026-05-12',
      strategy: 'deterministic',
    }),
    {
      headers: { 'content-type': 'application/json' },
      timeout: '120s',
      tags: { endpoint: 'pdf extract' },
    },
  )
  check(extract, {
    'pdf extract returns candidates': (response) =>
      response.status === 200 &&
      response.json('artifact_key') === s3Key &&
      response.json('backend.kind') === 'deterministic' &&
      Array.isArray(response.json('tables')),
  })
}
