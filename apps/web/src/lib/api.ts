import { createClient } from '@au-kpis/sdk'

const proxyPath = '/api/au-kpis'

export const apiBaseUrl = process.env.NEXT_PUBLIC_AU_KPIS_API_BASE_URL ?? proxyPath

export const client = createClient({
  baseUrl: apiBaseUrl,
  validate: true,
})
