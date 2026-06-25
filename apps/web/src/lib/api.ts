import { createClient } from '@au-kpis/sdk'

const defaultBaseUrl = '/api/au-kpis'

export const apiBaseUrl = process.env.NEXT_PUBLIC_AU_KPIS_API_BASE_URL ?? defaultBaseUrl

export const client = createClient({
  baseUrl: apiBaseUrl,
  validate: false,
})
