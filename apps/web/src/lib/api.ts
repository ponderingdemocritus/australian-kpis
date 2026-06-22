import { createClient } from '@au-kpis/sdk'

const defaultBaseUrl =
  process.env.NODE_ENV === 'production' ? '/api/au-kpis' : 'http://127.0.0.1:3000'

export const apiBaseUrl = process.env.NEXT_PUBLIC_AU_KPIS_API_BASE_URL ?? defaultBaseUrl

export const client = createClient({
  baseUrl: apiBaseUrl,
  validate: true,
})
