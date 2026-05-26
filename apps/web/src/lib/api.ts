import { createClient } from '@au-kpis/sdk'

const defaultBaseUrl = 'http://127.0.0.1:3000'

export const apiBaseUrl = import.meta.env.VITE_AU_KPIS_API_BASE_URL ?? defaultBaseUrl

export const client = createClient({
  baseUrl: apiBaseUrl,
  validate: true,
})
