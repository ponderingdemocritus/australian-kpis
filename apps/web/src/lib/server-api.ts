import 'server-only'

import { createClient } from '@au-kpis/sdk'

const baseUrl = process.env.AU_KPIS_API_BASE_URL ?? 'http://127.0.0.1:3000'

export const serverClient = createClient({
  apiKey: process.env.AU_KPIS_API_KEY,
  baseUrl,
  fetch: (input, init) => fetch(input, { ...init, cache: 'no-store' }),
  validate: true,
})
