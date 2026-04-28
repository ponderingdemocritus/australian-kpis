import type { components } from '@au-kpis/sdk-generated/types'

const DEFAULT_BASE_URL = 'https://api.au-kpis.example'

type FetchLike = typeof fetch
type HealthResponse = components['schemas']['HealthResponse']

export type CreateClientOptions = {
  apiKey?: string
  baseUrl?: string
  fetch?: FetchLike
}

export type AuKpisClient = {
  health: () => Promise<HealthResponse>
  openapi: () => Promise<unknown>
}

export class ApiRequestError extends Error {
  readonly body: string
  readonly status: number

  constructor(status: number, statusText: string, body: string) {
    super(`API request failed with ${status} ${statusText}`)
    this.name = 'ApiRequestError'
    this.status = status
    this.body = body
  }
}

export function createClient(options: CreateClientOptions = {}): AuKpisClient {
  const fetchImpl = options.fetch ?? globalThis.fetch

  if (fetchImpl === undefined) {
    throw new Error('createClient requires a fetch implementation')
  }

  const baseUrl = normalizeBaseUrl(options.baseUrl ?? DEFAULT_BASE_URL)

  return {
    health: () => requestJson<HealthResponse>(fetchImpl, baseUrl, options.apiKey, '/v1/health'),
    openapi: () => requestJson<unknown>(fetchImpl, baseUrl, options.apiKey, '/v1/openapi.json'),
  }
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '')
}

async function requestJson<T>(
  fetchImpl: FetchLike,
  baseUrl: string,
  apiKey: string | undefined,
  path: string,
): Promise<T> {
  const headers = new Headers({ accept: 'application/json' })

  if (apiKey !== undefined && apiKey.length > 0) {
    headers.set('x-api-key', apiKey)
  }

  const response = await fetchImpl(`${baseUrl}${path}`, { headers })

  if (!response.ok) {
    throw new ApiRequestError(response.status, response.statusText, await response.text())
  }

  return (await response.json()) as T
}
