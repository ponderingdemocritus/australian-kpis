import type {
  DataflowCodelistResponse,
  DataflowDetailResponse,
  DataflowsResponse,
  HealthResponse,
  ListDataflowsParams,
  ObservationsResponse,
  ObservationsRow,
  SeriesLookupResponse,
} from '@au-kpis/sdk-generated/client'

const DEFAULT_BASE_URL = 'https://api.au-kpis.example'
const DEFAULT_MAX_ATTEMPTS = 3
const DEFAULT_RETRY_DELAY_MS = 250

type FetchLike = typeof fetch

type Sleep = (ms: number) => Promise<void>

type RetryOptions = {
  /**
   * Total attempts, including the first request.
   */
  maxAttempts?: number
  /**
   * Fallback exponential backoff base when the response has no Retry-After header.
   */
  baseDelayMs?: number
  /**
   * Override for tests or custom schedulers.
   */
  sleep?: Sleep
}

export type CreateClientOptions = {
  apiKey?: string
  baseUrl?: string
  fetch?: FetchLike
  retry?: RetryOptions
  validate?: boolean
}

export type ObservationsListParams = {
  cursor?: string
  dataflow: string
  dimensions?: Record<string, string>
  frequency?: string
  limit?: number
  since?: string
  until?: string
}

export type ObservationLatestParams = {
  dataflow: string
  seriesKey: string
}

export type AuKpisClient = {
  dataflows: {
    codelists: (id: string, dim: string) => Promise<DataflowCodelistResponse>
    get: (id: string) => Promise<DataflowDetailResponse>
    list: (params?: ListDataflowsParams) => Promise<DataflowsResponse>
  }
  health: () => Promise<HealthResponse>
  observations: {
    latest: (params: ObservationLatestParams) => Promise<SeriesLookupResponse>
    list: (params: ObservationsListParams) => Promise<ObservationsResponse>
    stream: (params: ObservationsListParams) => AsyncIterable<ObservationsRow>
  }
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

export class ApiValidationError extends Error {
  readonly schema: SchemaName
  override readonly cause: unknown

  constructor(schema: SchemaName, cause: unknown) {
    super(`API response failed ${schema} validation`)
    this.name = 'ApiValidationError'
    this.schema = schema
    this.cause = cause
  }
}

type SchemaName =
  | 'DataflowCodelistResponse'
  | 'DataflowDetailResponse'
  | 'DataflowsResponse'
  | 'HealthResponse'
  | 'ObservationsResponse'
  | 'SeriesLookupResponse'

type SchemaModule = typeof import('@au-kpis/sdk-generated/zod')

let schemaModule: Promise<SchemaModule> | undefined

export function createClient(options: CreateClientOptions = {}): AuKpisClient {
  const fetchImpl = options.fetch ?? globalThis.fetch?.bind(globalThis)

  if (fetchImpl === undefined) {
    throw new Error('createClient requires a fetch implementation')
  }

  const context: RequestContext = {
    apiKey: options.apiKey,
    baseUrl: normalizeBaseUrl(options.baseUrl ?? DEFAULT_BASE_URL),
    fetchImpl,
    retry: normalizeRetry(options.retry),
    validate: options.validate === true,
  }

  const listObservations = (params: ObservationsListParams) =>
    requestJson<ObservationsResponse>(context, {
      path: '/v1/observations',
      query: observationsQuery(params),
      schema: 'ObservationsResponse',
    })

  return {
    dataflows: {
      codelists: (id, dim) =>
        requestJson<DataflowCodelistResponse>(context, {
          path: `/v1/dataflows/${encodePathSegment(id)}/codelists/${encodePathSegment(dim)}`,
          schema: 'DataflowCodelistResponse',
        }),
      get: (id) =>
        requestJson<DataflowDetailResponse>(context, {
          path: `/v1/dataflows/${encodePathSegment(id)}`,
          schema: 'DataflowDetailResponse',
        }),
      list: (params) =>
        requestJson<DataflowsResponse>(context, {
          path: '/v1/dataflows',
          query: params,
          schema: 'DataflowsResponse',
        }),
    },
    health: () =>
      requestJson<HealthResponse>(context, {
        path: '/v1/health',
        schema: 'HealthResponse',
      }),
    observations: {
      latest: (params) =>
        requestJson<SeriesLookupResponse>(context, {
          path: `/v1/series/${encodePathSegment(params.dataflow)}/${encodePathSegment(params.seriesKey)}`,
          schema: 'SeriesLookupResponse',
        }),
      list: listObservations,
      stream: (params) => streamObservations(params, listObservations),
    },
    openapi: () =>
      requestJson<unknown>(context, {
        path: '/v1/openapi.json',
      }),
  }
}

type RequestContext = {
  apiKey?: string
  baseUrl: string
  fetchImpl: FetchLike
  retry: Required<RetryOptions>
  validate: boolean
}

type RequestSpec = {
  path: string
  query?: Record<string, string | number | string[] | undefined>
  schema?: SchemaName
}

async function requestJson<T>(context: RequestContext, spec: RequestSpec): Promise<T> {
  const url = buildUrl(context.baseUrl, spec.path, spec.query)
  const headers = new Headers({ accept: 'application/json' })

  if (context.apiKey !== undefined && context.apiKey.length > 0) {
    headers.set('x-api-key', context.apiKey)
  }

  let lastNetworkError: unknown
  for (let attempt = 1; attempt <= context.retry.maxAttempts; attempt += 1) {
    try {
      const response = await context.fetchImpl(url, { headers, method: 'GET' })
      const body = [204, 205, 304].includes(response.status) ? '' : await response.text()

      if (response.ok) {
        const data = (body.length > 0 ? JSON.parse(body) : {}) as unknown
        return validateResponse<T>(context, spec.schema, data)
      }

      if (attempt < context.retry.maxAttempts && shouldRetry(response.status)) {
        await context.retry.sleep(retryDelayMs(response, context.retry, attempt))
        continue
      }

      throw new ApiRequestError(response.status, response.statusText, body)
    } catch (error) {
      if (error instanceof ApiRequestError || error instanceof ApiValidationError) {
        throw error
      }

      lastNetworkError = error
      if (attempt < context.retry.maxAttempts) {
        await context.retry.sleep(fallbackDelayMs(context.retry, attempt))
      }
    }
  }

  throw lastNetworkError instanceof Error ? lastNetworkError : new Error(String(lastNetworkError))
}

async function validateResponse<T>(
  context: RequestContext,
  schema: SchemaName | undefined,
  data: unknown,
): Promise<T> {
  if (!context.validate || schema === undefined) {
    return data as T
  }

  try {
    const module = await loadSchemas()
    return module.schemas[schema].parse(data) as T
  } catch (error) {
    throw new ApiValidationError(schema, error)
  }
}

function loadSchemas(): Promise<SchemaModule> {
  schemaModule ??= import('@au-kpis/sdk-generated/zod')
  return schemaModule
}

async function* streamObservations(
  params: ObservationsListParams,
  list: (params: ObservationsListParams) => Promise<ObservationsResponse>,
): AsyncGenerator<ObservationsRow, void, void> {
  let cursor = params.cursor

  do {
    const page = await list({ ...params, cursor })
    for (const observation of page.observations) {
      yield observation
    }
    cursor = page.pagination.next_cursor ?? undefined
  } while (cursor !== undefined)
}

function observationsQuery(params: ObservationsListParams): RequestSpec['query'] {
  const dimensions =
    params.dimensions === undefined ? undefined : dimensionsQuery(params.dimensions)

  return {
    cursor: params.cursor,
    dataflow: params.dataflow,
    'dimensions[]': dimensions,
    frequency: params.frequency,
    limit: params.limit,
    since: params.since,
    until: params.until,
  }
}

function dimensionsQuery(dimensions: Record<string, string>): string[] {
  return Object.entries(dimensions)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
}

function buildUrl(baseUrl: string, path: string, query: RequestSpec['query'] = {}): string {
  const url = new URL(`${baseUrl}${path}`)

  for (const [key, value] of Object.entries(query)) {
    if (value === undefined) {
      continue
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        url.searchParams.append(key, item)
      }
      continue
    }

    url.searchParams.set(key, value.toString())
  }

  return url.toString()
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '')
}

function encodePathSegment(value: string): string {
  return encodeURIComponent(value)
}

function normalizeRetry(retry: RetryOptions = {}): Required<RetryOptions> {
  return {
    baseDelayMs: retry.baseDelayMs ?? DEFAULT_RETRY_DELAY_MS,
    maxAttempts: Math.max(1, Math.trunc(retry.maxAttempts ?? DEFAULT_MAX_ATTEMPTS)),
    sleep: retry.sleep ?? defaultSleep,
  }
}

function shouldRetry(status: number): boolean {
  return (
    status === 408 ||
    status === 429 ||
    status === 500 ||
    status === 502 ||
    status === 503 ||
    status === 504
  )
}

function retryDelayMs(response: Response, retry: Required<RetryOptions>, attempt: number): number {
  const retryAfter = parseRetryAfter(response.headers.get('retry-after'))
  return retryAfter ?? fallbackDelayMs(retry, attempt)
}

function parseRetryAfter(value: string | null): number | undefined {
  if (value === null) {
    return undefined
  }

  const seconds = Number(value)
  if (Number.isFinite(seconds)) {
    return Math.max(0, seconds * 1000)
  }

  const dateMs = Date.parse(value)
  if (Number.isNaN(dateMs)) {
    return undefined
  }

  return Math.max(0, dateMs - Date.now())
}

function fallbackDelayMs(retry: Required<RetryOptions>, attempt: number): number {
  return retry.baseDelayMs * 2 ** Math.max(0, attempt - 1)
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}
