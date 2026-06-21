import { type NextRequest, NextResponse } from 'next/server'

const upstreamBaseUrl = process.env.AU_KPIS_API_BASE_URL ?? 'http://127.0.0.1:3000'

export const dynamic = 'force-dynamic'

type ProxyContext = {
  params: Promise<{
    path?: string[]
  }>
}

export async function GET(request: NextRequest, context: ProxyContext) {
  const { path = [] } = await context.params
  const upstreamUrl = buildUpstreamUrl(path, request.nextUrl.search)
  const headers = new Headers({
    accept: request.headers.get('accept') ?? 'application/json',
  })
  const apiKey = request.headers.get('x-api-key') ?? process.env.AU_KPIS_API_KEY

  if (apiKey !== undefined && apiKey.length > 0) {
    headers.set('x-api-key', apiKey)
  }

  try {
    const response = await fetch(upstreamUrl, {
      cache: 'no-store',
      headers,
    })
    const responseHeaders = proxiedResponseHeaders(response.headers)

    return new NextResponse(response.body, {
      headers: responseHeaders,
      status: response.status,
      statusText: response.statusText,
    })
  } catch (error) {
    return NextResponse.json(
      {
        detail: error instanceof Error ? error.message : 'Unknown upstream error',
        status: 502,
        title: 'API proxy request failed',
        type: 'about:blank',
      },
      { status: 502 },
    )
  }
}

export async function OPTIONS() {
  return new NextResponse(null, { status: 204 })
}

function buildUpstreamUrl(path: string[], search: string): string {
  const encodedPath = path.map((segment) => encodeURIComponent(segment)).join('/')
  const base = upstreamBaseUrl.replace(/\/+$/, '')

  return `${base}/${encodedPath}${search}`
}

function proxiedResponseHeaders(headers: Headers): Headers {
  const proxied = new Headers()
  const copiedHeaders = [
    'cache-control',
    'content-type',
    'retry-after',
    'x-ratelimit-limit',
    'x-ratelimit-remaining',
    'x-ratelimit-reset',
    'x-request-id',
  ]

  for (const header of copiedHeaders) {
    const value = headers.get(header)
    if (value !== null) {
      proxied.set(header, value)
    }
  }

  return proxied
}
