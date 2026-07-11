import { type NextRequest, NextResponse } from 'next/server'

const upstreamBaseUrl = process.env.AU_KPIS_API_BASE_URL ?? 'http://127.0.0.1:3000'

export const dynamic = 'force-dynamic'

type ProxyContext = {
  params: Promise<{
    path?: string[]
  }>
}

export async function GET(request: NextRequest, context: ProxyContext) {
  return proxyRequest(request, context, 'GET')
}

export async function HEAD(request: NextRequest, context: ProxyContext) {
  return proxyRequest(request, context, 'HEAD')
}

export async function POST(request: NextRequest, context: ProxyContext) {
  return proxyRequest(request, context, 'POST')
}

export async function DELETE(request: NextRequest, context: ProxyContext) {
  return proxyRequest(request, context, 'DELETE')
}

async function proxyRequest(
  request: NextRequest,
  context: ProxyContext,
  method: 'DELETE' | 'GET' | 'HEAD' | 'POST',
) {
  const { path = [] } = await context.params
  const upstreamUrl = buildUpstreamUrl(path, request.nextUrl.search)
  const requestId = request.headers.get('x-request-id') ?? crypto.randomUUID()
  const headers = new Headers({
    accept: request.headers.get('accept') ?? 'application/json',
    'x-request-id': requestId,
  })
  const apiKey = request.headers.get('x-api-key') ?? process.env.AU_KPIS_API_KEY
  const contentType = request.headers.get('content-type')

  if (apiKey !== undefined && apiKey.length > 0) {
    headers.set('x-api-key', apiKey)
  }
  if (contentType !== null) {
    headers.set('content-type', contentType)
  }

  const originId = process.env.AU_KPIS_BFF_ORIGIN_ID
  const originSecret = process.env.AU_KPIS_BFF_ORIGIN_SECRET
  if (originId && originSecret && originSecret.length >= 32) {
    const clientIp = trustedClientIp(request)
    const timestamp = Math.floor(Date.now() / 1000).toString()
    const upstream = new URL(upstreamUrl)
    const signature = await signOrigin(
      originSecret,
      originId,
      clientIp,
      timestamp,
      requestId,
      method,
      `${upstream.pathname}${upstream.search}`,
    )
    headers.set('x-au-kpis-origin-id', originId)
    headers.set('x-au-kpis-client-ip', clientIp)
    headers.set('x-au-kpis-origin-timestamp', timestamp)
    headers.set('x-au-kpis-origin-signature', signature)
  } else if (process.env.AU_KPIS_ORIGIN_AUTH_REQUIRED === 'true') {
    return NextResponse.json(
      {
        detail: 'BFF origin authentication is not configured',
        status: 503,
        title: 'Dependency unavailable',
        type: 'about:blank',
      },
      { status: 503 },
    )
  }

  try {
    const response = await fetch(upstreamUrl, {
      body: method === 'POST' ? await request.arrayBuffer() : undefined,
      cache: 'no-store',
      headers,
      method,
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

function trustedClientIp(request: NextRequest): string {
  return (
    request.headers.get('cf-connecting-ip') ??
    request.headers.get('x-au-kpis-client-ip') ??
    request.headers.get('x-forwarded-for')?.split(',')[0]?.trim() ??
    '0.0.0.0'
  )
}

async function signOrigin(
  secret: string,
  originId: string,
  clientIp: string,
  timestamp: string,
  requestId: string,
  method: string,
  pathAndQuery: string,
): Promise<string> {
  const encoder = new TextEncoder()
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { hash: 'SHA-256', name: 'HMAC' },
    false,
    ['sign'],
  )
  const value = [originId, clientIp, timestamp, requestId, method, pathAndQuery].join('\n')
  const signature = await crypto.subtle.sign('HMAC', key, encoder.encode(value))
  return Buffer.from(signature).toString('base64url')
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
    'content-disposition',
    'content-encoding',
    'content-type',
    'etag',
    'last-modified',
    'retry-after',
    'vary',
    'x-au-kpis-degraded',
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
