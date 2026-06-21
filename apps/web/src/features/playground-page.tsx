'use client'

import { Field } from '@/components/field'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { NativeSelect } from '@/components/ui/native-select'
import { apiBaseUrl, client } from '@/lib/api'
import { nationalRegion } from '@/lib/observations'
import { useQuery } from '@tanstack/react-query'
import { Play, RefreshCw, SquareTerminal } from 'lucide-react'
import { type FormEvent, useEffect, useMemo, useState } from 'react'

type PlaygroundForm = {
  dataflow: string
  limit: string
  region: string
  since: string
  until: string
}

type PlaygroundParams = {
  dataflow: string
  limit: number
  region: string
  since?: string
  until?: string
}

const initialForm: PlaygroundForm = {
  dataflow: 'abs.cpi',
  limit: '4',
  region: nationalRegion,
  since: '2024-03-01',
  until: '',
}

export function PlaygroundPage() {
  const [form, setForm] = useState<PlaygroundForm>(initialForm)
  const [submittedParams, setSubmittedParams] = useState<PlaygroundParams>(() =>
    normalizeForm(initialForm),
  )

  const dataflowsQuery = useQuery({
    queryFn: () => client.dataflows.list(),
    queryKey: ['dataflows'],
  })

  const dataflows = dataflowsQuery.data?.dataflows ?? []
  const activeDataflow = dataflows.find((dataflow) => dataflow.id === form.dataflow)

  useEffect(() => {
    if (activeDataflow !== undefined || dataflows[0] === undefined) {
      return
    }

    const nextForm = { ...initialForm, dataflow: dataflows[0].id }
    setForm(nextForm)
    setSubmittedParams(normalizeForm(nextForm))
  }, [activeDataflow, dataflows])

  const detailQuery = useQuery({
    enabled: form.dataflow.length > 0,
    queryFn: () => client.dataflows.get(form.dataflow),
    queryKey: ['dataflow', form.dataflow],
  })

  const regionDimension = detailQuery.data?.dimensions.find(
    (dimension) => dimension.id === 'region',
  )

  const regionsQuery = useQuery({
    enabled: form.dataflow.length > 0 && regionDimension !== undefined,
    queryFn: () => client.dataflows.codelists(form.dataflow, 'region'),
    queryKey: ['codelist', form.dataflow, 'region'],
  })

  const regions = regionsQuery.data?.codelist.codes ?? []

  const responseQuery = useQuery({
    enabled: submittedParams.dataflow.length > 0,
    queryFn: () =>
      client.observations.list({
        dataflow: submittedParams.dataflow,
        dimensions: { region: submittedParams.region },
        limit: submittedParams.limit,
        since: submittedParams.since,
        until: submittedParams.until,
      }),
    queryKey: ['playground-observations', submittedParams],
  })

  const previewParams = useMemo(() => normalizeForm(form), [form])
  const curlSnippet = useMemo(() => buildCurlSnippet(apiBaseUrl, previewParams), [previewParams])
  const sdkSnippet = useMemo(() => buildSdkSnippet(previewParams), [previewParams])
  const responseText =
    responseQuery.data === undefined
      ? responseQuery.error instanceof Error
        ? responseQuery.error.message
        : 'Loading response'
      : JSON.stringify(responseQuery.data, null, 2)

  const loading = [dataflowsQuery, detailQuery, regionsQuery, responseQuery].some(
    (query) => query.isLoading,
  )

  const runQuery = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setSubmittedParams(normalizeForm(form))
  }

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-5 sm:px-6 lg:py-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold tracking-normal">Playground</h1>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            Run a live observations query and copy the matching curl or SDK call.
          </p>
        </div>
        <Badge variant="secondary">{submittedParams.limit} row limit</Badge>
      </div>

      {loading ? <LoadingBanner /> : null}

      <section className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]">
        <div className="flex min-w-0 flex-col gap-5">
          <Card className="min-w-0">
            <CardHeader>
              <CardTitle>Query controls</CardTitle>
              <CardDescription>
                Build a request from the same fields exposed by the SDK.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <form className="flex flex-col gap-4" onSubmit={runQuery}>
                <Field htmlFor="playground-dataflow" label="Dataflow">
                  <NativeSelect
                    disabled={dataflows.length === 0}
                    id="playground-dataflow"
                    onChange={(event) =>
                      setForm((current) => ({
                        ...current,
                        dataflow: event.target.value,
                        region: nationalRegion,
                      }))
                    }
                    value={form.dataflow}
                  >
                    {dataflows.map((dataflow) => (
                      <option key={dataflow.id} value={dataflow.id}>
                        {dataflow.name}
                      </option>
                    ))}
                  </NativeSelect>
                </Field>

                <Field htmlFor="playground-region" label="Region">
                  <NativeSelect
                    disabled={regions.length === 0}
                    id="playground-region"
                    onChange={(event) =>
                      setForm((current) => ({ ...current, region: event.target.value }))
                    }
                    value={form.region}
                  >
                    {regions.map((region) => (
                      <option key={region.id} value={region.id}>
                        {region.name}
                      </option>
                    ))}
                  </NativeSelect>
                </Field>

                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-1">
                  <Field htmlFor="playground-since" label="Since">
                    <Input
                      id="playground-since"
                      onChange={(event) =>
                        setForm((current) => ({ ...current, since: event.target.value }))
                      }
                      type="date"
                      value={form.since}
                    />
                  </Field>

                  <Field htmlFor="playground-until" label="Until">
                    <Input
                      id="playground-until"
                      onChange={(event) =>
                        setForm((current) => ({ ...current, until: event.target.value }))
                      }
                      type="date"
                      value={form.until}
                    />
                  </Field>
                </div>

                <Field htmlFor="playground-limit" label="Limit">
                  <Input
                    id="playground-limit"
                    max={10000}
                    min={1}
                    onChange={(event) =>
                      setForm((current) => ({ ...current, limit: event.target.value }))
                    }
                    type="number"
                    value={form.limit}
                  />
                </Field>

                <Button type="submit">
                  <Play aria-hidden="true" data-icon="inline-start" />
                  Run query
                </Button>
              </form>
            </CardContent>
          </Card>

          <Card className="min-w-0">
            <CardHeader>
              <CardTitle>Request</CardTitle>
              <CardDescription>{activeDataflow?.frequency ?? 'quarterly'} data</CardDescription>
            </CardHeader>
            <CardContent className="text-sm text-muted-foreground">
              {submittedParams.dataflow} · {submittedParams.region} · limit {submittedParams.limit}
            </CardContent>
          </Card>
        </div>

        <div className="flex min-w-0 flex-col gap-5">
          <Card className="min-w-0">
            <CardHeader>
              <div>
                <CardTitle>Response</CardTitle>
                <CardDescription>
                  JSON returned by <code>client.observations.list()</code>.
                </CardDescription>
              </div>
              <CardAction>
                <SquareTerminal aria-hidden="true" className="text-muted-foreground" />
              </CardAction>
            </CardHeader>
            <CardContent>
              <textarea
                aria-label="Playground response JSON"
                className="h-[420px] w-full resize-none overflow-auto rounded-md border bg-muted/40 p-4 font-mono text-xs leading-relaxed text-foreground outline-none focus:border-primary focus:ring-2 focus:ring-ring/20 md:h-[520px]"
                data-testid="playground-response"
                readOnly
                spellCheck={false}
                value={responseText}
              />
            </CardContent>
          </Card>

          <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
            <SnippetCard label="Curl" testId="playground-curl" value={curlSnippet} />
            <SnippetCard label="SDK" testId="playground-sdk" value={sdkSnippet} />
          </div>
        </div>
      </section>
    </div>
  )
}

function LoadingBanner() {
  return (
    <div className="flex items-center gap-2 rounded-md border bg-card p-3 text-sm text-muted-foreground">
      <RefreshCw aria-hidden="true" className="animate-spin" />
      Running observations query
    </div>
  )
}

function SnippetCard({ label, testId, value }: { label: string; testId: string; value: string }) {
  return (
    <Card className="min-w-0">
      <CardHeader>
        <CardTitle>{label}</CardTitle>
      </CardHeader>
      <CardContent>
        <textarea
          aria-label={`${label} snippet`}
          className="min-h-40 w-full resize-none overflow-auto rounded-md border bg-muted/40 p-4 font-mono text-xs leading-relaxed text-foreground outline-none focus:border-primary focus:ring-2 focus:ring-ring/20"
          data-testid={testId}
          readOnly
          spellCheck={false}
          value={value}
        />
      </CardContent>
    </Card>
  )
}

function normalizeForm(form: PlaygroundForm): PlaygroundParams {
  return {
    dataflow: form.dataflow,
    limit: normalizeLimit(form.limit),
    region: form.region,
    since: form.since.length > 0 ? form.since : undefined,
    until: form.until.length > 0 ? form.until : undefined,
  }
}

function normalizeLimit(value: string): number {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed)) {
    return 10
  }
  return Math.min(10_000, Math.max(1, parsed))
}

function buildCurlSnippet(baseUrl: string, params: PlaygroundParams): string {
  const lines = [
    `curl -sS --get '${baseUrl}/v1/observations'`,
    `  --data-urlencode 'dataflow=${params.dataflow}'`,
    `  --data-urlencode 'dimensions[region]=${params.region}'`,
    `  --data-urlencode 'limit=${params.limit}'`,
  ]

  if (params.since !== undefined) {
    lines.push(`  --data-urlencode 'since=${params.since}'`)
  }
  if (params.until !== undefined) {
    lines.push(`  --data-urlencode 'until=${params.until}'`)
  }

  return lines.join(' \\\n')
}

function buildSdkSnippet(params: PlaygroundParams): string {
  const lines = [
    'const response = await client.observations.list({',
    `  dataflow: '${escapeSnippet(params.dataflow)}',`,
    '  dimensions: {',
    `    region: '${escapeSnippet(params.region)}',`,
    '  },',
    `  limit: ${params.limit},`,
  ]

  if (params.since !== undefined) {
    lines.push(`  since: '${escapeSnippet(params.since)}',`)
  }
  if (params.until !== undefined) {
    lines.push(`  until: '${escapeSnippet(params.until)}',`)
  }

  lines.push('})')
  return lines.join('\n')
}

function escapeSnippet(value: string): string {
  return value.replaceAll("'", "\\'")
}
