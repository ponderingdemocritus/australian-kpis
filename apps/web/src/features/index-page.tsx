'use client'

import { type ChartPoint, PlotChart } from '@/components/plot-chart'
import { Badge } from '@/components/ui/badge'
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  type CodelistLookup,
  type FeaturedIndicator,
  type IndexData,
  type LatestIndicatorRow,
  type ObservationGroup,
  deriveIndexData,
  featuredDataflowIds,
} from '@/features/index-data'
import { apiBaseUrl, client } from '@/lib/api'
import { formatDate, formatDelta, formatObservationValue } from '@/lib/format'
import { collectObservationRows, comparisonColors } from '@/lib/observations'
import type { Dataflow } from '@au-kpis/sdk-generated/client'
import { useQuery } from '@tanstack/react-query'
import {
  BookOpen,
  CircleCheck,
  FileJson,
  Gauge,
  RefreshCw,
  Search,
  Server,
  ShieldCheck,
  Webhook,
} from 'lucide-react'
import type { ReactNode } from 'react'
import { useMemo } from 'react'

const endpointDefinitions = [
  { id: 'health', label: 'Health', method: 'GET', path: '/v1/health' },
  { id: 'openapi', label: 'OpenAPI', method: 'GET', path: '/v1/openapi.json' },
  { id: 'dataflows', label: 'Dataflows', method: 'GET', path: '/v1/dataflows' },
  {
    id: 'dataflow-detail',
    label: 'Dataflow detail',
    method: 'GET',
    path: '/v1/dataflows/{id}',
  },
  {
    id: 'codelists',
    label: 'Codelists',
    method: 'GET',
    path: '/v1/dataflows/{id}/codelists/{dim}',
  },
  { id: 'observations', label: 'Observations', method: 'GET', path: '/v1/observations' },
  {
    id: 'series',
    label: 'Series lookup',
    method: 'GET',
    path: '/v1/series/{dataflow}/{series_key}',
  },
  { id: 'search', label: 'Catalog search', method: 'GET', path: '/v1/search' },
  { id: 'subscriptions', label: 'Subscriptions', method: 'POST', path: '/v1/subscriptions' },
] as const

const chartPalette = ['#0f766e', '#2563eb', '#c2410c', '#7c3aed', '#64748b']
const indexObservationLimit = 240

type OpenApiDocument = {
  info?: {
    contact?: {
      email?: string
    }
    license?: {
      name?: string
    }
    title?: string
    version?: string
  }
  openapi?: string
  paths?: Record<string, Record<string, unknown>>
}

type EndpointState = 'error' | 'loading' | 'not-advertised' | 'schema' | 'success'

export function IndexPage() {
  const healthQuery = useQuery({
    queryFn: () => client.health(),
    queryKey: ['index', 'health'],
  })

  const openapiQuery = useQuery({
    queryFn: () => client.openapi(),
    queryKey: ['index', 'openapi'],
  })

  const dataflowsQuery = useQuery({
    queryFn: () => client.dataflows.list(),
    queryKey: ['index', 'dataflows'],
  })

  const dataflows = dataflowsQuery.data?.dataflows ?? []
  const featuredIds = dataflows
    .filter((dataflow) =>
      featuredDataflowIds.includes(dataflow.id as (typeof featuredDataflowIds)[number]),
    )
    .map((dataflow) => dataflow.id)
  const effectiveFeaturedIds =
    featuredIds.length > 0 ? featuredIds : dataflows.slice(0, 3).map((dataflow) => dataflow.id)
  const featuredIdsKey = effectiveFeaturedIds
  const catalogRows = dataflows.filter((dataflow) => effectiveFeaturedIds.includes(dataflow.id))

  const detailsQuery = useQuery({
    enabled: effectiveFeaturedIds.length > 0,
    queryFn: () => Promise.all(effectiveFeaturedIds.map((id) => client.dataflows.get(id))),
    queryKey: ['index', 'featured-dataflow-details', featuredIdsKey],
  })

  const details = detailsQuery.data ?? []

  const codelistsQuery = useQuery({
    enabled: details.length > 0,
    queryFn: () =>
      Promise.all(
        details.flatMap((detail) =>
          detail.dimensions.map(async (dimension): Promise<CodelistLookup> => {
            const response = await client.dataflows.codelists(detail.dataflow.id, dimension.id)
            return {
              codes: response.codelist.codes,
              dataflowId: detail.dataflow.id,
              dimensionId: dimension.id,
            }
          }),
        ),
      ),
    queryKey: [
      'index',
      'featured-codelists',
      details.map((detail) => [
        detail.dataflow.id,
        detail.dimensions.map((dimension) => dimension.id),
      ]),
    ],
  })

  const observationsQuery = useQuery({
    enabled: details.length > 0,
    queryFn: () =>
      Promise.all(
        details.map(
          async (detail): Promise<ObservationGroup> => ({
            detail,
            observations: await collectObservationRows(
              detail.dataflow.id,
              undefined,
              indexObservationLimit,
            ),
          }),
        ),
      ),
    queryKey: ['index', 'featured-observations', featuredIdsKey],
  })

  const searchQuery = useQuery({
    queryFn: () => client.search.catalog({ limit: 5, q: 'index' }),
    queryKey: ['index', 'search', 'index'],
  })

  const codelists = codelistsQuery.data ?? []
  const observationGroups = observationsQuery.data ?? []
  const indexData = useMemo<IndexData>(
    () =>
      deriveIndexData({
        codelists,
        dataflows,
        groups: observationGroups,
      }),
    [codelists, dataflows, observationGroups],
  )
  const openapi = asOpenApiDocument(openapiQuery.data)
  const headlineFeatured = indexData.featured.find((item) => item.id === 'abs-cpi')
  const headlineObservation = headlineFeatured?.current ?? indexData.latestRows[0]?.observation
  const headlineDataflow = headlineFeatured?.dataflow ?? indexData.latestRows[0]?.dataflow

  const seriesQuery = useQuery({
    enabled: headlineObservation !== undefined && headlineDataflow !== undefined,
    queryFn: () =>
      client.observations.latest({
        dataflow: required(headlineDataflow).id,
        seriesKey: required(headlineObservation).series_key,
      }),
    queryKey: [
      'index',
      'series',
      headlineDataflow?.id ?? 'none',
      headlineObservation?.series_key ?? 'none',
    ],
  })

  const sourceMix = indexData.sourceMix
  const frequencyMix = indexData.frequencyMix
  const trendChart = indexData.trendChart
  const regionalChart = indexData.regionalChart
  const regionalLegend = regionalChartRegions(regionalChart)
  const latestPeriod = headlineObservation?.time ? formatDate(headlineObservation.time) : 'waiting'
  const attributions = uniqueText(details.map((detail) => detail.dataflow.attribution))
  const endpointStates = endpointDefinitions.map((endpoint) => ({
    ...endpoint,
    state: endpointState(endpoint.id, {
      codelistsQuery,
      dataflowsQuery,
      detailsQuery,
      healthQuery,
      observationsQuery,
      openapi,
      openapiQuery,
      searchQuery,
      seriesQuery,
    }),
  }))
  const coveredEndpoints = endpointStates.filter((endpoint) =>
    ['schema', 'success'].includes(endpoint.state),
  ).length
  const error = [
    healthQuery,
    openapiQuery,
    dataflowsQuery,
    detailsQuery,
    codelistsQuery,
    observationsQuery,
    searchQuery,
    seriesQuery,
  ].find((query) => query.isError)?.error
  const loading = [
    healthQuery,
    openapiQuery,
    dataflowsQuery,
    detailsQuery,
    codelistsQuery,
    observationsQuery,
    searchQuery,
    seriesQuery,
  ].some((query) => query.isLoading)

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-5 sm:px-6 lg:py-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold tracking-normal">Index</h1>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            Latest loaded Australian economic indicators from the public API.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="secondary">{dataflows.length.toLocaleString('en-AU')} dataflows</Badge>
          <Badge variant="outline">{latestPeriod}</Badge>
        </div>
      </div>

      {error instanceof Error ? <StatusBanner message={error.message} tone="error" /> : null}
      {loading ? <StatusBanner message="Loading API index data" tone="loading" /> : null}

      <section
        className="grid grid-cols-1 items-stretch gap-4 md:grid-cols-3"
        data-testid="index-value-board"
      >
        {indexData.featured.map((indicator) => (
          <FeaturedIndicatorCard indicator={indicator} key={indicator.id} />
        ))}
      </section>

      <section className="grid grid-cols-1 gap-5 xl:grid-cols-2">
        <ChartCard
          chart={
            <PlotChart
              ariaLabel="National CPI index trend"
              colors={comparisonColors}
              data={trendChart}
              height={290}
            />
          }
          description="Australia, quarterly index"
          metric={formatObservationValue(headlineObservation?.value, headlineObservation?.unit)}
          testId="index-trend-chart"
          title="Consumer Price Index"
        />
        <ChartCard
          chart={
            <PlotChart
              ariaLabel="State and territory CPI comparison"
              colors={comparisonColors}
              data={regionalChart}
              height={290}
            />
          }
          description="Latest regional CPI lines from observations"
          legend={regionalLegend}
          metric={`${regionalLegend.length} regions`}
          testId="index-region-chart"
          title="CPI by state/territory"
        />
      </section>

      <AttributionList attributions={attributions} />

      <LatestIndicatorValues rows={indexData.latestRows.slice(0, 8)} />

      <section className="grid grid-cols-1 gap-5 xl:grid-cols-2">
        <DistributionCard
          description={`${dataflows.length} dataflows grouped by source`}
          items={sourceMix}
          title="Source mix"
        />
        <DistributionCard
          description={`${dataflows.length} dataflows grouped by cadence`}
          items={frequencyMix}
          title="Frequency mix"
        />
      </section>

      <section
        className="rounded-lg border bg-card p-4 text-card-foreground shadow-sm"
        data-testid="endpoint-map"
      >
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-base font-semibold">Endpoint map</h2>
            <p className="text-sm text-muted-foreground">Readable endpoints are checked live.</p>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <CircleCheck aria-hidden="true" className="text-primary" />
            <span className="font-medium">{coveredEndpoints} covered</span>
          </div>
        </div>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-9">
          {endpointStates.map((endpoint) => (
            <EndpointTile
              key={endpoint.id}
              label={endpoint.label}
              method={endpoint.method}
              path={endpoint.path}
              state={endpoint.state}
            />
          ))}
        </div>
      </section>

      <section className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1.45fr)_minmax(320px,0.85fr)]">
        <DataflowCatalog rows={catalogRows} observationGroups={observationGroups} />
        <ApiContractCard
          apiBaseUrl={apiBaseUrl}
          openapi={openapi}
          searchResults={searchQuery.data?.results.length ?? 0}
          seriesRevision={
            seriesQuery.data?.revision?.is_revision === true
              ? `revision ${seriesQuery.data.revision.revision_no}`
              : 'latest revision'
          }
        />
      </section>
    </div>
  )
}

function EndpointTile({
  label,
  method,
  path,
  state,
}: {
  label: string
  method: string
  path: string
  state: EndpointState
}) {
  return (
    <div className="min-w-0 rounded-md border bg-background px-3 py-2">
      <div className="flex items-center justify-between gap-2">
        <p className="truncate text-xs font-medium">{label}</p>
        <EndpointStateBadge state={state} />
      </div>
      <p className="mt-1 text-[11px] text-muted-foreground">{method}</p>
      <p className="mt-1 truncate font-mono text-[11px] text-muted-foreground">{path}</p>
    </div>
  )
}

function EndpointStateBadge({ state }: { state: EndpointState }) {
  const label =
    state === 'success'
      ? '200'
      : state === 'schema'
        ? 'schema'
        : state === 'not-advertised'
          ? 'n/a'
          : state === 'loading'
            ? '...'
            : 'err'
  const variant =
    state === 'error'
      ? 'destructive'
      : state === 'loading' || state === 'not-advertised'
        ? 'outline'
        : 'secondary'

  return <Badge variant={variant}>{label}</Badge>
}

function FeaturedIndicatorCard({ indicator }: { indicator: FeaturedIndicator }) {
  const current = indicator.current

  return (
    <Card className="min-w-0" data-testid={`featured-${indicator.id}`}>
      <CardHeader>
        <CardDescription>{indicator.dataflow?.source_id.toUpperCase() ?? 'API'}</CardDescription>
        <CardTitle className="text-3xl tabular-nums">
          {formatObservationValue(current?.value, current?.unit)}
        </CardTitle>
        <CardAction>
          <Gauge aria-hidden="true" className="text-muted-foreground" />
        </CardAction>
      </CardHeader>
      <CardContent className="flex flex-col gap-2 text-sm text-muted-foreground">
        <p className="font-medium text-card-foreground">{indicator.label}</p>
        <p>
          {indicator.seriesLabel} · {current === undefined ? 'waiting' : formatDate(current.time)}
        </p>
        <p>
          {current?.unit ?? 'unit unavailable'} · change{' '}
          {formatDelta(current?.value, indicator.previous?.value)}
        </p>
      </CardContent>
    </Card>
  )
}

function ChartCard({
  chart,
  description,
  metric,
  legend = [],
  testId,
  title,
}: {
  chart: ReactNode
  description: string
  legend?: string[]
  metric: string
  testId: string
  title: string
}) {
  return (
    <Card className="min-w-0" data-testid={testId}>
      <CardHeader>
        <div>
          <h2 className="leading-none font-semibold">{title}</h2>
          <CardDescription>{description}</CardDescription>
        </div>
        <CardAction className="text-right">
          <p className="text-lg font-semibold tabular-nums">{metric}</p>
          <p className="text-xs text-muted-foreground">latest</p>
        </CardAction>
      </CardHeader>
      <CardContent>
        {chart}
        {legend.length > 0 ? (
          <ul className="mt-3 flex flex-wrap gap-3 text-xs text-muted-foreground">
            {legend.map((item, index) => (
              <li className="flex items-center gap-2" key={item}>
                <span
                  aria-hidden="true"
                  className="size-2 rounded-full"
                  style={{
                    backgroundColor:
                      comparisonColors[item] ?? chartPalette[index % chartPalette.length],
                  }}
                />
                {item}
              </li>
            ))}
          </ul>
        ) : null}
      </CardContent>
    </Card>
  )
}

function DistributionCard({
  description,
  items,
  title,
}: {
  description: string
  items: DistributionItem[]
  title: string
}) {
  return (
    <Card className="min-w-0">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-3">
          {items.map((item, index) => (
            <BarListRow
              color={chartPalette[index % chartPalette.length] ?? 'var(--chart-1)'}
              key={item.label}
              label={item.label}
              max={items[0]?.value ?? 1}
              value={item.value}
            />
          ))}
        </div>
      </CardContent>
    </Card>
  )
}

function BarListRow({
  color,
  label,
  max,
  value,
}: {
  color: string
  label: string
  max: number
  value: number
}) {
  const width = max === 0 ? 0 : Math.max(8, Math.round((value / max) * 100))

  return (
    <div className="grid gap-1">
      <div className="flex items-center justify-between gap-3 text-sm">
        <span className="truncate">{label}</span>
        <span className="font-mono tabular-nums text-muted-foreground">{value}</span>
      </div>
      <div className="h-2 overflow-hidden rounded-full bg-muted">
        <div
          className="h-full rounded-full"
          style={{ backgroundColor: color, width: `${width}%` }}
        />
      </div>
    </div>
  )
}

function LatestIndicatorValues({ rows }: { rows: LatestIndicatorRow[] }) {
  return (
    <Card className="min-w-0">
      <CardHeader>
        <CardTitle>Latest indicator values</CardTitle>
        <CardDescription>Last loaded observation per featured series.</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <Table aria-label="Latest indicator values" className="min-w-[760px]">
            <TableHeader>
              <TableRow>
                <TableHead>Indicator</TableHead>
                <TableHead>Series</TableHead>
                <TableHead>Period</TableHead>
                <TableHead>Unit</TableHead>
                <TableHead className="text-right">Value</TableHead>
                <TableHead className="text-right">Change</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={`${row.dataflow.id}-${row.observation.series_key}`}>
                  <TableCell className="font-medium">{row.dataflow.name}</TableCell>
                  <TableCell>{row.seriesLabel}</TableCell>
                  <TableCell>{formatDate(row.observation.time)}</TableCell>
                  <TableCell>{row.observation.unit}</TableCell>
                  <TableCell className="text-right font-mono">
                    {formatObservationValue(row.observation.value, row.observation.unit)}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatDelta(row.observation.value, row.previous?.value)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  )
}

function DataflowCatalog({
  observationGroups,
  rows,
}: {
  observationGroups: ObservationGroup[]
  rows: Dataflow[]
}) {
  return (
    <Card className="min-w-0">
      <CardHeader>
        <CardTitle>Dataflows</CardTitle>
        <CardDescription>Latest catalog entries and fetched observation counts.</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <Table aria-label="Dataflow catalog" className="min-w-[680px]">
            <TableHeader>
              <TableRow>
                <TableHead>Dataflow ID</TableHead>
                <TableHead>Name</TableHead>
                <TableHead>Frequency</TableHead>
                <TableHead>Source</TableHead>
                <TableHead className="text-right">Rows</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.slice(0, 6).map((dataflow) => (
                <TableRow key={dataflow.id}>
                  <TableCell className="font-mono text-xs">{dataflow.id}</TableCell>
                  <TableCell className="font-medium">{dataflow.name}</TableCell>
                  <TableCell>{dataflow.frequency}</TableCell>
                  <TableCell>{dataflow.source_id.toUpperCase()}</TableCell>
                  <TableCell className="text-right font-mono">
                    {(
                      observationGroups.find((group) => group.detail.dataflow.id === dataflow.id)
                        ?.observations.length ?? 0
                    ).toLocaleString('en-AU')}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  )
}

function ApiContractCard({
  apiBaseUrl,
  openapi,
  searchResults,
  seriesRevision,
}: {
  apiBaseUrl: string
  openapi: OpenApiDocument
  searchResults: number
  seriesRevision: string
}) {
  const paths = Object.keys(openapi.paths ?? {})

  return (
    <Card className="min-w-0">
      <CardHeader>
        <CardTitle>API contract</CardTitle>
        <CardDescription>Schema, search, and side-effect endpoint coverage.</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4 text-sm">
        <ContractRow icon={<FileJson aria-hidden="true" />} label="Title">
          {openapi.info?.title ?? 'Australian KPIs API'}
        </ContractRow>
        <ContractRow icon={<BookOpen aria-hidden="true" />} label="Schema">
          OpenAPI {openapi.openapi ?? '3.x'} · {paths.length} paths
        </ContractRow>
        <ContractRow icon={<Search aria-hidden="true" />} label="Search">
          {searchResults} catalog matches for index
        </ContractRow>
        <ContractRow icon={<ShieldCheck aria-hidden="true" />} label="Series">
          {seriesRevision}
        </ContractRow>
        <ContractRow icon={<Webhook aria-hidden="true" />} label="Webhooks">
          {openapi.paths?.['/v1/subscriptions'] === undefined ? 'not advertised' : 'POST available'}
        </ContractRow>
        <ContractRow icon={<Server aria-hidden="true" />} label="Base URL">
          <span className="break-all font-mono">{apiBaseUrl}</span>
        </ContractRow>
      </CardContent>
    </Card>
  )
}

function ContractRow({
  children,
  icon,
  label,
}: {
  children: ReactNode
  icon: ReactNode
  label: string
}) {
  return (
    <div className="grid grid-cols-[1.5rem_6rem_minmax(0,1fr)] items-start gap-3">
      <span className="text-muted-foreground">{icon}</span>
      <span className="text-muted-foreground">{label}</span>
      <span className="min-w-0">{children}</span>
    </div>
  )
}

function AttributionList({ attributions }: { attributions: string[] }) {
  if (attributions.length === 0) {
    return null
  }

  return (
    <section
      className="rounded-md border bg-muted/40 px-4 py-3 text-sm text-muted-foreground"
      data-testid="index-attribution"
    >
      <p className="font-medium text-card-foreground">Attribution</p>
      <ul className="mt-2 flex flex-col gap-1">
        {attributions.map((attribution) => (
          <li key={attribution}>{attribution}</li>
        ))}
      </ul>
    </section>
  )
}

function StatusBanner({ message, tone }: { message: string; tone: 'error' | 'loading' }) {
  return (
    <div
      className={
        tone === 'error'
          ? 'rounded-md border border-destructive/40 bg-card p-3 text-sm text-destructive'
          : 'flex items-center gap-2 rounded-md border bg-card p-3 text-sm text-muted-foreground'
      }
      role={tone === 'error' ? 'alert' : 'status'}
    >
      {tone === 'loading' ? <RefreshCw aria-hidden="true" className="animate-spin" /> : null}
      {message}
    </div>
  )
}

type DistributionItem = {
  label: string
  value: number
}

function regionalChartRegions(chart: ChartPoint[]): string[] {
  return Array.from(new Set(chart.map((point) => point.region)))
}

function uniqueText(values: string[]): string[] {
  return Array.from(new Set(values.filter((value) => value.trim().length > 0)))
}

function endpointState(
  id: (typeof endpointDefinitions)[number]['id'],
  queries: {
    codelistsQuery: QueryState
    dataflowsQuery: QueryState
    detailsQuery: QueryState
    healthQuery: QueryState
    observationsQuery: QueryState
    openapi: OpenApiDocument
    openapiQuery: QueryState
    searchQuery: QueryState
    seriesQuery: QueryState
  },
): EndpointState {
  if (id === 'subscriptions') {
    if (queries.openapiQuery.isError) {
      return 'error'
    }
    if (!queries.openapiQuery.isSuccess) {
      return 'loading'
    }
    return queries.openapi.paths?.['/v1/subscriptions'] === undefined ? 'not-advertised' : 'schema'
  }

  const query =
    id === 'health'
      ? queries.healthQuery
      : id === 'openapi'
        ? queries.openapiQuery
        : id === 'dataflows'
          ? queries.dataflowsQuery
          : id === 'dataflow-detail'
            ? queries.detailsQuery
            : id === 'codelists'
              ? queries.codelistsQuery
              : id === 'observations'
                ? queries.observationsQuery
                : id === 'series'
                  ? queries.seriesQuery
                  : queries.searchQuery

  if (query.isError) {
    return 'error'
  }
  if (query.isSuccess) {
    return 'success'
  }
  return 'loading'
}

type QueryState = {
  isError: boolean
  isLoading: boolean
  isSuccess: boolean
}

function asOpenApiDocument(value: unknown): OpenApiDocument {
  if (value === null || typeof value !== 'object') {
    return {}
  }
  return value as OpenApiDocument
}

function required<T>(value: T | undefined): T {
  if (value === undefined) {
    throw new Error('required value was not available')
  }
  return value
}
