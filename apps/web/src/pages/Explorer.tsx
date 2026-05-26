import { type ChartPoint, PlotChart } from '@/components/plot-chart'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { NativeSelect } from '@/components/ui/native-select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { apiBaseUrl, client } from '@/lib/api'
import { formatDate, formatValue } from '@/lib/format'
import type { Code, ObservationsRow } from '@au-kpis/sdk-generated/client'
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query'
import { Database, LineChart, RefreshCw } from 'lucide-react'
import type { ReactNode } from 'react'
import { useEffect, useMemo, useState } from 'react'

const queryClient = new QueryClient()

const nationalRegion = 'AUS'

const comparisonColors: Record<string, string> = {
  Australia: '#0f766e',
  'New South Wales': '#c2410c',
  Queensland: '#2563eb',
  Victoria: '#b45309',
}

export function ExplorerPage() {
  return (
    <QueryClientProvider client={queryClient}>
      <ExplorerView />
    </QueryClientProvider>
  )
}

function ExplorerView() {
  const [selectedDataflow, setSelectedDataflow] = useState('abs.cpi')
  const [selectedRegion, setSelectedRegion] = useState(nationalRegion)

  const dataflowsQuery = useQuery({
    queryFn: () => client.dataflows.list(),
    queryKey: ['dataflows'],
  })

  const dataflows = dataflowsQuery.data?.dataflows ?? []
  const activeDataflow = dataflows.find((dataflow) => dataflow.id === selectedDataflow)

  useEffect(() => {
    if (activeDataflow === undefined && dataflows[0] !== undefined) {
      setSelectedDataflow(dataflows[0].id)
    }
  }, [activeDataflow, dataflows])

  const detailQuery = useQuery({
    enabled: selectedDataflow.length > 0,
    queryFn: () => client.dataflows.get(selectedDataflow),
    queryKey: ['dataflow', selectedDataflow],
  })

  const regionDimension = detailQuery.data?.dimensions.find(
    (dimension) => dimension.id === 'region',
  )

  const regionsQuery = useQuery({
    enabled: selectedDataflow.length > 0 && regionDimension !== undefined,
    queryFn: () => client.dataflows.codelists(selectedDataflow, 'region'),
    queryKey: ['codelist', selectedDataflow, 'region'],
  })

  const regions = regionsQuery.data?.codelist.codes ?? []
  const stateRegions = useMemo(
    () => regions.filter((code) => code.id !== nationalRegion && code.parent_id === nationalRegion),
    [regions],
  )

  const nationalQuery = useQuery({
    enabled: selectedDataflow.length > 0,
    queryFn: () => collectObservations(selectedDataflow, nationalRegion),
    queryKey: ['observations', selectedDataflow, nationalRegion],
  })

  const selectedQuery = useQuery({
    enabled: selectedDataflow.length > 0,
    queryFn: () => collectObservations(selectedDataflow, selectedRegion),
    queryKey: ['observations', selectedDataflow, selectedRegion],
  })

  const comparisonQuery = useQuery({
    enabled: selectedDataflow.length > 0 && stateRegions.length > 0,
    queryFn: async () => {
      const rows = await Promise.all(
        stateRegions.map(async (region) => ({
          observations: await collectObservations(selectedDataflow, region.id),
          region,
        })),
      )
      return rows
    },
    queryKey: ['comparison', selectedDataflow, stateRegions.map((region) => region.id).join(',')],
  })

  const nationalRows = nationalQuery.data ?? []
  const selectedRows = selectedQuery.data ?? []
  const comparisonRows = comparisonQuery.data ?? []
  const selectedRegionName = regionName(regions, selectedRegion)
  const latestObservation = selectedRows.at(-1)

  const nationalChart = toChartPoints(nationalRows, 'Australia')
  const stateChart = comparisonRows.flatMap(({ observations, region }) =>
    toChartPoints(observations, region.name),
  )

  const loading = [
    dataflowsQuery,
    detailQuery,
    regionsQuery,
    nationalQuery,
    selectedQuery,
    comparisonQuery,
  ].some((query) => query.isLoading)

  const error = [
    dataflowsQuery,
    detailQuery,
    regionsQuery,
    nationalQuery,
    selectedQuery,
    comparisonQuery,
  ].find((query) => query.isError)?.error

  return (
    <main className="min-h-screen bg-background text-foreground">
      <header className="border-b border-border bg-card">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-3">
            <span className="flex size-9 items-center justify-center rounded-md bg-primary text-primary-foreground">
              <Database aria-hidden="true" className="size-4" />
            </span>
            <div>
              <p className="text-sm font-semibold">Australian KPIs</p>
              <p className="text-xs text-muted-foreground">Reference client</p>
            </div>
          </div>
          <nav aria-label="Primary">
            <Button aria-current="page" size="sm" variant="outline">
              <LineChart data-icon="inline-start" />
              Explorer
            </Button>
          </nav>
        </div>
      </header>

      <div className="mx-auto grid max-w-7xl grid-cols-1 gap-5 px-6 py-6 lg:grid-cols-[280px_minmax(0,1fr)]">
        <aside className="flex flex-col gap-4">
          <Card>
            <CardHeader>
              <CardTitle>Explorer</CardTitle>
              <CardDescription>Browse dataflows and dimensions through the SDK.</CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              <Field label="Dataflow" htmlFor="dataflow">
                <NativeSelect
                  disabled={dataflows.length === 0}
                  id="dataflow"
                  onChange={(event) => {
                    setSelectedDataflow(event.target.value)
                    setSelectedRegion(nationalRegion)
                  }}
                  value={selectedDataflow}
                >
                  {dataflows.map((dataflow) => (
                    <option key={dataflow.id} value={dataflow.id}>
                      {dataflow.name}
                    </option>
                  ))}
                </NativeSelect>
              </Field>

              <Field label="Region" htmlFor="region">
                <NativeSelect
                  disabled={regions.length === 0}
                  id="region"
                  onChange={(event) => setSelectedRegion(event.target.value)}
                  value={selectedRegion}
                >
                  {regions.map((region) => (
                    <option key={region.id} value={region.id}>
                      {region.name}
                    </option>
                  ))}
                </NativeSelect>
              </Field>

              <div className="rounded-md border border-border bg-muted/40 p-3 text-sm">
                <p className="font-medium">{activeDataflow?.frequency ?? 'quarterly'}</p>
                <p className="mt-1 text-muted-foreground">
                  API base: <span className="break-all font-mono text-xs">{apiBaseUrl}</span>
                </p>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Latest observation</CardTitle>
              <CardDescription>{selectedRegionName}</CardDescription>
            </CardHeader>
            <CardContent>
              <div data-testid="latest-observation">
                <p className="text-3xl font-semibold">
                  {formatValue(latestObservation?.value)}
                  <span className="ml-2 text-sm font-normal text-muted-foreground">index</span>
                </p>
                <p className="mt-2 text-sm text-muted-foreground">
                  {latestObservation === undefined
                    ? 'Waiting for observations'
                    : formatDate(latestObservation.time)}
                </p>
              </div>
            </CardContent>
          </Card>
        </aside>

        <section className="flex min-w-0 flex-col gap-5">
          <div>
            <h1 className="text-2xl font-semibold tracking-normal">Consumer Price Index</h1>
            <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
              {activeDataflow?.description ??
                'Quarterly Consumer Price Index observations from the Australian Bureau of Statistics.'}
            </p>
          </div>

          {error instanceof Error ? <ErrorBanner message={error.message} /> : null}
          {loading ? <LoadingBanner /> : null}

          <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
            <ChartCard
              chart={
                <PlotChart
                  ariaLabel="National CPI line chart"
                  colors={comparisonColors}
                  data={nationalChart}
                />
              }
              description="Australia, quarterly index"
              latest={nationalRows.at(-1)}
              testId="national-cpi-chart"
              title="National CPI"
            />
            <ChartCard
              chart={
                <PlotChart
                  ariaLabel="State comparison line chart"
                  colors={comparisonColors}
                  data={stateChart}
                />
              }
              description="State and territory comparison"
              legend={stateRegions}
              testId="state-comparison-chart"
              title="State comparison"
            />
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Observations</CardTitle>
              <CardDescription>
                {selectedRegionName} observations returned by{' '}
                <code>client.observations.stream()</code>.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Table aria-label="Observations">
                <TableHeader>
                  <TableRow>
                    <TableHead>Region</TableHead>
                    <TableHead>Quarter</TableHead>
                    <TableHead className="text-right">Index</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {selectedRows
                    .slice()
                    .reverse()
                    .map((observation) => (
                      <TableRow key={`${observation.series_key}-${observation.time}`}>
                        <TableCell>{selectedRegionName}</TableCell>
                        <TableCell>{formatDate(observation.time)}</TableCell>
                        <TableCell className="text-right font-medium">
                          {formatValue(observation.value)}
                        </TableCell>
                        <TableCell>{observation.status}</TableCell>
                      </TableRow>
                    ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <p className="text-xs text-muted-foreground">
            {activeDataflow?.attribution ?? 'Source: Australian Bureau of Statistics'}
          </p>
        </section>
      </div>
    </main>
  )
}

type FieldProps = {
  children: ReactNode
  htmlFor: string
  label: string
}

function Field({ children, htmlFor, label }: FieldProps) {
  return (
    <div className="flex flex-col gap-2">
      <label className="text-sm font-medium" htmlFor={htmlFor}>
        {label}
      </label>
      {children}
    </div>
  )
}

type ChartCardProps = {
  chart: ReactNode
  description: string
  latest?: ObservationsRow
  legend?: Code[]
  testId: string
  title: string
}

function ChartCard({ chart, description, latest, legend = [], testId, title }: ChartCardProps) {
  return (
    <Card data-testid={testId}>
      <CardHeader className="flex-row items-start justify-between gap-4">
        <div>
          <CardTitle>{title}</CardTitle>
          <CardDescription>{description}</CardDescription>
        </div>
        {latest === undefined ? null : (
          <div className="text-right">
            <p className="text-lg font-semibold">{formatValue(latest.value)}</p>
            <p className="text-xs text-muted-foreground">{formatDate(latest.time)}</p>
          </div>
        )}
      </CardHeader>
      <CardContent>
        {chart}
        {legend.length > 0 ? (
          <ul className="mt-3 flex flex-wrap gap-3 text-xs text-muted-foreground">
            {legend.map((region) => (
              <li className="flex items-center gap-2" key={region.id}>
                <span
                  aria-hidden="true"
                  className="size-2 rounded-full"
                  style={{ backgroundColor: comparisonColors[region.name] ?? '#475569' }}
                />
                {region.name}
              </li>
            ))}
          </ul>
        ) : null}
      </CardContent>
    </Card>
  )
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div
      className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-900"
      role="alert"
    >
      Explorer could not load API data: {message}
    </div>
  )
}

function LoadingBanner() {
  return (
    <div className="flex items-center gap-2 rounded-md border border-border bg-card p-3 text-sm text-muted-foreground">
      <RefreshCw aria-hidden="true" className="size-4 animate-spin" />
      Loading CPI data
    </div>
  )
}

async function collectObservations(dataflow: string, region: string): Promise<ObservationsRow[]> {
  const observations: ObservationsRow[] = []
  for await (const observation of client.observations.stream({
    dataflow,
    dimensions: { region },
    limit: 100,
  })) {
    observations.push(observation)
  }

  return observations.sort((left, right) => Date.parse(left.time) - Date.parse(right.time))
}

function toChartPoints(rows: ObservationsRow[], region: string): ChartPoint[] {
  return rows.flatMap((row) =>
    row.value === null || row.value === undefined
      ? []
      : [
          {
            date: new Date(row.time),
            label: formatDate(row.time),
            region,
            value: row.value,
          },
        ],
  )
}

function regionName(regions: Code[], id: string): string {
  return regions.find((region) => region.id === id)?.name ?? id
}
