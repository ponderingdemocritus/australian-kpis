import { Field } from '@/components/field'
import { PlotChart } from '@/components/plot-chart'
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
import {
  collectObservations,
  comparisonColors,
  nationalRegion,
  regionName,
  stateRegions,
  toChartPoints,
} from '@/lib/observations'
import type { Code, ObservationsRow } from '@au-kpis/sdk-generated/client'
import { useQuery } from '@tanstack/react-query'
import { RefreshCw } from 'lucide-react'
import type { ReactNode } from 'react'
import { useEffect, useMemo, useState } from 'react'

export function ExplorerPage() {
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
  const states = useMemo(() => stateRegions(regions), [regions])

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
    enabled: selectedDataflow.length > 0 && states.length > 0,
    queryFn: async () => {
      const rows = await Promise.all(
        states.map(async (region) => ({
          observations: await collectObservations(selectedDataflow, region.id),
          region,
        })),
      )
      return rows
    },
    queryKey: ['comparison', selectedDataflow, states.map((region) => region.id).join(',')],
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
            legend={states}
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
