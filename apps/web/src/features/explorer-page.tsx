'use client'

import { Field } from '@/components/field'
import { PlotChart } from '@/components/plot-chart'
import { Badge } from '@/components/ui/badge'
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
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
  nationalRegionId,
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
  const activeNationalRegion = useMemo(() => nationalRegionId(regions), [regions])
  const states = useMemo(() => stateRegions(regions), [regions])
  const observationDimensionIds = useMemo(
    () => detailQuery.data?.dimensions.map((dimension) => dimension.id) ?? ['region'],
    [detailQuery.data],
  )

  useEffect(() => {
    if (regions.length === 0) {
      return
    }
    if (!regions.some((region) => region.id === selectedRegion)) {
      setSelectedRegion(activeNationalRegion)
    }
  }, [activeNationalRegion, regions, selectedRegion])

  const nationalQuery = useQuery({
    enabled: selectedDataflow.length > 0 && regions.length > 0,
    queryFn: () =>
      collectObservations(selectedDataflow, activeNationalRegion, observationDimensionIds),
    queryKey: ['observations', selectedDataflow, activeNationalRegion, observationDimensionIds],
  })

  const selectedQuery = useQuery({
    enabled: selectedDataflow.length > 0,
    queryFn: () => collectObservations(selectedDataflow, selectedRegion, observationDimensionIds),
    queryKey: ['observations', selectedDataflow, selectedRegion, observationDimensionIds],
  })

  const comparisonQuery = useQuery({
    enabled: selectedDataflow.length > 0 && states.length > 0,
    queryFn: async () => {
      const rows = await Promise.all(
        states.map(async (region) => ({
          observations: await collectObservations(
            selectedDataflow,
            region.id,
            observationDimensionIds,
          ),
          region,
        })),
      )
      return rows
    },
    queryKey: [
      'comparison',
      selectedDataflow,
      states.map((region) => region.id).join(','),
      observationDimensionIds,
    ],
  })

  const nationalRows = nationalQuery.data ?? []
  const selectedRows = selectedQuery.data ?? []
  const comparisonRows = comparisonQuery.data ?? []
  const selectedRegionName = regionName(regions, selectedRegion)
  const latestObservation = selectedRows.at(-1)
  const previousObservation = selectedRows.at(-2)
  const annualObservation =
    latestObservation === undefined
      ? undefined
      : samePeriodPreviousYear(selectedRows, latestObservation)

  const nationalChart = toChartPoints(nationalRows, 'Australia')
  const stateChart = comparisonRows.flatMap(({ observations, region }) =>
    toChartPoints(observations, region.name),
  )
  const stateLatestValues = comparisonRows
    .map(({ observations }) => observations.at(-1)?.value)
    .filter((value): value is number => typeof value === 'number')
  const stateSpread =
    stateLatestValues.length === 0
      ? undefined
      : Math.max(...stateLatestValues) - Math.min(...stateLatestValues)

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
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-5 sm:px-6 lg:py-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <h1 className="font-display text-3xl">Explorer</h1>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            Consumer Price Index trends by region, shown as index values and simple change cards.
          </p>
        </div>
        <Badge variant="secondary">{activeDataflow?.frequency ?? 'quarterly'}</Badge>
      </div>

      {error instanceof Error ? <ErrorBanner message={error.message} /> : null}
      {loading ? <LoadingBanner /> : null}

      <section className="grid grid-cols-1 items-start gap-4 lg:grid-cols-[minmax(240px,280px)_minmax(0,1fr)]">
        <Card className="order-2 min-w-0 lg:order-none">
          <CardHeader>
            <CardTitle>Filters</CardTitle>
            <CardDescription>Pick the dataflow and region.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <Field htmlFor="dataflow" label="Dataflow">
              <NativeSelect
                disabled={dataflows.length === 0}
                id="dataflow"
                onChange={(event) => {
                  setSelectedDataflow(event.target.value)
                  setSelectedRegion(activeNationalRegion)
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

            <Field htmlFor="region" label="Region">
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

            <div className="rounded-md border bg-muted/40 p-3 text-sm">
              <p className="font-medium">{activeDataflow?.source_id?.toUpperCase() ?? 'ABS'}</p>
              <p className="mt-1 break-all font-mono text-xs text-muted-foreground">{apiBaseUrl}</p>
            </div>
          </CardContent>
        </Card>

        <div className="order-1 grid min-w-0 grid-cols-1 gap-4 sm:grid-cols-2 lg:order-none xl:grid-cols-4">
          <MetricCard
            label="Latest index"
            testId="latest-observation"
            value={formatValue(latestObservation?.value)}
            detail={
              latestObservation === undefined
                ? 'Waiting for observations'
                : `${selectedRegionName}, ${formatDate(latestObservation.time)}`
            }
          />
          <MetricCard
            label="Quarterly change"
            value={formatDelta(latestObservation?.value, previousObservation?.value)}
            detail="Latest quarter movement"
          />
          <MetricCard
            label="Annual change"
            testId="annual-change"
            value={formatDelta(latestObservation?.value, annualObservation?.value)}
            detail={
              annualObservation === undefined
                ? 'Same period previous year'
                : `Since ${formatDate(annualObservation.time)}`
            }
          />
          <MetricCard
            label="State spread"
            value={formatValue(stateSpread)}
            detail="Highest minus lowest latest state index"
          />
        </div>
      </section>

      <section className="flex min-w-0 flex-col gap-5">
        <div className="min-w-0">
          <h2 className="text-xl font-semibold tracking-normal">Consumer Price Index</h2>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            {activeDataflow?.description ??
              'Quarterly Consumer Price Index observations from the Australian Bureau of Statistics.'}
          </p>
        </div>

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

        <Card className="min-w-0">
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

type MetricCardProps = {
  detail: string
  label: string
  testId?: string
  value: string
}

function MetricCard({ detail, label, testId, value }: MetricCardProps) {
  return (
    <Card className="min-w-0" data-testid={testId}>
      <CardHeader className="gap-1">
        <CardDescription>{label}</CardDescription>
        <CardTitle className="text-2xl">{value}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground">{detail}</p>
      </CardContent>
    </Card>
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
    <Card className="min-w-0" data-testid={testId}>
      <CardHeader>
        <div>
          <h3 className="leading-none font-semibold">{title}</h3>
          <CardDescription>{description}</CardDescription>
        </div>
        {latest === undefined ? null : (
          <CardAction className="text-right">
            <p className="text-lg font-semibold">{formatValue(latest.value)}</p>
            <p className="text-xs text-muted-foreground">{formatDate(latest.time)}</p>
          </CardAction>
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
                  style={{ backgroundColor: comparisonColors[region.name] ?? 'var(--chart-3)' }}
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
      className="rounded-md border border-destructive/40 bg-card p-3 text-sm text-destructive"
      role="alert"
    >
      Explorer could not load API data: {message}
    </div>
  )
}

function LoadingBanner() {
  return (
    <div className="flex items-center gap-2 rounded-md border bg-card p-3 text-sm text-muted-foreground">
      <RefreshCw aria-hidden="true" className="animate-spin" />
      Loading CPI data
    </div>
  )
}

function formatDelta(current: number | null | undefined, previous: number | null | undefined) {
  if (current === null || current === undefined || previous === null || previous === undefined) {
    return 'n/a'
  }

  const delta = current - previous
  const sign = delta > 0 ? '+' : ''
  return `${sign}${formatValue(delta)}`
}

function samePeriodPreviousYear(
  rows: ObservationsRow[],
  current: ObservationsRow,
): ObservationsRow | undefined {
  const currentDate = new Date(current.time)
  if (Number.isNaN(currentDate.getTime())) {
    return undefined
  }

  const targetTime = Date.UTC(
    currentDate.getUTCFullYear() - 1,
    currentDate.getUTCMonth(),
    currentDate.getUTCDate(),
  )

  return rows.find(
    (row) => row.series_key === current.series_key && Date.parse(row.time) === targetTime,
  )
}
