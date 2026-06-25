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
import { client } from '@/lib/api'
import { formatDate, formatValue } from '@/lib/format'
import {
  collectObservations,
  comparisonColors,
  defaultComparedRegionIds,
  nationalRegion,
  nationalRegionId,
  regionById,
  stateRegions,
  toChartPoints,
} from '@/lib/observations'
import { useQuery } from '@tanstack/react-query'
import { GitCompareArrows, RefreshCw } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'

const defaultComparedRegions = [nationalRegion, 'NSW', 'VIC']

export function ComparePage() {
  const [selectedDataflow, setSelectedDataflow] = useState('abs.cpi')
  const [selectedRegions, setSelectedRegions] = useState<string[]>(defaultComparedRegions)

  const dataflowsQuery = useQuery({
    queryFn: () => client.dataflows.list(),
    queryKey: ['dataflows'],
  })

  const dataflows = dataflowsQuery.data?.dataflows ?? []
  const activeDataflow = dataflows.find((dataflow) => dataflow.id === selectedDataflow)

  useEffect(() => {
    if (activeDataflow === undefined && dataflows[0] !== undefined) {
      setSelectedDataflow(dataflows[0].id)
      setSelectedRegions(defaultComparedRegions)
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
  const defaultSelectedRegions = useMemo(() => defaultComparedRegionIds(regions), [regions])
  const regionOptions = useMemo(
    () => [regionById(regions, activeNationalRegion), ...stateRegions(regions)],
    [activeNationalRegion, regions],
  )
  const observationDimensionIds = useMemo(
    () => detailQuery.data?.dimensions.map((dimension) => dimension.id) ?? ['region'],
    [detailQuery.data],
  )

  useEffect(() => {
    if (regions.length === 0 || defaultSelectedRegions.length === 0) {
      return
    }
    const validRegionIds = new Set(regionOptions.map((region) => region.id))
    if (selectedRegions.some((regionId) => !validRegionIds.has(regionId))) {
      setSelectedRegions(defaultSelectedRegions)
    }
  }, [defaultSelectedRegions, regionOptions, regions.length, selectedRegions])

  const comparisonQuery = useQuery({
    enabled: selectedDataflow.length > 0 && selectedRegions.length > 0 && regions.length > 0,
    queryFn: async () =>
      Promise.all(
        selectedRegions.map(async (regionId) => {
          const region = regionById(regions, regionId)
          return {
            observations: await collectObservations(
              selectedDataflow,
              region.id,
              observationDimensionIds,
            ),
            region,
          }
        }),
      ),
    queryKey: [
      'compare-observations',
      selectedDataflow,
      selectedRegions.join(','),
      observationDimensionIds,
    ],
  })

  const comparisonRows = comparisonQuery.data ?? []
  const chartPoints = comparisonRows.flatMap(({ observations, region }) =>
    toChartPoints(observations, region.name),
  )
  const latestRows = comparisonRows.map(({ observations, region }) => ({
    latest: observations.at(-1),
    region,
  }))
  const latestValues = latestRows
    .map(({ latest }) => latest?.value)
    .filter((value): value is number => typeof value === 'number')
  const spread =
    latestValues.length === 0 ? undefined : Math.max(...latestValues) - Math.min(...latestValues)

  const loading = [dataflowsQuery, detailQuery, regionsQuery, comparisonQuery].some(
    (query) => query.isLoading,
  )
  const error = [dataflowsQuery, detailQuery, regionsQuery, comparisonQuery].find(
    (query) => query.isError,
  )?.error

  const toggleRegion = (regionId: string, checked: boolean) => {
    setSelectedRegions((current) => {
      if (checked) {
        return current.includes(regionId) ? current : [...current, regionId]
      }
      return current.length === 1 ? current : current.filter((id) => id !== regionId)
    })
  }

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-5 sm:px-6 lg:py-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold tracking-normal">Compare</h1>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            Overlay regional CPI series to see which areas moved together and which diverged.
          </p>
        </div>
        <Badge variant="secondary">{selectedRegions.length} active lines</Badge>
      </div>

      {error instanceof Error ? <ErrorBanner message={error.message} /> : null}
      {loading ? <LoadingBanner /> : null}

      <section className="grid grid-cols-1 items-start gap-4 lg:grid-cols-[minmax(240px,280px)_minmax(0,1fr)]">
        <Card className="order-2 min-w-0 lg:order-none">
          <CardHeader>
            <CardTitle>Series controls</CardTitle>
            <CardDescription>Choose which regions appear on the chart.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <Field htmlFor="compare-dataflow" label="Dataflow">
              <NativeSelect
                disabled={dataflows.length === 0}
                id="compare-dataflow"
                onChange={(event) => {
                  setSelectedDataflow(event.target.value)
                  setSelectedRegions(defaultSelectedRegions)
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

            <fieldset className="flex flex-col gap-3">
              <legend className="text-sm font-medium">Series</legend>
              {regionOptions.map((region) => (
                <label
                  className="flex items-center justify-between gap-3 rounded-md border bg-card px-3 py-2 text-sm"
                  key={region.id}
                >
                  <span className="flex min-w-0 items-center gap-2">
                    <span
                      aria-hidden="true"
                      className="size-2 shrink-0 rounded-full"
                      style={{ backgroundColor: comparisonColors[region.name] ?? 'var(--chart-3)' }}
                    />
                    <span className="truncate">{region.name}</span>
                  </span>
                  <input
                    checked={selectedRegions.includes(region.id)}
                    className="size-4 accent-primary"
                    disabled={selectedRegions.length === 1 && selectedRegions.includes(region.id)}
                    onChange={(event) => toggleRegion(region.id, event.target.checked)}
                    type="checkbox"
                  />
                </label>
              ))}
            </fieldset>
          </CardContent>
        </Card>

        <div className="order-1 grid min-w-0 grid-cols-1 gap-4 sm:grid-cols-3 lg:order-none">
          <MetricCard
            detail={activeDataflow?.frequency ?? 'quarterly'}
            label="Selected series"
            value={`${selectedRegions.length}`}
          />
          <MetricCard
            detail="Latest selected range"
            label="Index spread"
            value={formatValue(spread)}
          />
          <MetricCard
            detail="Last observation per selected region"
            label="Latest quarter"
            value={
              latestRows[0]?.latest === undefined
                ? 'waiting'
                : formatDate(latestRows[0].latest.time)
            }
          />
        </div>
      </section>

      <section className="flex min-w-0 flex-col gap-5">
        <Card className="min-w-0" data-testid="compare-chart">
          <CardHeader>
            <div>
              <CardTitle>Consumer Price Index</CardTitle>
              <CardDescription>
                {activeDataflow?.description ??
                  'Quarterly Consumer Price Index across Australian regions.'}
              </CardDescription>
            </div>
            <CardAction>
              <GitCompareArrows aria-hidden="true" className="text-muted-foreground" />
            </CardAction>
          </CardHeader>
          <CardContent>
            <PlotChart
              ariaLabel="Compared CPI line chart"
              colors={comparisonColors}
              data={chartPoints}
              height={320}
            />
            <ul className="mt-3 flex flex-wrap gap-3 text-xs text-muted-foreground">
              {latestRows.map(({ region }) => (
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
          </CardContent>
        </Card>

        <Card className="min-w-0">
          <CardHeader>
            <CardTitle>Latest values</CardTitle>
            <CardDescription>Last observation per selected series.</CardDescription>
          </CardHeader>
          <CardContent>
            <Table aria-label="Compared series">
              <TableHeader>
                <TableRow>
                  <TableHead>Region</TableHead>
                  <TableHead>Quarter</TableHead>
                  <TableHead className="text-right">Index</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {latestRows.map(({ latest, region }) => (
                  <TableRow key={region.id}>
                    <TableCell>{region.name}</TableCell>
                    <TableCell>
                      {latest === undefined ? 'waiting' : formatDate(latest.time)}
                    </TableCell>
                    <TableCell className="text-right font-medium">
                      {formatValue(latest?.value)}
                    </TableCell>
                    <TableCell>{latest?.status ?? 'waiting'}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </section>
    </div>
  )
}

function MetricCard({ detail, label, value }: { detail: string; label: string; value: string }) {
  return (
    <Card className="min-w-0">
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

function ErrorBanner({ message }: { message: string }) {
  return (
    <div
      className="rounded-md border border-destructive/40 bg-card p-3 text-sm text-destructive"
      role="alert"
    >
      Compare could not load API data: {message}
    </div>
  )
}

function LoadingBanner() {
  return (
    <div className="flex items-center gap-2 rounded-md border bg-card p-3 text-sm text-muted-foreground">
      <RefreshCw aria-hidden="true" className="animate-spin" />
      Loading comparison data
    </div>
  )
}
