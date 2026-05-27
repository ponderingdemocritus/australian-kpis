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
import { client } from '@/lib/api'
import { formatDate, formatValue } from '@/lib/format'
import {
  collectObservations,
  comparisonColors,
  nationalRegion,
  regionById,
  stateRegions,
  toChartPoints,
} from '@/lib/observations'
import type { ObservationsRow } from '@au-kpis/sdk-generated/client'
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
  const regionOptions = useMemo(
    () => [regionById(regions, nationalRegion), ...stateRegions(regions)],
    [regions],
  )

  const comparisonQuery = useQuery({
    enabled: selectedDataflow.length > 0 && selectedRegions.length > 0 && regions.length > 0,
    queryFn: async () =>
      Promise.all(
        selectedRegions.map(async (regionId) => {
          const region = regionById(regions, regionId)
          return {
            observations: await collectObservations(selectedDataflow, region.id),
            region,
          }
        }),
      ),
    queryKey: ['compare-observations', selectedDataflow, selectedRegions.join(',')],
  })

  const comparisonRows = comparisonQuery.data ?? []
  const chartPoints = comparisonRows.flatMap(({ observations, region }) =>
    toChartPoints(observations, region.name),
  )
  const latestRows = comparisonRows.map(({ observations, region }) => ({
    latest: observations.at(-1),
    region,
  }))

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
    <div className="mx-auto grid max-w-7xl grid-cols-1 gap-5 px-6 py-6 lg:grid-cols-[280px_minmax(0,1fr)]">
      <aside className="flex flex-col gap-4">
        <Card>
          <CardHeader>
            <CardTitle>Series controls</CardTitle>
            <CardDescription>Overlay regional series from the same dataflow.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <Field label="Dataflow" htmlFor="compare-dataflow">
              <NativeSelect
                disabled={dataflows.length === 0}
                id="compare-dataflow"
                onChange={(event) => {
                  setSelectedDataflow(event.target.value)
                  setSelectedRegions(defaultComparedRegions)
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
                  className="flex items-center justify-between gap-3 rounded-md border border-border bg-card px-3 py-2 text-sm"
                  key={region.id}
                >
                  <span className="flex items-center gap-2">
                    <span
                      aria-hidden="true"
                      className="size-2 rounded-full"
                      style={{ backgroundColor: comparisonColors[region.name] ?? '#475569' }}
                    />
                    {region.name}
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

        <Card>
          <CardHeader>
            <CardTitle>Selected series</CardTitle>
            <CardDescription>{selectedRegions.length} active lines</CardDescription>
          </CardHeader>
          <CardContent className="text-sm text-muted-foreground">
            {activeDataflow?.frequency ?? 'quarterly'} observations ordered by quarter.
          </CardContent>
        </Card>
      </aside>

      <section className="flex min-w-0 flex-col gap-5">
        <div>
          <h1 className="text-2xl font-semibold tracking-normal">Compare</h1>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            Multiple regional CPI series rendered on one chart for direct comparison.
          </p>
        </div>

        {error instanceof Error ? <ErrorBanner message={error.message} /> : null}
        {loading ? <LoadingBanner /> : null}

        <Card data-testid="compare-chart">
          <CardHeader className="flex-row items-start justify-between gap-4">
            <div>
              <CardTitle>Consumer Price Index</CardTitle>
              <CardDescription>
                {activeDataflow?.description ??
                  'Quarterly Consumer Price Index across Australian regions.'}
              </CardDescription>
            </div>
            <GitCompareArrows aria-hidden="true" className="mt-1 size-5 text-muted-foreground" />
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
                    style={{ backgroundColor: comparisonColors[region.name] ?? '#475569' }}
                  />
                  {region.name}
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>

        <Card>
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

function ErrorBanner({ message }: { message: string }) {
  return (
    <div
      className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-900"
      role="alert"
    >
      Compare could not load API data: {message}
    </div>
  )
}

function LoadingBanner() {
  return (
    <div className="flex items-center gap-2 rounded-md border border-border bg-card p-3 text-sm text-muted-foreground">
      <RefreshCw aria-hidden="true" className="size-4 animate-spin" />
      Loading comparison data
    </div>
  )
}
