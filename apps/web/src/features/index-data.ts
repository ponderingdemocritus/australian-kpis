import type { ChartPoint } from '@/components/plot-chart'
import { nationalRegion, regionName, toChartPoints } from '@/lib/observations'
import type {
  Code,
  Dataflow,
  DataflowDetailResponse,
  ObservationsRow,
} from '@au-kpis/sdk-generated/client'

export const featuredDataflowIds = ['abs.cpi', 'rba.cash_rate', 'abs.labour_force'] as const

export type CodelistLookup = {
  codes: Code[]
  dataflowId: string
  dimensionId: string
}

export type ObservationGroup = {
  detail: DataflowDetailResponse
  observations: ObservationsRow[]
}

export type FeaturedIndicator = {
  current?: ObservationsRow
  dataflow?: Dataflow
  id: 'abs-cpi' | 'rba-cash-rate' | 'abs-labour-force'
  label: string
  previous?: ObservationsRow
  seriesLabel: string
}

export type LatestIndicatorRow = {
  dataflow: Dataflow
  observation: ObservationsRow
  previous?: ObservationsRow
  seriesLabel: string
}

export type DistributionItem = {
  label: string
  value: number
}

export type IndexData = {
  featured: FeaturedIndicator[]
  frequencyMix: DistributionItem[]
  latestRows: LatestIndicatorRow[]
  regionalChart: ChartPoint[]
  sourceMix: DistributionItem[]
  trendChart: ChartPoint[]
}

const featuredDefinitions = [
  {
    dataflowId: 'abs.cpi',
    id: 'abs-cpi',
    label: 'Consumer Price Index',
    region: nationalRegion,
  },
  {
    dataflowId: 'rba.cash_rate',
    id: 'rba-cash-rate',
    label: 'Cash Rate Target',
    region: undefined,
  },
  {
    dataflowId: 'abs.labour_force',
    id: 'abs-labour-force',
    label: 'Labour Force',
    region: nationalRegion,
  },
] as const satisfies readonly {
  dataflowId: (typeof featuredDataflowIds)[number]
  id: FeaturedIndicator['id']
  label: string
  region: string | undefined
}[]

export function deriveIndexData(input: {
  codelists: CodelistLookup[]
  dataflows: Dataflow[]
  groups: ObservationGroup[]
}): IndexData {
  const cpiGroup = input.groups.find((group) => group.detail.dataflow.id === 'abs.cpi')
  const latestRows = latestIndicatorRows(input.groups, input.codelists)

  return {
    featured: featuredDefinitions.map((definition) =>
      featuredIndicator(
        definition.id,
        definition.label,
        definition.dataflowId,
        definition.region,
        input,
      ),
    ),
    frequencyMix: distributionBy(input.dataflows, (dataflow) => dataflow.frequency),
    latestRows,
    regionalChart: regionalComparisonChart(cpiGroup, input.codelists),
    sourceMix: distributionBy(input.dataflows, (dataflow) => dataflow.source_id),
    trendChart: toChartPoints(
      cpiGroup?.observations.filter(
        (observation) => observation.dimensions.region === nationalRegion,
      ) ?? [],
      'Australia',
    ),
  }
}

function featuredIndicator(
  id: FeaturedIndicator['id'],
  label: string,
  dataflowId: string,
  region: string | undefined,
  input: {
    codelists: CodelistLookup[]
    groups: ObservationGroup[]
  },
): FeaturedIndicator {
  const group = input.groups.find((candidate) => candidate.detail.dataflow.id === dataflowId)
  const rows =
    group?.observations.filter((observation) =>
      region === undefined ? true : observation.dimensions.region === region,
    ) ?? []
  const current = latestObservation(rows)
  const previous = current === undefined ? undefined : previousObservation(rows, current)

  return {
    current,
    dataflow: group?.detail.dataflow,
    id,
    label,
    previous,
    seriesLabel:
      current === undefined || group === undefined
        ? label
        : seriesLabel(group.detail.dataflow, current, input.codelists),
  }
}

function latestIndicatorRows(
  groups: ObservationGroup[],
  codelists: CodelistLookup[],
): LatestIndicatorRow[] {
  const bySeries = new Map<string, LatestIndicatorRow>()

  for (const group of groups) {
    for (const observation of group.observations) {
      const key = `${group.detail.dataflow.id}:${observation.series_key}`
      const existing = bySeries.get(key)

      if (
        existing !== undefined &&
        compareObservationOrder(existing.observation, observation) >= 0
      ) {
        continue
      }

      bySeries.set(key, {
        dataflow: group.detail.dataflow,
        observation,
        previous: previousObservation(group.observations, observation),
        seriesLabel: seriesLabel(group.detail.dataflow, observation, codelists),
      })
    }
  }

  return Array.from(bySeries.values()).sort((left, right) => {
    const leftValue = left.observation.value ?? Number.NEGATIVE_INFINITY
    const rightValue = right.observation.value ?? Number.NEGATIVE_INFINITY
    return rightValue - leftValue || left.seriesLabel.localeCompare(right.seriesLabel)
  })
}

function latestObservation(rows: ObservationsRow[]): ObservationsRow | undefined {
  return sortByTime(rows).at(-1)
}

function previousObservation(
  rows: ObservationsRow[],
  current: ObservationsRow,
): ObservationsRow | undefined {
  const seriesRows = sortByTime(
    rows.filter((row) => row.series_key === current.series_key && row.time !== current.time),
  )
  return seriesRows.at(-1)
}

function seriesLabel(
  dataflow: Dataflow,
  observation: ObservationsRow,
  codelists: CodelistLookup[],
): string {
  const region = observation.dimensions.region
  if (region !== undefined) {
    const regionCodes =
      codelists.find(
        (codelist) => codelist.dataflowId === dataflow.id && codelist.dimensionId === 'region',
      )?.codes ?? []
    return regionName(regionCodes, region)
  }

  return dataflow.name
}

function regionalComparisonChart(
  cpiGroup: ObservationGroup | undefined,
  codelists: CodelistLookup[],
): ChartPoint[] {
  if (cpiGroup === undefined) {
    return []
  }

  const regionCodes =
    codelists.find(
      (codelist) =>
        codelist.dataflowId === cpiGroup.detail.dataflow.id && codelist.dimensionId === 'region',
    )?.codes ?? []
  const byRegion = new Map<string, ObservationsRow[]>()

  for (const observation of cpiGroup.observations) {
    const region = observation.dimensions.region
    if (region === undefined || region === nationalRegion) {
      continue
    }
    byRegion.set(region, [...(byRegion.get(region) ?? []), observation])
  }

  return Array.from(byRegion.entries()).flatMap(([region, rows]) =>
    toChartPoints(rows, regionName(regionCodes, region)),
  )
}

function distributionBy(
  dataflows: Dataflow[],
  getKey: (dataflow: Dataflow) => string,
): DistributionItem[] {
  const counts = new Map<string, number>()
  for (const dataflow of dataflows) {
    const key = getKey(dataflow)
    counts.set(key, (counts.get(key) ?? 0) + 1)
  }

  return Array.from(counts.entries())
    .map(([label, value]) => ({ label, value }))
    .sort((left, right) => right.value - left.value || left.label.localeCompare(right.label))
}

function sortByTime(rows: ObservationsRow[]): ObservationsRow[] {
  return [...rows].sort(compareObservationOrder)
}

function compareObservationOrder(left: ObservationsRow, right: ObservationsRow): number {
  const timeComparison = left.time.localeCompare(right.time)
  return timeComparison === 0 ? left.revision_no - right.revision_no : timeComparison
}
