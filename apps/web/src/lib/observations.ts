import type { ChartPoint } from '@/components/plot-chart'
import { client } from '@/lib/api'
import { formatDate } from '@/lib/format'
import type { Code, ObservationsRow } from '@au-kpis/sdk-generated/client'

export const absNationalRegion = '50'
export const legacyNationalRegion = 'AUS'
export const nationalRegion = absNationalRegion

export const comparisonColors: Record<string, string> = {
  Australia: '#0f766e',
  'New South Wales': '#c2410c',
  Sydney: '#c2410c',
  Queensland: '#2563eb',
  Brisbane: '#2563eb',
  Victoria: '#b45309',
  Melbourne: '#b45309',
}

const regionDisplayOrder = [
  'Australia',
  'New South Wales',
  'Victoria',
  'Queensland',
  'South Australia',
  'Western Australia',
  'Tasmania',
  'Northern Territory',
  'Australian Capital Territory',
]

export async function collectObservations(
  dataflow: string,
  region: string,
  dimensionIds: string[] = ['region'],
  limit = 32,
): Promise<ObservationsRow[]> {
  return collectObservationRows(dataflow, observationDimensions(region, dimensionIds), limit)
}

export async function collectObservationRows(
  dataflow: string,
  dimensions?: Record<string, string>,
  limit = 100,
): Promise<ObservationsRow[]> {
  if (limit <= 0) {
    return []
  }

  const observations: ObservationsRow[] = []
  for await (const observation of client.observations.stream({
    dataflow,
    dimensions,
    limit: Math.min(limit, 100),
  })) {
    observations.push(observation)
    if (observations.length >= limit) {
      break
    }
  }

  return observations.sort((left, right) => Date.parse(left.time) - Date.parse(right.time))
}

export async function collectLatestObservationRows(
  dataflow: string,
  dimensions?: Record<string, string>,
  limit = 100,
): Promise<ObservationsRow[]> {
  if (limit <= 0) {
    return []
  }

  const bySeries = new Map<string, ObservationsRow[]>()
  for await (const observation of client.observations.stream({
    dataflow,
    dimensions,
    limit: 100,
  })) {
    const observations = bySeries.get(observation.series_key) ?? []
    observations.push(observation)
    if (observations.length > limit) {
      observations.shift()
    }
    bySeries.set(observation.series_key, observations)
  }

  return Array.from(bySeries.values())
    .flat()
    .sort((left, right) => Date.parse(left.time) - Date.parse(right.time))
}

export function toChartPoints(rows: ObservationsRow[], region: string): ChartPoint[] {
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

export function regionName(regions: Code[], id: string): string {
  return regions.find((region) => region.id === id)?.name ?? id
}

export function nationalRegionId(regions: Code[]): string {
  if (regions.some((region) => region.id === absNationalRegion)) {
    return absNationalRegion
  }
  if (regions.some((region) => region.id === legacyNationalRegion)) {
    return legacyNationalRegion
  }
  return regions[0]?.id ?? nationalRegion
}

export function regionById(regions: Code[], id: string): Code {
  return (
    regions.find((region) => region.id === id) ?? {
      codelist_id: 'CL_ABS_CPI_REGION',
      id,
      name: id,
      parent_id: null,
    }
  )
}

export function stateRegions(regions: Code[]): Code[] {
  const national = nationalRegionId(regions)
  const childRegions = regions.filter((code) => code.id !== national && code.parent_id === national)
  if (childRegions.length > 0) {
    return sortRegionsForDisplay(childRegions)
  }
  return sortRegionsForDisplay(regions.filter((code) => code.id !== national))
}

export function orderedRegions(regions: Code[]): Code[] {
  if (regions.length === 0) {
    return []
  }
  const national = nationalRegionId(regions)
  return [regionById(regions, national), ...regions.filter((region) => region.id !== national)]
}

export function defaultComparedRegionIds(regions: Code[]): string[] {
  const national = nationalRegionId(regions)
  const comparison = stateRegions(regions)
    .slice(0, 2)
    .map((region) => region.id)
  return [national, ...comparison]
}

function sortRegionsForDisplay(regions: Code[]): Code[] {
  return [...regions].sort((left, right) => {
    const leftIndex = regionDisplayOrder.indexOf(left.name)
    const rightIndex = regionDisplayOrder.indexOf(right.name)
    const leftRank = leftIndex === -1 ? Number.MAX_SAFE_INTEGER : leftIndex
    const rightRank = rightIndex === -1 ? Number.MAX_SAFE_INTEGER : rightIndex
    return (
      leftRank - rightRank || left.name.localeCompare(right.name) || left.id.localeCompare(right.id)
    )
  })
}

export function observationDimensions(
  region: string,
  dimensionIds: string[],
): Record<string, string> {
  const dimensions: Record<string, string> = { region }
  if (dimensionIds.includes('measure')) {
    dimensions.measure = '1'
  }
  if (dimensionIds.includes('index')) {
    dimensions.index = '10001'
  }
  if (dimensionIds.includes('tsest')) {
    dimensions.tsest = '10'
  }
  if (dimensionIds.includes('freq')) {
    dimensions.freq = 'Q'
  }
  return dimensions
}
