import type { ChartPoint } from '@/components/plot-chart'
import { client } from '@/lib/api'
import { formatDate } from '@/lib/format'
import type { Code, ObservationsRow } from '@au-kpis/sdk-generated/client'

export const absNationalRegion = '50'
export const legacyNationalRegion = 'AUS'
export const nationalRegion = absNationalRegion
const recentObservationSince = '2020-01-01'

export const comparisonColors: Record<string, string> = {
  Australia: '#0f766e',
  'New South Wales': '#c2410c',
  Sydney: '#c2410c',
  Queensland: '#2563eb',
  Brisbane: '#2563eb',
  Victoria: '#b45309',
  Melbourne: '#b45309',
}

export async function collectObservations(
  dataflow: string,
  region: string,
  dimensionIds: string[] = ['region'],
  limit = 32,
): Promise<ObservationsRow[]> {
  const page = await client.observations.list({
    dataflow,
    dimensions: observationDimensions(region, dimensionIds),
    limit,
    since: recentObservationSince,
  })

  return page.observations.sort((left, right) => Date.parse(left.time) - Date.parse(right.time))
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
    return childRegions
  }
  return regions.filter((code) => code.id !== national)
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
