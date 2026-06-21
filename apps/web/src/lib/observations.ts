import type { ChartPoint } from '@/components/plot-chart'
import { client } from '@/lib/api'
import { formatDate } from '@/lib/format'
import type { Code, ObservationsRow } from '@au-kpis/sdk-generated/client'

export const nationalRegion = 'AUS'

export const comparisonColors: Record<string, string> = {
  Australia: '#0f766e',
  'New South Wales': '#c2410c',
  Queensland: '#2563eb',
  Victoria: '#b45309',
}

export async function collectObservations(
  dataflow: string,
  region: string,
  limit = 100,
): Promise<ObservationsRow[]> {
  return collectObservationRows(dataflow, { region }, limit)
}

export async function collectObservationRows(
  dataflow: string,
  dimensions?: Record<string, string>,
  limit = 100,
): Promise<ObservationsRow[]> {
  const observations: ObservationsRow[] = []
  for await (const observation of client.observations.stream({
    dataflow,
    dimensions,
    limit,
  })) {
    observations.push(observation)
  }

  return observations.sort((left, right) => Date.parse(left.time) - Date.parse(right.time))
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

export function regionById(regions: Code[], id: string): Code {
  return (
    regions.find((region) => region.id === id) ?? {
      codelist_id: 'CL_REGION_AU',
      id,
      name: id,
      parent_id: null,
    }
  )
}

export function stateRegions(regions: Code[]): Code[] {
  return regions.filter((code) => code.id !== nationalRegion && code.parent_id === nationalRegion)
}
