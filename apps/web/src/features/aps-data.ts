import type { ScorecardConfig, ScorecardSnapshot } from '@au-kpis/sdk'

export type ApsIndicatorConfig = ScorecardConfig['indicators'][number]
export type ApsContribution = ScorecardSnapshot['contributions'][number]
export type ApsSubIndex = ScorecardSnapshot['sub_indexes'][number]

const scoreFormatter = new Intl.NumberFormat('en-AU', {
  maximumFractionDigits: 1,
  minimumFractionDigits: 1,
})

const percentFormatter = new Intl.NumberFormat('en-AU', {
  maximumFractionDigits: 0,
})

const rawFormatter = new Intl.NumberFormat('en-AU', {
  maximumFractionDigits: 2,
})

const dateFormatter = new Intl.DateTimeFormat('en-AU', {
  day: '2-digit',
  month: 'short',
  timeZone: 'UTC',
  year: 'numeric',
})

export function formatApsScore(value: number | null | undefined): string {
  return value === null || value === undefined ? 'n/a' : scoreFormatter.format(value)
}

export function formatApsPercent(value: number | null | undefined): string {
  return value === null || value === undefined ? 'n/a' : `${percentFormatter.format(value)}%`
}

export function formatApsRawValue(value: number | null | undefined): string {
  return value === null || value === undefined ? 'n/a' : rawFormatter.format(value)
}

export function formatApsDate(value: string | null | undefined): string {
  if (value === null || value === undefined || value.length === 0) {
    return 'not available'
  }
  return dateFormatter.format(new Date(value))
}

export function scoreOffset(score: number): number {
  return Math.min(100, Math.max(0, score))
}

export function zoneLabel(zone: ScorecardSnapshot['zone']): string {
  if (zone === 'green') {
    return 'abundance'
  }
  if (zone === 'yellow') {
    return 'mixed'
  }
  return 'scarcity'
}

export function trendLabel(trend: ScorecardSnapshot['trend']): string {
  if (trend === 'up') {
    return 'improving'
  }
  if (trend === 'down') {
    return 'declining'
  }
  if (trend === 'flat') {
    return 'flat'
  }
  return 'unavailable'
}

export function coverageStatusLabel(status: ApsContribution['coverage_status']): string {
  return status
    .split('_')
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(' ')
}

export function directionLabel(direction: ApsIndicatorConfig['direction']): string {
  return direction === 'higher_is_better' ? 'Higher is better' : 'Lower is better'
}

export function sourceLabel(contribution: ApsContribution): string {
  return contribution.source_dataflow_id.split('.')[0]?.toUpperCase() ?? 'SOURCE'
}

export function sortedContributions(contributions: ApsContribution[]): ApsContribution[] {
  return [...contributions].sort((left, right) => {
    if (left.axis !== right.axis) {
      return left.axis.localeCompare(right.axis)
    }
    if (left.component !== right.component) {
      return left.component.localeCompare(right.component)
    }
    return left.label.localeCompare(right.label)
  })
}
