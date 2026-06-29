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

export function apsAxisDisplayScore(axis: ApsSubIndex['axis'], score: number): number {
  return axis === 'orientation' ? 100 * (0.5 + 0.5 * score) : score * 100
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

export function tokenLabel(value: string): string {
  return value
    .split(/[-_]/)
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(' ')
}

export function sourceLabel(contribution: ApsContribution): string {
  return contribution.source_dataflow_id.split('.')[0]?.toUpperCase() ?? 'SOURCE'
}

export function sortedContributions(contributions: ApsContribution[]): ApsContribution[] {
  return [...contributions].sort((left, right) => {
    // Surface resolved rows first so missing/pending inputs cluster at the bottom.
    const leftResolved = left.coverage_status === 'resolved' ? 0 : 1
    const rightResolved = right.coverage_status === 'resolved' ? 0 : 1
    if (leftResolved !== rightResolved) {
      return leftResolved - rightResolved
    }
    if (left.axis !== right.axis) {
      return left.axis.localeCompare(right.axis)
    }
    if (left.component !== right.component) {
      return left.component.localeCompare(right.component)
    }
    return left.label.localeCompare(right.label)
  })
}

/**
 * One-line plain-language definitions for each APS zone, used by the score
 * explainer so a first-time reader knows how to read the headline number.
 */
export function zoneDescription(zone: ScorecardSnapshot['zone']): string {
  if (zone === 'green') {
    return 'Abundance — the nation is positioned closer to abundance than scarcity.'
  }
  if (zone === 'yellow') {
    return 'Mixed — abundance and scarcity signals are roughly balanced.'
  }
  return 'Scarcity — the nation is positioned closer to scarcity than abundance.'
}

/** Solid zone color + white text for the zone badge and score marker pill. */
export function zoneSolidClass(zone: ScorecardSnapshot['zone']): string {
  if (zone === 'green') {
    return 'bg-emerald-700 text-white'
  }
  if (zone === 'yellow') {
    return 'bg-amber-700 text-white'
  }
  return 'bg-red-700 text-white'
}

/** Vivid zone color for the marker dot icon (icon, not text — contrast rule N/A). */
export function zoneDotClass(zone: ScorecardSnapshot['zone']): string {
  if (zone === 'green') {
    return 'text-emerald-600'
  }
  if (zone === 'yellow') {
    return 'text-amber-600'
  }
  return 'text-red-600'
}

/** Directional tone for trend, used to pick an arrow color (paired with text). */
export function trendTone(trend: ScorecardSnapshot['trend']): 'positive' | 'negative' | 'neutral' {
  if (trend === 'up') {
    return 'positive'
  }
  if (trend === 'down') {
    return 'negative'
  }
  return 'neutral'
}

/**
 * One-line definition per coverage status so the provenance table is decodable
 * without prior knowledge of the internal enum.
 */
export function coverageStatusDescription(status: ApsContribution['coverage_status']): string {
  switch (status) {
    case 'resolved':
      return 'Resolved — a current observation was found and contributes to the score.'
    case 'visible_unscored':
      return 'Visible Unscored — indicator displayed but not yet contributing to the score.'
    case 'manual_pending':
      return 'Manual Pending — awaiting a curated/manual value before it can be scored.'
    case 'coverage_gap':
      return 'Coverage Gap — expected data is missing for this period.'
    case 'stale':
      return 'Stale — the latest observation is older than the expected cadence.'
    case 'missing_expected':
      return 'Missing Expected — a required input is absent from the source.'
    default:
      return coverageStatusLabel(status)
  }
}

/** Semantic color grouping for coverage badges (collapses arbitrary hues). */
export function coverageBadgeClass(status: ApsContribution['coverage_status']): string {
  switch (status) {
    case 'manual_pending':
      // Pending action — neutral blue.
      return 'bg-sky-700 text-white'
    case 'coverage_gap':
    case 'stale':
      // Needs attention — amber (collapsed amber/orange into one).
      return 'bg-amber-700 text-white'
    case 'missing_expected':
      // Blocking absence — red.
      return 'bg-red-700 text-white'
    default:
      return ''
  }
}

/**
 * Canonical ordering of coverage statuses (healthy → blocking), shared by the
 * provenance legend and the coverage donut so both read in the same order.
 */
export const ALL_COVERAGE_STATUSES: ApsContribution['coverage_status'][] = [
  'resolved',
  'visible_unscored',
  'manual_pending',
  'coverage_gap',
  'stale',
  'missing_expected',
]

/**
 * Chart color (CSS var) per coverage status for the donut. Kept aligned with the
 * badge color semantics in {@link coverageBadgeClass}: green = healthy, gold =
 * needs attention, blue = pending, red = blocking, olive = neutral context.
 */
export function coverageChartColorVar(status: ApsContribution['coverage_status']): string {
  switch (status) {
    case 'resolved':
      return 'var(--chart-2)' // gumleaf green — healthy
    case 'visible_unscored':
      return 'var(--chart-4)' // bush olive — neutral context
    case 'manual_pending':
      return 'var(--chart-1)' // coast blue — pending action
    case 'coverage_gap':
    case 'stale':
      return 'var(--chart-3)' // wattle gold — needs attention
    case 'missing_expected':
      return 'var(--chart-5)' // clay red — blocking absence
    default:
      return 'var(--muted)'
  }
}
