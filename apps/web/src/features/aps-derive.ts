import type { ApsSnapshotSummary, PublishedApsSnapshot } from '@au-kpis/sdk'
import {
  ALL_COVERAGE_STATUSES,
  type ApsContribution,
  type ApsSubIndex,
  coverageChartColorVar,
  coverageStatusLabel,
  scoreOffset,
  tokenLabel,
} from './aps-data'

type Axis = ApsSubIndex['axis']
type CoverageStatus = ApsContribution['coverage_status']

// Throughput scores are already in [0,1]; orientation is in [-1,1] with the
// scoring direction baked in (value 1 / +1 == "best"). Both fold to a common
// [0,1] "progress to best" via this helper — never branch on `direction`.
function toUnit(value: number, axis: Axis): number {
  const unit = axis === 'orientation' ? (value + 1) / 2 : value
  return Math.min(1, Math.max(0, unit))
}

// Map an axis-scaled score onto the shared 0–100 plane used by every chart.
// Orientation uses (0.5 + 0.5*s)*100, matching the APS formula's orientation
// term (APS = 100 * T * (0.5 + 0.5 * O)).
function axisScoreTo100(score: number, axis: Axis): number {
  return scoreOffset(toUnit(score, axis) * 100)
}

type SnapshotPoint = PublishedApsSnapshot | ApsSnapshotSummary

function findAxis(snapshot: SnapshotPoint, axis: Axis): ApsSubIndex | undefined {
  return snapshot.sub_indexes.find((sub) => sub.axis === axis)
}

function parseTime(value: string): number {
  const time = Date.parse(value)
  return Number.isNaN(time) ? 0 : time
}

// Only contributions that actually move the score: a non-null normalized_value
// (implies coverage resolved/stale) and a positive weight.
function scoredContributions(contributions: ApsContribution[]): ApsContribution[] {
  return contributions.filter(
    (contribution) =>
      contribution.normalized_value !== null &&
      contribution.normalized_value !== undefined &&
      contribution.weight > 0,
  )
}

/* ---------------------------------------------------------------- scatter -- */

export interface ApsScatterPoint {
  x: number // throughput on 0–100
  y: number // orientation on 0–100
  asOf: string
  score: number // APS 0–100 (point size / tooltip)
}

export interface ApsScatterData {
  current: ApsScatterPoint | null
  trail: ApsScatterPoint[] // oldest → newest, includes current as last element
}

function scatterPoint(snapshot: SnapshotPoint): ApsScatterPoint | null {
  const throughput = findAxis(snapshot, 'throughput')
  const orientation = findAxis(snapshot, 'orientation')
  if (!throughput || !orientation || snapshot.score === null || snapshot.score === undefined) {
    return null
  }
  return {
    x: axisScoreTo100(throughput.score, 'throughput'),
    y: axisScoreTo100(orientation.score, 'orientation'),
    asOf: snapshot.snapshot_date,
    score: scoreOffset(snapshot.score),
  }
}

/**
 * Place the nation on a Throughput (x) vs Orientation (y) plane, with a faint
 * trail of prior snapshots. The trail is sorted oldest→newest and always ends
 * at the current point (de-duped if history already includes "now").
 */
export function deriveApsScatter(
  snapshot: PublishedApsSnapshot,
  history: ApsSnapshotSummary[] = [],
): ApsScatterData {
  const current = scatterPoint(snapshot)
  const trail = history
    .map(scatterPoint)
    .filter((point): point is ApsScatterPoint => point !== null)
    .sort((left, right) => parseTime(left.asOf) - parseTime(right.asOf))

  if (current && trail.at(-1)?.asOf !== current.asOf) {
    trail.push(current)
  }

  return { current, trail }
}

/* ------------------------------------------------------------------ radar -- */

export interface ApsRadarPoint {
  component: string
  label: string
  score: number | null // 0–100, axis-correct; null renders as a gap
  axis: Axis
  coveragePct: number
}

const AXIS_ORDER: Record<Axis, number> = { throughput: 0, orientation: 1 }

/**
 * Flatten every sub-index component onto one 0–100 radar. Orientation spokes are
 * mapped axis-correctly via (0.5 + 0.5*s)*100. Zero-coverage spokes are kept
 * (the radar needs a stable axis set) but expose `coveragePct` for dimming.
 */
export function deriveApsRadar(snapshot: PublishedApsSnapshot): ApsRadarPoint[] {
  const points: ApsRadarPoint[] = []
  for (const sub of snapshot.sub_indexes) {
    for (const component of sub.components) {
      points.push({
        component: component.component,
        label: tokenLabel(component.component),
        score: component.coverage_pct > 0 ? axisScoreTo100(component.score, sub.axis) : null,
        axis: sub.axis,
        coveragePct: component.coverage_pct,
      })
    }
  }
  return points.sort(
    (left, right) =>
      AXIS_ORDER[left.axis] - AXIS_ORDER[right.axis] ||
      left.component.localeCompare(right.component),
  )
}

/* ------------------------------------------------------------ constraints -- */

export interface ApsConstraint {
  indicatorId: string
  label: string
  component: string
  axis: Axis
  drag: number // weight * headroom (higher = bigger drag on the score)
  headroom: number // 0–1, distance from best
  normalizedValue: number
  weight: number
  unit: string
}

/**
 * Indicators dragging the score down now, ranked by weighted shortfall from
 * best (weight * headroom). Direction is already baked into normalized_value.
 */
export function deriveApsConstraints(snapshot: PublishedApsSnapshot, topN = 5): ApsConstraint[] {
  return scoredContributions(snapshot.contributions)
    .map((contribution) => {
      const normalizedValue = contribution.normalized_value as number
      const headroom = 1 - toUnit(normalizedValue, contribution.axis)
      return {
        indicatorId: contribution.indicator_id,
        label: contribution.label,
        component: contribution.component,
        axis: contribution.axis,
        drag: contribution.weight * headroom,
        headroom,
        normalizedValue,
        weight: contribution.weight,
        unit: contribution.unit,
      }
    })
    .sort(
      (left, right) =>
        right.drag - left.drag ||
        right.weight - left.weight ||
        left.label.localeCompare(right.label),
    )
    .slice(0, topN)
}

/* ------------------------------------------------------------------ donut -- */

export interface ApsCoverageSlice {
  status: CoverageStatus
  label: string
  count: number
  colorVar: string
}

export interface ApsCoverageDonut {
  slices: ApsCoverageSlice[] // non-zero statuses only, canonical order
  total: number // donut centre count
}

/** Coverage-status breakdown for the donut, with the total for the centre. */
export function deriveApsCoverageDonut(snapshot: PublishedApsSnapshot): ApsCoverageDonut {
  const counts = new Map<CoverageStatus, number>()
  for (const contribution of snapshot.contributions) {
    counts.set(contribution.coverage_status, (counts.get(contribution.coverage_status) ?? 0) + 1)
  }

  const seen = new Set<CoverageStatus>(ALL_COVERAGE_STATUSES)
  const extras = [...counts.keys()].filter((status) => !seen.has(status))
  const ordered = [...ALL_COVERAGE_STATUSES, ...extras]

  const slices = ordered
    .filter((status) => (counts.get(status) ?? 0) > 0)
    .map((status) => ({
      status,
      label: coverageStatusLabel(status),
      count: counts.get(status) ?? 0,
      colorVar: coverageChartColorVar(status),
    }))

  return { slices, total: snapshot.contributions.length }
}
