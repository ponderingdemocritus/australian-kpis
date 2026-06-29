import type { ScorecardSnapshot } from '@au-kpis/sdk'
import { describe, expect, it } from 'vitest'
import {
  deriveApsConstraints,
  deriveApsCoverageDonut,
  deriveApsRadar,
  deriveApsScatter,
  deriveApsUplift,
} from './aps-derive'

type Sub = ScorecardSnapshot['sub_indexes'][number]
type Component = Sub['components'][number]
type Contribution = ScorecardSnapshot['contributions'][number]

function component(overrides: Partial<Component>): Component {
  return { component: 'housing', coverage_pct: 100, score: 0.5, weight: 0.1, ...overrides }
}

function subIndex(overrides: Partial<Sub>): Sub {
  return {
    axis: 'throughput',
    components: [],
    confidence_band: { high: 1, low: 0 },
    coverage_pct: 100,
    score: 0.5,
    weight: 0.5,
    ...overrides,
  }
}

function contribution(overrides: Partial<Contribution>): Contribution {
  return {
    attribution: 'Source',
    axis: 'throughput',
    component: 'housing',
    confidence: 'high',
    coverage_status: 'resolved',
    dimensions: {},
    direction: 'higher_is_better',
    indicator_id: 'id',
    label: 'Indicator',
    latest_period: '2026-05-01',
    license: 'CC-BY-4.0',
    measure_id: 'value',
    normalized_value: 0.5,
    notes: null,
    raw_value: 50,
    series_key: null,
    source_artifact_id: null,
    source_dataflow_id: 'source.dataflow',
    source_url: 'https://example.test',
    unit: 'index',
    weight: 0.1,
    ...overrides,
  } as Contribution
}

function snapshot(overrides: Partial<ScorecardSnapshot>): ScorecardSnapshot {
  return {
    as_of: '2026-06-23',
    confidence: 'medium',
    confidence_band: { high: 83, low: 58 },
    config_version: 'aps.v1',
    contributions: [],
    coverage_pct: 84,
    latest_period: '2026-05-01',
    score: 72.4,
    scorecard_id: 'aps',
    sub_indexes: [],
    trend: 'up',
    zone: 'green',
    ...overrides,
  } as ScorecardSnapshot
}

// A representative snapshot: T sub-index 0.73 (housing 0.78, planning 0.66),
// O sub-index 0.59 (ai 0.62, governance 0.50). Orientation values are in [-1,1].
function baseSnapshot(): ScorecardSnapshot {
  return snapshot({
    score: 72.4,
    sub_indexes: [
      subIndex({
        axis: 'throughput',
        score: 0.73,
        weight: 0.55,
        components: [
          component({ component: 'housing', score: 0.78, weight: 0.35 }),
          component({ component: 'planning', score: 0.66, weight: 0.2 }),
        ],
      }),
      subIndex({
        axis: 'orientation',
        score: 0.59,
        weight: 0.45,
        components: [
          component({ component: 'ai', score: 0.62, weight: 0.2 }),
          component({ component: 'governance', score: 0.5, coverage_pct: 0, weight: 0.15 }),
        ],
      }),
    ],
    contributions: [
      contribution({
        indicator_id: 'housing.approvals',
        label: 'Housing approvals',
        axis: 'throughput',
        component: 'housing',
        normalized_value: 0.8,
        weight: 0.35,
      }),
      contribution({
        indicator_id: 'planning.time',
        label: 'Planning time',
        axis: 'throughput',
        component: 'planning',
        direction: 'lower_is_better',
        normalized_value: 0.5,
        weight: 0.2,
      }),
      contribution({
        indicator_id: 'ai.adoption',
        label: 'AI adoption',
        axis: 'orientation',
        component: 'ai',
        normalized_value: 0.4,
        weight: 0.2,
      }),
      contribution({
        indicator_id: 'oversight',
        label: 'Oversight strength',
        axis: 'orientation',
        component: 'governance',
        coverage_status: 'manual_pending',
        normalized_value: null,
        raw_value: null,
        weight: 0.15,
      }),
      contribution({
        indicator_id: 'spend',
        label: 'Control vs enable',
        axis: 'orientation',
        component: 'spend',
        coverage_status: 'visible_unscored',
        normalized_value: null,
        raw_value: null,
        weight: 0,
      }),
    ],
  })
}

describe('deriveApsScatter', () => {
  it('maps the current sub-indexes onto a 0-100 T/O plane', () => {
    const { current } = deriveApsScatter(baseSnapshot())
    expect(current).not.toBeNull()
    expect(current?.x).toBeCloseTo(73, 5) // throughput 0.73 * 100
    expect(current?.y).toBeCloseTo(79.5, 5) // orientation (0.5 + 0.5*0.59) * 100
  })

  it('builds an oldest->newest trail ending at the current point', () => {
    const history = [
      snapshot({
        as_of: '2026-04-01',
        sub_indexes: [
          subIndex({ axis: 'throughput', score: 0.6 }),
          subIndex({ axis: 'orientation', score: 0.2 }),
        ],
      }),
      snapshot({
        as_of: '2026-02-01',
        sub_indexes: [
          subIndex({ axis: 'throughput', score: 0.5 }),
          subIndex({ axis: 'orientation', score: 0 }),
        ],
      }),
    ]
    const { trail } = deriveApsScatter(baseSnapshot(), history)
    expect(trail.map((p) => p.asOf)).toEqual(['2026-02-01', '2026-04-01', '2026-06-23'])
    expect(trail[0]?.x).toBeCloseTo(50, 5)
    expect(trail[0]?.y).toBeCloseTo(50, 5) // (0.5 + 0.5*0) * 100
    expect(trail.at(-1)?.x).toBeCloseTo(73, 5)
  })

  it('returns just the current point when history is empty', () => {
    const { trail } = deriveApsScatter(baseSnapshot(), [])
    expect(trail).toHaveLength(1)
    expect(trail[0]?.asOf).toBe('2026-06-23')
  })

  it('returns null current when an axis is missing', () => {
    const { current } = deriveApsScatter(
      snapshot({ sub_indexes: [subIndex({ axis: 'throughput', score: 0.5 })] }),
    )
    expect(current).toBeNull()
  })
})

describe('deriveApsRadar', () => {
  it('flattens components onto a 0-100 axis-correct scale, throughput first', () => {
    const points = deriveApsRadar(baseSnapshot())
    expect(points.map((p) => p.component)).toEqual(['housing', 'planning', 'ai', 'governance'])
    expect(points.find((p) => p.component === 'housing')?.score).toBeCloseTo(78, 5)
    expect(points.find((p) => p.component === 'planning')?.score).toBeCloseTo(66, 5)
    // orientation components use (0.5 + 0.5*s) * 100
    expect(points.find((p) => p.component === 'ai')?.score).toBeCloseTo(81, 5)
    expect(points.find((p) => p.component === 'governance')?.score).toBeCloseTo(75, 5)
  })

  it('carries axis + coverage so low-coverage spokes can be dimmed', () => {
    const governance = deriveApsRadar(baseSnapshot()).find((p) => p.component === 'governance')
    expect(governance?.axis).toBe('orientation')
    expect(governance?.coveragePct).toBe(0)
  })
})

describe('deriveApsConstraints', () => {
  it('ranks scored indicators by weighted drag (1 - progress to best)', () => {
    const constraints = deriveApsConstraints(baseSnapshot())
    // planning drag 0.2*0.5=0.10 ; housing 0.35*0.2=0.07 ; ai 0.2*(1-0.65)=0.07
    expect(constraints.map((c) => c.indicatorId)).toEqual([
      'planning.time',
      'housing.approvals',
      'ai.adoption',
    ])
    expect(constraints[0]?.drag).toBeCloseTo(0.1, 5)
  })

  it('excludes unscored rows (null normalized_value or zero weight)', () => {
    const ids = deriveApsConstraints(baseSnapshot()).map((c) => c.indicatorId)
    expect(ids).not.toContain('oversight')
    expect(ids).not.toContain('spend')
  })

  it('respects the topN limit', () => {
    expect(deriveApsConstraints(baseSnapshot(), 1)).toHaveLength(1)
  })
})

describe('deriveApsUplift', () => {
  it('ranks by estimated APS gain, which differs from raw drag order', () => {
    const uplift = deriveApsUplift(baseSnapshot())
    // ai gain ~25.6 (orientation, full axis share) > planning ~14.5 > housing ~10.1
    expect(uplift.map((u) => u.indicatorId)).toEqual([
      'ai.adoption',
      'planning.time',
      'housing.approvals',
    ])
    expect(uplift[0]?.estimatedApsGain).toBeGreaterThan(uplift[1]?.estimatedApsGain ?? 0)
  })

  it('excludes unscored rows', () => {
    const ids = deriveApsUplift(baseSnapshot()).map((u) => u.indicatorId)
    expect(ids).not.toContain('oversight')
  })
})

describe('deriveApsCoverageDonut', () => {
  it('counts contributions by status with a total for the centre', () => {
    const { slices, total } = deriveApsCoverageDonut(baseSnapshot())
    expect(total).toBe(5)
    const resolved = slices.find((s) => s.status === 'resolved')
    expect(resolved?.count).toBe(3)
    expect(slices.find((s) => s.status === 'manual_pending')?.count).toBe(1)
    expect(slices.find((s) => s.status === 'visible_unscored')?.count).toBe(1)
  })

  it('omits zero-count statuses and keeps a canonical order', () => {
    const statuses = deriveApsCoverageDonut(baseSnapshot()).slices.map((s) => s.status)
    expect(statuses).toEqual(['resolved', 'visible_unscored', 'manual_pending'])
    expect(statuses).not.toContain('missing_expected')
  })

  it('every slice carries a chart colour variable', () => {
    for (const slice of deriveApsCoverageDonut(baseSnapshot()).slices) {
      expect(slice.colorVar).toMatch(/var\(--/)
    }
  })
})
