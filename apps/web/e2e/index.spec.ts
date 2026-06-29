import { AxeBuilder } from '@axe-core/playwright'
import { type Page, expect, test } from '@playwright/test'

type ApsIndicatorFixture = {
  axis: 'orientation' | 'throughput'
  cadence: string
  component: string
  confidence: 'high' | 'low' | 'medium'
  coverage_status:
    | 'coverage_gap'
    | 'manual_pending'
    | 'missing_expected'
    | 'resolved'
    | 'stale'
    | 'visible_unscored'
  description: string
  dimension_selector: Record<string, string>
  direction: 'higher_is_better' | 'lower_is_better'
  display_label: string
  indicator_id: string
  measure_id: string
  normalization: { best: number; worst: number }
  provenance: {
    attribution: string
    license: string
    reviewed_at: string
    reviewed_by: string
    source_url: string
  }
  source_dataflow_id: string
  unit: string
  weight: number
}

type ApsContributionFixture = {
  attribution: string
  axis: 'orientation' | 'throughput'
  component: string
  confidence: 'high' | 'low' | 'medium'
  coverage_status: ApsIndicatorFixture['coverage_status']
  dimensions: Record<string, string>
  direction: ApsIndicatorFixture['direction']
  indicator_id: string
  label: string
  latest_period: string | null
  license: string
  measure_id: string
  normalized_value: number | null
  notes: string | null
  raw_value: number | null
  series_key: string | null
  source_artifact_id: string | null
  source_dataflow_id: string
  source_url: string
  unit: string
  weight: number
}

test('APS homepage renders latest scorecard data first', async ({ page }) => {
  await mockApsApi(page)
  await page.goto('/')

  await expect(
    page.getByRole('heading', { exact: true, name: 'Abundance Position Score' }),
  ).toBeVisible()

  const scoreCard = page.getByTestId('aps-score-card')
  await expect(scoreCard).toContainText('72.4')
  await expect(scoreCard).toContainText('abundance')
  await expect(scoreCard).toContainText('84%')
  await expect(scoreCard).toContainText('3/5')
  await expect(page.getByTestId('aps-score-spectrum')).toContainText('APS 72.4')

  const throughputOrientation = page.getByTestId('aps-throughput-orientation')
  await expect(throughputOrientation).toContainText('Throughput')
  await expect(throughputOrientation).toContainText('Orientation')
  await expect(throughputOrientation).toContainText('79.5')
  await expect(page.getByTestId('aps-systems-radar')).toBeVisible()

  // Constraints rank by drag (planning highest); uplift by APS gain (AI highest).
  // Long axis labels wrap into tspans, so assert on a non-wrapping substring.
  await expect(page.getByTestId('aps-constraints')).toContainText('Planning')
  await expect(page.getByTestId('aps-uplift')).toContainText('AI adoption')

  const donut = page.getByTestId('aps-coverage-donut')
  await expect(donut).toContainText('5')
  await expect(donut).toContainText('indicators')
  await expect(donut).toContainText('Resolved')

  const drilldowns = page.getByTestId('aps-source-drilldowns')
  await expect(drilldowns).toContainText('Housing approvals')
  await expect(drilldowns).toContainText('abs.building_approvals')
  await expect(drilldowns).toContainText('Resolved')
  await expect(drilldowns).toContainText('Manual Pending')
  await expect(drilldowns).toContainText('Visible Unscored')

  // Provenance (source link, license, unit, fingerprints) lives behind an expandable row.
  await drilldowns.getByRole('button', { name: 'Expand Housing approvals provenance' }).click()
  await expect(drilldowns).toContainText('https://example.test/source')
  await expect(drilldowns).toContainText('CC-BY-4.0')
  await expect(drilldowns).toContainText('Source attribution')
  await expect(drilldowns).toContainText('index')
  await expect(drilldowns).toContainText('aaaaaaaa')
})

test('APS homepage surfaces methodology and source link', async ({ page }) => {
  await mockApsApi(page)
  await page.goto('/')
  await expect(page.getByTestId('aps-score-card')).toBeVisible()

  await page.getByText('Methodology & sources').click()
  await expect(page.getByText('APS = 100 * T * (0.5 + 0.5 * O)')).toBeVisible()
})

test('APS homepage recovers from a transient error via Retry', async ({ page }) => {
  let failConfig = true
  await page.route('**/v1/scorecards/aps/config', async (route) => {
    if (failConfig) {
      await route.fulfill({ body: 'scorecard unavailable', status: 503 })
      return
    }
    await route.fulfill({ json: apsConfig })
  })
  await page.route('**/v1/scorecards/aps/latest', async (route) => {
    await route.fulfill({ json: apsSnapshot })
  })
  await page.route('**/v1/scorecards/aps/history*', async (route) => {
    await route.fulfill({ json: [] })
  })

  await page.goto('/')
  await expect(page.getByTestId('aps-error')).toBeVisible()

  failConfig = false
  await page.getByRole('button', { name: 'Retry' }).click()
  await expect(page.getByTestId('aps-score-card')).toBeVisible()
})

test('APS throughput-vs-orientation card renders with a historical trail', async ({ page }) => {
  await mockApsApi(page, [
    { ...apsSnapshot, as_of: '2026-03-01', score: 40 },
    { ...apsSnapshot, as_of: '2026-05-01', score: 55 },
  ])
  await page.goto('/')

  const throughputOrientation = page.getByTestId('aps-throughput-orientation')
  await expect(throughputOrientation).toBeVisible()
  await expect(throughputOrientation).toContainText('Throughput')
  // The scatter renders an SVG surface once the (non-gating) history resolves.
  await expect(throughputOrientation.locator('.recharts-surface').first()).toBeVisible()
})

test('APS homepage mobile layout avoids horizontal overflow', async ({ page }) => {
  await page.setViewportSize({ height: 844, width: 390 })
  await mockApsApi(page)
  await page.goto('/')

  await expect(page.getByRole('button', { name: 'Open navigation' })).toBeVisible()
  await expect(page.getByTestId('aps-score-card')).toBeVisible()
  await expect(page.getByTestId('aps-source-drilldowns')).toBeVisible()

  // On phones the wide table is replaced by stacked cards (no in-card horizontal scroll).
  await expect(page.locator('table[aria-label="APS source drilldowns"]')).toBeHidden()

  const hasHorizontalOverflow = await page.evaluate(
    () => document.documentElement.scrollWidth > window.innerWidth,
  )
  expect(hasHorizontalOverflow).toBe(false)
})

test('APS homepage exposes loading, empty, and error states', async ({ page }) => {
  let releaseConfig: (() => void) | undefined
  const configGate = new Promise<void>((resolve) => {
    releaseConfig = () => resolve()
  })

  await page.route('**/v1/scorecards/aps/config', async (route) => {
    await configGate
    await route.fulfill({ json: apsConfig })
  })
  await page.route('**/v1/scorecards/aps/latest', async (route) => {
    await route.fulfill({ json: apsSnapshot })
  })
  await page.route('**/v1/scorecards/aps/history*', async (route) => {
    await route.fulfill({ json: [] })
  })

  await page.goto('/')
  await expect(page.getByTestId('aps-loading')).toBeVisible()
  releaseConfig?.()
  await expect(page.getByTestId('aps-score-card')).toBeVisible()

  await page.unroute('**/v1/scorecards/aps/config')
  await page.unroute('**/v1/scorecards/aps/latest')
  await page.route('**/v1/scorecards/aps/config', async (route) => {
    await route.fulfill({ json: { ...apsConfig, indicators: [] } })
  })
  await page.route('**/v1/scorecards/aps/latest', async (route) => {
    await route.fulfill({ json: { ...apsSnapshot, contributions: [] } })
  })
  await page.reload()
  await expect(page.getByTestId('aps-empty')).toBeVisible()

  await page.unroute('**/v1/scorecards/aps/config')
  await page.unroute('**/v1/scorecards/aps/latest')
  await page.route('**/v1/scorecards/aps/config', async (route) => {
    await route.fulfill({ body: 'scorecard unavailable', status: 503 })
  })
  await page.route('**/v1/scorecards/aps/latest', async (route) => {
    await route.fulfill({ json: apsSnapshot })
  })
  await page.reload()
  await expect(page.getByTestId('aps-error')).toContainText('APS scorecard unavailable')
})

test('APS navigation keeps Explorer, Compare, and Playground available as data tools', async ({
  page,
}) => {
  await mockApsApi(page)
  await page.goto('/')

  await page.getByRole('link', { name: 'Explorer' }).click()
  await expect(page).toHaveURL(/\/explorer$/)
  await expect(page.getByRole('heading', { name: 'Explorer' })).toBeVisible()

  await page.getByRole('link', { name: 'Compare' }).click()
  await expect(page).toHaveURL(/\/compare$/)
  await expect(page.getByRole('heading', { name: 'Compare' })).toBeVisible()

  await page.getByRole('link', { name: 'Playground' }).click()
  await expect(page).toHaveURL(/\/playground$/)
  await expect(page.getByRole('heading', { name: 'Playground' })).toBeVisible()
})

test('APS homepage has no WCAG AA accessibility violations (light and dark)', async ({ page }) => {
  await mockApsApi(page)
  await page.goto('/')
  await expect(page.getByTestId('aps-score-card')).toBeVisible()

  // Expand the legend so every coverage-status badge variant is contrast-checked.
  await page.getByText('What do these statuses mean?').click()

  const light = await new AxeBuilder({ page }).analyze()
  expect(light.violations).toEqual([])

  await page.getByRole('button', { name: 'Switch to dark theme' }).click()
  await expect(page.locator('html')).toHaveClass(/dark/)

  const dark = await new AxeBuilder({ page }).analyze()
  expect(dark.violations).toEqual([])
})

async function mockApsApi(page: Page, history: unknown[] = []) {
  await page.route('**/v1/scorecards/aps/config', async (route) => {
    await route.fulfill({ json: apsConfig })
  })
  await page.route('**/v1/scorecards/aps/latest', async (route) => {
    await route.fulfill({ json: apsSnapshot })
  })
  // History powers the scatter trail and is a non-gating query — mock it so
  // tests stay hermetic and never reach the upstream proxy.
  await page.route('**/v1/scorecards/aps/history*', async (route) => {
    await route.fulfill({ json: history })
  })
}

const apsConfig = {
  attribution: 'Derived scorecard configuration maintained by australian-kpis.',
  description: 'National Australia scorecard.',
  formula: 'APS = 100 * T * (0.5 + 0.5 * O)',
  id: 'aps',
  indicators: [
    indicator({
      direction: 'higher_is_better',
      display_label: 'Housing approvals',
      indicator_id: 'housing.approvals',
      source_dataflow_id: 'abs.building_approvals',
      weight: 0.35,
    }),
    indicator({
      component: 'planning',
      direction: 'lower_is_better',
      display_label: 'Planning approval time',
      indicator_id: 'planning.nsw-da-processing',
      source_dataflow_id: 'state_planning.nsw_da_processing',
      weight: 0.2,
    }),
    indicator({
      component: 'governance',
      coverage_status: 'manual_pending',
      direction: 'higher_is_better',
      display_label: 'Oversight strength',
      indicator_id: 'oversight.reviewed-strength',
      source_dataflow_id: 'curated.oversight_strength',
      weight: 0.15,
    }),
    indicator({
      component: 'ai',
      direction: 'higher_is_better',
      display_label: 'AI adoption',
      indicator_id: 'ai.adoption',
      source_dataflow_id: 'naic.ai_adoption_tracker',
      weight: 0.2,
    }),
    indicator({
      component: 'spend',
      coverage_status: 'visible_unscored',
      direction: 'higher_is_better',
      display_label: 'Control vs enable spend ratio',
      indicator_id: 'spend.control-vs-enable',
      source_dataflow_id: 'curated.control_enable_spend',
      weight: 0.1,
    }),
  ],
  label: 'Abundance Position Score',
  license: 'Apache-2.0',
  version: 'aps.v1',
}

const apsSnapshot = {
  as_of: '2026-06-23',
  confidence: 'medium',
  confidence_band: { high: 83.1, low: 58.2 },
  config_version: 'aps.v1',
  contributions: [
    contribution({
      indicator_id: 'housing.approvals',
      label: 'Housing approvals',
      normalized_value: 0.8,
      raw_value: 25000,
      source_artifact_id: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      source_dataflow_id: 'abs.building_approvals',
      weight: 0.35,
    }),
    contribution({
      component: 'planning',
      direction: 'lower_is_better',
      indicator_id: 'planning.nsw-da-processing',
      label: 'Planning approval time',
      latest_period: '2026-04-01',
      normalized_value: 0.5,
      raw_value: 48,
      series_key: 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
      source_artifact_id: 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
      source_dataflow_id: 'state_planning.nsw_da_processing',
      weight: 0.2,
    }),
    contribution({
      axis: 'orientation',
      component: 'governance',
      coverage_status: 'manual_pending',
      indicator_id: 'oversight.reviewed-strength',
      label: 'Oversight strength',
      latest_period: null,
      normalized_value: null,
      raw_value: null,
      series_key: null,
      source_artifact_id: null,
      source_dataflow_id: 'curated.oversight_strength',
    }),
    contribution({
      axis: 'orientation',
      component: 'ai',
      indicator_id: 'ai.adoption',
      label: 'AI adoption',
      latest_period: '2026-01-01',
      normalized_value: 0.4,
      raw_value: 62,
      source_dataflow_id: 'naic.ai_adoption_tracker',
      weight: 0.2,
    }),
    contribution({
      axis: 'orientation',
      component: 'spend',
      coverage_status: 'visible_unscored',
      indicator_id: 'spend.control-vs-enable',
      label: 'Control vs enable spend ratio',
      latest_period: null,
      normalized_value: null,
      raw_value: null,
      series_key: null,
      source_artifact_id: null,
      source_dataflow_id: 'curated.control_enable_spend',
      weight: 0,
    }),
  ],
  coverage_pct: 84,
  latest_period: '2026-05-01',
  score: 72.4,
  scorecard_id: 'aps',
  sub_indexes: [
    {
      axis: 'throughput',
      components: [
        { component: 'housing', coverage_pct: 100, score: 0.78, weight: 0.35 },
        { component: 'planning', coverage_pct: 100, score: 0.66, weight: 0.2 },
      ],
      confidence_band: { high: 0.86, low: 0.6 },
      coverage_pct: 91,
      score: 0.73,
      weight: 0.55,
    },
    {
      axis: 'orientation',
      components: [
        { component: 'governance', coverage_pct: 0, score: 0.5, weight: 0.15 },
        { component: 'ai', coverage_pct: 100, score: 0.62, weight: 0.2 },
      ],
      confidence_band: { high: 0.8, low: 0.38 },
      coverage_pct: 76,
      score: 0.59,
      weight: 0.45,
    },
  ],
  trend: 'up',
  zone: 'green',
}

function indicator(overrides: Partial<ApsIndicatorFixture>): ApsIndicatorFixture {
  return {
    axis: 'throughput',
    cadence: 'monthly',
    component: 'housing',
    confidence: 'high',
    coverage_status: 'resolved',
    description: 'Scorecard input.',
    dimension_selector: { region: 'AUS' },
    direction: 'higher_is_better',
    display_label: 'Indicator',
    indicator_id: 'indicator',
    measure_id: 'value',
    normalization: { best: 100, worst: 0 },
    provenance: {
      attribution: 'Source attribution',
      license: 'CC-BY-4.0',
      reviewed_at: '2026-06-23',
      reviewed_by: 'aps-curation',
      source_url: 'https://example.test/source',
    },
    source_dataflow_id: 'source.dataflow',
    unit: 'index',
    weight: 0.1,
    ...overrides,
  }
}

function contribution(overrides: Partial<ApsContributionFixture>): ApsContributionFixture {
  return {
    attribution: 'Source attribution',
    axis: 'throughput',
    component: 'housing',
    confidence: 'high',
    coverage_status: 'resolved',
    dimensions: { region: 'AUS' },
    direction: 'higher_is_better',
    indicator_id: 'indicator',
    label: 'Indicator',
    latest_period: '2026-05-01',
    license: 'CC-BY-4.0',
    measure_id: 'value',
    normalized_value: 0.72,
    notes: null,
    raw_value: 72,
    series_key: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    source_artifact_id: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    source_dataflow_id: 'source.dataflow',
    source_url: 'https://example.test/source',
    unit: 'index',
    weight: 0.1,
    ...overrides,
  }
}
