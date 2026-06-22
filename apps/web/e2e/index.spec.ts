import { AxeBuilder } from '@axe-core/playwright'
import { expect, test } from '@playwright/test'

test('Index surfaces fixture-backed latest indicator values first', async ({ page }) => {
  await page.goto('/')

  await expect(page.getByRole('heading', { exact: true, name: 'Index' })).toBeVisible()

  const valueBoard = page.getByTestId('index-value-board')
  await expect(valueBoard).toBeVisible()
  await expect(valueBoard.getByTestId('featured-abs-cpi')).toContainText('Consumer Price Index')
  await expect(valueBoard.getByTestId('featured-abs-cpi')).toContainText('136.9')
  await expect(valueBoard.getByTestId('featured-abs-cpi')).toContainText('June 2024')
  await expect(valueBoard.getByTestId('featured-abs-cpi')).toContainText('index')

  await expect(valueBoard.getByTestId('featured-rba-cash-rate')).toContainText('Cash Rate Target')
  await expect(valueBoard.getByTestId('featured-rba-cash-rate')).toContainText('4.35')
  await expect(valueBoard.getByTestId('featured-rba-cash-rate')).toContainText('percent')

  await expect(valueBoard.getByTestId('featured-abs-labour-force')).toContainText('Labour Force')
  await expect(valueBoard.getByTestId('featured-abs-labour-force')).toContainText('14,423')
  await expect(valueBoard.getByTestId('featured-abs-labour-force')).toContainText(
    'thousand persons',
  )

  const latestValues = page.getByRole('table', { name: 'Latest indicator values' })
  const nswRow = latestValues.getByRole('row').filter({ hasText: 'New South Wales' })
  await expect(nswRow).toHaveCount(1)
  await expect(nswRow).toContainText('139.2')

  await expect(page.getByTestId('endpoint-map')).toContainText('Catalog search')
  await expect(page.getByTestId('index-attribution')).toContainText(
    'Source: Australian Bureau of Statistics',
  )
  await expect(page.getByTestId('index-attribution')).toContainText(
    'Source: Reserve Bank of Australia',
  )
  await expect(page.getByText('API base:')).toContainText('127.0.0.1:3000')
  await expect(page.getByText('Latest loaded: Jun 2024')).toHaveCount(0)
  await expect(page.getByRole('link', { name: 'Methodology' })).toHaveCount(0)
  await expect(page.getByText('Loading API index data')).toHaveCount(0)

  await expect(page).toHaveScreenshot('index-values-dashboard.png')
})

test('Index keeps API status context below the value dashboard', async ({ page }) => {
  await page.goto('/')

  const valueBoard = page.getByTestId('index-value-board')
  const endpointMap = page.getByTestId('endpoint-map')
  await expect(valueBoard).toBeVisible()
  await expect(endpointMap).toBeVisible()

  const valueBox = await valueBoard.boundingBox()
  const endpointBox = await endpointMap.boundingBox()
  expect(valueBox).not.toBeNull()
  expect(endpointBox).not.toBeNull()
  if (valueBox === null || endpointBox === null) {
    throw new Error('Index layout boxes were not available')
  }

  expect(valueBox.y + valueBox.height).toBeLessThanOrEqual(endpointBox.y)
  await expect(endpointMap).toContainText('Health')
  await expect(endpointMap).toContainText('Observations')
  await expect(endpointMap).toContainText('Catalog search')
})

test('Index mobile value dashboard stacks without horizontal overflow', async ({ page }) => {
  await page.setViewportSize({ height: 844, width: 390 })
  await page.goto('/')

  await expect(page.getByRole('button', { name: 'Open navigation' })).toBeVisible()
  const valueBoard = page.getByTestId('index-value-board')
  const endpointMap = page.getByTestId('endpoint-map')
  await expect(valueBoard).toBeVisible()
  await expect(endpointMap).toBeVisible()

  const valueBox = await valueBoard.boundingBox()
  const endpointBox = await endpointMap.boundingBox()
  expect(valueBox).not.toBeNull()
  expect(endpointBox).not.toBeNull()
  if (valueBox === null || endpointBox === null) {
    throw new Error('Index mobile layout boxes were not available')
  }
  expect(valueBox.y + valueBox.height).toBeLessThanOrEqual(endpointBox.y)

  const hasHorizontalOverflow = await page.evaluate(
    () => document.documentElement.scrollWidth > window.innerWidth,
  )
  expect(hasHorizontalOverflow).toBe(false)
})

test('Index value dashboard has no WCAG AA accessibility violations', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByTestId('index-value-board')).toBeVisible()

  const results = await new AxeBuilder({ page }).analyze()

  expect(results.violations).toEqual([])
})
