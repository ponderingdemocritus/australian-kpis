import { AxeBuilder } from '@axe-core/playwright'
import { expect, test } from '@playwright/test'

test('renders the national CPI chart journey with state comparison', async ({ page }) => {
  await page.goto('/explorer')

  await expect(page.getByRole('navigation', { name: 'Dashboard sections' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Explorer' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Consumer Price Index' })).toBeVisible()
  await expect(page.getByText('Annual change')).toBeVisible()
  await expect(page.getByLabel('Dataflow')).toHaveValue('abs.cpi')
  await expect(page.getByLabel('Region')).toHaveValue('AUS')
  await expect(page.getByRole('heading', { name: 'National CPI' })).toBeVisible()
  await expect(page.getByTestId('national-cpi-chart')).toContainText('136.9')
  await expect(page.getByRole('heading', { name: 'State comparison' })).toBeVisible()
  const stateComparison = page.getByTestId('state-comparison-chart')
  await expect(stateComparison.getByText('New South Wales')).toBeVisible()
  await expect(stateComparison.getByText('Victoria')).toBeVisible()
  await expect(stateComparison.getByText('Queensland')).toBeVisible()

  await page.getByLabel('Region').selectOption('NSW')

  await expect(page.getByTestId('latest-observation')).toContainText('139.2')
  await expect(page.getByRole('table', { name: 'Observations' })).toContainText('New South Wales')
  await expect(page).toHaveScreenshot('explorer-national-cpi.png')
})

test('Explorer mobile dashboard stacks without horizontal overflow', async ({ page }) => {
  await page.setViewportSize({ height: 844, width: 390 })
  await page.goto('/explorer')

  await expect(page.getByRole('button', { name: 'Open navigation' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Explorer' })).toBeVisible()
  await expect(page.getByTestId('national-cpi-chart')).toBeVisible()
  await expect(page.getByTestId('state-comparison-chart')).toBeVisible()
  await expect(page.getByRole('table', { name: 'Observations' })).toBeVisible()

  const hasHorizontalOverflow = await page.evaluate(
    () => document.documentElement.scrollWidth > window.innerWidth,
  )
  expect(hasHorizontalOverflow).toBe(false)
})

test('has no WCAG AA accessibility violations in the loaded Explorer state', async ({ page }) => {
  await page.goto('/explorer')
  await expect(page.getByRole('heading', { name: 'National CPI' })).toBeVisible()

  const results = await new AxeBuilder({ page }).analyze()

  expect(results.violations).toEqual([])
})
