import { AxeBuilder } from '@axe-core/playwright'
import { expect, test } from '@playwright/test'

test('renders the national CPI chart journey with state comparison', async ({ page }) => {
  await page.goto('/')

  await expect(page.getByRole('heading', { exact: true, name: 'Explorer' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Consumer Price Index' })).toBeVisible()
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

test('search finds CPI and opens the result in Explorer', async ({ page }) => {
  await page.goto('/')

  await page.getByRole('button', { name: 'Search' }).click()
  await expect(page.getByRole('heading', { name: 'Search' })).toBeVisible()
  await expect(page.getByLabel('Search catalog')).toHaveValue('CPI')

  const result = page.getByTestId('search-result-dataflow-abs.cpi')
  await expect(result.getByRole('heading', { name: 'Consumer Price Index' })).toBeVisible()
  await expect(result).toContainText('abs')

  await result.getByRole('button', { name: 'Open in Explore' }).click()

  await expect(page.getByRole('button', { name: 'Explorer' })).toHaveAttribute(
    'aria-current',
    'page',
  )
  await expect(page.getByRole('heading', { name: 'Consumer Price Index' })).toBeVisible()
  await expect(page.getByLabel('Dataflow')).toHaveValue('abs.cpi')
  await expect(page.getByTestId('latest-observation')).toContainText('136.9')
})

test('has no WCAG AA accessibility violations in the loaded Explorer state', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('heading', { name: 'National CPI' })).toBeVisible()

  const results = await new AxeBuilder({ page }).analyze()

  expect(results.violations).toEqual([])
})
