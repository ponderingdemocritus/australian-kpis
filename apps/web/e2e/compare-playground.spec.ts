import { AxeBuilder } from '@axe-core/playwright'
import { expect, test } from '@playwright/test'

test('Compare overlays multiple selected series on one chart', async ({ page }) => {
  await page.goto('/')

  await page.getByRole('button', { name: 'Compare' }).click()

  await expect(page.getByRole('heading', { name: 'Compare' })).toBeVisible()
  await expect(page.getByLabel('Dataflow')).toHaveValue('abs.cpi')

  const chart = page.getByTestId('compare-chart')
  await expect(chart.getByText('Australia', { exact: true })).toBeVisible()
  await expect(chart.getByText('New South Wales', { exact: true })).toBeVisible()
  await expect(chart.getByText('Victoria', { exact: true })).toBeVisible()

  await expect(page.getByLabel('Queensland')).not.toBeChecked()
  await page.getByLabel('Queensland').check()

  await expect(chart.getByText('Queensland')).toBeVisible()
  await expect(page.getByRole('table', { name: 'Compared series' })).toContainText('138.0')
})

test('Playground runs a live observations query and shows curl plus SDK snippets', async ({
  page,
}) => {
  await page.goto('/')

  await page.getByRole('button', { name: 'Playground' }).click()

  await expect(page.getByRole('heading', { name: 'Playground' })).toBeVisible()
  await expect(page.getByLabel('Region')).toContainText('New South Wales')
  await page.getByLabel('Region').selectOption('NSW')
  await page.getByLabel('Since').fill('2024-03-01')
  await page.getByLabel('Limit').fill('2')
  await page.getByRole('button', { name: 'Run query' }).click()

  const response = page.getByTestId('playground-response')
  await expect(response).toHaveValue(/"observations"/)
  await expect(response).toHaveValue(/"region": "NSW"/)
  await expect(response).toHaveValue(/"value": 139\.2/)

  await expect(page.getByTestId('playground-curl')).toHaveValue(/\/v1\/observations/)
  await expect(page.getByTestId('playground-curl')).toHaveValue(/dimensions\[region\]=NSW/)
  await expect(page.getByTestId('playground-sdk')).toHaveValue(/client\.observations\.list/)
  await expect(page.getByTestId('playground-sdk')).toHaveValue(/region: 'NSW'/)
})

test('Compare and Playground have no WCAG AA accessibility violations', async ({ page }) => {
  await page.goto('/')

  await page.getByRole('button', { name: 'Compare' }).click()
  await expect(page.getByTestId('compare-chart')).toContainText('Australia')
  let results = await new AxeBuilder({ page }).analyze()
  expect(results.violations).toEqual([])

  await page.getByRole('button', { name: 'Playground' }).click()
  await expect(page.getByTestId('playground-response')).toHaveValue(/"observations"/)
  results = await new AxeBuilder({ page }).analyze()
  expect(results.violations).toEqual([])
})
