import { defineConfig, devices } from '@playwright/test'

const port = Number(process.env.AU_KPIS_WEB_PORT ?? 4173)
const baseURL = process.env.AU_KPIS_WEB_BASE_URL ?? `http://127.0.0.1:${port}`

export default defineConfig({
  expect: {
    toHaveScreenshot: {
      maxDiffPixelRatio: 0.05,
    },
  },
  forbidOnly: Boolean(process.env.CI),
  fullyParallel: false,
  outputDir: 'test-results',
  reporter: process.env.CI ? [['github'], ['list']] : 'list',
  snapshotPathTemplate: '{testDir}/__snapshots__/{arg}{ext}',
  testDir: './e2e',
  use: {
    baseURL,
    trace: 'on-first-retry',
  },
  workers: 1,
  webServer: {
    command: `pnpm --filter @au-kpis/sdk-generated build && pnpm --filter @au-kpis/sdk build && pnpm --filter @au-kpis/web exec next dev --hostname 127.0.0.1 --port ${port}`,
    env: {
      NEXT_PUBLIC_AU_KPIS_API_BASE_URL:
        process.env.NEXT_PUBLIC_AU_KPIS_API_BASE_URL ??
        process.env.VITE_AU_KPIS_API_BASE_URL ??
        'http://127.0.0.1:3000',
    },
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    url: baseURL,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'], viewport: { height: 900, width: 1440 } },
    },
  ],
})
