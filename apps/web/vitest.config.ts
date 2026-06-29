import { defineConfig } from 'vitest/config'

// Unit tests cover the pure APS derivation helpers in src/. Browser/behaviour
// coverage lives in e2e/ (Playwright) and must be excluded here so Vitest does
// not try to load @playwright/test specs.
export default defineConfig({
  test: {
    environment: 'node',
    include: ['src/**/*.test.{ts,tsx}'],
    exclude: ['e2e/**', 'node_modules/**'],
  },
})
