import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { NextConfig } from 'next'

const configDir = dirname(fileURLToPath(import.meta.url))

const nextConfig: NextConfig = {
  output: 'standalone',
  outputFileTracingRoot: join(configDir, '../..'),
  transpilePackages: ['@au-kpis/sdk', '@au-kpis/sdk-generated'],
}

export default nextConfig
