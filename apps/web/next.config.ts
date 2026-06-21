import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
  transpilePackages: ['@au-kpis/sdk', '@au-kpis/sdk-generated'],
}

export default nextConfig
