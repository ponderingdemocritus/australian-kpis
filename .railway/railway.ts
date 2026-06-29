import {
  bucket,
  defineRailway,
  github,
  group,
  image,
  project,
  redis,
  service,
  volume,
} from 'railway/iac'

const repo = 'ponderingdemocritus/australian-kpis'

const restartPolicy = {
  restartPolicyType: 'ON_FAILURE' as const,
  restartPolicyMaxRetries: 10,
  overlapSeconds: 20,
  drainingSeconds: 30,
}

const rustWatchPatterns = [
  '/Cargo.lock',
  '/Cargo.toml',
  '/crates/**',
  '/infra/docker/**',
  '/infra/migrations/**',
  '/rust-toolchain.toml',
]

const bucketEnv = {
  AU_KPIS_OBJECT_STORE__ENDPOINT: '${{Bucket.ENDPOINT}}',
  AU_KPIS_OBJECT_STORE__BUCKET: '${{Bucket.BUCKET}}',
  AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID: '${{Bucket.ACCESS_KEY_ID}}',
  AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY: '${{Bucket.SECRET_ACCESS_KEY}}',
  AU_KPIS_OBJECT_STORE__REGION: '${{Bucket.REGION}}',
}

const timescaleDatabaseUrl =
  'postgresql://au_kpis:${{Timescale.POSTGRES_PASSWORD}}@${{Timescale.RAILWAY_PRIVATE_DOMAIN}}:5432/au_kpis'

export default defineRailway(() => {
  const timescaleData = volume('Timescale Data', {
    sizeMB: 10240,
  })

  const timescale = service('Timescale', {
    source: image('timescale/timescaledb:2.17.2-pg16'),
    env: {
      POSTGRES_DB: 'au_kpis',
      POSTGRES_USER: 'au_kpis',
      POSTGRES_PASSWORD: {
        description: 'Generated password for the TimescaleDB au_kpis user.',
        generator: 'secret(32)',
      },
      PGDATA: '/var/lib/postgresql/data/pgdata',
    },
    volumeMounts: {
      '/var/lib/postgresql/data': timescaleData,
    },
    deploy: {
      restartPolicyType: 'ON_FAILURE',
      restartPolicyMaxRetries: 10,
    },
  })

  const cache = redis('Redis')
  const artifacts = bucket('Bucket', { region: 'iad' })

  const pdfExtractor = service('pdf-extractor', {
    source: github(repo),
    build: {
      builder: 'DOCKERFILE',
      dockerfilePath: 'infra/docker/au-kpis-pdf-extractor.Dockerfile',
      watchPatterns: [
        '/apps/pdf-extractor/**',
        '/infra/docker/au-kpis-pdf-extractor.Dockerfile',
        '/.railway/railway.ts',
      ],
    },
    env: {
      PORT: '8000',
      ...bucketEnv,
    },
    deploy: {
      healthcheckPath: '/health',
      healthcheckTimeout: 300,
      ...restartPolicy,
    },
  })

  const api = service('api', {
    source: github(repo),
    build: {
      builder: 'DOCKERFILE',
      dockerfilePath: 'infra/docker/au-kpis-api.Dockerfile',
      watchPatterns: [
        ...rustWatchPatterns,
        '/infra/docker/au-kpis-api.Dockerfile',
        '/.railway/railway.ts',
      ],
    },
    env: {
      AU_KPIS_DATABASE__URL: timescaleDatabaseUrl,
      AU_KPIS_CACHE__URL: cache.env.REDIS_URL,
      AU_KPIS_HTTP__CORS_ALLOWED_ORIGINS: '["https://${{web.RAILWAY_PUBLIC_DOMAIN}}"]',
      AU_KPIS_TELEMETRY__SERVICE_NAME: 'au-kpis-api',
      AU_KPIS_TELEMETRY__LOG_FORMAT: 'json',
      AU_KPIS_TELEMETRY__LOG_LEVEL: 'info',
    },
    deploy: {
      healthcheckPath: '/v1/health',
      healthcheckTimeout: 300,
      ...restartPolicy,
    },
  })

  const web = service('web', {
    source: github(repo),
    build: {
      builder: 'DOCKERFILE',
      dockerfilePath: 'infra/docker/au-kpis-web.Dockerfile',
      watchPatterns: [
        '/apps/web/**',
        '/infra/docker/au-kpis-web.Dockerfile',
        '/package.json',
        '/packages/sdk/**',
        '/packages/sdk-generated/**',
        '/pnpm-lock.yaml',
        '/pnpm-workspace.yaml',
        '/tsconfig.base.json',
        '/turbo.json',
        '/.railway/railway.ts',
      ],
    },
    env: {
      AU_KPIS_API_BASE_URL: 'http://${{api.RAILWAY_PRIVATE_DOMAIN}}:${{api.PORT}}',
    },
    deploy: {
      healthcheckPath: '/',
      healthcheckTimeout: 300,
      ...restartPolicy,
    },
  })

  const ingestion = service('ingestion', {
    source: github(repo),
    build: {
      builder: 'DOCKERFILE',
      dockerfilePath: 'infra/docker/au-kpis-ingestion.Dockerfile',
      watchPatterns: [
        ...rustWatchPatterns,
        '/infra/docker/au-kpis-ingestion.Dockerfile',
        '/.railway/railway.ts',
      ],
    },
    env: {
      AU_KPIS_DATABASE__URL: timescaleDatabaseUrl,
      AU_KPIS_PDF_BASE_URL: 'http://${{pdf-extractor.RAILWAY_PRIVATE_DOMAIN}}:8000',
      AU_KPIS_TELEMETRY__SERVICE_NAME: 'au-kpis-ingestion',
      AU_KPIS_TELEMETRY__LOG_FORMAT: 'json',
      AU_KPIS_TELEMETRY__LOG_LEVEL: 'info',
      ...bucketEnv,
    },
    deploy: {
      healthcheckPath: '/metrics',
      healthcheckTimeout: 300,
      ...restartPolicy,
    },
  })

  const scheduler = service('scheduler', {
    source: github(repo),
    build: {
      builder: 'DOCKERFILE',
      dockerfilePath: 'infra/docker/au-kpis-scheduler.Dockerfile',
      watchPatterns: [
        ...rustWatchPatterns,
        '/infra/docker/au-kpis-scheduler.Dockerfile',
        '/.railway/railway.ts',
      ],
    },
    env: {
      AU_KPIS_DATABASE__URL: timescaleDatabaseUrl,
      AU_KPIS_TELEMETRY__SERVICE_NAME: 'au-kpis-scheduler',
      AU_KPIS_TELEMETRY__LOG_FORMAT: 'json',
      AU_KPIS_TELEMETRY__LOG_LEVEL: 'info',
    },
    deploy: {
      healthcheckPath: '/metrics',
      healthcheckTimeout: 300,
      ...restartPolicy,
    },
  })

  const dataPlane = group('Data Plane', [timescale, timescaleData, cache, artifacts])
  const runtime = group('Runtime', [pdfExtractor, ingestion, scheduler, api, web])

  return project('australian-kpis', {
    resources: [...dataPlane, ...runtime],
  })
})
