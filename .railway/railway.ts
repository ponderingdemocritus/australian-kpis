import { defineRailway, empty, group, preserve, project, redis, service } from 'railway/iac'

const region = 'asia-southeast1-eqsg3a'
const gib = 1024 * 1024 * 1024

const restartPolicy = {
  drainingSeconds: 30,
  overlapSeconds: 20,
  region,
  restartPolicyMaxRetries: 10,
  restartPolicyType: 'ON_FAILURE' as const,
  sleepApplication: false,
}

function deploy(replicas: number, cpu: number, memoryGiB: number, healthcheckPath?: string) {
  return {
    ...restartPolicy,
    healthcheckPath,
    healthcheckTimeout: healthcheckPath === undefined ? undefined : 300,
    limitOverride: {
      containers: { cpu, memoryBytes: memoryGiB * gib },
    },
    numReplicas: replicas,
  }
}

const telemetryEnv = (serviceName: string) => ({
  AU_KPIS_TELEMETRY__LOG_FORMAT: 'json',
  AU_KPIS_TELEMETRY__LOG_LEVEL: 'info',
  AU_KPIS_TELEMETRY__SERVICE_NAME: serviceName,
  OTEL_EXPORTER_OTLP_ENDPOINT: 'http://otel-collector.railway.internal:4318/v1/traces',
})

const databaseEnv = () => ({
  AU_KPIS_DATABASE__URL: preserve(),
})

const objectStoreEnv = () => ({
  AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID: preserve(),
  AU_KPIS_OBJECT_STORE__BUCKET: preserve(),
  AU_KPIS_OBJECT_STORE__ENDPOINT: preserve(),
  AU_KPIS_OBJECT_STORE__DELETE_ENABLED: 'false',
  AU_KPIS_OBJECT_STORE__REGION: preserve(),
  AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY: preserve(),
})

export default defineRailway(() => {
  // Redis is intentionally the only Railway stateful service. It stores
  // disposable cache, rate-limit, and replay state only.
  const cache = redis('Redis', { region })

  // Code services are source-less in IaC. deploy.yml connects an immutable
  // signed GHCR digest, preventing repository-triggered Railway auto-deploys.
  const api = service('api', {
    source: empty(),
    env: {
      ...databaseEnv(),
      ...telemetryEnv('au-kpis-api'),
      AU_KPIS_BFF_ORIGIN_ID: preserve(),
      AU_KPIS_BFF_ORIGIN_SECRET: preserve(),
      AU_KPIS_CACHE__URL: cache.env.REDIS_URL,
      AU_KPIS_CLOUDFLARE_ORIGIN_ID: preserve(),
      AU_KPIS_CLOUDFLARE_ORIGIN_SECRET: preserve(),
      AU_KPIS_CURSOR_PRIOR_SIGNING_KEY: preserve(),
      AU_KPIS_CURSOR_PRIOR_VALID_UNTIL: preserve(),
      AU_KPIS_CURSOR_SIGNING_KEY: preserve(),
      AU_KPIS_HTTP__CORS_ALLOWED_ORIGINS: preserve(),
      AU_KPIS_METRICS_BEARER_TOKEN: preserve(),
      AU_KPIS_ORIGIN_AUTH_REQUIRED: 'true',
      AU_KPIS_WEBHOOK_ENCRYPTION_KEY: preserve(),
      AU_KPIS_WEBHOOK_ENCRYPTION_KEY_VERSION: preserve(),
      PORT: '3000',
    },
    deploy: deploy(2, 2, 2, '/readyz'),
  })

  const web = service('web', {
    source: empty(),
    build: {
      builder: 'DOCKERFILE',
      dockerfilePath: 'infra/docker/au-kpis-web.Dockerfile',
      watchPatterns: [
        '/.npmrc',
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
      AU_KPIS_BFF_ORIGIN_ID: preserve(),
      AU_KPIS_BFF_ORIGIN_SECRET: preserve(),
      AU_KPIS_ORIGIN_AUTH_REQUIRED: 'true',
      PORT: '3000',
    },
    deploy: deploy(2, 1, 1, '/'),
  })

  const pdfExtractor = service('pdf-extractor', {
    source: empty(),
    env: {
      ...objectStoreEnv(),
      PORT: '8000',
    },
    deploy: deploy(2, 2, 4, '/health'),
  })

  const ingestion = service('ingestion', {
    source: empty(),
    env: {
      ...databaseEnv(),
      ...objectStoreEnv(),
      ...telemetryEnv('au-kpis-ingestion'),
      AU_KPIS_PDF_BASE_URL: 'http://pdf-extractor.railway.internal:8000',
      PORT: '3000',
    },
    deploy: deploy(2, 2, 4, '/metrics'),
  })

  const scheduler = service('scheduler', {
    source: empty(),
    env: {
      ...databaseEnv(),
      ...telemetryEnv('au-kpis-scheduler'),
      PORT: '3000',
    },
    deploy: deploy(2, 0.5, 0.5, '/metrics'),
  })

  const webhookWorker = service('webhook-worker', {
    source: empty(),
    env: {
      ...databaseEnv(),
      ...telemetryEnv('au-kpis-webhook-worker'),
      AU_KPIS_WEBHOOK_ENCRYPTION_KEY: preserve(),
      AU_KPIS_WEBHOOK_ENCRYPTION_KEY_VERSION: preserve(),
    },
    deploy: deploy(2, 1, 1),
  })

  const otelCollector = service('otel-collector', {
    source: empty(),
    env: {
      GRAFANA_CLOUD_API_KEY: preserve(),
      AU_KPIS_METRICS_BEARER_TOKEN: preserve(),
      GRAFANA_CLOUD_INSTANCE_ID: preserve(),
      GRAFANA_CLOUD_OTLP_ENDPOINT: preserve(),
      PORT: '13133',
    },
    deploy: deploy(2, 1, 1, '/'),
  })

  const runtime = group('Stateless Runtime', [
    api,
    web,
    pdfExtractor,
    ingestion,
    scheduler,
    webhookWorker,
    otelCollector,
  ])
  const disposable = group('Disposable State', [cache])

  return project('australian-kpis', {
    environments: ['staging', 'production'],
    resources: [...runtime, ...disposable],
  })
})
