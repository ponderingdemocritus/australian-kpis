import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const read = (path) => readFileSync(path, 'utf8')

const railwayTs = read('.railway/railway.ts')
const railwayDocs = read('docs/deploy/railway.md')
const webToml = read('infra/railway/web.toml')
const webDockerfile = read('infra/docker/au-kpis-web.Dockerfile')
const dockerignore = read('.dockerignore')

const rustDockerfiles = [
  'infra/docker/au-kpis-api.Dockerfile',
  'infra/docker/au-kpis-ingestion.Dockerfile',
  'infra/docker/au-kpis-scheduler.Dockerfile',
]

function serviceBlock(source, serviceName) {
  const start = source.indexOf(`const ${serviceName} = service('${serviceName}'`)
  assert.notEqual(start, -1, `missing ${serviceName} service block`)

  const nextService = source.indexOf('\n  const ', start + 1)
  return source.slice(start, nextService === -1 ? undefined : nextService)
}

function quotedListBlock(source, name) {
  const assignment = new RegExp(`${name}\\s*[:=]\\s*\\[`)
  const match = assignment.exec(source)
  const start = match?.index ?? -1
  assert.notEqual(start, -1, `missing ${name}`)

  const end = source.indexOf(']', start)
  assert.notEqual(end, -1, `unterminated ${name}`)

  return source
    .slice(start, end)
    .split('\n')
    .map((line) => line.match(/['"]([^'"]+)['"]/)?.[1])
    .filter(Boolean)
}

test('Railway web service resolves the API private URL with an explicit API port', () => {
  const api = serviceBlock(railwayTs, 'api')
  const web = serviceBlock(railwayTs, 'web')

  assert.match(api, /PORT:\s*'3000'/)
  assert.match(
    web,
    /AU_KPIS_API_BASE_URL:\s*'http:\/\/\$\{\{api\.RAILWAY_PRIVATE_DOMAIN\}\}:\$\{\{api\.PORT\}\}'/,
  )
})

test('web Dockerfile uses lockfile-only pnpm fetch before copying source', () => {
  const beforeFullCopy = webDockerfile.slice(0, webDockerfile.indexOf('\nCOPY . .'))

  assert.match(beforeFullCopy, /COPY \.npmrc pnpm-lock\.yaml pnpm-workspace\.yaml \.\//)
  assert.match(beforeFullCopy, /RUN pnpm fetch(?: --filter @au-kpis\/web\.\.\.)?/)
  assert.doesNotMatch(beforeFullCopy, /COPY .*package\.json/)
  assert.match(
    webDockerfile,
    /RUN pnpm install --offline --frozen-lockfile --filter @au-kpis\/web\.\.\.\s*\\\n\s+&& pnpm --filter @au-kpis\/web\.\.\. build/,
  )
})

test('Railway web watch patterns mirror the Dockerfile build inputs', () => {
  const railwayWatch = quotedListBlock(serviceBlock(railwayTs, 'web'), 'watchPatterns')
  const tomlWatch = quotedListBlock(webToml, 'watchPatterns')
  const expectedCommon = [
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
  ]

  assert.deepEqual(railwayWatch, [...expectedCommon, '/.railway/railway.ts'])
  assert.deepEqual(tomlWatch, [...expectedCommon, '/infra/railway/web.toml'])
})

test('Railway deploy docs describe the cache-stable web install layer', () => {
  assert.match(railwayDocs, /web image uses\s+a lockfile-only `pnpm fetch` layer/)
  assert.doesNotMatch(railwayDocs, /package-manifest layer/)
})

test('Rust Dockerfiles use the prebuilt cargo-chef image without rustup downloads', () => {
  assert.match(dockerignore, /^rust-toolchain\.toml$/m)

  for (const path of rustDockerfiles) {
    const dockerfile = read(path)

    assert.match(
      dockerfile,
      /^FROM lukemathwalker\/cargo-chef:0\.1\.71-rust-1\.85\.0-bookworm AS chef/m,
      path,
    )
    assert.doesNotMatch(dockerfile, /cargo install cargo-chef/, path)
    assert.doesNotMatch(dockerfile, /COPY rust-toolchain\.toml/, path)
    assert.match(
      dockerfile,
      /COPY --from=planner \/app\/recipe\.json recipe\.json\nRUN cargo chef cook --release --locked --bin /,
      path,
    )
  }
})
