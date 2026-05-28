import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import openapi from '../../../openapi.json' with { type: 'json' }

const docsRoot = new URL('..', import.meta.url)
const repoRoot = new URL('../../..', import.meta.url)
const distRoot = new URL('dist/', docsRoot)
const docsBasePath = process.env.DOCS_BASE_PATH ?? '/'

function toDistRelativePath(assetPath) {
  let relativePath = assetPath.replace(/^\/+/, '')
  const basePath = docsBasePath.replace(/^\/+|\/+$/g, '')
  if (basePath && relativePath.startsWith(`${basePath}/`)) {
    relativePath = relativePath.slice(basePath.length + 1)
  }
  return relativePath
}

const html = await readFile(new URL('index.html', distRoot), 'utf8')
if (!html.includes('Australian KPIs API Reference')) {
  throw new Error('dist/index.html should contain the API reference title')
}

const assetMatches = [...html.matchAll(/src="([^"]+\.js)"/g)]
if (assetMatches.length === 0) {
  throw new Error('dist/index.html should reference a generated JavaScript asset')
}

const bundleText = await Promise.all(
  assetMatches.map(async ([, assetPath]) => {
    if (!assetPath) {
      throw new Error('generated script tag is missing its asset path')
    }

    const relativePath = toDistRelativePath(assetPath)
    return readFile(join(distRoot.pathname, relativePath), 'utf8')
  }),
)

const bundled = bundleText.join('\n')
const expectedPaths = ['/v1/openapi.json', '/v1/observations']
for (const apiPath of expectedPaths) {
  if (!openapi.paths?.[apiPath]) {
    throw new Error(`openapi.json is missing expected path ${apiPath}`)
  }
  if (!bundled.includes(apiPath)) {
    throw new Error(`generated docs bundle should include current OpenAPI path ${apiPath}`)
  }
}

const rootOpenapi = await readFile(new URL('openapi.json', repoRoot), 'utf8')
const parsed = JSON.parse(rootOpenapi)
if (parsed.info?.title !== openapi.info?.title) {
  throw new Error('docs verification should read the committed openapi.json artifact')
}

const assetsDir = new URL('assets/', distRoot)
const firstAsset = assetMatches[0]?.[1]
if (!firstAsset || !toDistRelativePath(firstAsset).startsWith('assets/')) {
  throw new Error('generated docs bundle should be emitted under dist/assets')
}
await readFile(join(assetsDir.pathname, toDistRelativePath(firstAsset).replace(/^assets\//, '')))

console.log(
  `Verified generated docs from openapi.json with ${Object.keys(openapi.paths ?? {}).length} paths.`,
)
