import { spawnSync } from 'node:child_process'
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..')
const generatedSource = resolve(root, 'packages/sdk-generated/src')
const openApiPath = resolve(root, 'openapi.json')

mkdirSync(generatedSource, { recursive: true })

const openApiJson = run('cargo', ['run', '-p', 'au-kpis-openapi'], { capture: true })
writeFileSync(openApiPath, openApiJson)

rmSync(resolve(generatedSource, 'client.ts'), { force: true })
rmSync(resolve(generatedSource, 'types.ts'), { force: true })

run('pnpm', [
  'exec',
  'openapi-typescript',
  'openapi.json',
  '-o',
  'packages/sdk-generated/src/types.ts',
])

run('pnpm', [
  'exec',
  'orval',
  '--input',
  'openapi.json',
  '--output',
  'packages/sdk-generated/src/client.ts',
  '--client',
  'fetch',
  '--mode',
  'single',
])

run('pnpm', [
  'exec',
  'biome',
  'format',
  '--write',
  'openapi.json',
  'packages/sdk-generated/src/client.ts',
  'packages/sdk-generated/src/types.ts',
])

for (const file of [
  openApiPath,
  resolve(generatedSource, 'client.ts'),
  resolve(generatedSource, 'types.ts'),
]) {
  normalizeTextFile(file)
}

function normalizeTextFile(file) {
  const normalized = readFileSync(file, 'utf8')
    .replace(/[ \t]+$/gm, '')
    .replace(/\n*$/, '\n')
  writeFileSync(file, normalized)
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: root,
    encoding: 'utf8',
    stdio: options.capture ? 'pipe' : 'inherit',
  })

  if (result.error !== undefined) {
    throw result.error
  }

  if (result.status !== 0) {
    if (options.capture && result.stderr.length > 0) {
      process.stderr.write(result.stderr)
    }
    throw new Error(`${command} ${args.join(' ')} failed with ${result.status}`)
  }

  if (options.capture && result.stderr.length > 0) {
    process.stderr.write(result.stderr)
  }

  return result.stdout
}
