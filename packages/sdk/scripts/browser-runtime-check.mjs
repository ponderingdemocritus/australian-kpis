import { spawnSync } from 'node:child_process'
import { readFileSync, rmSync, statSync } from 'node:fs'
import { mkdtemp } from 'node:fs/promises'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { dirname, extname, join, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const repoRoot = resolve(packageRoot, '../..')
const chrome =
  process.env.CHROME_BIN ??
  findExecutable(['google-chrome-stable', 'google-chrome', 'chromium-browser', 'chromium'])

if (chrome === undefined) {
  throw new Error('Chrome or Chromium executable is required for browser runtime check')
}

const server = createServer((request, response) => {
  const url = new URL(request.url ?? '/', 'http://127.0.0.1')

  if (url.pathname === '/') {
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' })
    response.end(browserPage())
    return
  }

  const file = resolve(repoRoot, `.${url.pathname}`)
  if (!file.startsWith(`${repoRoot}${sep}`)) {
    response.writeHead(403)
    response.end()
    return
  }

  try {
    const stat = statSync(file)
    if (!stat.isFile()) {
      response.writeHead(404)
      response.end()
      return
    }
    response.writeHead(200, { 'content-type': contentType(file) })
    response.end(readFileSync(file))
  } catch {
    response.writeHead(404)
    response.end()
  }
})

await new Promise((resolveListen) => {
  server.listen(0, '127.0.0.1', resolveListen)
})

try {
  const address = server.address()
  if (address === null || typeof address === 'string') {
    throw new Error('browser test server did not bind a TCP port')
  }

  const userDataDir = await mkdtemp(join(tmpdir(), 'au-kpis-sdk-browser-'))
  const result = spawnSync(
    chrome,
    [
      '--headless=new',
      '--disable-gpu',
      '--no-sandbox',
      `--user-data-dir=${userDataDir}`,
      '--dump-dom',
      `http://127.0.0.1:${address.port}/`,
    ],
    { encoding: 'utf8' },
  )
  rmSync(userDataDir, { force: true, recursive: true })

  if (result.status !== 0) {
    process.stderr.write(result.stderr)
    throw new Error(`browser runtime check failed with exit code ${result.status}`)
  }

  if (!result.stdout.includes('sdk-browser-pass')) {
    process.stderr.write(result.stdout)
    throw new Error('browser runtime check did not report success')
  }
} finally {
  server.close()
}

function browserPage() {
  return String.raw`<!doctype html>
<html lang="en">
  <body>
    <script type="module">
      import { createClient } from '/packages/sdk/dist/index.js'

      const client = createClient({
        baseUrl: 'https://api.example.test',
        fetch: async () =>
          new Response(JSON.stringify({ dataflows: [] }), {
            headers: { 'content-type': 'application/json' },
            status: 200,
          }),
      })
      const result = await client.dataflows.list()
      if (!Array.isArray(result.dataflows)) {
        throw new Error('expected dataflows array')
      }
      document.body.textContent = 'sdk-browser-pass'
    </script>
  </body>
</html>`
}

function findExecutable(candidates) {
  for (const candidate of candidates) {
    const result = spawnSync('sh', ['-lc', `command -v ${candidate}`], {
      encoding: 'utf8',
    })
    const path = result.stdout.trim()
    if (result.status === 0 && path.length > 0) {
      return path
    }
  }
  return undefined
}

function contentType(file) {
  switch (extname(file)) {
    case '.js':
      return 'text/javascript; charset=utf-8'
    case '.json':
      return 'application/json; charset=utf-8'
    default:
      return 'application/octet-stream'
  }
}
