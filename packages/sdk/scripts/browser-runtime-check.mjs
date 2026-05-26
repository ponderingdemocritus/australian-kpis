import { spawn, spawnSync } from 'node:child_process'
import { readFileSync, rmSync, statSync } from 'node:fs'
import { mkdtemp } from 'node:fs/promises'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { dirname, extname, join, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const repoRoot = resolve(packageRoot, '../..')
const chrome =
  process.env.CHROME_BIN && process.env.CHROME_BIN.length > 0
    ? process.env.CHROME_BIN
    : findExecutable(['google-chrome-stable', 'google-chrome', 'chromium-browser', 'chromium'])

if (chrome === undefined) {
  throw new Error('Chrome or Chromium executable is required for browser runtime check')
}

let reportBrowserResult
const browserResult = new Promise((resolve, reject) => {
  reportBrowserResult = { reject, resolve }
})

const server = createServer((request, response) => {
  const url = new URL(request.url ?? '/', 'http://127.0.0.1')

  if (request.method === 'POST' && url.pathname === '/pass') {
    readBody(request)
      .then((body) => {
        response.writeHead(204)
        response.end()
        if (body === 'sdk-browser-pass') {
          reportBrowserResult.resolve()
        } else {
          reportBrowserResult.reject(new Error(`unexpected browser pass body: ${body}`))
        }
      })
      .catch((error) => {
        response.writeHead(500)
        response.end()
        reportBrowserResult.reject(error)
      })
    return
  }

  if (request.method === 'POST' && url.pathname === '/fail') {
    readBody(request)
      .then((body) => {
        response.writeHead(204)
        response.end()
        reportBrowserResult.reject(new Error(`browser runtime check failed: ${body}`))
      })
      .catch((error) => {
        response.writeHead(500)
        response.end()
        reportBrowserResult.reject(error)
      })
    return
  }

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
  const browser = spawn(chrome, [
    '--headless=new',
    '--disable-background-networking',
    '--disable-component-update',
    '--disable-default-apps',
    '--disable-extensions',
    '--disable-features=MediaRouter,OptimizationHints,Translate',
    '--disable-gpu',
    '--disable-dev-shm-usage',
    '--no-default-browser-check',
    '--no-first-run',
    '--no-sandbox',
    `--user-data-dir=${userDataDir}`,
    `http://127.0.0.1:${address.port}/`,
  ])

  let browserStderr = ''
  browser.stderr.on('data', (chunk) => {
    browserStderr = `${browserStderr}${chunk.toString('utf8')}`.slice(-6_000)
  })

  const browserExit = new Promise((_, reject) => {
    browser.once('error', reject)
    browser.once('exit', (code, signal) => {
      reject(
        new Error(
          `browser exited before reporting success (${code ?? signal ?? 'unknown'})\n${browserStderr}`,
        ),
      )
    })
  })
  browserExit.catch(() => {})

  try {
    await withTimeout(Promise.race([browserResult, browserExit]), 30_000, () => browserStderr)
  } finally {
    await stopBrowser(browser)
    rmSync(userDataDir, { force: true, recursive: true })
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

      const report = (path, body) =>
        fetch(path, { body, method: 'POST' }).catch(() => {})

      window.addEventListener('error', (event) => {
        void report('/fail', event.message ?? 'unknown browser error')
      })
      window.addEventListener('unhandledrejection', (event) => {
        void report('/fail', String(event.reason ?? 'unknown rejected promise'))
      })

      const client = createClient({
        baseUrl: 'https://api.example.test',
        fetch: async () =>
          new Response(JSON.stringify({ dataflows: [] }), {
            headers: { 'content-type': 'application/json' },
            status: 200,
          }),
      })
      if (typeof client.dataflows.list !== 'function') {
        throw new Error('expected dataflows list method')
      }
      document.body.textContent = 'sdk-browser-pass'
      await report('/pass', 'sdk-browser-pass')
    </script>
  </body>
</html>`
}

async function readBody(request) {
  const chunks = []
  for await (const chunk of request) {
    chunks.push(Buffer.from(chunk))
  }
  return Buffer.concat(chunks).toString('utf8')
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

function withTimeout(promise, ms, stderr) {
  let timeout
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timeout = setTimeout(() => {
        reject(new Error(`browser runtime check timed out\n${stderr()}`))
      }, ms)
    }),
  ]).finally(() => {
    clearTimeout(timeout)
  })
}

async function stopBrowser(browser) {
  if (browser.exitCode !== null || browser.signalCode !== null) {
    return
  }

  browser.kill('SIGTERM')
  await Promise.race([
    new Promise((resolve) => {
      browser.once('exit', resolve)
    }),
    new Promise((resolve) => {
      setTimeout(resolve, 2_000)
    }),
  ])

  if (browser.exitCode === null && browser.signalCode === null) {
    browser.kill('SIGKILL')
  }
}
