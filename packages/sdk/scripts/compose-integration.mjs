import { createClient } from '../dist/index.js'

const baseUrl = process.env.AU_KPIS_SDK_BASE_URL ?? 'http://127.0.0.1:3000'
const seriesKey = 'a'.repeat(64)

const client = createClient({ baseUrl, validate: true })

const health = await client.health()
assertEqual(health.status, 'ok')

const dataflows = await client.dataflows.list({ frequency: 'quarterly', source: 'abs' })
assertEqual(dataflows.dataflows.length, 1)
assertEqual(dataflows.dataflows[0]?.id, 'abs.cpi')

const dataflow = await client.dataflows.get('abs.cpi')
assertEqual(dataflow.dataflow.id, 'abs.cpi')
assertEqual(dataflow.dimensions[0]?.id, 'region')

const codelist = await client.dataflows.codelists('abs.cpi', 'region')
assertEqual(codelist.codelist.codes[0]?.id, 'AUS')

const observations = await client.observations.list({
  dataflow: 'abs.cpi',
  dimensions: { region: 'AUS' },
  limit: 1,
})
assertEqual(observations.observations.length, 1)
assertEqual(observations.observations[0]?.value, 136.9)

const streamed = []
for await (const observation of client.observations.stream({
  dataflow: 'abs.cpi',
  dimensions: { region: 'AUS' },
  limit: 1,
})) {
  streamed.push(observation)
}
assertEqual(streamed.length, 1)
assertEqual(streamed[0]?.series_key, seriesKey)

const latest = await client.observations.latest({
  dataflow: 'abs.cpi',
  seriesKey,
})
assertEqual(latest.latest_observation?.value, 136.9)
assertEqual(latest.revision?.revision_no, 1)

console.log('SDK compose integration passed')

function assertEqual(actual, expected) {
  if (actual !== expected) {
    throw new Error(`expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`)
  }
}
