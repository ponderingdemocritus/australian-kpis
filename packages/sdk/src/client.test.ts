import type {
  DataflowCodelistResponse,
  DataflowDetailResponse,
  DataflowsResponse,
  ObservationsResponse,
  ObservationsRow,
  SeriesLookupResponse,
} from '@au-kpis/sdk-generated/client'
import { createClient } from './index'

const client = createClient({
  apiKey: 'test-key',
  baseUrl: 'https://api.example.test',
})

const health: Promise<{ status: string }> = client.health()
const document: Promise<unknown> = client.openapi()
const dataflows: Promise<DataflowsResponse> = client.dataflows.list({
  frequency: 'quarterly',
  source: 'abs',
})
const dataflow: Promise<DataflowDetailResponse> = client.dataflows.get('abs.cpi')
const codelist: Promise<DataflowCodelistResponse> = client.dataflows.codelists('abs.cpi', 'region')
const observations: Promise<ObservationsResponse> = client.observations.list({
  dataflow: 'abs.cpi',
  dimensions: {
    measure: 'All groups CPI',
    region: 'AUS',
  },
  limit: 100,
  since: '2010-01-01',
})
const stream: AsyncIterable<ObservationsRow> = client.observations.stream({
  dataflow: 'abs.cpi',
  limit: 1000,
})
const latest: Promise<SeriesLookupResponse> = client.observations.latest({
  dataflow: 'abs.cpi',
  seriesKey: 'a'.repeat(64),
})
const validatingClient = createClient({
  baseUrl: 'https://api.example.test',
  validate: true,
})
const validatingDataflows: Promise<DataflowsResponse> = validatingClient.dataflows.list()

void health
void document
void dataflows
void dataflow
void codelist
void observations
void stream
void latest
void validatingDataflows
