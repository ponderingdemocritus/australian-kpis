import { createClient } from './index'

const client = createClient({
  apiKey: 'test-key',
  baseUrl: 'https://api.example.test',
})

const health: Promise<{ status: string }> = client.health()
const document: Promise<unknown> = client.openapi()

void health
void document
