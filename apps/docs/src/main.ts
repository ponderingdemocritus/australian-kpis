import { createApiReference } from '@scalar/api-reference'
import '@scalar/api-reference/style.css'
import openApiDocument from '../../../openapi.json'
import './styles.css'

createApiReference('#app', {
  content: openApiDocument,
  layout: 'modern',
  metaData: {
    title: 'Australian KPIs API Reference',
    description: 'Generated from the committed OpenAPI contract.',
  },
  theme: 'default',
})
