import { describe, expect, it } from 'vitest'

import {
  type ApsContribution,
  apsAxisDisplayScore,
  sortedContributions,
  sourceLabel,
  tokenLabel,
} from './aps-data'

function contribution(
  axis: ApsContribution['axis'],
  component: string,
  label: string,
  sourceDataflowId: string,
): ApsContribution {
  return {
    attribution: 'Source: Example',
    axis,
    component,
    confidence: 'high',
    coverage_status: 'resolved',
    dimensions: { region: 'AUS' },
    direction: 'higher_is_better',
    indicator_id: label.toLowerCase().replaceAll(' ', '-'),
    label,
    latest_period: '2026-06-30',
    license: 'CC-BY-4.0',
    measure_id: 'value',
    normalized_value: 0.5,
    notes: null,
    raw_value: 1,
    series_key: null,
    source_artifact_id: null,
    source_dataflow_id: sourceDataflowId,
    source_url: 'https://example.test/source',
    unit: 'index',
    weight: 0.1,
  }
}

describe('APS data helpers', () => {
  it('orders contributions by axis, component, then label', () => {
    const contributions = [
      contribution('throughput', 'zoning', 'Beta', 'abs.beta'),
      contribution('orientation', 'ai_readiness', 'Gamma', 'naic.gamma'),
      contribution('throughput', 'approvals', 'Alpha', 'abs.alpha'),
    ]

    expect(sortedContributions(contributions).map((item) => item.label)).toEqual([
      'Gamma',
      'Alpha',
      'Beta',
    ])
  })

  it('formats source and token labels for drilldowns', () => {
    const item = contribution('throughput', 'ai_readiness', 'AI readiness', 'worldbank.bready')

    expect(sourceLabel(item)).toBe('WORLDBANK')
    expect(tokenLabel('ai_readiness')).toBe('Ai Readiness')
  })

  it('converts orientation axis scores from signed APS scale to display scale', () => {
    expect(apsAxisDisplayScore('throughput', 0.59)).toBe(59)
    expect(apsAxisDisplayScore('orientation', 0.59)).toBe(79.5)
  })
})
