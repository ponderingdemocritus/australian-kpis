'use client'

import {
  ApsDashboard,
  ApsEmptyState,
  ApsErrorState,
  ApsLoadingState,
} from '@/features/aps-components'
import { client } from '@/lib/api'
import type { ApsSnapshotSummary, PublishedApsSnapshot, ScorecardConfig } from '@au-kpis/sdk'
import { useQuery } from '@tanstack/react-query'

export function ApsPage({
  initialConfig,
  initialHistory,
  initialSnapshot,
}: {
  initialConfig?: ScorecardConfig
  initialHistory?: ApsSnapshotSummary[]
  initialSnapshot?: PublishedApsSnapshot
} = {}) {
  const configQuery = useQuery({
    initialData: initialConfig,
    queryFn: () => client.scorecards.aps.config(),
    queryKey: ['aps', 'config'],
    retry: 1,
  })
  const latestQuery = useQuery({
    initialData: initialSnapshot,
    queryFn: () => client.scorecards.aps.latest(),
    queryKey: ['aps', 'latest'],
    retry: 1,
  })
  // History powers the scatter trail. It must not gate loading or error — the
  // dashboard renders as soon as config + latest resolve and the trail fills in.
  const historyQuery = useQuery({
    initialData: initialHistory,
    queryFn: () => client.scorecards.aps.history(),
    queryKey: ['aps', 'history'],
    retry: 1,
  })

  if (configQuery.isLoading || latestQuery.isLoading) {
    return <ApsLoadingState />
  }

  const error = configQuery.error ?? latestQuery.error
  if (error instanceof Error) {
    return (
      <ApsErrorState
        message={error.message}
        onRetry={() => {
          void configQuery.refetch()
          void latestQuery.refetch()
        }}
      />
    )
  }

  if (configQuery.data === undefined || latestQuery.data === undefined) {
    return <ApsEmptyState />
  }

  if (configQuery.data.indicators.length === 0 || latestQuery.data.contributions.length === 0) {
    return <ApsEmptyState />
  }

  return (
    <ApsDashboard
      config={configQuery.data}
      history={historyQuery.data ?? []}
      snapshot={latestQuery.data}
    />
  )
}
