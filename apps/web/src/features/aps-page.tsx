'use client'

import {
  ApsDashboard,
  ApsEmptyState,
  ApsErrorState,
  ApsLoadingState,
} from '@/features/aps-components'
import { client } from '@/lib/api'
import { useQuery } from '@tanstack/react-query'

export function ApsPage() {
  const configQuery = useQuery({
    queryFn: () => client.scorecards.aps.config(),
    queryKey: ['aps', 'config'],
    retry: 1,
  })
  const latestQuery = useQuery({
    queryFn: () => client.scorecards.aps.latest(),
    queryKey: ['aps', 'latest'],
    retry: 1,
  })
  // History powers the scatter trail. It must not gate loading or error — the
  // dashboard renders as soon as config + latest resolve and the trail fills in.
  const historyQuery = useQuery({
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
