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
  })
  const latestQuery = useQuery({
    queryFn: () => client.scorecards.aps.latest(),
    queryKey: ['aps', 'latest'],
  })

  if (configQuery.isLoading || latestQuery.isLoading) {
    return <ApsLoadingState />
  }

  const error = configQuery.error ?? latestQuery.error
  if (error instanceof Error) {
    return <ApsErrorState message={error.message} />
  }

  if (configQuery.data === undefined || latestQuery.data === undefined) {
    return <ApsEmptyState />
  }

  if (configQuery.data.indicators.length === 0 || latestQuery.data.contributions.length === 0) {
    return <ApsEmptyState />
  }

  return <ApsDashboard config={configQuery.data} snapshot={latestQuery.data} />
}
