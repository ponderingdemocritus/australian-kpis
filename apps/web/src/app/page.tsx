import { ApsDashboard, ApsEmptyState } from '@/features/aps-components'
import { ApsPage } from '@/features/aps-page'
import { serverClient } from '@/lib/server-api'

export const dynamic = 'force-dynamic'

export default async function Page() {
  try {
    const [config, snapshot, history] = await Promise.all([
      serverClient.scorecards.aps.config(),
      serverClient.scorecards.aps.latest(),
      serverClient.scorecards.aps.history(),
    ])
    if (config.indicators.length === 0 || snapshot.contributions.length === 0) {
      return <ApsEmptyState />
    }
    return <ApsDashboard config={config} history={history} snapshot={snapshot} />
  } catch {
    return <ApsPage />
  }
}
