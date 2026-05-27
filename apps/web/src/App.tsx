import { Button } from '@/components/ui/button'
import { ComparePage } from '@/pages/Compare'
import { ExplorerPage } from '@/pages/Explorer'
import { PlaygroundPage } from '@/pages/Playground'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Database, GitCompareArrows, LineChart, SquareTerminal } from 'lucide-react'
import { useState } from 'react'

const queryClient = new QueryClient()

type PageId = 'explorer' | 'compare' | 'playground'

const pages: Array<{
  icon: typeof LineChart
  id: PageId
  label: string
}> = [
  { icon: LineChart, id: 'explorer', label: 'Explorer' },
  { icon: GitCompareArrows, id: 'compare', label: 'Compare' },
  { icon: SquareTerminal, id: 'playground', label: 'Playground' },
]

export function App() {
  const [activePage, setActivePage] = useState<PageId>('explorer')

  return (
    <QueryClientProvider client={queryClient}>
      <main className="min-h-screen bg-background text-foreground">
        <header className="border-b border-border bg-card">
          <div className="mx-auto flex max-w-7xl flex-col gap-4 px-6 py-4 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-center gap-3">
              <span className="flex size-9 items-center justify-center rounded-md bg-primary text-primary-foreground">
                <Database aria-hidden="true" className="size-4" />
              </span>
              <div>
                <p className="text-sm font-semibold">Australian KPIs</p>
                <p className="text-xs text-muted-foreground">Reference client</p>
              </div>
            </div>
            <nav aria-label="Primary" className="flex flex-wrap gap-2">
              {pages.map((page) => {
                const Icon = page.icon
                const active = activePage === page.id
                return (
                  <Button
                    aria-current={active ? 'page' : undefined}
                    key={page.id}
                    onClick={() => setActivePage(page.id)}
                    size="sm"
                    type="button"
                    variant={active ? 'outline' : 'ghost'}
                  >
                    <Icon aria-hidden="true" className="size-4" />
                    {page.label}
                  </Button>
                )
              })}
            </nav>
          </div>
        </header>

        {activePage === 'explorer' ? <ExplorerPage /> : null}
        {activePage === 'compare' ? <ComparePage /> : null}
        {activePage === 'playground' ? <PlaygroundPage /> : null}
      </main>
    </QueryClientProvider>
  )
}
