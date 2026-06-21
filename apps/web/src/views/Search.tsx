import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { client } from '@/lib/api'
import type { SearchResult } from '@au-kpis/sdk'
import { useQuery } from '@tanstack/react-query'
import { Database, RefreshCw, Search } from 'lucide-react'
import { type FormEvent, useMemo, useState } from 'react'

type SearchPageProps = {
  onSelectDataflow: (dataflowId: string) => void
}

export function SearchPage({ onSelectDataflow }: SearchPageProps) {
  const [draftQuery, setDraftQuery] = useState('CPI')
  const [submittedQuery, setSubmittedQuery] = useState('CPI')
  const normalizedQuery = submittedQuery.trim()

  const searchQuery = useQuery({
    enabled: normalizedQuery.length > 0,
    queryFn: () => client.search.catalog({ limit: 8, q: normalizedQuery }),
    queryKey: ['search', normalizedQuery],
  })

  const results = useMemo(
    () => searchQuery.data?.results.filter(hasOpenableDataflow) ?? [],
    [searchQuery.data?.results],
  )

  const runSearch = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setSubmittedQuery(draftQuery)
  }

  return (
    <div className="mx-auto flex max-w-7xl flex-col gap-5 px-6 py-6">
      <section>
        <h1 className="text-2xl font-semibold tracking-normal">Search</h1>
        <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
          Find an indicator, then open it in Explore with the matching dataset selected.
        </p>
      </section>

      <Card>
        <CardHeader>
          <CardTitle>Find indicators</CardTitle>
          <CardDescription>
            Search datasets and measures by name, source, or keyword.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form className="flex flex-col gap-3 sm:flex-row" onSubmit={runSearch}>
            <label className="sr-only" htmlFor="catalog-search">
              Search catalog
            </label>
            <input
              className="h-10 flex-1 rounded-md border border-border bg-card px-3 text-sm text-foreground shadow-panel outline-none transition-colors focus:border-primary focus:ring-2 focus:ring-ring/20"
              id="catalog-search"
              onChange={(event) => setDraftQuery(event.target.value)}
              placeholder="Search CPI, ASX, AEMO..."
              type="search"
              value={draftQuery}
            />
            <Button className="sm:w-auto" type="submit">
              <Search aria-hidden="true" className="size-4" />
              Search
            </Button>
          </form>
        </CardContent>
      </Card>

      {searchQuery.isLoading ? <LoadingBanner /> : null}
      {searchQuery.error instanceof Error ? (
        <ErrorBanner message={searchQuery.error.message} />
      ) : null}

      <section
        aria-label="Search results"
        className="grid grid-cols-1 gap-4"
        data-testid="search-results"
      >
        {results.map((result) => (
          <SearchResultCard
            key={`${result.kind}:${result.id}`}
            onSelectDataflow={onSelectDataflow}
            result={result}
          />
        ))}
      </section>

      {!searchQuery.isLoading && results.length === 0 ? (
        <Card>
          <CardContent className="flex items-start gap-3 p-5">
            <Search aria-hidden="true" className="mt-0.5 size-4 text-muted-foreground" />
            <div>
              <p className="text-sm font-medium">No matching datasets</p>
              <p className="mt-1 text-sm text-muted-foreground">
                Try CPI, market, AEMO, budget, or APRA.
              </p>
            </div>
          </CardContent>
        </Card>
      ) : null}
    </div>
  )
}

function SearchResultCard({
  onSelectDataflow,
  result,
}: {
  onSelectDataflow: (dataflowId: string) => void
  result: SearchResult
}) {
  const targetDataflow = result.dataflow_ids[0] ?? result.id

  return (
    <Card data-testid={`search-result-${result.kind}-${result.id}`}>
      <CardContent className="flex flex-col gap-4 p-5 sm:flex-row sm:items-start sm:justify-between">
        <div className="flex min-w-0 gap-3">
          <span className="mt-1 flex size-9 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground">
            <Database aria-hidden="true" className="size-4" />
          </span>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="text-base font-semibold tracking-normal">{result.name}</h2>
              <span className="rounded-md border border-border px-2 py-0.5 text-xs text-muted-foreground">
                {result.kind}
              </span>
            </div>
            {result.description === undefined || result.description === null ? null : (
              <p className="mt-1 max-w-3xl text-sm text-muted-foreground">{result.description}</p>
            )}
            <dl className="mt-3 flex flex-wrap gap-x-5 gap-y-1 text-xs text-muted-foreground">
              <div>
                <dt className="sr-only">Source</dt>
                <dd>{result.source_id ?? 'catalog'}</dd>
              </div>
              <div>
                <dt className="sr-only">Dataset id</dt>
                <dd className="font-mono">{targetDataflow}</dd>
              </div>
            </dl>
          </div>
        </div>
        <Button onClick={() => onSelectDataflow(targetDataflow)} type="button" variant="outline">
          Open in Explore
        </Button>
      </CardContent>
    </Card>
  )
}

function hasOpenableDataflow(result: SearchResult): boolean {
  return result.dataflow_ids.length > 0
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div
      className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-900"
      role="alert"
    >
      Search could not load API data: {message}
    </div>
  )
}

function LoadingBanner() {
  return (
    <div className="flex items-center gap-2 rounded-md border border-border bg-card p-3 text-sm text-muted-foreground">
      <RefreshCw aria-hidden="true" className="size-4 animate-spin" />
      Searching catalog
    </div>
  )
}
