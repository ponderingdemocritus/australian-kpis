'use client'

import { Badge } from '@/components/ui/badge'
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  type ApsContribution,
  type ApsIndicatorConfig,
  type ApsSubIndex,
  coverageStatusLabel,
  directionLabel,
  formatApsDate,
  formatApsPercent,
  formatApsRawValue,
  formatApsScore,
  scoreOffset,
  sortedContributions,
  sourceLabel,
  tokenLabel,
  trendLabel,
  zoneLabel,
} from '@/features/aps-data'
import { cn } from '@/lib/utils'
import type { ScorecardConfig, ScorecardSnapshot } from '@au-kpis/sdk'
import {
  AlertTriangle,
  ArrowDown,
  ArrowRight,
  ArrowUp,
  CircleDot,
  Gauge,
  Info,
  Layers3,
  Scale,
  ShieldCheck,
} from 'lucide-react'

export function ApsLoadingState() {
  return (
    <div
      aria-live="polite"
      className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-5 sm:px-6 lg:py-6"
      data-testid="aps-loading"
    >
      <div className="space-y-3">
        <Skeleton className="h-7 w-64" />
        <Skeleton className="h-4 w-full max-w-2xl" />
      </div>
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.65fr)]">
        <Skeleton className="h-80" />
        <Skeleton className="h-80" />
      </div>
      <Skeleton className="h-72" />
    </div>
  )
}

export function ApsErrorState({ message }: { message: string }) {
  return (
    <div className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6" data-testid="aps-error">
      <Card className="border-destructive/40">
        <CardHeader>
          <CardAction>
            <AlertTriangle aria-hidden="true" className="text-destructive" />
          </CardAction>
          <CardTitle>APS scorecard unavailable</CardTitle>
          <CardDescription>{message}</CardDescription>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          The dashboard needs the APS config and latest scorecard endpoints before it can render the
          scorecard.
        </CardContent>
      </Card>
    </div>
  )
}

export function ApsEmptyState() {
  return (
    <div className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6" data-testid="aps-empty">
      <Card>
        <CardHeader>
          <CardAction>
            <Info aria-hidden="true" className="text-muted-foreground" />
          </CardAction>
          <CardTitle>APS scorecard has no configured indicators</CardTitle>
          <CardDescription>
            Config and latest responses loaded, but there is no indicator register to render.
          </CardDescription>
        </CardHeader>
      </Card>
    </div>
  )
}

export function ApsDashboard({
  config,
  snapshot,
}: {
  config: ScorecardConfig
  snapshot: ScorecardSnapshot
}) {
  const contributions = sortedContributions(snapshot.contributions)
  const resolvedCount = snapshot.contributions.filter(
    (contribution) => contribution.coverage_status === 'resolved',
  ).length

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-5 sm:px-6 lg:py-6">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold tracking-normal">Abundance Position Score</h1>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            APS is the first-screen scorecard view for national throughput, orientation, coverage,
            and source provenance.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="secondary">Config {config.version}</Badge>
          <Badge variant="outline">{formatApsDate(snapshot.latest_period ?? snapshot.as_of)}</Badge>
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.65fr)]">
        <ApsScoreCard config={config} resolvedCount={resolvedCount} snapshot={snapshot} />
        <ApsConfidenceCard snapshot={snapshot} />
      </section>

      <section className="grid gap-4 lg:grid-cols-2" data-testid="aps-sub-indexes">
        {snapshot.sub_indexes.map((subIndex) => (
          <ApsSubIndexCard key={subIndex.axis} subIndex={subIndex} />
        ))}
      </section>

      <section className="grid gap-5 xl:grid-cols-[minmax(0,1.25fr)_minmax(360px,0.75fr)]">
        <ApsSourceDrilldowns contributions={contributions} />
        <ApsConfigPanel indicators={config.indicators} />
      </section>
    </div>
  )
}

function ApsScoreCard({
  config,
  resolvedCount,
  snapshot,
}: {
  config: ScorecardConfig
  resolvedCount: number
  snapshot: ScorecardSnapshot
}) {
  const offset = scoreOffset(snapshot.score)

  return (
    <Card className="min-w-0" data-testid="aps-score-card">
      <CardHeader>
        <div>
          <CardDescription>{config.label}</CardDescription>
          <CardTitle className="mt-2 text-5xl tabular-nums sm:text-6xl">
            {formatApsScore(snapshot.score)}
          </CardTitle>
        </div>
        <CardAction>
          <Badge variant="secondary">{zoneLabel(snapshot.zone)}</Badge>
        </CardAction>
      </CardHeader>
      <CardContent className="space-y-5">
        <div
          aria-label={`APS score ${formatApsScore(snapshot.score)} on a 0 to 100 spectrum`}
          className="relative pt-8"
          data-testid="aps-score-spectrum"
        >
          <div className="grid grid-cols-3 overflow-hidden rounded-md border text-center text-xs font-medium">
            <div className="bg-red-500/10 py-2 text-red-700">Scarcity</div>
            <div className="bg-amber-500/10 py-2 text-amber-700">Mixed</div>
            <div className="bg-emerald-600/10 py-2 text-emerald-700">Abundance</div>
          </div>
          <div
            className="-translate-x-1/2 absolute top-0 flex flex-col items-center"
            style={{ left: `${offset}%` }}
          >
            <CircleDot aria-hidden="true" className="size-5 fill-background text-primary" />
            <span className="mt-1 rounded-md bg-foreground px-2 py-1 text-xs font-medium text-background">
              APS {formatApsScore(snapshot.score)}
            </span>
          </div>
        </div>

        <div className="grid gap-3 sm:grid-cols-3">
          <Metric label="Trend" value={trendLabel(snapshot.trend)} />
          <Metric label="Coverage" value={formatApsPercent(snapshot.coverage_pct)} />
          <Metric
            label="Resolved inputs"
            value={`${resolvedCount}/${snapshot.contributions.length}`}
          />
        </div>
      </CardContent>
    </Card>
  )
}

function ApsConfidenceCard({ snapshot }: { snapshot: ScorecardSnapshot }) {
  return (
    <Card className="min-w-0" data-testid="aps-confidence-card">
      <CardHeader>
        <CardAction>
          <ShieldCheck aria-hidden="true" className="text-muted-foreground" />
        </CardAction>
        <CardTitle>Confidence</CardTitle>
        <CardDescription>Coverage-aware band from the API snapshot.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <Metric label="Confidence rating" value={snapshot.confidence} />
        <div className="rounded-md border bg-background p-3">
          <div className="flex items-center justify-between gap-3 text-sm">
            <span className="text-muted-foreground">Low</span>
            <span className="font-mono">{formatApsScore(snapshot.confidence_band.low)}</span>
          </div>
          <div className="mt-3 h-2 rounded-full bg-muted">
            <div
              className="h-2 rounded-full bg-primary"
              style={{
                marginLeft: `${scoreOffset(snapshot.confidence_band.low)}%`,
                width: `${Math.max(
                  2,
                  scoreOffset(snapshot.confidence_band.high) -
                    scoreOffset(snapshot.confidence_band.low),
                )}%`,
              }}
            />
          </div>
          <div className="mt-3 flex items-center justify-between gap-3 text-sm">
            <span className="text-muted-foreground">High</span>
            <span className="font-mono">{formatApsScore(snapshot.confidence_band.high)}</span>
          </div>
        </div>
        <Metric label="Snapshot date" value={formatApsDate(snapshot.as_of)} />
      </CardContent>
    </Card>
  )
}

function ApsSubIndexCard({ subIndex }: { subIndex: ApsSubIndex }) {
  return (
    <Card className="min-w-0">
      <CardHeader>
        <CardAction>
          <Gauge aria-hidden="true" className="text-muted-foreground" />
        </CardAction>
        <CardTitle>{tokenLabel(subIndex.axis)}</CardTitle>
        <CardDescription>
          Weight {formatApsPercent(subIndex.weight * 100)} · coverage{' '}
          {formatApsPercent(subIndex.coverage_pct)}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-end justify-between gap-4">
          <span className="text-4xl font-semibold tabular-nums">
            {formatApsScore(subIndex.score * 100)}
          </span>
          <span className="text-sm text-muted-foreground">
            {formatApsScore(subIndex.confidence_band.low * 100)}-
            {formatApsScore(subIndex.confidence_band.high * 100)}
          </span>
        </div>
        <div className="space-y-2">
          {subIndex.components.map((component) => (
            <div className="grid gap-1" key={component.component}>
              <div className="flex items-center justify-between gap-3 text-sm">
                <span className="truncate">{tokenLabel(component.component)}</span>
                <span className="font-mono">{formatApsScore(component.score * 100)}</span>
              </div>
              <div className="h-2 overflow-hidden rounded-full bg-muted">
                <div
                  className="h-full rounded-full bg-primary"
                  style={{ width: `${scoreOffset(component.score * 100)}%` }}
                />
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}

function ApsSourceDrilldowns({ contributions }: { contributions: ApsContribution[] }) {
  return (
    <Card className="min-w-0" data-testid="aps-source-drilldowns">
      <CardHeader>
        <CardAction>
          <Layers3 aria-hidden="true" className="text-muted-foreground" />
        </CardAction>
        <CardTitle>Source drilldowns</CardTitle>
        <CardDescription>Contribution rows expose coverage state and provenance.</CardDescription>
      </CardHeader>
      <CardContent>
        <Table aria-label="APS source drilldowns" className="min-w-[900px]">
          <TableHeader>
            <TableRow>
              <TableHead>Indicator</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Latest period</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Provenance</TableHead>
              <TableHead className="text-right">Value</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {contributions.map((contribution) => (
              <TableRow key={contribution.indicator_id}>
                <TableCell className="max-w-[260px] whitespace-normal">
                  <div className="font-medium">{contribution.label}</div>
                  <div className="text-xs text-muted-foreground">
                    {tokenLabel(contribution.component)}
                  </div>
                </TableCell>
                <TableCell>
                  <div className="font-medium">{sourceLabel(contribution)}</div>
                  <div className="font-mono text-xs text-muted-foreground">
                    {contribution.source_dataflow_id}
                  </div>
                </TableCell>
                <TableCell>{formatApsDate(contribution.latest_period)}</TableCell>
                <TableCell>
                  <CoverageBadge status={contribution.coverage_status} />
                </TableCell>
                <TableCell className="max-w-[260px] whitespace-normal">
                  <div className="font-mono text-xs">
                    {contribution.series_key ?? 'series unavailable'}
                  </div>
                  <div className="mt-1 font-mono text-xs text-muted-foreground">
                    {contribution.source_artifact_id ?? 'artifact unavailable'}
                  </div>
                </TableCell>
                <TableCell className="text-right font-mono">
                  {formatApsRawValue(contribution.raw_value)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}

function ApsConfigPanel({ indicators }: { indicators: ApsIndicatorConfig[] }) {
  return (
    <Card className="min-w-0" data-testid="aps-config-panel">
      <CardHeader>
        <CardAction>
          <Scale aria-hidden="true" className="text-muted-foreground" />
        </CardAction>
        <CardTitle>Indicator register</CardTitle>
        <CardDescription>Weights and scoring direction come from APS config.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {indicators.map((indicator) => (
          <div className="rounded-md border bg-background p-3" key={indicator.indicator_id}>
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <p className="font-medium">{indicator.display_label}</p>
                <p className="text-xs text-muted-foreground">{indicator.source_dataflow_id}</p>
              </div>
              <Badge variant="outline">{formatApsPercent(indicator.weight * 100)}</Badge>
            </div>
            <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
              <span>{directionLabel(indicator.direction)}</span>
              <span aria-hidden="true">·</span>
              <span>{indicator.confidence} confidence</span>
              <span aria-hidden="true">·</span>
              <span>{coverageStatusLabel(indicator.coverage_status)}</span>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  )
}

function CoverageBadge({ status }: { status: ApsContribution['coverage_status'] }) {
  const variant =
    status === 'resolved' ? 'secondary' : status === 'visible_unscored' ? 'outline' : 'default'

  return (
    <Badge
      className={cn(
        status === 'coverage_gap' && 'bg-amber-600 text-white',
        status === 'manual_pending' && 'bg-sky-700 text-white',
        status === 'missing_expected' && 'bg-slate-700 text-white',
        status === 'stale' && 'bg-orange-700 text-white',
      )}
      variant={variant}
    >
      {coverageStatusLabel(status)}
    </Badge>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border bg-background p-3">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="mt-1 break-words font-medium capitalize">{value}</p>
    </div>
  )
}

export function TrendIcon({ trend }: { trend: ScorecardSnapshot['trend'] }) {
  const className = 'size-4'
  if (trend === 'up') {
    return <ArrowUp aria-hidden="true" className={className} />
  }
  if (trend === 'down') {
    return <ArrowDown aria-hidden="true" className={className} />
  }
  return <ArrowRight aria-hidden="true" className={className} />
}
