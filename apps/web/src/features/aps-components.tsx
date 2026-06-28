'use client'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
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
  coverageBadgeClass,
  coverageStatusDescription,
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
  trendTone,
  zoneDescription,
  zoneDotClass,
  zoneLabel,
  zoneSolidClass,
} from '@/features/aps-data'
import { cn } from '@/lib/utils'
import type { ScorecardConfig, ScorecardSnapshot } from '@au-kpis/sdk'
import {
  AlertTriangle,
  ArrowDown,
  ArrowRight,
  ArrowUp,
  CalendarDays,
  ChevronDown,
  CircleDot,
  ExternalLink,
  Gauge,
  Info,
  Layers3,
  Scale,
  ShieldCheck,
} from 'lucide-react'
import { useState } from 'react'
import type * as React from 'react'

// Shared column template for both two-up rows so their right rails align.
const SPLIT_GRID = 'lg:grid-cols-[minmax(0,1.3fr)_minmax(340px,0.7fr)]'

const ALL_COVERAGE_STATUSES: ApsContribution['coverage_status'][] = [
  'resolved',
  'visible_unscored',
  'manual_pending',
  'coverage_gap',
  'stale',
  'missing_expected',
]

export function ApsLoadingState() {
  return (
    <div
      aria-live="polite"
      className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-4 py-5 sm:px-6 lg:py-6"
      data-testid="aps-loading"
    >
      <div className="space-y-3">
        <Skeleton className="h-7 w-64" />
        <Skeleton className="h-4 w-full max-w-2xl" />
      </div>
      <div className={cn('grid gap-6', SPLIT_GRID)}>
        <Skeleton className="h-80" />
        <Skeleton className="h-80" />
      </div>
      <div className="grid gap-6 lg:grid-cols-2">
        <Skeleton className="h-56" />
        <Skeleton className="h-56" />
      </div>
      <div className={cn('grid gap-6', SPLIT_GRID)}>
        <Skeleton className="h-72" />
        <Skeleton className="h-72" />
      </div>
    </div>
  )
}

export function ApsErrorState({ message, onRetry }: { message: string; onRetry?: () => void }) {
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
        <CardContent className="space-y-4 text-sm text-muted-foreground">
          <p>
            The dashboard needs the APS config and latest scorecard endpoints before it can render
            the scorecard.
          </p>
          {onRetry ? (
            <Button onClick={onRetry} size="sm" variant="outline">
              Retry
            </Button>
          ) : null}
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
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-4 py-5 sm:px-6 lg:py-6">
      <section className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0">
          <h1 className="font-display text-3xl">Abundance Position Score</h1>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            APS is a 0–100 index of national economic abundance: higher means the nation is
            positioned closer to abundance than scarcity. It blends a throughput axis (T, how much
            is being produced and approved) with an orientation axis (O, how well that activity is
            aimed toward abundance).
          </p>
          <Methodology config={config} />
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="secondary">Config {config.version}</Badge>
          <Badge className="gap-1" variant="outline">
            <CalendarDays aria-hidden="true" className="size-3" />
            Data through {formatApsDate(snapshot.latest_period ?? snapshot.as_of)}
          </Badge>
        </div>
      </section>

      <section className={cn('grid gap-6', SPLIT_GRID)}>
        <ApsScoreCard config={config} resolvedCount={resolvedCount} snapshot={snapshot} />
        <ApsConfidenceCard snapshot={snapshot} />
      </section>

      <section className="grid gap-6 lg:grid-cols-2" data-testid="aps-sub-indexes">
        {snapshot.sub_indexes.map((subIndex) => (
          <ApsSubIndexCard key={subIndex.axis} subIndex={subIndex} />
        ))}
      </section>

      <section className={cn('grid gap-6', SPLIT_GRID)}>
        <ApsSourceDrilldowns contributions={contributions} />
        <ApsConfigPanel indicators={config.indicators} />
      </section>
    </div>
  )
}

function Methodology({ config }: { config: ScorecardConfig }) {
  return (
    <details className="group mt-3 text-sm">
      <summary className="inline-flex cursor-pointer list-none items-center gap-1 text-muted-foreground hover:text-foreground">
        <ChevronDown
          aria-hidden="true"
          className="size-3.5 transition-transform group-open:rotate-180"
        />
        Methodology &amp; sources
      </summary>
      <div className="mt-2 max-w-3xl space-y-2 rounded-lg border bg-muted/30 p-3">
        <p className="text-muted-foreground">{config.description}</p>
        <p className="font-mono text-xs">{config.formula}</p>
        <p className="text-xs text-muted-foreground">
          T → Throughput sub-index · O → Orientation sub-index
        </p>
        <p className="text-xs text-muted-foreground">
          {config.attribution} · Licensed under {config.license}
        </p>
      </div>
    </details>
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
          <p className="text-eyebrow">{config.label}</p>
          <CardTitle className="mt-2 text-stat text-5xl sm:text-6xl">
            {formatApsScore(snapshot.score)}
          </CardTitle>
        </div>
        <CardAction>
          <Badge className={cn('capitalize', zoneSolidClass(snapshot.zone))}>
            {zoneLabel(snapshot.zone)}
          </Badge>
        </CardAction>
      </CardHeader>
      <CardContent className="space-y-5">
        <div
          aria-label={`APS score ${formatApsScore(snapshot.score)} of 100, ${zoneLabel(
            snapshot.zone,
          )} zone`}
          className="relative pt-9"
          data-testid="aps-score-spectrum"
          role="img"
        >
          {/* Value pill — clamped so it never overflows the card at extreme scores. */}
          <div
            aria-hidden="true"
            className="-translate-x-1/2 absolute top-0"
            style={{ left: `clamp(2.5rem, ${offset}%, calc(100% - 2.5rem))` }}
          >
            <span
              className={cn(
                'inline-block whitespace-nowrap rounded-md px-2 py-0.5 font-semibold text-xs',
                zoneSolidClass(snapshot.zone),
              )}
            >
              APS {formatApsScore(snapshot.score)}
            </span>
          </div>
          {/* Continuous directional track (not authoritative zone boundaries). */}
          <div className="h-2.5 w-full rounded-full bg-gradient-to-r from-red-500/25 via-amber-500/25 to-emerald-500/30" />
          {/* Marker dot sits exactly on the score; colored by the server zone. */}
          <div
            aria-hidden="true"
            className="-translate-x-1/2 absolute top-[1.9rem]"
            style={{ left: `${offset}%` }}
          >
            <CircleDot className={cn('size-4 fill-background', zoneDotClass(snapshot.zone))} />
          </div>
          <div className="mt-2 flex justify-between text-muted-foreground text-xs">
            <span>Scarcity</span>
            <span>Abundance</span>
          </div>
        </div>

        <p className="text-muted-foreground text-sm">{zoneDescription(snapshot.zone)}</p>

        <div className="grid grid-cols-1 divide-y overflow-hidden rounded-lg border bg-muted/30 sm:grid-cols-3 sm:divide-x sm:divide-y-0">
          <Metric label="Trend" value={<TrendValue trend={snapshot.trend} />} />
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
  const low = scoreOffset(snapshot.confidence_band.low)
  const high = scoreOffset(snapshot.confidence_band.high)
  const score = scoreOffset(snapshot.score)

  return (
    <Card className="min-w-0" data-testid="aps-confidence-card">
      <CardHeader>
        <CardAction>
          <ShieldCheck aria-hidden="true" className="text-muted-foreground" />
        </CardAction>
        <CardTitle className="font-display">Confidence</CardTitle>
        <CardDescription>Coverage-aware band from the API snapshot.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <Metric
          label="Confidence rating"
          value={<span className="capitalize">{snapshot.confidence}</span>}
        />
        <div className="rounded-lg border bg-muted/30 p-3">
          <div className="flex items-center justify-between gap-3 text-sm">
            <span className="text-muted-foreground">Likely range (0–100)</span>
            <span className="font-mono">
              {formatApsScore(snapshot.confidence_band.low)}–
              {formatApsScore(snapshot.confidence_band.high)}
            </span>
          </div>
          <div
            aria-label={`Confidence band from ${formatApsScore(
              snapshot.confidence_band.low,
            )} to ${formatApsScore(snapshot.confidence_band.high)} on a 0 to 100 scale`}
            className="relative mt-3 h-2 rounded-full bg-muted"
            role="img"
          >
            <div
              className="absolute h-2 rounded-full bg-primary/70"
              style={{ left: `${low}%`, width: `${Math.max(2, high - low)}%` }}
            />
            {/* Point-estimate marker within the band. */}
            <div
              aria-hidden="true"
              className="-translate-x-1/2 -translate-y-1/2 absolute top-1/2 h-3.5 w-0.5 rounded bg-foreground"
              style={{ left: `${score}%` }}
            />
          </div>
          <div className="mt-1 flex justify-between text-[10px] text-muted-foreground">
            <span>0</span>
            <span>50</span>
            <span>100</span>
          </div>
        </div>
        <Metric label="Snapshot date" value={formatApsDate(snapshot.as_of)} />
      </CardContent>
    </Card>
  )
}

function ApsSubIndexCard({ subIndex }: { subIndex: ApsSubIndex }) {
  const bandLow = scoreOffset(subIndex.confidence_band.low * 100)
  const bandHigh = scoreOffset(subIndex.confidence_band.high * 100)
  const axisHelp =
    subIndex.axis === 'throughput'
      ? 'Throughput (T): how much the economy is producing and approving.'
      : 'Orientation (O): how well activity is aimed toward abundance.'

  return (
    <Card className="min-w-0">
      <CardHeader>
        <CardAction>
          <Gauge aria-hidden="true" className="text-muted-foreground" />
        </CardAction>
        <CardTitle className="font-display">{tokenLabel(subIndex.axis)}</CardTitle>
        <CardDescription>{axisHelp}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-end justify-between gap-4">
          <span className="text-stat text-4xl">
            {formatApsScore(subIndex.score * 100)}
            <span className="ml-1 align-baseline font-sans font-normal text-muted-foreground text-sm">
              /100
            </span>
          </span>
          <span className="text-muted-foreground text-xs">
            Weight {formatApsPercent(subIndex.weight * 100)} · coverage{' '}
            {formatApsPercent(subIndex.coverage_pct)}
          </span>
        </div>

        <div>
          <div className="flex items-center justify-between text-muted-foreground text-xs">
            <span>Confidence band</span>
            <span className="font-mono">
              {formatApsScore(subIndex.confidence_band.low * 100)}–
              {formatApsScore(subIndex.confidence_band.high * 100)}
            </span>
          </div>
          <div
            aria-label={`Sub-index confidence band from ${formatApsScore(
              subIndex.confidence_band.low * 100,
            )} to ${formatApsScore(subIndex.confidence_band.high * 100)} on a 0 to 100 scale`}
            className="relative mt-1 h-1.5 rounded-full bg-muted"
            role="img"
          >
            <div
              className="absolute h-1.5 rounded-full bg-primary/60"
              style={{ left: `${bandLow}%`, width: `${Math.max(2, bandHigh - bandLow)}%` }}
            />
          </div>
        </div>

        <div className="space-y-2">
          {subIndex.components.map((component) => (
            <div className="grid gap-1" key={component.component}>
              <div className="flex items-center justify-between gap-3 text-sm">
                <span className="truncate">{tokenLabel(component.component)}</span>
                <span className="font-mono">{formatApsScore(component.score * 100)}</span>
              </div>
              <div
                aria-label={tokenLabel(component.component)}
                aria-valuemax={100}
                aria-valuemin={0}
                aria-valuenow={Math.round(component.score * 100)}
                className="h-2 overflow-hidden rounded-full bg-muted"
                role="meter"
              >
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
        <CardDescription>
          Expand a row for the source link, license, and provenance behind each contribution.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Desktop: table with expandable rows. */}
        <div className="hidden md:block">
          <Table aria-label="APS source drilldowns" className="min-w-[640px]">
            <TableHeader>
              <TableRow>
                <TableHead className="w-10">
                  <span className="sr-only">Expand</span>
                </TableHead>
                <TableHead>Indicator</TableHead>
                <TableHead>Source</TableHead>
                <TableHead>Latest period</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Value</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {contributions.map((contribution) => (
                <ContributionRow contribution={contribution} key={contribution.indicator_id} />
              ))}
            </TableBody>
          </Table>
        </div>

        {/* Mobile: stacked cards (no in-card horizontal scroll). */}
        <div className="space-y-3 md:hidden">
          {contributions.map((contribution) => (
            <ContributionCard contribution={contribution} key={contribution.indicator_id} />
          ))}
        </div>

        <CoverageLegend />
      </CardContent>
    </Card>
  )
}

function ContributionRow({ contribution }: { contribution: ApsContribution }) {
  const [open, setOpen] = useState(false)

  return (
    <>
      <TableRow>
        <TableCell className="align-top">
          <button
            aria-expanded={open}
            aria-label={`${open ? 'Collapse' : 'Expand'} ${contribution.label} provenance`}
            className="flex size-6 items-center justify-center rounded-md text-muted-foreground hover:bg-accent hover:text-accent-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            onClick={() => setOpen((value) => !value)}
            type="button"
          >
            <ChevronDown
              aria-hidden="true"
              className={cn('size-4 transition-transform', open && 'rotate-180')}
            />
          </button>
        </TableCell>
        <TableCell className="whitespace-normal align-top">
          <div className="font-medium">{contribution.label}</div>
          <div className="text-muted-foreground text-xs">{tokenLabel(contribution.component)}</div>
        </TableCell>
        <TableCell className="whitespace-normal align-top">
          <div className="font-medium">{sourceLabel(contribution)}</div>
          <div
            className="truncate font-mono text-muted-foreground text-xs"
            title={contribution.source_dataflow_id}
          >
            {contribution.source_dataflow_id}
          </div>
        </TableCell>
        <TableCell className="align-top">{formatApsDate(contribution.latest_period)}</TableCell>
        <TableCell className="align-top">
          <CoverageBadge status={contribution.coverage_status} />
        </TableCell>
        <TableCell className="text-right align-top font-mono">
          <ContributionValue contribution={contribution} />
        </TableCell>
      </TableRow>
      {open ? (
        <TableRow>
          <TableCell className="bg-muted/30" colSpan={6}>
            <ContributionDetail contribution={contribution} />
          </TableCell>
        </TableRow>
      ) : null}
    </>
  )
}

function ContributionCard({ contribution }: { contribution: ApsContribution }) {
  const [open, setOpen] = useState(false)

  return (
    <div className="rounded-lg border p-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="font-medium">{contribution.label}</div>
          <div className="text-muted-foreground text-xs">
            {sourceLabel(contribution)} · {tokenLabel(contribution.component)}
          </div>
        </div>
        <CoverageBadge status={contribution.coverage_status} />
      </div>
      <dl className="mt-3 grid grid-cols-2 gap-2 text-sm">
        <div className="min-w-0">
          <dt className="text-muted-foreground text-xs">Latest period</dt>
          <dd>{formatApsDate(contribution.latest_period)}</dd>
        </div>
        <div className="min-w-0">
          <dt className="text-muted-foreground text-xs">Value</dt>
          <dd className="font-mono">
            <ContributionValue contribution={contribution} />
          </dd>
        </div>
      </dl>
      <button
        aria-expanded={open}
        className="mt-3 inline-flex items-center gap-1 text-muted-foreground text-xs hover:text-foreground"
        onClick={() => setOpen((value) => !value)}
        type="button"
      >
        <ChevronDown
          aria-hidden="true"
          className={cn('size-3.5 transition-transform', open && 'rotate-180')}
        />
        {open ? 'Hide' : 'Show'} provenance
      </button>
      {open ? (
        <div className="mt-3 border-t pt-3">
          <ContributionDetail contribution={contribution} />
        </div>
      ) : null}
    </div>
  )
}

function ContributionValue({ contribution }: { contribution: ApsContribution }) {
  if (contribution.raw_value === null || contribution.raw_value === undefined) {
    return <>n/a</>
  }
  return (
    <>
      {formatApsRawValue(contribution.raw_value)}{' '}
      <span className="text-muted-foreground">{contribution.unit}</span>
    </>
  )
}

function ContributionDetail({ contribution }: { contribution: ApsContribution }) {
  return (
    <dl className="grid gap-x-6 gap-y-2 text-sm sm:grid-cols-2">
      <DetailItem label="Source">
        <a
          className="inline-flex items-center gap-1 break-all text-primary underline-offset-4 hover:underline"
          href={contribution.source_url}
          rel="noreferrer"
          target="_blank"
        >
          {contribution.source_url}
          <ExternalLink aria-hidden="true" className="size-3 shrink-0" />
        </a>
      </DetailItem>
      <DetailItem label="License">{contribution.license}</DetailItem>
      <DetailItem label="Attribution">{contribution.attribution}</DetailItem>
      <DetailItem label="Unit">{contribution.unit}</DetailItem>
      <DetailItem label="Normalized value">
        {formatApsRawValue(contribution.normalized_value)}
      </DetailItem>
      {contribution.notes ? <DetailItem label="Notes">{contribution.notes}</DetailItem> : null}
      <DetailItem label="Series key">
        <span className="break-all font-mono text-xs">
          {contribution.series_key ?? 'unavailable'}
        </span>
      </DetailItem>
      <DetailItem label="Artifact id">
        <span className="break-all font-mono text-xs">
          {contribution.source_artifact_id ?? 'unavailable'}
        </span>
      </DetailItem>
    </dl>
  )
}

function DetailItem({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-muted-foreground text-xs">{label}</dt>
      <dd className="mt-0.5 break-words">{children}</dd>
    </div>
  )
}

function CoverageLegend() {
  return (
    <details className="group text-sm">
      <summary className="inline-flex cursor-pointer list-none items-center gap-1 text-muted-foreground hover:text-foreground">
        <ChevronDown
          aria-hidden="true"
          className="size-3.5 transition-transform group-open:rotate-180"
        />
        What do these statuses mean?
      </summary>
      <ul className="mt-2 space-y-2">
        {ALL_COVERAGE_STATUSES.map((status) => (
          <li className="flex items-start gap-2" key={status}>
            <CoverageBadge status={status} />
            <span className="text-muted-foreground text-xs">
              {coverageStatusDescription(status).replace(/^[^—]+— /, '')}
            </span>
          </li>
        ))}
      </ul>
    </details>
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
      <CardContent className="divide-y">
        {indicators.map((indicator) => (
          <div
            className="flex items-start justify-between gap-3 py-3 first:pt-0 last:pb-0"
            key={indicator.indicator_id}
          >
            <div className="min-w-0">
              <p className="font-medium">{indicator.display_label}</p>
              <p className="text-muted-foreground text-xs">{indicator.source_dataflow_id}</p>
              <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-muted-foreground text-xs">
                <span>{directionLabel(indicator.direction)}</span>
                <span aria-hidden="true">·</span>
                <span>{indicator.confidence} confidence</span>
                <span aria-hidden="true">·</span>
                <span>{coverageStatusLabel(indicator.coverage_status)}</span>
              </div>
            </div>
            <Badge variant="outline">{formatApsPercent(indicator.weight * 100)}</Badge>
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
    <Badge className={cn(coverageBadgeClass(status))} variant={variant}>
      {coverageStatusLabel(status)}
    </Badge>
  )
}

function Metric({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="p-3">
      <p className="text-eyebrow">{label}</p>
      <div className="mt-1 break-words font-semibold tabular-nums">{value}</div>
    </div>
  )
}

function TrendValue({ trend }: { trend: ScorecardSnapshot['trend'] }) {
  const tone = trendTone(trend)
  const color =
    tone === 'positive'
      ? 'text-emerald-700 dark:text-emerald-400'
      : tone === 'negative'
        ? 'text-red-700 dark:text-red-400'
        : 'text-muted-foreground'

  return (
    <span className={cn('flex items-center gap-1.5 capitalize', color)}>
      <TrendIcon trend={trend} />
      {trendLabel(trend)}
    </span>
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
