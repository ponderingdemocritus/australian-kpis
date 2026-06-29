'use client'

import {
  type ChartConfig,
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from '@/components/ui/chart'
import type { ApsRadarPoint, ApsScatterPoint } from '@/features/aps-derive'
import type { ApsCoverageSlice } from '@/features/aps-derive'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  ReferenceLine,
  Scatter,
  ScatterChart,
  XAxis,
  YAxis,
  ZAxis,
} from 'recharts'

const CHART_HEIGHT = 260

function ChartEmpty({ message }: { message: string }) {
  return (
    <div
      className="flex min-h-64 w-full items-center justify-center text-muted-foreground text-sm"
      style={{ height: CHART_HEIGHT }}
    >
      {message}
    </div>
  )
}

/* --------------------------------------------------------------- scatter -- */

const scatterConfig = {
  current: { color: 'var(--chart-1)', label: 'Position' },
  trail: { color: 'var(--chart-3)', label: 'History' },
} satisfies ChartConfig

export function ApsScatterChart({
  current,
  data,
}: {
  current: ApsScatterPoint | null
  data: ApsScatterPoint[]
}) {
  if (!current) {
    return <ChartEmpty message="Throughput and orientation sub-indexes are unavailable." />
  }

  return (
    <ChartContainer
      aria-label="Throughput versus orientation position on a 0 to 100 plane"
      className="aspect-auto min-h-64 w-full"
      config={scatterConfig}
      role="img"
      style={{ height: CHART_HEIGHT }}
    >
      <ScatterChart accessibilityLayer margin={{ bottom: 12, left: 0, right: 12, top: 8 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis
          axisLine={false}
          dataKey="x"
          domain={[0, 100]}
          name="Throughput"
          tickLine={false}
          tickMargin={8}
          type="number"
        />
        <YAxis
          axisLine={false}
          dataKey="y"
          domain={[0, 100]}
          name="Orientation"
          tickLine={false}
          tickMargin={8}
          width={36}
        />
        <ZAxis range={[60, 60]} type="number" />
        <ReferenceLine stroke="var(--border)" x={50} />
        <ReferenceLine stroke="var(--border)" y={50} />
        <ChartTooltip
          content={<ChartTooltipContent hideLabel />}
          cursor={{ strokeDasharray: '3 3' }}
        />
        <Scatter
          data={data}
          fill="var(--color-trail)"
          fillOpacity={0.4}
          isAnimationActive={false}
          line={{ stroke: 'var(--color-trail)', strokeOpacity: 0.4, strokeWidth: 1 }}
          name="History"
        />
        <Scatter
          data={[current]}
          fill="var(--color-current)"
          isAnimationActive={false}
          name="Position"
        />
      </ScatterChart>
    </ChartContainer>
  )
}

/* ----------------------------------------------------------------- radar -- */

const radarConfig = {
  score: { color: 'var(--chart-2)', label: 'Score' },
} satisfies ChartConfig

export function ApsRadarChart({ data }: { data: ApsRadarPoint[] }) {
  if (data.length === 0) {
    return <ChartEmpty message="No component scores available yet." />
  }

  return (
    <ChartContainer
      aria-label="Abundance systems component scores on a 0 to 100 radar"
      className="mx-auto aspect-square max-h-72"
      config={radarConfig}
      role="img"
    >
      <RadarChart data={data} margin={{ bottom: 8, left: 8, right: 8, top: 8 }}>
        <ChartTooltip content={<ChartTooltipContent />} cursor={false} />
        <PolarGrid />
        <PolarAngleAxis dataKey="label" tick={{ fontSize: 11 }} />
        <PolarRadiusAxis axisLine={false} domain={[0, 100]} tick={false} />
        <Radar
          dataKey="score"
          fill="var(--color-score)"
          fillOpacity={0.5}
          isAnimationActive={false}
          stroke="var(--color-score)"
        />
      </RadarChart>
    </ChartContainer>
  )
}

/* ------------------------------------------------------------------ bars -- */

export function ApsContributionBars<T extends { label: string }>({
  color,
  data,
  dataKey,
  label,
}: {
  color: string
  data: T[]
  dataKey: keyof T & string
  label: string
}) {
  if (data.length === 0) {
    return <ChartEmpty message="No scored indicators in this view yet." />
  }

  const config: ChartConfig = { [dataKey]: { color, label } }
  const height = Math.max(180, data.length * 44)
  // Detach from the generic so Recharts binds the chart to a plain record and
  // accepts the dynamic key as a string (mirrors the plot-chart.tsx pattern).
  const valueKey: string = dataKey
  const rows: Record<string, unknown>[] = data.map((row) => ({ ...row }))

  return (
    <ChartContainer
      aria-label={label}
      className="aspect-auto w-full"
      config={config}
      role="img"
      style={{ height }}
    >
      <BarChart accessibilityLayer data={rows} layout="vertical" margin={{ left: 8, right: 16 }}>
        <XAxis dataKey={valueKey} hide type="number" />
        <YAxis
          axisLine={false}
          dataKey="label"
          tick={{ fontSize: 11 }}
          tickLine={false}
          type="category"
          width={150}
        />
        <ChartTooltip content={<ChartTooltipContent />} cursor={{ fill: 'var(--muted)' }} />
        <Bar dataKey={valueKey} fill={color} isAnimationActive={false} radius={0} />
      </BarChart>
    </ChartContainer>
  )
}

/* ----------------------------------------------------------------- donut -- */

export function ApsCoverageDonut({
  slices,
  total,
}: {
  slices: ApsCoverageSlice[]
  total: number
}) {
  if (total === 0 || slices.length === 0) {
    return <ChartEmpty message="No indicators to summarise yet." />
  }

  const config = Object.fromEntries(
    slices.map((slice) => [slice.label, { color: slice.colorVar, label: slice.label }]),
  ) satisfies ChartConfig

  return (
    <div className="relative mx-auto w-full max-w-[260px]">
      <ChartContainer
        aria-label={`Coverage status breakdown across ${total} indicators`}
        className="aspect-square w-full"
        config={config}
        role="img"
      >
        <PieChart>
          <ChartTooltip content={<ChartTooltipContent hideLabel nameKey="label" />} />
          <Pie
            data={slices}
            dataKey="count"
            innerRadius={70}
            isAnimationActive={false}
            nameKey="label"
            outerRadius={100}
            paddingAngle={2}
            strokeWidth={2}
          >
            {slices.map((slice) => (
              <Cell fill={slice.colorVar} key={slice.status} />
            ))}
          </Pie>
        </PieChart>
      </ChartContainer>
      <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-stat text-4xl">{total}</span>
        <span className="text-eyebrow">indicators</span>
      </div>
    </div>
  )
}
