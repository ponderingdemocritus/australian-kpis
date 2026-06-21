'use client'

import { ChartContainer, ChartTooltip, ChartTooltipContent } from '@/components/ui/chart'
import { CartesianGrid, Line, LineChart, XAxis, YAxis } from 'recharts'

export type ChartPoint = {
  date: Date
  label: string
  region: string
  value: number
}

type PlotChartProps = {
  ariaLabel: string
  colors?: Record<string, string>
  data: ChartPoint[]
  height?: number
}

export function PlotChart({ ariaLabel, colors, data, height = 260 }: PlotChartProps) {
  const regions = Array.from(new Set(data.map((point) => point.region)))
  const chartData = toSeriesRows(data)
  const config = Object.fromEntries(
    regions.map((region, index) => [
      region,
      {
        label: region,
      },
    ]),
  )

  return (
    <ChartContainer
      aria-label={ariaLabel}
      className="aspect-auto min-h-64 w-full"
      config={config}
      role="img"
      style={{ height }}
    >
      <LineChart
        accessibilityLayer
        data={chartData}
        margin={{ bottom: 12, left: 0, right: 12, top: 8 }}
      >
        <CartesianGrid strokeDasharray="3 3" vertical={false} />
        <XAxis axisLine={false} dataKey="label" minTickGap={24} tickLine={false} tickMargin={10} />
        <YAxis
          axisLine={false}
          domain={['dataMin - 1', 'dataMax + 1']}
          tickLine={false}
          tickMargin={10}
          width={42}
        />
        <ChartTooltip content={<ChartTooltipContent indicator="line" />} />
        {regions.map((region, index) => (
          <Line
            dataKey={region}
            dot={{ r: 2.5 }}
            isAnimationActive={false}
            key={region}
            stroke={colors?.[region] ?? `var(--chart-${(index % 5) + 1})`}
            strokeWidth={2.25}
            type="monotone"
          />
        ))}
      </LineChart>
    </ChartContainer>
  )
}

function toSeriesRows(data: ChartPoint[]): Array<Record<string, number | string | undefined>> {
  const rows = new Map<string, Record<string, number | string | undefined>>()

  for (const point of data) {
    const key = point.date.toISOString()
    const row = rows.get(key) ?? { label: point.label }
    row[point.region] = point.value
    rows.set(key, row)
  }

  return Array.from(rows.entries())
    .sort(([left], [right]) => Date.parse(left) - Date.parse(right))
    .map(([, row]) => row)
}
