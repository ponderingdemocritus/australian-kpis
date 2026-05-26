import * as Plot from '@observablehq/plot'
import { useEffect, useRef, useState } from 'react'

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
  const ref = useRef<HTMLDivElement>(null)
  const [width, setWidth] = useState(640)

  useEffect(() => {
    const element = ref.current
    if (element === null) {
      return
    }

    const updateWidth = () => {
      const nextWidth = Math.max(280, Math.floor(element.clientWidth))
      setWidth((currentWidth) => (currentWidth === nextWidth ? currentWidth : nextWidth))
    }

    updateWidth()

    const observer = new ResizeObserver(updateWidth)
    observer.observe(element)

    return () => {
      observer.disconnect()
    }
  }, [])

  useEffect(() => {
    const element = ref.current
    if (element === null) {
      return
    }

    element.replaceChildren()
    if (data.length === 0) {
      return
    }

    const plot = Plot.plot({
      color: {
        domain: colors === undefined ? undefined : Object.keys(colors),
        legend: false,
        range: colors === undefined ? undefined : Object.values(colors),
      },
      grid: true,
      height,
      marginBottom: 36,
      marginLeft: 48,
      marginRight: 52,
      marginTop: 20,
      marks: [
        Plot.ruleY([0], { stroke: '#d9e1e7' }),
        Plot.lineY(data, {
          stroke: 'region',
          strokeWidth: 2.5,
          x: 'date',
          y: 'value',
        }),
        Plot.dot(data, {
          fill: 'region',
          r: 3.5,
          stroke: '#fff',
          strokeWidth: 1,
          x: 'date',
          y: 'value',
        }),
      ],
      style: {
        background: 'transparent',
        color: '#1b2633',
        fontFamily:
          'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
        fontSize: '12px',
      },
      width,
      x: {
        label: null,
        tickFormat: '%b %Y',
      },
      y: {
        label: 'Index',
        nice: true,
      },
    })

    plot.setAttribute('aria-hidden', 'true')
    element.append(plot)

    return () => {
      plot.remove()
    }
  }, [colors, data, height, width])

  return <div aria-label={ariaLabel} className="plot-frame" ref={ref} role="img" />
}
