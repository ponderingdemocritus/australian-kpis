export const valueFormatter = new Intl.NumberFormat('en-AU', {
  maximumFractionDigits: 1,
  minimumFractionDigits: 1,
})

const dateFormatter = new Intl.DateTimeFormat('en-AU', {
  month: 'short',
  timeZone: 'UTC',
  year: 'numeric',
})

export function formatDate(value: string): string {
  return dateFormatter.format(new Date(value))
}

export function formatValue(value: number | null | undefined): string {
  return value === null || value === undefined ? 'n/a' : valueFormatter.format(value)
}

const wholeNumberFormatter = new Intl.NumberFormat('en-AU', {
  maximumFractionDigits: 0,
})

const twoDecimalFormatter = new Intl.NumberFormat('en-AU', {
  maximumFractionDigits: 2,
  minimumFractionDigits: 2,
})

const deltaFormatter = new Intl.NumberFormat('en-AU', {
  maximumFractionDigits: 1,
  minimumFractionDigits: 1,
  signDisplay: 'always',
})

export function formatObservationValue(
  value: number | null | undefined,
  unit: string | undefined,
): string {
  if (value === null || value === undefined) {
    return 'n/a'
  }

  if (unit === 'thousand persons') {
    return wholeNumberFormatter.format(value)
  }

  if (unit === 'percent') {
    return twoDecimalFormatter.format(value)
  }

  return valueFormatter.format(value)
}

export function formatDelta(
  current: number | null | undefined,
  previous: number | null | undefined,
): string {
  if (current === null || current === undefined || previous === null || previous === undefined) {
    return 'n/a'
  }

  return deltaFormatter.format(current - previous)
}
