export const valueFormatter = new Intl.NumberFormat('en-AU', {
  maximumFractionDigits: 1,
  minimumFractionDigits: 1,
})

const dateFormatter = new Intl.DateTimeFormat('en-AU', {
  month: 'short',
  year: 'numeric',
})

export function formatDate(value: string): string {
  return dateFormatter.format(new Date(value))
}

export function formatValue(value: number | null | undefined): string {
  return value === null || value === undefined ? 'n/a' : valueFormatter.format(value)
}
