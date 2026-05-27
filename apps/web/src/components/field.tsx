import type { ReactNode } from 'react'

type FieldProps = {
  children: ReactNode
  htmlFor: string
  label: string
}

export function Field({ children, htmlFor, label }: FieldProps) {
  return (
    <div className="flex flex-col gap-2">
      <label className="text-sm font-medium" htmlFor={htmlFor}>
        {label}
      </label>
      {children}
    </div>
  )
}
