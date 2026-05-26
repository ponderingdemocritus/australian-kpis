import { cn } from '@/lib/utils'
import { ChevronDown } from 'lucide-react'
import * as React from 'react'

export type NativeSelectProps = React.SelectHTMLAttributes<HTMLSelectElement>

export const NativeSelect = React.forwardRef<HTMLSelectElement, NativeSelectProps>(
  ({ className, children, ...props }, ref) => (
    <span className="relative block">
      <select
        className={cn(
          'h-10 w-full appearance-none rounded-md border border-border bg-card px-3 pr-9 text-sm text-foreground shadow-panel outline-none transition-colors focus:border-primary focus:ring-2 focus:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-50',
          className,
        )}
        ref={ref}
        {...props}
      >
        {children}
      </select>
      <ChevronDown
        aria-hidden="true"
        className="pointer-events-none absolute right-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
      />
    </span>
  ),
)
NativeSelect.displayName = 'NativeSelect'
