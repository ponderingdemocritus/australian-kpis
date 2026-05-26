import { cn } from '@/lib/utils'
import * as React from 'react'

export const Card = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
  ({ className, ...props }, ref) => (
    <section
      className={cn(
        'rounded-lg border border-border bg-card text-foreground shadow-panel',
        className,
      )}
      ref={ref}
      {...props}
    />
  ),
)
Card.displayName = 'Card'

export const CardHeader = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
  ({ className, ...props }, ref) => (
    <div className={cn('flex flex-col gap-1.5 p-5 pb-3', className)} ref={ref} {...props} />
  ),
)
CardHeader.displayName = 'CardHeader'

export const CardTitle = React.forwardRef<
  HTMLHeadingElement,
  React.HTMLAttributes<HTMLHeadingElement>
>(({ className, ...props }, ref) => (
  <h2 className={cn('text-base font-semibold tracking-normal', className)} ref={ref} {...props} />
))
CardTitle.displayName = 'CardTitle'

export const CardDescription = React.forwardRef<
  HTMLParagraphElement,
  React.HTMLAttributes<HTMLParagraphElement>
>(({ className, ...props }, ref) => (
  <p className={cn('text-sm text-muted-foreground', className)} ref={ref} {...props} />
))
CardDescription.displayName = 'CardDescription'

export const CardContent = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
  ({ className, ...props }, ref) => (
    <div className={cn('p-5 pt-2', className)} ref={ref} {...props} />
  ),
)
CardContent.displayName = 'CardContent'
