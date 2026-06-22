import { DashboardShell } from '@/components/dashboard-shell'
import type { Metadata } from 'next'
import type React from 'react'
import './globals.css'
import { Providers } from './providers'

export const metadata: Metadata = {
  description: 'Reference dashboard for Australian economic indicators.',
  title: 'Australian KPIs',
}

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html data-scroll-behavior="smooth" lang="en" suppressHydrationWarning>
      <body className="font-sans antialiased">
        <Providers>
          <DashboardShell>{children}</DashboardShell>
        </Providers>
      </body>
    </html>
  )
}
