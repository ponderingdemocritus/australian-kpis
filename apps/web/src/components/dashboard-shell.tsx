'use client'

import { Button } from '@/components/ui/button'
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
} from '@/components/ui/sidebar'
import { apiBaseUrl } from '@/lib/api'
import { cn } from '@/lib/utils'
import {
  BarChart3,
  Database,
  GitCompareArrows,
  LayoutDashboard,
  SquareTerminal,
} from 'lucide-react'
import Link from 'next/link'
import { usePathname } from 'next/navigation'
import type React from 'react'

const navItems = [
  { href: '/', icon: LayoutDashboard, label: 'APS' },
  { href: '/explorer', icon: BarChart3, label: 'Explorer' },
  { href: '/compare', icon: GitCompareArrows, label: 'Compare' },
  { href: '/playground', icon: SquareTerminal, label: 'Playground' },
] as const

export function DashboardShell({ children }: { children: React.ReactNode }) {
  return (
    <SidebarProvider>
      <Sidebar
        aria-label="Dashboard navigation"
        collapsible="offcanvas"
        role="complementary"
        variant="sidebar"
      >
        <SidebarHeader className="p-4">
          <Link
            className="flex items-center gap-3 rounded-md outline-none focus-visible:ring-2 focus-visible:ring-sidebar-ring"
            href="/"
          >
            <span className="flex size-9 items-center justify-center rounded-md bg-sidebar-primary text-sidebar-primary-foreground">
              <Database aria-hidden="true" />
            </span>
            <span className="min-w-0">
              <span className="block text-sm font-semibold leading-5">Australian KPIs</span>
              <span className="block text-xs text-sidebar-foreground/70">APS and data tools</span>
            </span>
          </Link>
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupLabel>Data tools</SidebarGroupLabel>
            <SidebarGroupContent>
              <nav aria-label="Dashboard sections">
                <SidebarMenu>
                  {navItems.map((item) => (
                    <NavItem href={item.href} icon={item.icon} key={item.href} label={item.label} />
                  ))}
                </SidebarMenu>
              </nav>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
        <SidebarFooter className="p-4">
          <div className="rounded-md border border-sidebar-border bg-background px-3 py-2 text-xs text-sidebar-foreground/75">
            API base: <span className="break-all font-mono">{apiBaseUrl}</span>
          </div>
        </SidebarFooter>
      </Sidebar>
      <SidebarInset>
        <header className="sticky top-0 z-20 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80">
          <div className="flex min-h-14 items-center justify-between gap-3 px-4 md:px-6">
            <div className="flex min-w-0 items-center gap-3">
              <SidebarTrigger aria-label="Open navigation" className="md:hidden" />
              <div className="min-w-0">
                <p className="truncate text-sm font-semibold">Australian KPIs</p>
                <p className="truncate text-xs text-muted-foreground">APS scorecard dashboard</p>
              </div>
            </div>
            <Button asChild size="sm" variant="outline">
              <a href="https://github.com/ponderingdemocritus/australian-kpis">GitHub</a>
            </Button>
          </div>
        </header>
        {children}
      </SidebarInset>
    </SidebarProvider>
  )
}

function NavItem({
  href,
  icon: Icon,
  label,
}: {
  href: string
  icon: typeof BarChart3
  label: string
}) {
  const pathname = usePathname() ?? '/'
  const isActive = href === '/' ? pathname === '/' : pathname.startsWith(href)

  return (
    <SidebarMenuItem>
      <SidebarMenuButton asChild isActive={isActive} tooltip={label}>
        <Link aria-current={isActive ? 'page' : undefined} href={href}>
          <Icon aria-hidden="true" />
          <span className={cn('truncate', isActive && 'font-medium')}>{label}</span>
        </Link>
      </SidebarMenuButton>
    </SidebarMenuItem>
  )
}
