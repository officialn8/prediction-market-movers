'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { 
  Activity, Bell, TrendingUp, TrendingDown, Search, Plus, Settings, 
  LogOut, ChevronRight, Zap, Eye, Flame, Clock, Filter
} from 'lucide-react'
import Link from 'next/link'
import { SystemStatus } from '@/components/ui/SystemStatus'

interface User {
  id: string
  email: string
  name?: string
  tier: string
}

interface Mover {
  market_id: string
  token_id: string
  title: string
  outcome: string
  source?: string
  current_price: number
  price_change: number
  volume_24h: number | null
  composite_score: number
  window: string
}

export default function DashboardPage() {
  const router = useRouter()
  const [user, setUser] = useState<User | null>(null)
  const [movers, setMovers] = useState<Mover[]>([])
  const [window, setWindow] = useState<'1h' | '4h' | '24h'>('1h')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const token = localStorage.getItem('token')
    const userData = localStorage.getItem('user')
    
    if (!token || !userData) {
      router.push('/login')
      return
    }

    setUser(JSON.parse(userData))
    fetchMovers(window)
  }, [])

  useEffect(() => {
    if (user) {
      fetchMovers(window)
    }
  }, [window])

  const fetchMovers = async (w: string) => {
    setLoading(true)
    try {
      const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/markets/movers?window=${w}&limit=20`)
      const data = await res.json()
      setMovers(data)
    } catch (err) {
      console.error('Failed to fetch movers:', err)
    } finally {
      setLoading(false)
    }
  }

  const handleLogout = () => {
    localStorage.removeItem('token')
    localStorage.removeItem('user')
    router.push('/')
  }

  if (!user) return null

  return (
    <div className="min-h-screen bg-[#050505]">
      {/* Sidebar */}
      <aside className="fixed left-0 top-0 bottom-0 w-72 sidebar border-r border-white/5 flex flex-col">
        {/* Logo */}
        <div className="p-6 border-b border-white/5">
          <Link href="/" className="flex items-center gap-3">
            <div className="relative">
              <Activity className="w-8 h-8 text-primary-400" />
              <div className="absolute -top-0.5 -right-0.5 w-2.5 h-2.5 bg-emerald-500 rounded-full animate-pulse" />
            </div>
            <span className="font-bold text-xl tracking-tight">PMM</span>
          </Link>
        </div>

        {/* Nav */}
        <nav className="flex-1 p-4 space-y-1">
          <NavItem href="/dashboard" icon={<TrendingUp className="w-5 h-5" />} label="Dashboard" active />
          <NavItem href="/dashboard/alerts" icon={<Bell className="w-5 h-5" />} label="My Alerts" badge="3" />
          <NavItem href="/dashboard/watchlist" icon={<Eye className="w-5 h-5" />} label="Watchlist" />
          <NavItem href="/dashboard/markets" icon={<Activity className="w-5 h-5" />} label="All Markets" />
          
          <div className="pt-4 mt-4 border-t border-white/5">
            <NavItem href="/dashboard/settings" icon={<Settings className="w-5 h-5" />} label="Settings" />
          </div>
        </nav>

        {/* Plan & User */}
        <div className="p-4 border-t border-white/5">
          {/* Plan Card */}
          <div className="glass rounded-xl p-4 mb-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm text-gray-400">Current plan</span>
              <span className={`
                text-xs font-bold uppercase px-2 py-0.5 rounded
                ${user.tier === 'pro' ? 'bg-primary-500/20 text-primary-300' : 
                  user.tier === 'enterprise' ? 'bg-purple-500/20 text-purple-300' : 
                  'bg-gray-700 text-gray-400'}
              `}>
                {user.tier}
              </span>
            </div>
            {user.tier === 'free' && (
              <>
                <div className="text-sm text-gray-300 mb-3">
                  <span className="text-amber-400 font-medium">2/3</span> alerts used
                </div>
                <Link 
                  href="/pricing" 
                  className="flex items-center justify-center gap-2 w-full bg-gradient-to-r from-primary-600 to-primary-500 hover:from-primary-500 hover:to-primary-400 text-white text-sm font-medium py-2 rounded-lg transition-all"
                >
                  <Zap className="w-4 h-4" />
                  Upgrade to Pro
                </Link>
              </>
            )}
          </div>

          {/* User */}
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-full bg-gradient-to-br from-primary-500 to-primary-700 flex items-center justify-center font-medium">
              {(user.name || user.email)[0].toUpperCase()}
            </div>
            <div className="flex-1 min-w-0">
              <div className="font-medium text-white truncate">{user.name || user.email}</div>
              <div className="text-xs text-gray-500 truncate">{user.email}</div>
            </div>
            <button
              onClick={handleLogout}
              className="p-2 rounded-lg hover:bg-white/5 text-gray-500 hover:text-white transition"
              title="Sign out"
            >
              <LogOut className="w-4 h-4" />
            </button>
          </div>
        </div>
      </aside>

      {/* Main content */}
      <main className="ml-72 min-h-screen">
        {/* Top bar */}
        <header className="sticky top-0 z-40 glass-strong border-b border-white/5">
          <div className="flex items-center justify-between px-8 py-4">
            <div>
              <h1 className="text-2xl font-bold text-white">Dashboard</h1>
              <p className="text-sm text-gray-500">
                Welcome back, {user.name || user.email.split('@')[0]}
              </p>
            </div>
            
            <div className="flex items-center gap-4">
              {/* System Status */}
              <SystemStatus />

              {/* Search */}
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
                <input
                  type="text"
                  placeholder="Search markets..."
                  className="w-72 bg-white/5 border border-white/10 rounded-xl pl-10 pr-4 py-2.5 text-sm focus:outline-none focus:border-primary-500/50 focus:ring-1 focus:ring-primary-500/25 transition-all placeholder:text-gray-600"
                />
              </div>
              
              {/* New Alert */}
              <Link
                href="/dashboard/alerts/new"
                className="flex items-center gap-2 bg-primary-600 hover:bg-primary-500 text-white font-medium px-4 py-2.5 rounded-xl transition-all hover:shadow-lg hover:shadow-primary-500/25"
              >
                <Plus className="w-4 h-4" />
                New Alert
              </Link>
            </div>
          </div>
        </header>

        <div className="p-8">
          {/* Quick Stats */}
          <div className="grid grid-cols-4 gap-4 mb-8">
            <StatCard 
              label="Active Alerts" 
              value="3" 
              subtext="2 triggered today"
              icon={<Bell className="w-5 h-5" />}
              color="primary"
            />
            <StatCard 
              label="Watchlist" 
              value="12" 
              subtext="3 moving now"
              icon={<Eye className="w-5 h-5" />}
              color="purple"
            />
            <StatCard 
              label="Top Mover" 
              value="+12.4pp" 
              subtext="TRUMP 2028"
              icon={<TrendingUp className="w-5 h-5" />}
              color="emerald"
            />
            <StatCard 
              label="Markets Tracked" 
              value="847" 
              subtext="Polymarket + Kalshi"
              icon={<Activity className="w-5 h-5" />}
              color="amber"
            />
          </div>

          {/* Top Movers */}
          <div className="glass rounded-2xl border border-white/5 overflow-hidden">
            {/* Header */}
            <div className="flex items-center justify-between p-6 border-b border-white/5">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-xl bg-gradient-to-br from-primary-500/20 to-primary-600/20">
                  <Flame className="w-5 h-5 text-primary-400" />
                </div>
                <div>
                  <h2 className="text-lg font-semibold text-white">Top Movers</h2>
                  <p className="text-sm text-gray-500">Biggest price movements right now</p>
                </div>
              </div>
              
              <div className="flex items-center gap-3">
                {/* Window toggle */}
                <div className="flex bg-white/5 rounded-xl p-1">
                  {(['1h', '4h', '24h'] as const).map((w) => (
                    <button
                      key={w}
                      onClick={() => setWindow(w)}
                      className={`
                        px-4 py-1.5 rounded-lg text-sm font-medium transition-all
                        ${window === w 
                          ? 'bg-primary-600 text-white shadow-lg shadow-primary-500/25' 
                          : 'text-gray-400 hover:text-white'
                        }
                      `}
                    >
                      {w}
                    </button>
                  ))}
                </div>

                {/* Filter */}
                <button className="p-2 rounded-xl bg-white/5 hover:bg-white/10 text-gray-400 hover:text-white transition">
                  <Filter className="w-5 h-5" />
                </button>
              </div>
            </div>

            {/* Content */}
            <div className="divide-y divide-white/5">
              {loading ? (
                <div className="p-12 text-center">
                  <div className="inline-flex items-center gap-3 text-gray-400">
                    <div className="w-5 h-5 border-2 border-primary-500 border-t-transparent rounded-full animate-spin" />
                    Loading movers...
                  </div>
                </div>
              ) : movers.length === 0 ? (
                <div className="p-12 text-center text-gray-500">
                  No movers found for this window
                </div>
              ) : (
                movers.map((mover, idx) => (
                  <MoverRow key={mover.token_id} mover={mover} rank={idx + 1} />
                ))
              )}
            </div>

            {/* Footer */}
            {movers.length > 0 && (
              <div className="p-4 border-t border-white/5 bg-white/[0.02]">
                <Link 
                  href="/dashboard/markets"
                  className="flex items-center justify-center gap-2 text-sm text-gray-400 hover:text-primary-400 transition"
                >
                  View all markets <ChevronRight className="w-4 h-4" />
                </Link>
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  )
}

function NavItem({ 
  href, 
  icon, 
  label, 
  active = false,
  badge,
}: { 
  href: string
  icon: React.ReactNode
  label: string
  active?: boolean
  badge?: string
}) {
  return (
    <Link
      href={href}
      className={`
        flex items-center gap-3 px-4 py-3 rounded-xl transition-all
        ${active 
          ? 'bg-primary-600/10 text-primary-400 border border-primary-500/20' 
          : 'text-gray-400 hover:text-white hover:bg-white/5'
        }
      `}
    >
      {icon}
      <span className="flex-1 font-medium">{label}</span>
      {badge && (
        <span className="px-2 py-0.5 bg-primary-500/20 text-primary-300 text-xs font-bold rounded-full">
          {badge}
        </span>
      )}
    </Link>
  )
}

function StatCard({ 
  label, 
  value, 
  subtext, 
  icon,
  color = 'primary'
}: { 
  label: string
  value: string
  subtext: string
  icon: React.ReactNode
  color?: 'primary' | 'emerald' | 'purple' | 'amber'
}) {
  const colorClasses = {
    primary: 'from-primary-500/20 to-primary-600/20 text-primary-400',
    emerald: 'from-emerald-500/20 to-emerald-600/20 text-emerald-400',
    purple: 'from-purple-500/20 to-purple-600/20 text-purple-400',
    amber: 'from-amber-500/20 to-amber-600/20 text-amber-400',
  }

  return (
    <div className="glass rounded-2xl p-5 border border-white/5 hover:border-white/10 transition-colors group">
      <div className="flex items-start justify-between mb-3">
        <div className={`p-2.5 rounded-xl bg-gradient-to-br ${colorClasses[color]}`}>
          {icon}
        </div>
      </div>
      <div className="text-3xl font-bold text-white mb-1 font-mono">{value}</div>
      <div className="text-sm text-gray-500">{label}</div>
      <div className="text-xs text-gray-600 mt-1">{subtext}</div>
    </div>
  )
}

function MoverRow({ mover, rank }: { mover: Mover; rank: number }) {
  const isUp = mover.price_change > 0
  const isExtreme = Math.abs(mover.price_change) > 10
  const isTop3 = rank <= 3
  
  return (
    <div className={`
      flex items-center gap-4 p-5 hover:bg-white/[0.02] transition-colors cursor-pointer group
      ${isExtreme ? (isUp ? 'bg-emerald-500/[0.03]' : 'bg-red-500/[0.03]') : ''}
    `}>
      {/* Rank */}
      <div className={`
        w-8 h-8 rounded-lg flex items-center justify-center text-sm font-bold
        ${isTop3 
          ? 'bg-gradient-to-br from-amber-500/20 to-orange-500/20 text-amber-400' 
          : 'bg-white/5 text-gray-500'
        }
      `}>
        {rank}
      </div>

      {/* Source */}
      <div className={`
        px-2 py-1 rounded text-xs font-bold uppercase tracking-wider
        ${mover.source === 'kalshi' 
          ? 'bg-blue-500/20 text-blue-300' 
          : 'bg-purple-500/20 text-purple-300'
        }
      `}>
        {mover.source || 'PM'}
      </div>

      {/* Info */}
      <div className="flex-1 min-w-0">
        <div className="font-medium text-white truncate group-hover:text-primary-300 transition-colors">
          {mover.title}
        </div>
        <div className="flex items-center gap-3 text-sm text-gray-500 mt-0.5">
          <span>{mover.outcome}</span>
          {mover.volume_24h && (
            <>
              <span>•</span>
              <span>${(mover.volume_24h / 1000).toFixed(0)}K vol</span>
            </>
          )}
        </div>
      </div>

      {/* Price & Change */}
      <div className="text-right">
        <div className="font-mono font-semibold text-white text-lg">
          {(mover.current_price * 100).toFixed(0)}¢
        </div>
        <div className={`
          flex items-center justify-end gap-1 font-mono text-sm font-semibold
          ${isUp ? 'text-emerald-400' : 'text-red-400'}
        `}>
          {isUp ? <TrendingUp className="w-3.5 h-3.5" /> : <TrendingDown className="w-3.5 h-3.5" />}
          {isUp ? '+' : ''}{mover.price_change.toFixed(1)}pp
          {isTop3 && <Flame className="w-3.5 h-3.5 text-orange-400 ml-1" />}
        </div>
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
        <button 
          className="p-2 rounded-lg bg-primary-600/20 hover:bg-primary-600 text-primary-400 hover:text-white transition-all"
          title="Set alert"
        >
          <Bell className="w-4 h-4" />
        </button>
        <button 
          className="p-2 rounded-lg bg-white/5 hover:bg-white/10 text-gray-400 hover:text-white transition-all"
          title="Add to watchlist"
        >
          <Eye className="w-4 h-4" />
        </button>
      </div>
    </div>
  )
}
