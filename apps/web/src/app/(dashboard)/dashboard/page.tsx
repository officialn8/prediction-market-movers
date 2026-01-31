'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Activity, Bell, TrendingUp, TrendingDown, Search, Plus, Settings, LogOut } from 'lucide-react'
import Link from 'next/link'

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
    // Check auth
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
    <div className="min-h-screen bg-gray-950">
      {/* Sidebar */}
      <aside className="fixed left-0 top-0 bottom-0 w-64 bg-gray-900 border-r border-gray-800 p-4">
        <div className="flex items-center gap-2 mb-8">
          <Activity className="w-8 h-8 text-primary-500" />
          <span className="font-bold text-xl">PMM</span>
        </div>

        <nav className="space-y-1">
          <NavItem href="/dashboard" icon={<TrendingUp />} label="Dashboard" active />
          <NavItem href="/dashboard/alerts" icon={<Bell />} label="My Alerts" />
          <NavItem href="/dashboard/watchlist" icon={<Activity />} label="Watchlist" />
          <NavItem href="/dashboard/settings" icon={<Settings />} label="Settings" />
        </nav>

        <div className="absolute bottom-4 left-4 right-4">
          <div className="glass rounded-lg p-3 mb-3">
            <div className="text-sm text-gray-400">Current plan</div>
            <div className="font-medium capitalize">{user.tier}</div>
            {user.tier === 'free' && (
              <Link href="/pricing" className="text-xs text-primary-400 hover:text-primary-300">
                Upgrade →
              </Link>
            )}
          </div>
          <button
            onClick={handleLogout}
            className="flex items-center gap-2 text-gray-400 hover:text-white transition w-full px-3 py-2"
          >
            <LogOut className="w-4 h-4" />
            Sign out
          </button>
        </div>
      </aside>

      {/* Main content */}
      <main className="ml-64 p-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-2xl font-bold">Dashboard</h1>
            <p className="text-gray-400">Welcome back, {user.name || user.email}</p>
          </div>
          <div className="flex items-center gap-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
              <input
                type="text"
                placeholder="Search markets..."
                className="bg-gray-800 border border-gray-700 rounded-lg pl-10 pr-4 py-2 w-64 focus:outline-none focus:border-primary-500"
              />
            </div>
            <Link
              href="/dashboard/alerts/new"
              className="bg-primary-600 hover:bg-primary-700 px-4 py-2 rounded-lg flex items-center gap-2 transition"
            >
              <Plus className="w-4 h-4" />
              New Alert
            </Link>
          </div>
        </div>

        {/* Top Movers */}
        <div className="glass rounded-xl p-6">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-lg font-semibold">Top Movers</h2>
            <div className="flex gap-1 bg-gray-800 rounded-lg p-1">
              {(['1h', '4h', '24h'] as const).map((w) => (
                <button
                  key={w}
                  onClick={() => setWindow(w)}
                  className={`px-3 py-1 rounded text-sm transition ${
                    window === w ? 'bg-primary-600 text-white' : 'text-gray-400 hover:text-white'
                  }`}
                >
                  {w}
                </button>
              ))}
            </div>
          </div>

          {loading ? (
            <div className="text-center py-12 text-gray-400">Loading...</div>
          ) : movers.length === 0 ? (
            <div className="text-center py-12 text-gray-400">No movers found</div>
          ) : (
            <div className="space-y-2">
              {movers.map((mover) => (
                <MoverRow key={mover.token_id} mover={mover} />
              ))}
            </div>
          )}
        </div>
      </main>
    </div>
  )
}

function NavItem({ 
  href, 
  icon, 
  label, 
  active = false 
}: { 
  href: string
  icon: React.ReactNode
  label: string
  active?: boolean 
}) {
  return (
    <Link
      href={href}
      className={`flex items-center gap-3 px-3 py-2 rounded-lg transition ${
        active 
          ? 'bg-primary-600/20 text-primary-400' 
          : 'text-gray-400 hover:text-white hover:bg-white/5'
      }`}
    >
      {icon}
      {label}
    </Link>
  )
}

function MoverRow({ mover }: { mover: Mover }) {
  const isUp = mover.price_change > 0
  
  return (
    <div className="flex items-center justify-between py-3 px-4 bg-white/5 rounded-lg hover:bg-white/10 transition">
      <div className="flex-1">
        <div className="font-medium truncate max-w-md">{mover.title}</div>
        <div className="text-sm text-gray-400">{mover.outcome}</div>
      </div>
      <div className="text-right">
        <div className="font-mono">{(mover.current_price * 100).toFixed(0)}¢</div>
        <div className={`text-sm flex items-center justify-end gap-1 ${isUp ? 'text-green-400' : 'text-red-400'}`}>
          {isUp ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
          {isUp ? '+' : ''}{mover.price_change.toFixed(1)}pp
        </div>
      </div>
      {mover.volume_24h && (
        <div className="text-right ml-6 text-sm text-gray-400">
          ${(mover.volume_24h / 1000).toFixed(0)}k vol
        </div>
      )}
    </div>
  )
}
