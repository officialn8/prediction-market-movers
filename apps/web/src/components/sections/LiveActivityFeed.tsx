'use client'

import { useEffect, useState, useRef } from 'react'
import { TrendingUp, TrendingDown, Bell, DollarSign, Pause, Play, Volume2, VolumeX } from 'lucide-react'

type ActivityType = 'price_spike' | 'alert_triggered' | 'large_buy' | 'price_drop'

interface Activity {
  id: string
  type: ActivityType
  market: string
  data: string
  timestamp: number
}

const mockActivities: Omit<Activity, 'id'>[] = [
  { type: 'price_spike', market: 'BTC $100K JAN', data: '+2.1pp ($45K vol)', timestamp: 2 },
  { type: 'alert_triggered', market: 'Trump 2028', data: '@trader_mike', timestamp: 8 },
  { type: 'large_buy', market: 'Fed March Cut', data: '$120K detected', timestamp: 15 },
  { type: 'price_drop', market: 'NASDAQ Record', data: '-1.8pp', timestamp: 23 },
  { type: 'price_spike', market: 'ETH $5K', data: '+3.4pp ($89K vol)', timestamp: 31 },
]

export function LiveActivityFeed() {
  const [activities, setActivities] = useState<Activity[]>([])
  const [isPaused, setIsPaused] = useState(false)
  const [soundEnabled, setSoundEnabled] = useState(false)
  const feedRef = useRef<HTMLDivElement>(null)

  // Simulate incoming activities
  useEffect(() => {
    if (isPaused) return

    // Initial load
    const initial = mockActivities.map((a, i) => ({ ...a, id: `${i}` }))
    setActivities(initial)

    // Add new activity every 5 seconds
    const interval = setInterval(() => {
      const random = mockActivities[Math.floor(Math.random() * mockActivities.length)]
      const newActivity: Activity = {
        ...random,
        id: Date.now().toString(),
        timestamp: 0,
      }

      setActivities(prev => {
        const updated = [newActivity, ...prev.slice(0, 19)] // Keep last 20
        return updated.map((a, i) => ({
          ...a,
          timestamp: i === 0 ? 0 : a.timestamp + 5,
        }))
      })

      // Play sound if enabled
      if (soundEnabled && random.type === 'price_spike') {
        // Audio would be played here
      }
    }, 5000)

    return () => clearInterval(interval)
  }, [isPaused, soundEnabled])

  const getActivityIcon = (type: ActivityType) => {
    switch (type) {
      case 'price_spike':
        return <TrendingUp className="w-4 h-4 text-emerald-400" />
      case 'price_drop':
        return <TrendingDown className="w-4 h-4 text-red-400" />
      case 'alert_triggered':
        return <Bell className="w-4 h-4 text-amber-400" />
      case 'large_buy':
        return <DollarSign className="w-4 h-4 text-purple-400" />
    }
  }

  const getActivityBg = (type: ActivityType) => {
    switch (type) {
      case 'price_spike':
        return 'bg-emerald-500/10 border-emerald-500/20'
      case 'price_drop':
        return 'bg-red-500/10 border-red-500/20'
      case 'alert_triggered':
        return 'bg-amber-500/10 border-amber-500/20'
      case 'large_buy':
        return 'bg-purple-500/10 border-purple-500/20'
    }
  }

  return (
    <div className="glass rounded-2xl overflow-hidden border border-white/10">
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-white/10">
        <div className="flex items-center gap-3">
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500"></span>
          </span>
          <span className="font-semibold text-white">Live Activity</span>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={() => setIsPaused(!isPaused)}
            className="p-2 rounded-lg hover:bg-white/10 text-gray-400 hover:text-white transition-colors"
          >
            {isPaused ? <Play className="w-4 h-4" /> : <Pause className="w-4 h-4" />}
          </button>
          <button
            onClick={() => setSoundEnabled(!soundEnabled)}
            className="p-2 rounded-lg hover:bg-white/10 text-gray-400 hover:text-white transition-colors"
          >
            {soundEnabled ? <Volume2 className="w-4 h-4" /> : <VolumeX className="w-4 h-4" />}
          </button>
        </div>
      </div>

      {/* Feed */}
      <div ref={feedRef} className="h-80 overflow-y-auto p-4 space-y-2">
        {activities.map((activity, idx) => (
          <div
            key={activity.id}
            className={`
              flex items-center gap-3 p-3 rounded-xl border transition-all duration-500
              ${getActivityBg(activity.type)}
              ${idx === 0 ? 'animate-slide-in scale-[1.02]' : ''}
            `}
            style={{
              opacity: idx === 0 ? 1 : Math.max(0.3, 1 - idx * 0.08),
            }}
          >
            {/* Time */}
            <span className="text-xs text-gray-500 font-mono w-8 shrink-0">
              ⏱️ {activity.timestamp}s
            </span>

            {/* Icon */}
            <div className="shrink-0">
              {getActivityIcon(activity.type)}
            </div>

            {/* Content */}
            <div className="flex-1 min-w-0">
              <span className="font-medium text-white">{activity.market}:</span>{' '}
              <span className="text-gray-400">
                {activity.type === 'alert_triggered' ? 'Alert triggered for ' : ''}
                {activity.type === 'large_buy' ? 'Large buy ' : ''}
                {activity.data}
              </span>
            </div>
          </div>
        ))}
      </div>

      {/* Footer hint */}
      <div className="p-3 bg-white/5 border-t border-white/10 text-center text-sm text-gray-500">
        Showing other users' alerts triggering in real-time
      </div>
    </div>
  )
}
