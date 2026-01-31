'use client'

import { useEffect, useState } from 'react'
import { Activity, Zap } from 'lucide-react'

interface SystemMetrics {
  connected: boolean
  latency_ms: number
  messages_received: number
  last_updated: number
}

interface SystemStatusData {
  services: {
    polymarket_wss?: SystemMetrics
    kalshi_wss?: SystemMetrics
  }
  timestamp: number
}

export function SystemStatus() {
  const [status, setStatus] = useState<SystemStatusData | null>(null)
  
  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/system/status`)
        const data = await res.json()
        setStatus(data)
      } catch (err) {
        // console.error(err)
      }
    }

    fetchStatus()
    const interval = setInterval(fetchStatus, 3000)
    return () => clearInterval(interval)
  }, [])

  if (!status || !status.services) return null

  const pm = status.services.polymarket_wss
  const kalshi = status.services.kalshi_wss

  return (
    <div className="flex items-center gap-4 bg-white/5 border border-white/10 rounded-xl px-4 py-2 text-xs font-mono">
      {/* Polymarket Status */}
      {pm && (
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${pm.connected ? 'bg-emerald-500 animate-pulse' : 'bg-red-500'}`} />
          <span className="text-gray-400 font-bold">PM</span>
          <span className={`${pm.latency_ms < 100 ? 'text-emerald-400' : 'text-amber-400'}`}>
            {pm.latency_ms.toFixed(0)}ms
          </span>
        </div>
      )}

      {/* Separator */}
      <div className="w-px h-4 bg-white/10" />

      {/* Kalshi Status */}
      {kalshi && (
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${kalshi.connected ? 'bg-emerald-500 animate-pulse' : 'bg-red-500'}`} />
          <span className="text-gray-400 font-bold">KAL</span>
          <span className={`${kalshi.latency_ms < 100 ? 'text-emerald-400' : 'text-amber-400'}`}>
            {kalshi.latency_ms.toFixed(0)}ms
          </span>
        </div>
      )}
    </div>
  )
}
