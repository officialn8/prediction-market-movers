'use client'

import { useEffect, useState } from 'react'
import { TrendingUp, TrendingDown, Flame } from 'lucide-react'

interface TickerItem {
  id: string
  title: string
  change: number
  isHot?: boolean
}

// Mock data - replace with real WebSocket data
const mockTickers: TickerItem[] = [
  { id: '1', title: 'TRUMP 2028', change: 8.2, isHot: true },
  { id: '2', title: 'BTC $100K JAN', change: -3.1 },
  { id: '3', title: 'FED RATE CUT MAR', change: 12.4, isHot: true },
  { id: '4', title: 'NASDAQ RECORD', change: -1.8 },
  { id: '5', title: 'ETH $5K', change: 5.7 },
  { id: '6', title: 'SUPERBOWL KC', change: 2.3 },
]

export function LiveTicker() {
  const [tickers, setTickers] = useState<TickerItem[]>(mockTickers)
  const [isPaused, setIsPaused] = useState(false)

  return (
    <div 
      className="relative overflow-hidden bg-gradient-to-r from-gray-900 via-gray-800 to-gray-900 border-b border-gray-800"
      onMouseEnter={() => setIsPaused(true)}
      onMouseLeave={() => setIsPaused(false)}
    >
      {/* Live indicator */}
      <div className="absolute left-0 top-0 bottom-0 z-10 flex items-center px-4 bg-gradient-to-r from-gray-900 via-gray-900 to-transparent">
        <div className="flex items-center gap-2 pr-4">
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500"></span>
          </span>
          <span className="text-xs font-semibold text-red-400 uppercase tracking-wider">Live</span>
        </div>
      </div>

      {/* Ticker content */}
      <div 
        className={`flex whitespace-nowrap ${isPaused ? '' : 'animate-scroll'}`}
        style={{ paddingLeft: '100px' }}
      >
        {/* Duplicate for seamless loop */}
        {[...tickers, ...tickers].map((item, idx) => (
          <TickerItemComponent key={`${item.id}-${idx}`} item={item} />
        ))}
      </div>

      {/* Fade edges */}
      <div className="absolute right-0 top-0 bottom-0 w-20 bg-gradient-to-l from-gray-900 to-transparent pointer-events-none" />
    </div>
  )
}

function TickerItemComponent({ item }: { item: TickerItem }) {
  const isUp = item.change > 0
  const isExtreme = Math.abs(item.change) > 5

  return (
    <div className={`
      flex items-center gap-3 px-6 py-2.5 border-r border-gray-800
      ${isExtreme ? 'bg-gradient-to-r from-transparent via-white/5 to-transparent' : ''}
    `}>
      <span className="text-sm font-medium text-gray-300">
        {item.title}
      </span>
      
      <div className={`
        flex items-center gap-1 font-mono text-sm font-semibold
        ${isUp ? 'text-emerald-400' : 'text-red-400'}
        ${isExtreme ? (isUp ? 'text-emerald-300' : 'text-red-300') : ''}
      `}>
        {isUp ? <TrendingUp className="w-3.5 h-3.5" /> : <TrendingDown className="w-3.5 h-3.5" />}
        <span>{isUp ? '+' : ''}{item.change.toFixed(1)}pp</span>
      </div>

      {item.isHot && (
        <Flame className="w-4 h-4 text-orange-400 animate-pulse" />
      )}
    </div>
  )
}
