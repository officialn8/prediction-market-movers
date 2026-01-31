'use client'

import { useState } from 'react'
import { TrendingUp, TrendingDown, Bell, Eye, Flame, Clock, ExternalLink } from 'lucide-react'
import Link from 'next/link'

interface MoverCardProps {
  market: {
    id: string
    title: string
    outcome: string
    source: 'polymarket' | 'kalshi'
    category?: string
    currentPrice: number
    previousPrice: number
    priceChange: number
    volume24h: number
    movedAgo: string
    alertsSet: number
    watching: number
    streak?: number
    isTop3?: boolean
  }
}

export function MoverCard({ market }: MoverCardProps) {
  const [isHovered, setIsHovered] = useState(false)
  const isUp = market.priceChange > 0
  const isExtreme = Math.abs(market.priceChange) > 10
  const isStrong = Math.abs(market.priceChange) > 5

  const getChangeColor = () => {
    if (isExtreme) return isUp ? 'text-emerald-300' : 'text-red-300'
    if (isStrong) return isUp ? 'text-emerald-400' : 'text-red-400'
    return isUp ? 'text-emerald-500' : 'text-red-500'
  }

  const getBorderColor = () => {
    if (isExtreme) return isUp ? 'border-emerald-500/50' : 'border-red-500/50'
    return 'border-white/10'
  }

  return (
    <div
      className={`
        relative overflow-hidden rounded-2xl border transition-all duration-300
        ${getBorderColor()}
        ${isExtreme ? 'animate-pulse-border' : ''}
        hover:border-primary-500/50 hover:shadow-lg hover:shadow-primary-500/10
        bg-gradient-to-br from-gray-900/90 to-gray-950/90
        backdrop-blur-sm
      `}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      {/* Glow effect for extreme moves */}
      {isExtreme && (
        <div className={`
          absolute inset-0 
          ${isUp ? 'bg-emerald-500/5' : 'bg-red-500/5'}
          animate-glow
        `} />
      )}

      <div className="relative p-5">
        {/* Header: Source & Category */}
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <span className={`
              text-xs font-bold uppercase tracking-wider px-2 py-0.5 rounded
              ${market.source === 'polymarket' ? 'bg-purple-500/20 text-purple-300' : 'bg-blue-500/20 text-blue-300'}
            `}>
              {market.source}
            </span>
            <span className="text-xs text-gray-500 uppercase tracking-wider">
              {market.outcome}
            </span>
            {market.category && (
              <span className="text-xs text-gray-600">
                • {market.category}
              </span>
            )}
          </div>

          {/* Price change badge */}
          <div className={`
            flex items-center gap-1.5 px-3 py-1 rounded-full font-mono text-sm font-bold
            ${isUp 
              ? isExtreme ? 'bg-emerald-500/20 text-emerald-300' : 'bg-emerald-500/10 text-emerald-400'
              : isExtreme ? 'bg-red-500/20 text-red-300' : 'bg-red-500/10 text-red-400'
            }
          `}>
            {isUp ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
            {isUp ? '+' : ''}{market.priceChange.toFixed(1)}pp
            {market.isTop3 && <Flame className="w-4 h-4 text-orange-400 animate-pulse" />}
          </div>
        </div>

        {/* Title */}
        <h3 className="text-lg font-semibold text-white mb-3 line-clamp-2 leading-tight">
          {market.title}
        </h3>

        {/* Price info */}
        <div className="flex items-center gap-4 mb-4">
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-bold font-mono text-white">
              {(market.currentPrice * 100).toFixed(0)}¢
            </span>
            <span className="text-sm text-gray-500 line-through">
              {(market.previousPrice * 100).toFixed(0)}¢
            </span>
          </div>
          <span className="text-gray-600">|</span>
          <span className="text-sm text-gray-400">
            ${(market.volume24h / 1000).toFixed(0)}K vol
          </span>
          <span className="text-gray-600">|</span>
          <span className="flex items-center gap-1 text-sm text-gray-400">
            <Clock className="w-3.5 h-3.5" />
            {market.movedAgo}
          </span>
        </div>

        {/* Social proof row */}
        <div className="flex items-center gap-4 mb-4 text-sm">
          <span className="flex items-center gap-1.5 text-amber-400/80">
            <Bell className="w-3.5 h-3.5" />
            {market.alertsSet} alerts set
          </span>
          <span className="flex items-center gap-1.5 text-primary-400/80">
            <Eye className="w-3.5 h-3.5" />
            {market.watching.toLocaleString()} watching
          </span>
          {market.streak && market.streak > 2 && (
            <span className="flex items-center gap-1.5 text-purple-400/80">
              📈 {market.streak}th consecutive {isUp ? '↑' : '↓'}
            </span>
          )}
        </div>

        {/* Actions */}
        <div className="flex gap-2">
          <button className="flex-1 flex items-center justify-center gap-2 bg-primary-600 hover:bg-primary-500 text-white font-medium py-2.5 px-4 rounded-xl transition-all duration-200 hover:shadow-lg hover:shadow-primary-500/25">
            <Bell className="w-4 h-4" />
            Set Alert
          </button>
          <Link 
            href={`/market/${market.id}`}
            className="flex items-center justify-center gap-2 glass hover:bg-white/10 text-gray-300 hover:text-white font-medium py-2.5 px-4 rounded-xl transition-all duration-200"
          >
            <ExternalLink className="w-4 h-4" />
            Details
          </Link>
        </div>
      </div>

      {/* Hover gradient overlay */}
      <div className={`
        absolute inset-0 pointer-events-none transition-opacity duration-300
        bg-gradient-to-t from-primary-500/5 to-transparent
        ${isHovered ? 'opacity-100' : 'opacity-0'}
      `} />
    </div>
  )
}

// Compact version for lists
export function MoverCardCompact({ market }: MoverCardProps) {
  const isUp = market.priceChange > 0
  const isExtreme = Math.abs(market.priceChange) > 10

  return (
    <div className={`
      flex items-center justify-between p-4 rounded-xl 
      bg-white/5 hover:bg-white/10 border border-transparent hover:border-primary-500/30
      transition-all duration-200 cursor-pointer group
      ${isExtreme ? 'ring-1 ring-inset ' + (isUp ? 'ring-emerald-500/30' : 'ring-red-500/30') : ''}
    `}>
      {/* Left: Info */}
      <div className="flex-1 min-w-0 mr-4">
        <div className="flex items-center gap-2 mb-1">
          <span className={`
            text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded
            ${market.source === 'polymarket' ? 'bg-purple-500/20 text-purple-300' : 'bg-blue-500/20 text-blue-300'}
          `}>
            {market.source}
          </span>
          {market.isTop3 && <Flame className="w-3 h-3 text-orange-400" />}
        </div>
        <h4 className="font-medium text-white truncate group-hover:text-primary-300 transition-colors">
          {market.title}
        </h4>
        <div className="text-xs text-gray-500 mt-0.5">
          {market.movedAgo} • {market.alertsSet} alerts
        </div>
      </div>

      {/* Right: Price */}
      <div className="text-right">
        <div className="font-mono font-semibold text-white">
          {(market.currentPrice * 100).toFixed(0)}¢
        </div>
        <div className={`
          flex items-center justify-end gap-1 font-mono text-sm font-semibold
          ${isUp ? 'text-emerald-400' : 'text-red-400'}
        `}>
          {isUp ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
          {isUp ? '+' : ''}{market.priceChange.toFixed(1)}pp
        </div>
      </div>
    </div>
  )
}
