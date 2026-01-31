'use client'

import { useEffect, useState, useRef } from 'react'

interface Stat {
  value: number
  label: string
  prefix?: string
  suffix?: string
  decimals?: number
}

interface AnimatedStatsProps {
  stats: Stat[]
}

export function AnimatedStats({ stats }: AnimatedStatsProps) {
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      {stats.map((stat, idx) => (
        <AnimatedStatCard key={idx} stat={stat} delay={idx * 100} />
      ))}
    </div>
  )
}

function AnimatedStatCard({ stat, delay }: { stat: Stat; delay: number }) {
  const [displayValue, setDisplayValue] = useState(0)
  const [isVisible, setIsVisible] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true)
          observer.disconnect()
        }
      },
      { threshold: 0.1 }
    )

    if (ref.current) {
      observer.observe(ref.current)
    }

    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    if (!isVisible) return

    const timeout = setTimeout(() => {
      const duration = 1500
      const steps = 60
      const increment = stat.value / steps
      let current = 0

      const interval = setInterval(() => {
        current += increment
        if (current >= stat.value) {
          setDisplayValue(stat.value)
          clearInterval(interval)
        } else {
          setDisplayValue(current)
        }
      }, duration / steps)

      return () => clearInterval(interval)
    }, delay)

    return () => clearTimeout(timeout)
  }, [isVisible, stat.value, delay])

  const formatValue = () => {
    const v = displayValue
    if (v >= 1000000) return `${(v / 1000000).toFixed(1)}M`
    if (v >= 1000) return `${(v / 1000).toFixed(stat.decimals ?? 1)}K`
    return v.toFixed(stat.decimals ?? 0)
  }

  return (
    <div 
      ref={ref}
      className="relative group"
    >
      <div className="absolute inset-0 bg-gradient-to-br from-primary-500/10 to-transparent rounded-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
      
      <div className="relative glass rounded-2xl p-6 text-center border border-white/5 hover:border-primary-500/30 transition-all duration-300">
        <div className="text-4xl font-bold font-mono mb-2 bg-gradient-to-r from-white to-gray-300 bg-clip-text text-transparent">
          {stat.prefix}{formatValue()}{stat.suffix}
        </div>
        <div className="text-sm text-gray-400 uppercase tracking-wider font-medium">
          {stat.label}
        </div>
        
        {/* Subtle glow effect */}
        <div className="absolute inset-0 rounded-2xl bg-primary-500/5 blur-xl opacity-0 group-hover:opacity-100 transition-opacity duration-500 -z-10" />
      </div>
    </div>
  )
}

// Specific stats component for hero
export function HeroStats() {
  return (
    <AnimatedStats
      stats={[
        { value: 2.4, label: 'Volume/hr', prefix: '$', suffix: 'M', decimals: 1 },
        { value: 847, label: 'Active Markets' },
        { value: 23, label: 'Alerts Triggered' },
        { value: 4291, label: 'Watching Now' },
      ]}
    />
  )
}
