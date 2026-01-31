'use client'

import { TrendingUp, Bell, AlertTriangle } from 'lucide-react'

interface MissedOpportunity {
  title: string
  entryTime: string
  entryPrice: number
  peakPrice: number
  percentGain: number
  duration: string
}

const mockMissed: MissedOpportunity[] = [
  {
    title: 'Fed Rate Cut March',
    entryTime: '8:42 AM',
    entryPrice: 0.23,
    peakPrice: 0.67,
    percentGain: 191,
    duration: '4 hours',
  },
  {
    title: 'NVIDIA Earnings Beat',
    entryTime: '2:15 PM',
    entryPrice: 0.41,
    peakPrice: 0.78,
    percentGain: 90,
    duration: '2 hours',
  },
]

export function WhatYouMissed() {
  return (
    <section className="relative">
      {/* Background accent */}
      <div className="absolute inset-0 bg-gradient-to-br from-amber-500/5 via-transparent to-red-500/5 rounded-3xl" />
      
      <div className="relative glass rounded-3xl p-8 border border-amber-500/20">
        {/* Header */}
        <div className="flex items-center gap-3 mb-6">
          <div className="p-2 rounded-xl bg-amber-500/10">
            <AlertTriangle className="w-6 h-6 text-amber-400" />
          </div>
          <div>
            <h2 className="text-2xl font-bold text-white">
              💸 What You Missed Today
            </h2>
            <p className="text-gray-400 text-sm">
              These opportunities passed while you weren't looking
            </p>
          </div>
        </div>

        {/* Opportunities */}
        <div className="space-y-4 mb-6">
          {mockMissed.map((opp, idx) => (
            <MissedCard key={idx} opportunity={opp} />
          ))}
        </div>

        {/* CTA */}
        <div className="flex items-center justify-between p-4 bg-gradient-to-r from-primary-600/20 to-primary-500/10 rounded-2xl border border-primary-500/30">
          <div>
            <div className="font-semibold text-white">Never miss again</div>
            <div className="text-sm text-gray-400">
              Set smart alerts and catch the next move
            </div>
          </div>
          <button className="flex items-center gap-2 bg-primary-600 hover:bg-primary-500 text-white font-medium py-2.5 px-5 rounded-xl transition-all duration-200 hover:shadow-lg hover:shadow-primary-500/25">
            <Bell className="w-4 h-4" />
            Set Smart Alerts
          </button>
        </div>
      </div>
    </section>
  )
}

function MissedCard({ opportunity }: { opportunity: MissedOpportunity }) {
  return (
    <div className="p-4 bg-white/5 rounded-xl border border-white/5 hover:border-white/10 transition-colors">
      <div className="flex items-center justify-between mb-3">
        <div>
          <div className="font-medium text-white">{opportunity.title}</div>
          <div className="text-sm text-gray-500">
            If you caught this at {opportunity.entryTime}
          </div>
        </div>
        <div className="text-right">
          <div className="text-2xl font-bold text-emerald-400">
            +{opportunity.percentGain}%
          </div>
          <div className="text-xs text-gray-500">
            in {opportunity.duration}
          </div>
        </div>
      </div>

      {/* Price journey visualization */}
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 text-sm">
          <span className="text-gray-500">Entry:</span>
          <span className="font-mono text-white">{(opportunity.entryPrice * 100).toFixed(0)}¢</span>
        </div>
        <div className="flex-1 h-1 bg-gradient-to-r from-amber-500 via-emerald-500 to-emerald-400 rounded-full relative">
          <TrendingUp className="absolute right-0 top-1/2 -translate-y-1/2 translate-x-1/2 w-4 h-4 text-emerald-400" />
        </div>
        <div className="flex items-center gap-2 text-sm">
          <span className="text-gray-500">Peak:</span>
          <span className="font-mono text-emerald-400 font-semibold">{(opportunity.peakPrice * 100).toFixed(0)}¢</span>
        </div>
      </div>
    </div>
  )
}
