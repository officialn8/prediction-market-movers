'use client'

import Link from 'next/link'
import { Bell, Plus, ArrowLeft, Zap } from 'lucide-react'

export default function AlertsPage() {
  return (
    <div className="min-h-screen bg-[#050505] p-8">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="flex items-center gap-4 mb-8">
          <Link 
            href="/dashboard" 
            className="p-2 rounded-lg hover:bg-white/5 text-gray-400 hover:text-white transition"
          >
            <ArrowLeft className="w-5 h-5" />
          </Link>
          <div className="flex-1">
            <h1 className="text-2xl font-bold text-white">My Alerts</h1>
            <p className="text-sm text-gray-500">Manage your price alerts</p>
          </div>
          <Link
            href="/dashboard/alerts/new"
            className="flex items-center gap-2 bg-primary-600 hover:bg-primary-500 text-white font-medium px-4 py-2.5 rounded-xl transition-all"
          >
            <Plus className="w-4 h-4" />
            New Alert
          </Link>
        </div>

        {/* Coming Soon Card */}
        <div className="glass rounded-2xl border border-white/5 p-12 text-center">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-primary-500/20 mb-6">
            <Bell className="w-8 h-8 text-primary-400" />
          </div>
          <h2 className="text-xl font-semibold text-white mb-2">Alerts Coming Soon</h2>
          <p className="text-gray-400 mb-6 max-w-md mx-auto">
            Set custom price alerts for any market. Get notified instantly when prices hit your targets.
          </p>
          <div className="inline-flex items-center gap-2 bg-amber-500/10 border border-amber-500/20 rounded-full px-4 py-2">
            <Zap className="w-4 h-4 text-amber-400" />
            <span className="text-sm text-amber-300">Launching next week</span>
          </div>
        </div>
      </div>
    </div>
  )
}
