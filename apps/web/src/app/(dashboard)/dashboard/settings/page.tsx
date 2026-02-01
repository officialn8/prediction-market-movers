'use client'

import Link from 'next/link'
import { Settings, ArrowLeft, Zap, User, Bell, CreditCard, Key } from 'lucide-react'

export default function SettingsPage() {
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
          <div>
            <h1 className="text-2xl font-bold text-white">Settings</h1>
            <p className="text-sm text-gray-500">Manage your account</p>
          </div>
        </div>

        {/* Settings Preview Cards */}
        <div className="grid md:grid-cols-2 gap-4 mb-8">
          <SettingCard 
            icon={<User className="w-5 h-5" />}
            title="Profile"
            description="Update your name and email"
            color="primary"
          />
          <SettingCard 
            icon={<Bell className="w-5 h-5" />}
            title="Notifications"
            description="Configure alert preferences"
            color="purple"
          />
          <SettingCard 
            icon={<CreditCard className="w-5 h-5" />}
            title="Billing"
            description="Manage your subscription"
            color="emerald"
          />
          <SettingCard 
            icon={<Key className="w-5 h-5" />}
            title="API Keys"
            description="Generate API access tokens"
            color="amber"
          />
        </div>

        {/* Coming Soon Notice */}
        <div className="glass rounded-2xl border border-white/5 p-8 text-center">
          <div className="inline-flex items-center justify-center w-12 h-12 rounded-xl bg-white/5 mb-4">
            <Settings className="w-6 h-6 text-gray-400" />
          </div>
          <h2 className="text-lg font-semibold text-white mb-2">Full Settings Coming Soon</h2>
          <p className="text-gray-400 mb-4 max-w-md mx-auto">
            We're building a complete settings experience. For now, manage your subscription via email.
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

function SettingCard({ 
  icon, 
  title, 
  description, 
  color 
}: { 
  icon: React.ReactNode
  title: string
  description: string
  color: 'primary' | 'purple' | 'emerald' | 'amber'
}) {
  const colorClasses = {
    primary: 'from-primary-500/20 to-primary-600/20 text-primary-400',
    purple: 'from-purple-500/20 to-purple-600/20 text-purple-400',
    emerald: 'from-emerald-500/20 to-emerald-600/20 text-emerald-400',
    amber: 'from-amber-500/20 to-amber-600/20 text-amber-400',
  }

  return (
    <div className="glass rounded-xl p-5 border border-white/5 hover:border-white/10 transition-colors cursor-not-allowed opacity-60">
      <div className="flex items-start gap-4">
        <div className={`p-2.5 rounded-xl bg-gradient-to-br ${colorClasses[color]}`}>
          {icon}
        </div>
        <div>
          <h3 className="font-medium text-white">{title}</h3>
          <p className="text-sm text-gray-500">{description}</p>
        </div>
      </div>
    </div>
  )
}
