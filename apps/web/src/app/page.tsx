'use client'

import Link from 'next/link'
import { ArrowRight, Zap, Bell, LineChart, Shield, TrendingUp, Activity, ChevronRight, Check, Star } from 'lucide-react'
import { LiveTicker } from '@/components/ui/LiveTicker'
import { HeroStats } from '@/components/ui/AnimatedStats'
import { MoverCard } from '@/components/ui/MoverCard'
import { WhatYouMissed } from '@/components/sections/WhatYouMissed'
import { LiveActivityFeed } from '@/components/sections/LiveActivityFeed'

// Mock data for demo
const mockMovers = [
  {
    id: '1',
    title: 'Will Trump win the 2028 Presidential Election?',
    outcome: 'YES',
    source: 'polymarket' as const,
    category: 'Politics',
    currentPrice: 0.46,
    previousPrice: 0.34,
    priceChange: 12.4,
    volume24h: 847000,
    movedAgo: '3 min ago',
    alertsSet: 142,
    watching: 1243,
    streak: 5,
    isTop3: true,
  },
  {
    id: '2',
    title: 'Bitcoin above $100,000 on January 31?',
    outcome: 'YES',
    source: 'kalshi' as const,
    category: 'Crypto',
    currentPrice: 0.67,
    previousPrice: 0.58,
    priceChange: 9.1,
    volume24h: 523000,
    movedAgo: '8 min ago',
    alertsSet: 89,
    watching: 876,
    isTop3: true,
  },
  {
    id: '3',
    title: 'Fed Rate Cut in March 2026 FOMC Meeting?',
    outcome: 'YES',
    source: 'polymarket' as const,
    category: 'Economics',
    currentPrice: 0.73,
    previousPrice: 0.65,
    priceChange: 8.2,
    volume24h: 412000,
    movedAgo: '12 min ago',
    alertsSet: 67,
    watching: 654,
    isTop3: true,
  },
]

export default function LandingPage() {
  return (
    <div className="min-h-screen">
      {/* Live Ticker - Always Visible */}
      <LiveTicker />

      {/* Nav */}
      <nav className="sticky top-0 z-50 glass-strong border-b border-white/5">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center gap-3">
              <div className="relative">
                <Activity className="w-8 h-8 text-primary-400" />
                <div className="absolute -top-1 -right-1 w-3 h-3 bg-emerald-500 rounded-full animate-pulse" />
              </div>
              <span className="font-bold text-xl tracking-tight">PMM</span>
            </div>
            
            <div className="hidden md:flex items-center gap-8">
              <Link href="#features" className="text-gray-400 hover:text-white transition">Features</Link>
              <Link href="#pricing" className="text-gray-400 hover:text-white transition">Pricing</Link>
              <Link href="/docs" className="text-gray-400 hover:text-white transition">API</Link>
            </div>

            <div className="flex items-center gap-4">
              <Link href="/login" className="text-gray-300 hover:text-white transition font-medium">
                Log in
              </Link>
              <Link href="/signup" className="btn-primary flex items-center gap-2">
                Get Started <ArrowRight className="w-4 h-4" />
              </Link>
            </div>
          </div>
        </div>
      </nav>

      {/* Hero */}
      <section className="relative pt-20 pb-32 px-4 overflow-hidden">
        {/* Background glows */}
        <div className="hero-glow absolute top-0 left-1/4 -translate-x-1/2" />
        <div className="hero-glow absolute top-40 right-1/4 translate-x-1/2 opacity-50" />

        <div className="max-w-6xl mx-auto relative">
          <div className="text-center mb-16">
            {/* Badge */}
            <div className="inline-flex items-center gap-2 glass rounded-full px-4 py-2 mb-8 animate-float">
              <div className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
              </div>
              <span className="text-sm font-medium text-gray-300">
                <span className="text-emerald-400">847 traders</span> watching right now
              </span>
            </div>
            
            {/* Headline */}
            <h1 className="text-5xl sm:text-6xl lg:text-7xl font-bold mb-6 tracking-tight text-balance">
              <span className="gradient-text">Never Miss</span>
              <br />
              <span className="text-white">Another Move</span>
            </h1>
            
            <p className="text-xl text-gray-400 mb-10 max-w-2xl mx-auto text-balance">
              Real-time prediction market analytics. Know what's moving, 
              why it's moving, and get alerts before everyone else.
            </p>
            
            {/* CTAs */}
            <div className="flex flex-col sm:flex-row gap-4 justify-center mb-16">
              <Link href="/signup" className="btn-primary text-lg flex items-center justify-center gap-2">
                Start Free Trial <ArrowRight className="w-5 h-5" />
              </Link>
              <Link href="/demo" className="btn-ghost text-lg flex items-center justify-center gap-2">
                Watch Demo <ChevronRight className="w-5 h-5" />
              </Link>
            </div>

            {/* Social Proof */}
            <div className="flex items-center justify-center gap-6 text-sm text-gray-500">
              <div className="flex -space-x-2">
                {[1,2,3,4,5].map(i => (
                  <div key={i} className="w-8 h-8 rounded-full bg-gradient-to-br from-primary-500 to-primary-700 border-2 border-gray-900" />
                ))}
              </div>
              <span>Join <strong className="text-white">2,400+</strong> traders using PMM</span>
              <div className="flex items-center gap-1 text-amber-400">
                {[1,2,3,4,5].map(i => <Star key={i} className="w-4 h-4 fill-current" />)}
              </div>
            </div>
          </div>

          {/* Live Stats */}
          <HeroStats />
        </div>
      </section>

      {/* Top Movers Preview */}
      <section className="py-20 px-4 relative">
        <div className="max-w-7xl mx-auto">
          <div className="flex items-center justify-between mb-8">
            <div>
              <h2 className="text-3xl font-bold text-white mb-2">🔥 Top Movers Right Now</h2>
              <p className="text-gray-400">See what's moving across Polymarket and Kalshi</p>
            </div>
            <Link href="/dashboard" className="btn-ghost flex items-center gap-2">
              View All <ArrowRight className="w-4 h-4" />
            </Link>
          </div>

          <div className="grid md:grid-cols-3 gap-6">
            {mockMovers.map(mover => (
              <MoverCard key={mover.id} market={mover} />
            ))}
          </div>
        </div>
      </section>

      {/* FOMO Section */}
      <section className="py-20 px-4">
        <div className="max-w-4xl mx-auto">
          <WhatYouMissed />
        </div>
      </section>

      {/* Features & Live Feed */}
      <section className="py-20 px-4 bg-gradient-to-b from-transparent via-gray-900/30 to-transparent" id="features">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-4xl font-bold text-white mb-4">
              Built for Serious Traders
            </h2>
            <p className="text-xl text-gray-400 max-w-2xl mx-auto">
              Everything you need to spot opportunities and act fast
            </p>
          </div>

          <div className="grid lg:grid-cols-2 gap-12 items-start">
            {/* Features */}
            <div className="grid sm:grid-cols-2 gap-6">
              <FeatureCard
                icon={<Zap className="w-6 h-6" />}
                title="Real-time WebSocket"
                description="Sub-second price updates. No refresh needed, ever."
                gradient="from-amber-500 to-orange-500"
              />
              <FeatureCard
                icon={<Bell className="w-6 h-6" />}
                title="Smart Alerts"
                description="Price thresholds, volume spikes, and momentum triggers."
                gradient="from-primary-500 to-cyan-500"
              />
              <FeatureCard
                icon={<LineChart className="w-6 h-6" />}
                title="Composite Scoring"
                description="Our algorithm ranks movers by quality, not just movement."
                gradient="from-purple-500 to-pink-500"
              />
              <FeatureCard
                icon={<Shield className="w-6 h-6" />}
                title="Full API Access"
                description="Build your own tools with our REST and WebSocket APIs."
                gradient="from-emerald-500 to-teal-500"
              />
            </div>

            {/* Live Activity Feed */}
            <div>
              <h3 className="text-lg font-semibold text-white mb-4">See It In Action</h3>
              <LiveActivityFeed />
            </div>
          </div>
        </div>
      </section>

      {/* Pricing */}
      <section className="py-20 px-4" id="pricing">
        <div className="max-w-5xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-4xl font-bold text-white mb-4">Simple, Fair Pricing</h2>
            <p className="text-xl text-gray-400">Start free. Upgrade when you need more.</p>
          </div>
          
          <div className="grid md:grid-cols-3 gap-8">
            <PricingCard
              tier="Free"
              price="$0"
              description="Get started with the basics"
              features={[
                '3 custom alerts',
                'Daily market snapshots',
                'Basic dashboard',
                'Email notifications',
              ]}
              cta="Get Started"
              href="/signup"
            />
            <PricingCard
              tier="Pro"
              price="$9"
              period="/month"
              description="For serious traders"
              features={[
                '25 custom alerts',
                'Real-time WebSocket',
                'Full API access',
                'Slack & Discord alerts',
                'Historical data (30 days)',
              ]}
              cta="Start Pro Trial"
              href="/signup?plan=pro"
              highlighted
            />
            <PricingCard
              tier="Enterprise"
              price="$49"
              period="/month"
              description="For teams and power users"
              features={[
                '100 custom alerts',
                'Everything in Pro',
                'Team dashboard',
                'Priority support',
                'Custom integrations',
              ]}
              cta="Contact Sales"
              href="/contact"
            />
          </div>

          <p className="text-center text-gray-500 mt-8">
            All plans include a 7-day free trial. Cancel anytime.
          </p>
        </div>
      </section>

      {/* Final CTA */}
      <section className="py-20 px-4">
        <div className="max-w-4xl mx-auto">
          <div className="relative glass-strong rounded-3xl p-12 text-center overflow-hidden">
            {/* Background gradient */}
            <div className="absolute inset-0 bg-gradient-to-br from-primary-600/10 via-transparent to-purple-600/10" />
            
            <div className="relative">
              <h2 className="text-4xl font-bold text-white mb-4">
                Ready to Get an Edge?
              </h2>
              <p className="text-xl text-gray-400 mb-8 max-w-lg mx-auto">
                Join traders who catch moves before the crowd. Start free, no credit card required.
              </p>
              <Link href="/signup" className="btn-primary text-lg inline-flex items-center gap-2">
                Start Free Trial <ArrowRight className="w-5 h-5" />
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-gray-800/50 py-12 px-4">
        <div className="max-w-6xl mx-auto">
          <div className="flex flex-col md:flex-row justify-between items-center gap-6">
            <div className="flex items-center gap-2">
              <Activity className="w-6 h-6 text-primary-500" />
              <span className="font-bold text-lg">Prediction Market Movers</span>
            </div>
            <div className="flex gap-8 text-sm text-gray-400">
              <Link href="/privacy" className="hover:text-white transition">Privacy</Link>
              <Link href="/terms" className="hover:text-white transition">Terms</Link>
              <Link href="/docs" className="hover:text-white transition">API Docs</Link>
              <a href="https://twitter.com/pmm_app" className="hover:text-white transition">Twitter</a>
            </div>
            <div className="text-sm text-gray-500">
              © 2026 PMM. All rights reserved.
            </div>
          </div>
        </div>
      </footer>
    </div>
  )
}

function FeatureCard({ 
  icon, 
  title, 
  description, 
  gradient 
}: { 
  icon: React.ReactNode
  title: string
  description: string
  gradient: string
}) {
  return (
    <div className="card-interactive p-6 group">
      <div className={`
        inline-flex p-3 rounded-xl mb-4
        bg-gradient-to-br ${gradient}
        opacity-80 group-hover:opacity-100 transition-opacity
      `}>
        {icon}
      </div>
      <h3 className="font-semibold text-lg text-white mb-2">{title}</h3>
      <p className="text-gray-400 text-sm leading-relaxed">{description}</p>
    </div>
  )
}

function PricingCard({ 
  tier, 
  price, 
  period = '', 
  description, 
  features, 
  cta, 
  href, 
  highlighted = false 
}: { 
  tier: string
  price: string
  period?: string
  description: string
  features: string[]
  cta: string
  href: string
  highlighted?: boolean
}) {
  return (
    <div className={`
      relative rounded-2xl p-8 transition-all duration-300
      ${highlighted 
        ? 'bg-gradient-to-b from-primary-900/50 to-gray-900/50 border-2 border-primary-500 scale-105 shadow-xl shadow-primary-500/10' 
        : 'glass border border-white/10 hover:border-white/20'
      }
    `}>
      {highlighted && (
        <div className="absolute -top-4 left-1/2 -translate-x-1/2 bg-primary-500 text-white text-xs font-bold px-3 py-1 rounded-full">
          MOST POPULAR
        </div>
      )}

      <div className="mb-6">
        <span className="text-sm font-semibold text-primary-400 uppercase tracking-wider">{tier}</span>
        <div className="mt-2">
          <span className="text-5xl font-bold text-white">{price}</span>
          <span className="text-gray-400">{period}</span>
        </div>
        <p className="text-gray-400 text-sm mt-2">{description}</p>
      </div>

      <ul className="space-y-3 mb-8">
        {features.map((f, i) => (
          <li key={i} className="flex items-center gap-3 text-sm text-gray-300">
            <Check className={`w-5 h-5 ${highlighted ? 'text-primary-400' : 'text-emerald-400'}`} />
            {f}
          </li>
        ))}
      </ul>

      <Link 
        href={href}
        className={`
          block text-center py-3 rounded-xl font-medium transition-all duration-200
          ${highlighted 
            ? 'bg-primary-600 hover:bg-primary-500 text-white shadow-lg shadow-primary-500/25 hover:shadow-xl hover:shadow-primary-500/30' 
            : 'glass hover:bg-white/10 text-white'
          }
        `}
      >
        {cta}
      </Link>
    </div>
  )
}
