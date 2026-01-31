'use client'

import Link from 'next/link'
import { ArrowRight, Zap, Bell, LineChart, Shield, TrendingUp, Activity } from 'lucide-react'

export default function LandingPage() {
  return (
    <div className="min-h-screen">
      {/* Nav */}
      <nav className="fixed top-0 left-0 right-0 z-50 glass">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center gap-2">
              <Activity className="w-8 h-8 text-primary-500" />
              <span className="font-bold text-xl">PMM</span>
            </div>
            <div className="flex items-center gap-4">
              <Link href="/login" className="text-gray-300 hover:text-white transition">
                Log in
              </Link>
              <Link 
                href="/signup" 
                className="bg-primary-600 hover:bg-primary-700 px-4 py-2 rounded-lg font-medium transition"
              >
                Get Started
              </Link>
            </div>
          </div>
        </div>
      </nav>

      {/* Hero */}
      <section className="pt-32 pb-20 px-4">
        <div className="max-w-4xl mx-auto text-center">
          <div className="inline-flex items-center gap-2 bg-primary-900/30 border border-primary-700/30 rounded-full px-4 py-1.5 mb-6">
            <Zap className="w-4 h-4 text-primary-400" />
            <span className="text-sm text-primary-300">Real-time WebSocket feeds</span>
          </div>
          
          <h1 className="text-5xl sm:text-6xl font-bold mb-6">
            <span className="gradient-text">Prediction Market</span>
            <br />
            Analytics & Alerts
          </h1>
          
          <p className="text-xl text-gray-400 mb-8 max-w-2xl mx-auto">
            Track price movements across Polymarket with instant alerts. 
            Spot opportunities before the market catches up.
          </p>
          
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link 
              href="/signup" 
              className="bg-primary-600 hover:bg-primary-700 px-8 py-3 rounded-lg font-medium text-lg flex items-center justify-center gap-2 transition"
            >
              Start Free <ArrowRight className="w-5 h-5" />
            </Link>
            <Link 
              href="/demo" 
              className="glass hover:bg-white/10 px-8 py-3 rounded-lg font-medium text-lg transition"
            >
              View Demo
            </Link>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-3 gap-8 mt-16 max-w-2xl mx-auto">
            <div>
              <div className="text-3xl font-bold text-primary-400">26k+</div>
              <div className="text-gray-500 text-sm">Msgs/min processed</div>
            </div>
            <div>
              <div className="text-3xl font-bold text-primary-400">&lt;1s</div>
              <div className="text-gray-500 text-sm">Alert latency</div>
            </div>
            <div>
              <div className="text-3xl font-bold text-primary-400">500+</div>
              <div className="text-gray-500 text-sm">Markets tracked</div>
            </div>
          </div>
        </div>
      </section>

      {/* Features */}
      <section className="py-20 px-4 bg-gradient-to-b from-transparent to-gray-900/50">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-3xl font-bold text-center mb-12">
            Everything you need to trade smarter
          </h2>
          
          <div className="grid md:grid-cols-3 gap-8">
            <FeatureCard
              icon={<Zap className="w-8 h-8" />}
              title="Real-time Data"
              description="WebSocket-first architecture captures every price tick. No refresh needed."
            />
            <FeatureCard
              icon={<Bell className="w-8 h-8" />}
              title="Instant Alerts"
              description="Set custom price thresholds and get notified via email or webhook instantly."
            />
            <FeatureCard
              icon={<LineChart className="w-8 h-8" />}
              title="Smart Scoring"
              description="Composite scoring combines price movement, volume, and momentum."
            />
            <FeatureCard
              icon={<TrendingUp className="w-8 h-8" />}
              title="Top Movers"
              description="See what's moving now. 1h, 4h, and 24h windows with quality filtering."
            />
            <FeatureCard
              icon={<Shield className="w-8 h-8" />}
              title="API Access"
              description="Build your own tools. Full REST API with real-time WebSocket option."
            />
            <FeatureCard
              icon={<Activity className="w-8 h-8" />}
              title="Volume Spikes"
              description="Detect unusual activity before prices move with volume anomaly detection."
            />
          </div>
        </div>
      </section>

      {/* Pricing */}
      <section className="py-20 px-4" id="pricing">
        <div className="max-w-5xl mx-auto">
          <h2 className="text-3xl font-bold text-center mb-4">Simple pricing</h2>
          <p className="text-gray-400 text-center mb-12">Start free, upgrade when you need more.</p>
          
          <div className="grid md:grid-cols-3 gap-8">
            <PricingCard
              tier="Free"
              price="$0"
              description="Get started with the basics"
              features={[
                '3 custom alerts',
                '10 watchlist items',
                'Basic dashboard',
                'Community support',
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
                '100 watchlist items',
                'API access',
                'Webhook notifications',
                'Priority support',
              ]}
              cta="Upgrade to Pro"
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
                'Unlimited watchlist',
                'Full API access',
                'Custom integrations',
                'Dedicated support',
              ]}
              cta="Contact Sales"
              href="/contact"
            />
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-20 px-4">
        <div className="max-w-3xl mx-auto text-center glass rounded-2xl p-12">
          <h2 className="text-3xl font-bold mb-4">Ready to get an edge?</h2>
          <p className="text-gray-400 mb-8">
            Join traders who catch moves before the crowd.
          </p>
          <Link 
            href="/signup" 
            className="bg-primary-600 hover:bg-primary-700 px-8 py-3 rounded-lg font-medium text-lg inline-flex items-center gap-2 transition"
          >
            Start Free Trial <ArrowRight className="w-5 h-5" />
          </Link>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-gray-800 py-12 px-4">
        <div className="max-w-6xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4">
          <div className="flex items-center gap-2">
            <Activity className="w-6 h-6 text-primary-500" />
            <span className="font-bold">Prediction Market Movers</span>
          </div>
          <div className="flex gap-6 text-sm text-gray-400">
            <Link href="/privacy" className="hover:text-white transition">Privacy</Link>
            <Link href="/terms" className="hover:text-white transition">Terms</Link>
            <Link href="/docs" className="hover:text-white transition">API Docs</Link>
          </div>
          <div className="text-sm text-gray-500">
            © 2026 PMM. All rights reserved.
          </div>
        </div>
      </footer>
    </div>
  )
}

function FeatureCard({ icon, title, description }: { icon: React.ReactNode; title: string; description: string }) {
  return (
    <div className="glass rounded-xl p-6 hover:bg-white/5 transition">
      <div className="text-primary-400 mb-4">{icon}</div>
      <h3 className="font-semibold text-lg mb-2">{title}</h3>
      <p className="text-gray-400 text-sm">{description}</p>
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
    <div className={`rounded-xl p-6 ${highlighted ? 'bg-primary-900/30 border-2 border-primary-500' : 'glass'}`}>
      <div className="mb-4">
        <span className="text-sm font-medium text-primary-400">{tier}</span>
      </div>
      <div className="mb-4">
        <span className="text-4xl font-bold">{price}</span>
        <span className="text-gray-400">{period}</span>
      </div>
      <p className="text-gray-400 text-sm mb-6">{description}</p>
      <ul className="space-y-3 mb-6">
        {features.map((f, i) => (
          <li key={i} className="flex items-center gap-2 text-sm">
            <span className="text-primary-400">✓</span>
            {f}
          </li>
        ))}
      </ul>
      <Link 
        href={href}
        className={`block text-center py-2.5 rounded-lg font-medium transition ${
          highlighted 
            ? 'bg-primary-600 hover:bg-primary-700' 
            : 'glass hover:bg-white/10'
        }`}
      >
        {cta}
      </Link>
    </div>
  )
}
