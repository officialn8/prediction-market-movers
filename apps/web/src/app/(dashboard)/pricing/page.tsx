'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { Check, Zap, Crown, Building2 } from 'lucide-react'
import { useSession, checkout, openCustomerPortal } from '@/lib/auth-client'

const tiers = [
  {
    name: 'Free',
    price: '$0',
    period: '/mo',
    description: 'Get started with prediction market tracking',
    icon: Zap,
    features: [
      '3 price alerts',
      'Daily market snapshots',
      'Basic dashboard',
      'Email notifications',
    ],
    cta: 'Current Plan',
    disabled: true,
    tier: 'free',
  },
  {
    name: 'Pro',
    price: '$9',
    period: '/mo',
    description: 'For serious traders who need an edge',
    icon: Crown,
    features: [
      '25 price alerts',
      'Real-time WebSocket updates',
      'Full dashboard access',
      'API access',
      'Slack/Discord alerts',
      'Historical data (30 days)',
    ],
    cta: 'Upgrade to Pro',
    highlighted: true,
    tier: 'pro',
  },
  {
    name: 'Enterprise',
    price: '$49',
    period: '/mo',
    description: 'For teams and power users',
    icon: Building2,
    features: [
      '100 price alerts',
      'Everything in Pro',
      'Priority support',
      'Historical data (1 year)',
      'Custom alert conditions',
      'Team dashboard',
    ],
    cta: 'Upgrade to Enterprise',
    tier: 'enterprise',
  },
]

export default function PricingPage() {
  const router = useRouter()
  const { data: session, isPending } = useSession()
  const [loading, setLoading] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const handleUpgrade = async (tier: string) => {
    setLoading(tier)
    setError(null)

    try {
      // Check if user is logged in
      if (!session?.user) {
        router.push(`/signup?plan=${tier}`)
        return
      }

      // Use BetterAuth's Polar checkout
      const slug = tier === 'pro' ? 'pmm-pro' : 'pmm-enterprise'
      await checkout({ products: [slug] })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Something went wrong')
    } finally {
      setLoading(null)
    }
  }

  const handleManageSubscription = async () => {
    try {
      await openCustomerPortal()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to open portal')
    }
  }

  return (
    <div className="min-h-screen py-12 px-4">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold mb-4">
            <span className="gradient-text">Simple, Transparent Pricing</span>
          </h1>
          <p className="text-xl text-gray-400">
            Choose the plan that fits your trading style
          </p>
        </div>

        {/* Error */}
        {error && (
          <div className="max-w-md mx-auto mb-8 p-4 bg-red-900/30 border border-red-700 rounded-lg text-red-200">
            {error}
          </div>
        )}

        {/* Pricing Cards */}
        <div className="grid md:grid-cols-3 gap-8">
          {tiers.map((tier) => (
            <div
              key={tier.name}
              className={`rounded-2xl p-8 transition ${
                tier.highlighted
                  ? 'bg-gradient-to-b from-primary-900/50 to-primary-950/50 border-2 border-primary-500 scale-105'
                  : 'glass border border-white/10'
              }`}
            >
              {/* Icon */}
              <div className={`w-12 h-12 rounded-xl flex items-center justify-center mb-4 ${
                tier.highlighted ? 'bg-primary-600' : 'bg-white/10'
              }`}>
                <tier.icon className="w-6 h-6" />
              </div>

              {/* Name & Price */}
              <h3 className="text-2xl font-bold mb-2">{tier.name}</h3>
              <div className="flex items-baseline mb-4">
                <span className="text-4xl font-bold">{tier.price}</span>
                <span className="text-gray-400 ml-1">{tier.period}</span>
              </div>
              <p className="text-gray-400 mb-6">{tier.description}</p>

              {/* Features */}
              <ul className="space-y-3 mb-8">
                {tier.features.map((feature) => (
                  <li key={feature} className="flex items-center gap-3">
                    <Check className={`w-5 h-5 ${tier.highlighted ? 'text-primary-400' : 'text-green-400'}`} />
                    <span className="text-gray-300">{feature}</span>
                  </li>
                ))}
              </ul>

              {/* CTA */}
              <button
                onClick={() => !tier.disabled && handleUpgrade(tier.tier)}
                disabled={tier.disabled || loading === tier.tier}
                className={`w-full py-3 rounded-lg font-medium transition ${
                  tier.disabled
                    ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                    : tier.highlighted
                    ? 'bg-primary-600 hover:bg-primary-700 text-white'
                    : 'bg-white/10 hover:bg-white/20 text-white'
                }`}
              >
                {loading === tier.tier ? 'Loading...' : tier.cta}
              </button>
            </div>
          ))}
        </div>

        {/* FAQ or Trust Signals */}
        <div className="mt-16 text-center">
          <p className="text-gray-400">
            All plans include a 7-day free trial. Cancel anytime.
          </p>
          <div className="flex justify-center gap-8 mt-6 text-sm text-gray-500">
            <span>🔒 Secure checkout via Polar</span>
            <span>💳 No credit card for free plan</span>
            <span>📧 Support: support@pmm.com</span>
          </div>
        </div>
      </div>
    </div>
  )
}
