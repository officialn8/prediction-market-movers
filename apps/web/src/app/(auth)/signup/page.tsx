'use client'

import { useState } from 'react'
import Link from 'next/link'
import { useRouter, useSearchParams } from 'next/navigation'
import { Activity, Mail, Lock, User, Loader2, Check } from 'lucide-react'

export default function SignupPage() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const plan = searchParams.get('plan') || 'free'

  const [name, setName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    setLoading(true)

    try {
      const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password, name }),
      })

      const data = await res.json()

      if (!res.ok) {
        throw new Error(data.detail || 'Registration failed')
      }

      // Store token
      localStorage.setItem('token', data.access_token)
      localStorage.setItem('user', JSON.stringify(data.user))

      // If paid plan selected, redirect to checkout
      if (plan !== 'free') {
        router.push(`/checkout?plan=${plan}`)
      } else {
        router.push('/dashboard')
      }
    } catch (err: any) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4 py-12">
      <div className="w-full max-w-md">
        {/* Logo */}
        <Link href="/" className="flex items-center justify-center gap-2 mb-8">
          <Activity className="w-10 h-10 text-primary-500" />
          <span className="font-bold text-2xl">PMM</span>
        </Link>

        {/* Plan indicator */}
        {plan !== 'free' && (
          <div className="bg-primary-900/30 border border-primary-700/30 rounded-lg px-4 py-3 mb-6 text-center">
            <span className="text-primary-300">
              Creating account with <strong className="text-primary-200">{plan.charAt(0).toUpperCase() + plan.slice(1)}</strong> plan
            </span>
          </div>
        )}

        {/* Card */}
        <div className="glass rounded-xl p-8">
          <h1 className="text-2xl font-bold text-center mb-2">Create your account</h1>
          <p className="text-gray-400 text-center mb-6">Start tracking prediction markets</p>

          {error && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-lg px-4 py-3 mb-6 text-red-400 text-sm">
              {error}
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">Name</label>
              <div className="relative">
                <User className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-500" />
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-lg pl-10 pr-4 py-2.5 focus:outline-none focus:border-primary-500 transition"
                  placeholder="Your name"
                />
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Email</label>
              <div className="relative">
                <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-500" />
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-lg pl-10 pr-4 py-2.5 focus:outline-none focus:border-primary-500 transition"
                  placeholder="you@example.com"
                  required
                />
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Password</label>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-500" />
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full bg-white/5 border border-white/10 rounded-lg pl-10 pr-4 py-2.5 focus:outline-none focus:border-primary-500 transition"
                  placeholder="••••••••"
                  required
                  minLength={8}
                />
              </div>
              <p className="text-xs text-gray-500 mt-1">Minimum 8 characters</p>
            </div>

            <div className="flex items-start gap-2 text-sm">
              <input type="checkbox" required className="rounded border-gray-600 mt-0.5" />
              <span className="text-gray-400">
                I agree to the{' '}
                <Link href="/terms" className="text-primary-400 hover:text-primary-300">Terms of Service</Link>
                {' '}and{' '}
                <Link href="/privacy" className="text-primary-400 hover:text-primary-300">Privacy Policy</Link>
              </span>
            </div>

            <button
              type="submit"
              disabled={loading}
              className="w-full bg-primary-600 hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed py-2.5 rounded-lg font-medium flex items-center justify-center gap-2 transition"
            >
              {loading ? (
                <>
                  <Loader2 className="w-5 h-5 animate-spin" />
                  Creating account...
                </>
              ) : (
                <>
                  Create account
                  {plan !== 'free' && ' & continue to checkout'}
                </>
              )}
            </button>
          </form>

          {/* Features reminder */}
          <div className="mt-6 pt-6 border-t border-white/10">
            <p className="text-sm text-gray-400 mb-3">What you get with {plan === 'free' ? 'Free' : plan.charAt(0).toUpperCase() + plan.slice(1)}:</p>
            <ul className="space-y-2 text-sm">
              {plan === 'free' ? (
                <>
                  <Feature text="3 custom alerts" />
                  <Feature text="10 watchlist items" />
                  <Feature text="Basic dashboard" />
                </>
              ) : plan === 'pro' ? (
                <>
                  <Feature text="25 custom alerts" />
                  <Feature text="100 watchlist items" />
                  <Feature text="API access" />
                  <Feature text="Webhook notifications" />
                </>
              ) : (
                <>
                  <Feature text="100 custom alerts" />
                  <Feature text="Unlimited watchlist" />
                  <Feature text="Full API access" />
                  <Feature text="Priority support" />
                </>
              )}
            </ul>
          </div>

          <div className="mt-6 text-center text-sm text-gray-400">
            Already have an account?{' '}
            <Link href="/login" className="text-primary-400 hover:text-primary-300 transition">
              Sign in
            </Link>
          </div>
        </div>
      </div>
    </div>
  )
}

function Feature({ text }: { text: string }) {
  return (
    <li className="flex items-center gap-2 text-gray-300">
      <Check className="w-4 h-4 text-primary-400" />
      {text}
    </li>
  )
}
