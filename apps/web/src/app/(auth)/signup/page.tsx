'use client'

import { Suspense, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import Link from 'next/link'
import { Mail, Lock, User, ArrowRight, Loader2, Eye, EyeOff, Check, Zap } from 'lucide-react'

function SignupForm() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const plan = searchParams.get('plan') || 'free'
  
  const [name, setName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [showPassword, setShowPassword] = useState(false)
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
        body: JSON.stringify({ name, email, password, plan }),
      })

      const data = await res.json()

      if (!res.ok) {
        throw new Error(data.detail || 'Registration failed')
      }

      localStorage.setItem('token', data.access_token)
      localStorage.setItem('user', JSON.stringify(data.user))
      
      // Redirect to checkout if Pro/Enterprise, otherwise dashboard
      if (plan !== 'free') {
        router.push('/pricing')
      } else {
        router.push('/dashboard')
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Something went wrong')
    } finally {
      setLoading(false)
    }
  }

  const passwordStrength = () => {
    if (password.length === 0) return { strength: 0, text: '', color: '' }
    if (password.length < 6) return { strength: 1, text: 'Weak', color: 'bg-red-500' }
    if (password.length < 10) return { strength: 2, text: 'Fair', color: 'bg-amber-500' }
    if (password.length >= 10 && /[A-Z]/.test(password) && /[0-9]/.test(password)) {
      return { strength: 4, text: 'Strong', color: 'bg-emerald-500' }
    }
    return { strength: 3, text: 'Good', color: 'bg-primary-500' }
  }

  const { strength, text, color } = passwordStrength()

  return (
    <div className="w-full max-w-md">
      {/* Plan Badge */}
      {plan !== 'free' && (
        <div className="flex justify-center mb-6">
          <div className="inline-flex items-center gap-2 bg-primary-500/20 border border-primary-500/30 rounded-full px-4 py-2">
            <Zap className="w-4 h-4 text-primary-400" />
            <span className="text-sm font-medium text-primary-300">
              Starting with {plan.charAt(0).toUpperCase() + plan.slice(1)} plan
            </span>
          </div>
        </div>
      )}

      {/* Card */}
      <div className="glass-strong rounded-3xl p-8 border border-white/10">
        {/* Header */}
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-white mb-2">Create your account</h1>
          <p className="text-gray-400">
            Start tracking prediction markets in seconds
          </p>
        </div>

        {/* Error */}
        {error && (
          <div className="mb-6 p-4 bg-red-500/10 border border-red-500/20 rounded-xl text-red-400 text-sm">
            {error}
          </div>
        )}

        {/* Form */}
        <form onSubmit={handleSubmit} className="space-y-5">
          {/* Name */}
          <div>
            <label htmlFor="name" className="block text-sm font-medium text-gray-300 mb-2">
              Name
            </label>
            <div className="relative">
              <User className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-500" />
              <input
                id="name"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-xl pl-12 pr-4 py-3.5 text-white placeholder:text-gray-600 focus:outline-none focus:border-primary-500/50 focus:ring-1 focus:ring-primary-500/25 transition-all"
                placeholder="John Doe"
                required
              />
            </div>
          </div>

          {/* Email */}
          <div>
            <label htmlFor="email" className="block text-sm font-medium text-gray-300 mb-2">
              Email
            </label>
            <div className="relative">
              <Mail className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-500" />
              <input
                id="email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-xl pl-12 pr-4 py-3.5 text-white placeholder:text-gray-600 focus:outline-none focus:border-primary-500/50 focus:ring-1 focus:ring-primary-500/25 transition-all"
                placeholder="you@example.com"
                required
              />
            </div>
          </div>

          {/* Password */}
          <div>
            <label htmlFor="password" className="block text-sm font-medium text-gray-300 mb-2">
              Password
            </label>
            <div className="relative">
              <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-500" />
              <input
                id="password"
                type={showPassword ? 'text' : 'password'}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-xl pl-12 pr-12 py-3.5 text-white placeholder:text-gray-600 focus:outline-none focus:border-primary-500/50 focus:ring-1 focus:ring-primary-500/25 transition-all"
                placeholder="••••••••"
                required
                minLength={6}
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-300 transition"
              >
                {showPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
              </button>
            </div>
            
            {/* Password strength */}
            {password.length > 0 && (
              <div className="mt-2">
                <div className="flex gap-1 mb-1">
                  {[1, 2, 3, 4].map((i) => (
                    <div
                      key={i}
                      className={`h-1 flex-1 rounded-full transition-colors ${
                        i <= strength ? color : 'bg-white/10'
                      }`}
                    />
                  ))}
                </div>
                <p className="text-xs text-gray-500">{text} password</p>
              </div>
            )}
          </div>

          {/* Submit */}
          <button
            type="submit"
            disabled={loading}
            className="w-full flex items-center justify-center gap-2 bg-gradient-to-r from-primary-600 to-primary-500 hover:from-primary-500 hover:to-primary-400 disabled:opacity-50 disabled:cursor-not-allowed text-white font-medium py-3.5 rounded-xl transition-all hover:shadow-lg hover:shadow-primary-500/25"
          >
            {loading ? (
              <Loader2 className="w-5 h-5 animate-spin" />
            ) : (
              <>
                Create account <ArrowRight className="w-5 h-5" />
              </>
            )}
          </button>
        </form>

        {/* Features */}
        <div className="mt-8 p-4 bg-white/5 rounded-xl">
          <p className="text-sm font-medium text-gray-300 mb-3">What you get:</p>
          <ul className="space-y-2">
            {[
              'Real-time market tracking',
              'Custom price alerts',
              'Multi-platform support',
              'No credit card required',
            ].map((feature) => (
              <li key={feature} className="flex items-center gap-2 text-sm text-gray-400">
                <Check className="w-4 h-4 text-emerald-400" />
                {feature}
              </li>
            ))}
          </ul>
        </div>

        {/* Sign in link */}
        <p className="text-center text-gray-400 mt-8">
          Already have an account?{' '}
          <Link href="/login" className="text-primary-400 hover:text-primary-300 font-medium transition">
            Sign in
          </Link>
        </p>

        {/* Terms */}
        <p className="text-center text-xs text-gray-600 mt-4">
          By signing up, you agree to our{' '}
          <Link href="/terms" className="text-gray-400 hover:text-gray-300 underline">Terms</Link>
          {' '}and{' '}
          <Link href="/privacy" className="text-gray-400 hover:text-gray-300 underline">Privacy Policy</Link>
        </p>
      </div>
    </div>
  )
}

// Wrap in Suspense for useSearchParams (Next.js 14 requirement)
export default function SignupPage() {
  return (
    <Suspense fallback={
      <div className="w-full max-w-md">
        <div className="glass-strong rounded-3xl p-8 border border-white/10 animate-pulse">
          <div className="h-8 bg-white/10 rounded mb-4" />
          <div className="h-4 bg-white/10 rounded w-2/3 mx-auto" />
        </div>
      </div>
    }>
      <SignupForm />
    </Suspense>
  )
}
