import type { Metadata } from 'next'
import { DM_Sans, IBM_Plex_Mono } from 'next/font/google'
import './globals.css'
import { Providers } from '@/components/providers'

const dmSans = DM_Sans({ 
  subsets: ['latin'],
  variable: '--font-sans',
  display: 'swap',
})

const ibmPlexMono = IBM_Plex_Mono({ 
  subsets: ['latin'],
  weight: ['400', '500', '600'],
  variable: '--font-mono',
  display: 'swap',
})

export const metadata: Metadata = {
  metadataBase: new URL('https://pmm.app'),
  title: 'PMM | Prediction Market Movers',
  description: 'Real-time prediction market analytics. Track price movements across Polymarket and Kalshi with instant alerts.',
  keywords: ['prediction markets', 'polymarket', 'kalshi', 'analytics', 'trading', 'alerts'],
  openGraph: {
    title: 'PMM | Prediction Market Movers',
    description: 'Real-time prediction market analytics. Never miss another move.',
    type: 'website',
    siteName: 'PMM',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'PMM | Prediction Market Movers',
    description: 'Real-time prediction market analytics. Never miss another move.',
    creator: '@pmm_app',
  },
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" className={`dark ${dmSans.variable} ${ibmPlexMono.variable}`}>
      <head>
        <link rel="icon" href="/favicon.ico" />
        <meta name="theme-color" content="#050505" />
      </head>
      <body className="font-sans antialiased">
        <Providers>
          {children}
        </Providers>
      </body>
    </html>
  )
}
