import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'
import { Providers } from '@/components/providers'
import { Toaster } from 'react-hot-toast'
import { Sidebar } from '@/components/layout/Sidebar'
import ChatWrapper from '@/components/chat/ChatWrapper'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'AI Agent Platform - Multi-Agent Orchestration',
  description: 'Multi-Agent AI Platform with LangGraph, RRF Fusion, and Hybrid RAG',
  icons: {
    icon: '/favicon.ico',
  },
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" suppressHydrationWarning className="dark">
      <body className={`${inter.className} bg-[#070b14] text-gray-100 min-h-screen`}>
        <Providers>
          <div className="flex h-screen overflow-hidden bg-[#070b14]">
            <Sidebar />
            <main className="flex-1 overflow-y-auto bg-[#070b14]">
              {children}
            </main>
          </div>
          <Toaster
            position="top-right"
            toastOptions={{
              duration: 4000,
              style: {
                background: '#363636',
                color: '#fff',
              },
              success: {
                duration: 3000,
                iconTheme: {
                  primary: '#10b981',
                  secondary: '#fff',
                },
              },
              error: {
                duration: 5000,
                iconTheme: {
                  primary: '#ef4444',
                  secondary: '#fff',
                },
              },
            }}
          />
          <ChatWrapper />
        </Providers>
      </body>
    </html>
  )
}
