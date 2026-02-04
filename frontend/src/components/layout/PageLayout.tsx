'use client'

import React from 'react'

interface PageLayoutProps {
  children: React.ReactNode
  className?: string
  title?: string
  subtitle?: string
}

export function PageLayout({ children, className = '', title, subtitle }: PageLayoutProps) {
  return (
    <div className={`min-h-screen bg-[#070b14] p-6 ${className}`}>
      {(title || subtitle) && (
        <div className="mb-6">
          {title && <h1 className="text-2xl font-bold text-white">{title}</h1>}
          {subtitle && <p className="text-gray-400 mt-1">{subtitle}</p>}
        </div>
      )}
      {children}
    </div>
  )
}

export default PageLayout
