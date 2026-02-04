'use client'

import React from 'react'
import { cn } from '@/lib/utils'

interface TextareaProps extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {
  label?: string
  error?: string
  helperText?: string
}

export function Textarea({
  label,
  error,
  helperText,
  className = '',
  rows = 3,
  ...props
}: TextareaProps) {
  return (
    <div className="w-full">
      {label && (
        <label className="block text-sm font-medium text-gray-300 mb-1.5">
          {label}
        </label>
      )}
      <textarea
        rows={rows}
        className={cn(
          // Base styles
          'w-full px-3 py-2.5 rounded-lg transition-all duration-200 resize-none',
          // Background and text
          'bg-gray-800/50 text-white',
          // Border
          'border border-white/10 hover:border-white/20',
          // Focus
          'focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500/50',
          // Placeholder
          'placeholder:text-gray-500',
          // Error state
          error && 'border-red-500/50 focus:ring-red-500/50',
          // Custom class
          className
        )}
        {...props}
      />
      {helperText && !error && (
        <p className="mt-1.5 text-xs text-gray-500">{helperText}</p>
      )}
      {error && (
        <p className="mt-1.5 text-xs text-red-400">{error}</p>
      )}
    </div>
  )
}

export default Textarea
