'use client'

import React from 'react'
import { cn } from '@/lib/utils'

interface SelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {
  label?: string
  error?: string
  helperText?: string
  options?: { value: string; label: string; disabled?: boolean }[]
}

export function Select({
  label,
  error,
  helperText,
  options = [],
  className = '',
  children,
  ...props
}: SelectProps) {
  return (
    <div className="w-full">
      {label && (
        <label className="block text-sm font-medium text-gray-300 mb-1.5">
          {label}
        </label>
      )}
      <select
        className={cn(
          // Base styles
          'w-full px-3 py-2.5 rounded-lg transition-all duration-200',
          // Background and text - explicit colors for dark theme
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
        style={{
          // Force option colors for browser compatibility
          colorScheme: 'dark',
        }}
        {...props}
      >
        {children || options.map((option) => (
          <option
            key={option.value}
            value={option.value}
            disabled={option.disabled}
            className="bg-gray-800 text-white py-2"
            style={{ backgroundColor: '#1f2937', color: '#ffffff' }}
          >
            {option.label}
          </option>
        ))}
      </select>
      {helperText && !error && (
        <p className="mt-1.5 text-xs text-gray-500">{helperText}</p>
      )}
      {error && (
        <p className="mt-1.5 text-xs text-red-400">{error}</p>
      )}
    </div>
  )
}

export default Select
