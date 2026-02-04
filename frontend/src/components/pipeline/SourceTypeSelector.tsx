/**
 * SourceTypeSelector Component
 *
 * Displays all 9 source type categories (70+ types) in an organized,
 * categorized view with icons and search functionality.
 */

import React, { useState, useMemo } from 'react'
import { SourceType, SOURCE_TYPE_CATEGORIES, SourceTypeCategory } from '@/types/pipeline-canonical'

interface SourceTypeSelectorProps {
  value: SourceType | null
  onChange: (sourceType: SourceType) => void
  className?: string
  disabled?: boolean
}

export const SourceTypeSelector: React.FC<SourceTypeSelectorProps> = ({
  value,
  onChange,
  className = '',
  disabled = false,
}) => {
  const [searchQuery, setSearchQuery] = useState('')
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(
    new Set(['file', 'database']) // Expand file and database by default
  )

  // Filter categories and types based on search query
  const filteredCategories = useMemo(() => {
    if (!searchQuery.trim()) {
      return SOURCE_TYPE_CATEGORIES
    }

    const query = searchQuery.toLowerCase()
    return SOURCE_TYPE_CATEGORIES.map((category) => ({
      ...category,
      types: category.types.filter(
        (type) =>
          type.label.toLowerCase().includes(query) ||
          type.description.toLowerCase().includes(query) ||
          type.value.toLowerCase().includes(query)
      ),
    })).filter((category) => category.types.length > 0)
  }, [searchQuery])

  const toggleCategory = (categoryId: string) => {
    setExpandedCategories((prev) => {
      const next = new Set(prev)
      if (next.has(categoryId)) {
        next.delete(categoryId)
      } else {
        next.add(categoryId)
      }
      return next
    })
  }

  const handleSelectType = (sourceType: SourceType) => {
    if (!disabled) {
      onChange(sourceType)
    }
  }

  // Auto-expand categories when searching
  React.useEffect(() => {
    if (searchQuery.trim()) {
      setExpandedCategories(new Set(filteredCategories.map((c) => c.id)))
    }
  }, [searchQuery, filteredCategories])

  return (
    <div className={`source-type-selector ${className}`}>
      {/* Search Input */}
      <div className="mb-4">
        <input
          type="text"
          placeholder="Search source types..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          disabled={disabled}
          className="w-full px-4 py-2 border border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
        />
      </div>

      {/* Categories */}
      <div className="space-y-2 max-h-[600px] overflow-y-auto">
        {filteredCategories.length === 0 ? (
          <div className="text-center py-8 text-gray-400">
            No source types found matching "{searchQuery}"
          </div>
        ) : (
          filteredCategories.map((category) => (
            <div
              key={category.id}
              className="border border-gray-200 rounded-lg overflow-hidden"
            >
              {/* Category Header */}
              <button
                onClick={() => toggleCategory(category.id)}
                disabled={disabled}
                className="w-full px-4 py-3 bg-gray-800/50 hover:bg-gray-700 flex items-center justify-between transition-colors"
              >
                <div className="flex items-center space-x-3">
                  <span className="text-2xl">{category.icon}</span>
                  <div className="text-left">
                    <div className="font-semibold text-white">
                      {category.label}
                    </div>
                    <div className="text-sm text-gray-400">
                      {category.description}
                    </div>
                  </div>
                </div>
                <svg
                  className={`w-5 h-5 transition-transform ${
                    expandedCategories.has(category.id) ? 'rotate-180' : ''
                  }`}
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M19 9l-7 7-7-7"
                  />
                </svg>
              </button>

              {/* Category Types */}
              {expandedCategories.has(category.id) && (
                <div className="bg-gray-800 divide-y divide-gray-100">
                  {category.types.map((type) => (
                    <button
                      key={type.value}
                      onClick={() => handleSelectType(type.value)}
                      disabled={disabled}
                      className={`w-full px-6 py-3 text-left hover:bg-blue-900/30 transition-colors ${
                        value === type.value
                          ? 'bg-blue-100 border-l-4 border-blue-500'
                          : ''
                      } ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}
                    >
                      <div className="flex items-center justify-between">
                        <div>
                          <div className="font-medium text-white">
                            {type.label}
                          </div>
                          <div className="text-sm text-gray-400">
                            {type.description}
                          </div>
                        </div>
                        {value === type.value && (
                          <svg
                            className="w-5 h-5 text-blue-500"
                            fill="currentColor"
                            viewBox="0 0 20 20"
                          >
                            <path
                              fillRule="evenodd"
                              d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                              clipRule="evenodd"
                            />
                          </svg>
                        )}
                      </div>
                    </button>
                  ))}
                </div>
              )}
            </div>
          ))
        )}
      </div>

      {/* Selected Source Type Display */}
      {value && (
        <div className="mt-4 p-3 bg-blue-900/30 border border-blue-600/50 rounded-lg">
          <div className="text-sm text-blue-900">
            <span className="font-semibold">Selected:</span>{' '}
            {SOURCE_TYPE_CATEGORIES.flatMap((c) => c.types).find(
              (t) => t.value === value
            )?.label || value}
          </div>
        </div>
      )}
    </div>
  )
}

export default SourceTypeSelector
