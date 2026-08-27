'use client'

/**
 * SchemaDefinitionPanel - Intelligent 4-Mode Schema Builder
 *
 * Modes:
 * A. Auto-Infer from Sample - Upload file, backend infers schema
 * B. Import from Source - Connect to source DB, extract INFORMATION_SCHEMA
 * C. Manual with Suggestions - Type column names, AI suggests types
 * D. Clone from Existing - Select existing feed, clone schema
 *
 * All modes converge to a unified ColumnTable editor.
 * UI writes to schema_details metadata table ONLY.
 */

import React, { useState, useCallback } from 'react'
import type { ColumnDefinition, DataType, SchemaConfig } from '@/types/pipeline-canonical'

interface SchemaDefinitionPanelProps {
  schema: Partial<SchemaConfig>
  onChange: (schema: Partial<SchemaConfig>) => void
  feedName?: string
  sourceType?: string
}

type SchemaMode = 'manual' | 'auto-infer' | 'import-source' | 'clone'

const DATA_TYPES: DataType[] = [
  'string', 'integer', 'bigint', 'float', 'double', 'decimal',
  'boolean', 'date', 'timestamp', 'binary', 'array', 'struct', 'map',
]

const PII_LEVELS = [
  { value: 'none', label: 'None', color: 'gray' },
  { value: 'pii', label: 'PII', color: 'yellow' },
  { value: 'phi', label: 'PHI', color: 'orange' },
  { value: 'pci', label: 'PCI', color: 'red' },
  { value: 'sensitive', label: 'Sensitive', color: 'purple' },
] as const

// Name-based type suggestions
const TYPE_SUGGESTIONS: Record<string, DataType> = {
  id: 'bigint',
  _id: 'string',
  name: 'string',
  email: 'string',
  phone: 'string',
  address: 'string',
  amount: 'decimal',
  price: 'decimal',
  cost: 'decimal',
  total: 'decimal',
  quantity: 'integer',
  count: 'integer',
  age: 'integer',
  date: 'date',
  created: 'timestamp',
  updated: 'timestamp',
  modified: 'timestamp',
  timestamp: 'timestamp',
  flag: 'boolean',
  is_: 'boolean',
  has_: 'boolean',
  active: 'boolean',
  enabled: 'boolean',
  lat: 'double',
  lng: 'double',
  longitude: 'double',
  latitude: 'double',
}

function suggestType(columnName: string): DataType {
  const lower = columnName.toLowerCase()

  // Exact match
  if (TYPE_SUGGESTIONS[lower]) return TYPE_SUGGESTIONS[lower]

  // Prefix match
  for (const [prefix, type] of Object.entries(TYPE_SUGGESTIONS)) {
    if (prefix.endsWith('_') && lower.startsWith(prefix)) return type
  }

  // Suffix match
  if (lower.endsWith('_id') || lower.endsWith('id')) return 'bigint'
  if (lower.endsWith('_at') || lower.endsWith('_date') || lower.endsWith('_time')) return 'timestamp'
  if (lower.endsWith('_amount') || lower.endsWith('_price') || lower.endsWith('_cost')) return 'decimal'
  if (lower.endsWith('_count') || lower.endsWith('_num') || lower.endsWith('_qty')) return 'integer'
  if (lower.startsWith('is_') || lower.startsWith('has_')) return 'boolean'

  return 'string'
}

function suggestPII(columnName: string): ColumnDefinition['pii'] {
  const lower = columnName.toLowerCase()
  if (/ssn|social_security|tax_id|national_id/.test(lower)) return 'pii'
  if (/email|phone|address|zip|postal/.test(lower)) return 'pii'
  if (/credit_card|card_number|cvv|account_number/.test(lower)) return 'pci'
  if (/diagnosis|medication|patient|medical/.test(lower)) return 'phi'
  if (/password|secret|token|api_key/.test(lower)) return 'sensitive'
  return 'none'
}

export function SchemaDefinitionPanel({
  schema,
  onChange,
  feedName = '',
  sourceType = '',
}: SchemaDefinitionPanelProps) {
  const [mode, setMode] = useState<SchemaMode>('manual')
  const [newColumnName, setNewColumnName] = useState('')
  const [sampleFile, setSampleFile] = useState<File | null>(null)
  const [inferLoading, setInferLoading] = useState(false)

  const columns = schema.columns || []

  const updateColumns = useCallback(
    (newColumns: ColumnDefinition[]) => {
      onChange({
        ...schema,
        columns: newColumns,
        primary_keys: newColumns.filter((c) => c.pk).map((c) => c.name),
      })
    },
    [schema, onChange]
  )

  const addColumn = useCallback(() => {
    if (!newColumnName.trim()) return

    const name = newColumnName.trim().toLowerCase().replace(/\s+/g, '_')

    // Check for duplicates
    if (columns.some((c) => c.name === name)) return

    const newCol: ColumnDefinition = {
      name,
      type: suggestType(name),
      nullable: true,
      pii: suggestPII(name),
      pk: false,
    }

    updateColumns([...columns, newCol])
    setNewColumnName('')
  }, [newColumnName, columns, updateColumns])

  const updateColumn = useCallback(
    (index: number, field: keyof ColumnDefinition, value: any) => {
      const updated = [...columns]
      updated[index] = { ...updated[index], [field]: value }
      updateColumns(updated)
    },
    [columns, updateColumns]
  )

  const removeColumn = useCallback(
    (index: number) => {
      updateColumns(columns.filter((_, i) => i !== index))
    },
    [columns, updateColumns]
  )

  const handleAutoInfer = useCallback(async () => {
    if (!sampleFile) return
    setInferLoading(true)

    try {
      const formData = new FormData()
      formData.append('file', sampleFile)

      const response = await fetch('/api/v2/data-agent/infer-schema', {
        method: 'POST',
        body: formData,
      })

      if (response.ok) {
        const result = await response.json()
        const inferredColumns: ColumnDefinition[] = (result.columns || []).map(
          (col: any) => ({
            name: col.name,
            type: col.type || suggestType(col.name),
            nullable: col.nullable ?? true,
            pii: suggestPII(col.name),
            pk: col.pk || false,
          })
        )
        updateColumns(inferredColumns)
      }
    } catch {
      // Fallback: if API not available, show error
      console.error('Schema inference not available')
    } finally {
      setInferLoading(false)
    }
  }, [sampleFile, updateColumns])

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-gray-900">Schema Definition</h3>
        <span className="text-sm text-gray-500">{columns.length} columns</span>
      </div>

      {/* Mode selector tabs */}
      <div className="flex gap-1 bg-gray-100 rounded-lg p-1">
        {[
          { key: 'manual' as const, label: 'Manual Entry' },
          { key: 'auto-infer' as const, label: 'Auto-Infer' },
          { key: 'import-source' as const, label: 'Import' },
          { key: 'clone' as const, label: 'Clone' },
        ].map((tab) => (
          <button
            key={tab.key}
            type="button"
            onClick={() => setMode(tab.key)}
            className={`flex-1 py-2 px-3 text-sm rounded-md transition-colors ${
              mode === tab.key
                ? 'bg-white text-gray-900 shadow-sm font-medium'
                : 'text-gray-500 hover:text-gray-700'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Auto-infer mode */}
      {mode === 'auto-infer' && (
        <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg space-y-3">
          <p className="text-sm text-blue-700">
            Upload a sample file and we&apos;ll infer the schema automatically.
          </p>
          <div className="flex gap-3">
            <input
              type="file"
              accept=".csv,.json,.parquet,.avro,.xlsx"
              onChange={(e) => setSampleFile(e.target.files?.[0] || null)}
              className="flex-1 text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-medium file:bg-blue-100 file:text-blue-700 hover:file:bg-blue-200"
            />
            <button
              type="button"
              onClick={handleAutoInfer}
              disabled={!sampleFile || inferLoading}
              className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {inferLoading ? 'Inferring...' : 'Infer Schema'}
            </button>
          </div>
        </div>
      )}

      {/* Import from source mode */}
      {mode === 'import-source' && (
        <div className="p-4 bg-green-50 border border-green-200 rounded-lg">
          <p className="text-sm text-green-700">
            Connect to your source database and import the table schema via INFORMATION_SCHEMA.
            This feature requires the source connection to be configured first.
          </p>
          <button
            type="button"
            disabled
            className="mt-3 px-4 py-2 bg-green-100 text-green-700 rounded-md text-sm font-medium cursor-not-allowed opacity-50"
          >
            Import from Source (Coming Soon)
          </button>
        </div>
      )}

      {/* Clone mode */}
      {mode === 'clone' && (
        <div className="p-4 bg-purple-50 border border-purple-200 rounded-lg">
          <p className="text-sm text-purple-700">
            Clone schema from an existing feed. Select a feed to copy its schema definition.
          </p>
          <button
            type="button"
            disabled
            className="mt-3 px-4 py-2 bg-purple-100 text-purple-700 rounded-md text-sm font-medium cursor-not-allowed opacity-50"
          >
            Select Feed to Clone (Coming Soon)
          </button>
        </div>
      )}

      {/* Add column input (manual mode) */}
      {mode === 'manual' && (
        <div className="flex gap-2">
          <input
            type="text"
            value={newColumnName}
            onChange={(e) => setNewColumnName(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && addColumn()}
            placeholder="Enter column name..."
            className="flex-1 px-3 py-2 border border-gray-300 rounded-md text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
          <button
            type="button"
            onClick={addColumn}
            disabled={!newColumnName.trim()}
            className="px-4 py-2 bg-gray-900 text-white rounded-md text-sm font-medium hover:bg-gray-800 disabled:opacity-50"
          >
            + Add
          </button>
        </div>
      )}

      {/* Column table (all modes converge here) */}
      {columns.length > 0 && (
        <div className="border border-gray-200 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Column</th>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                <th className="px-3 py-2 text-center text-xs font-medium text-gray-500 uppercase">Nullable</th>
                <th className="px-3 py-2 text-center text-xs font-medium text-gray-500 uppercase">PK</th>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">PII</th>
                <th className="px-3 py-2 w-10"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {columns.map((col, idx) => (
                <tr key={col.name} className="hover:bg-gray-50">
                  <td className="px-3 py-2">
                    <code className="text-sm font-mono text-gray-900">{col.name}</code>
                  </td>
                  <td className="px-3 py-2">
                    <select
                      value={col.type}
                      onChange={(e) => updateColumn(idx, 'type', e.target.value)}
                      className="w-full px-2 py-1 border border-gray-200 rounded text-xs bg-white"
                    >
                      {DATA_TYPES.map((t) => (
                        <option key={t} value={t}>{t.toUpperCase()}</option>
                      ))}
                    </select>
                  </td>
                  <td className="px-3 py-2 text-center">
                    <input
                      type="checkbox"
                      checked={col.nullable !== false}
                      onChange={(e) => updateColumn(idx, 'nullable', e.target.checked)}
                      className="w-4 h-4 text-blue-600 rounded"
                    />
                  </td>
                  <td className="px-3 py-2 text-center">
                    <input
                      type="checkbox"
                      checked={col.pk === true}
                      onChange={(e) => updateColumn(idx, 'pk', e.target.checked)}
                      className="w-4 h-4 text-blue-600 rounded"
                    />
                  </td>
                  <td className="px-3 py-2">
                    <select
                      value={col.pii || 'none'}
                      onChange={(e) => updateColumn(idx, 'pii', e.target.value)}
                      className="w-full px-2 py-1 border border-gray-200 rounded text-xs bg-white"
                    >
                      {PII_LEVELS.map((p) => (
                        <option key={p.value} value={p.value}>{p.label}</option>
                      ))}
                    </select>
                  </td>
                  <td className="px-3 py-2">
                    <button
                      type="button"
                      onClick={() => removeColumn(idx)}
                      className="text-gray-400 hover:text-red-500 transition-colors"
                    >
                      ✕
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {columns.length === 0 && (
        <div className="text-center py-8 text-gray-400 text-sm">
          No columns defined yet. Add columns above or use Auto-Infer to get started.
        </div>
      )}

      {/* Smart suggestions */}
      {columns.length > 0 && (
        <SchemaSuggestions columns={columns} />
      )}
    </div>
  )
}

function SchemaSuggestions({ columns }: { columns: ColumnDefinition[] }) {
  const suggestions: Array<{ type: 'info' | 'warning' | 'error'; message: string }> = []

  // Check for primary key
  if (!columns.some((c) => c.pk)) {
    const idCol = columns.find((c) => c.name.endsWith('_id') || c.name === 'id')
    if (idCol) {
      suggestions.push({
        type: 'info',
        message: `Consider marking "${idCol.name}" as primary key`,
      })
    }
  }

  // Check for partition candidate
  const dateCol = columns.find((c) =>
    c.type === 'date' || c.type === 'timestamp' || c.name.includes('date')
  )
  if (dateCol) {
    suggestions.push({
      type: 'info',
      message: `Consider partitioning by "${dateCol.name}" for better query performance`,
    })
  }

  // Check for PII columns that might need encryption
  const piiCols = columns.filter((c) => c.pii && c.pii !== 'none')
  if (piiCols.length > 0) {
    suggestions.push({
      type: 'warning',
      message: `${piiCols.length} column(s) marked as sensitive - FPE encryption will be applied automatically`,
    })
  }

  if (suggestions.length === 0) return null

  const colors = {
    info: 'bg-blue-50 border-blue-200 text-blue-700',
    warning: 'bg-yellow-50 border-yellow-200 text-yellow-700',
    error: 'bg-red-50 border-red-200 text-red-700',
  }

  return (
    <div className="space-y-2">
      {suggestions.map((s, i) => (
        <div key={i} className={`px-3 py-2 text-xs rounded-md border ${colors[s.type]}`}>
          {s.message}
        </div>
      ))}
    </div>
  )
}

export default SchemaDefinitionPanel
