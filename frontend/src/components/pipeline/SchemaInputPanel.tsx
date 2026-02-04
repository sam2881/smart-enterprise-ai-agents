'use client'

/**
 * SchemaInputPanel - Multi-Mode Schema Input Component
 *
 * Supports:
 * 1. Manual column entry (existing)
 * 2. Upload schema (JSON/YAML)
 * 3. Paste schema text
 * 4. Infer from source
 *
 * Tracks:
 * - Schema version
 * - Column lineage (inferred vs user-defined)
 * - Business descriptions
 * - Sensitivity classification
 */

import React, { useState, useCallback, useRef } from 'react'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Textarea } from '@/components/ui/Textarea'
import { Card } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { cn } from '@/lib/utils'
import { ColumnDefinition, DataType, PIIClassification, V2QualityRule } from '@/types/pipeline-canonical'
import {
  Upload,
  FileText,
  Wand2,
  Plus,
  Trash2,
  FileJson,
  ClipboardPaste,
  Sparkles,
  AlertCircle,
  CheckCircle,
  Key,
  Shield,
  Eye,
  EyeOff,
  ChevronDown,
  ChevronRight,
  Clock,
} from 'lucide-react'

// =============================================================================
// Types
// =============================================================================

interface ExtendedColumnDefinition extends ColumnDefinition {
  source?: 'inferred' | 'user_defined' | 'uploaded'
  business_description?: string
  description?: string  // General description field
  sensitivity?: 'public' | 'internal' | 'confidential' | 'restricted'
  is_business_key?: boolean
  is_partition_key?: boolean
  is_watermark?: boolean
  quality_rules?: V2QualityRule[]
}

interface SchemaInputPanelProps {
  columns: ColumnDefinition[]
  onChange: (columns: ColumnDefinition[]) => void
  sourceType?: string
  className?: string
  v2Mode?: boolean
  onBusinessKeysChange?: (keys: string[]) => void
  onPartitionKeysChange?: (keys: string[]) => void
  onWatermarkColumnChange?: (column: string | undefined) => void
  onQualityRulesChange?: (rules: V2QualityRule[]) => void
}

const PII_OPTIONS: { value: PIIClassification; label: string }[] = [
  { value: 'none', label: 'None' },
  { value: 'pii', label: 'PII' },
  { value: 'phi', label: 'PHI' },
  { value: 'pci', label: 'PCI' },
  { value: 'sensitive', label: 'Sensitive' },
]

const QUALITY_RULE_TYPES = [
  { value: 'not_null', label: 'Not Null' },
  { value: 'unique', label: 'Unique' },
  { value: 'range', label: 'Range' },
  { value: 'regex', label: 'Regex' },
  { value: 'in_set', label: 'In Set' },
]

type InputMode = 'manual' | 'upload' | 'paste' | 'infer'

// =============================================================================
// Constants
// =============================================================================

const DATA_TYPES: { value: DataType; label: string }[] = [
  { value: 'string', label: 'STRING' },
  { value: 'integer', label: 'INTEGER' },
  { value: 'bigint', label: 'BIGINT' },
  { value: 'float', label: 'FLOAT' },
  { value: 'double', label: 'DOUBLE' },
  { value: 'decimal', label: 'DECIMAL' },
  { value: 'boolean', label: 'BOOLEAN' },
  { value: 'date', label: 'DATE' },
  { value: 'timestamp', label: 'TIMESTAMP' },
  { value: 'binary', label: 'BINARY' },
  { value: 'array', label: 'ARRAY' },
  { value: 'map', label: 'MAP' },
  { value: 'struct', label: 'STRUCT' },
]

const SENSITIVITY_OPTIONS = [
  { value: 'public', label: 'Public' },
  { value: 'internal', label: 'Internal' },
  { value: 'confidential', label: 'Confidential' },
  { value: 'restricted', label: 'Restricted (PII/PHI)' },
]

// =============================================================================
// Quality Rule Adder Sub-Component
// =============================================================================

function QualityRuleAdder({ onAdd }: { onAdd: (rule: V2QualityRule) => void }) {
  const [ruleType, setRuleType] = useState('')
  const [paramStr, setParamStr] = useState('')

  const handleAdd = () => {
    if (!ruleType) return
    let params: Record<string, any> = {}
    if (paramStr.trim()) {
      try {
        params = JSON.parse(paramStr)
      } catch {
        // Treat as simple value based on rule type
        if (ruleType === 'range') {
          const parts = paramStr.split(',').map((s) => s.trim())
          params = { min: parts[0], max: parts[1] }
        } else if (ruleType === 'regex') {
          params = { pattern: paramStr }
        } else if (ruleType === 'in_set') {
          params = { values: paramStr.split(',').map((s) => s.trim()) }
        }
      }
    }
    onAdd({ rule_type: ruleType, params, severity: 'ERROR' })
    setRuleType('')
    setParamStr('')
  }

  return (
    <div className="flex items-center gap-2 mt-1">
      <select
        value={ruleType}
        onChange={(e) => setRuleType(e.target.value)}
        className="px-2 py-1 text-xs rounded bg-gray-800/50 text-white border border-white/10 focus:outline-none focus:ring-1 focus:ring-blue-500/50"
        style={{ colorScheme: 'dark' }}
      >
        <option value="">Add rule...</option>
        {QUALITY_RULE_TYPES.map((rt) => (
          <option key={rt.value} value={rt.value} className="bg-gray-800">{rt.label}</option>
        ))}
      </select>
      {(ruleType === 'range' || ruleType === 'regex' || ruleType === 'in_set') && (
        <input
          placeholder={ruleType === 'range' ? 'min, max' : ruleType === 'regex' ? 'pattern' : 'val1, val2, ...'}
          value={paramStr}
          onChange={(e) => setParamStr(e.target.value)}
          className="px-2 py-1 text-xs rounded bg-gray-800/50 text-white border border-white/10 focus:outline-none focus:ring-1 focus:ring-blue-500/50 flex-1"
        />
      )}
      <button
        onClick={handleAdd}
        disabled={!ruleType}
        className="px-2 py-1 text-xs rounded bg-blue-600 text-white hover:bg-blue-500 disabled:opacity-50"
      >
        +
      </button>
    </div>
  )
}

// =============================================================================
// Main Component
// =============================================================================

export function SchemaInputPanel({
  columns,
  onChange,
  sourceType,
  className,
  v2Mode = false,
  onBusinessKeysChange,
  onPartitionKeysChange,
  onWatermarkColumnChange,
  onQualityRulesChange,
}: SchemaInputPanelProps) {
  const [inputMode, setInputMode] = useState<InputMode>('manual')
  const [pasteText, setPasteText] = useState('')
  const [parseError, setParseError] = useState<string | null>(null)
  const [isInferring, setIsInferring] = useState(false)
  const [expandedColumns, setExpandedColumns] = useState<Set<number>>(new Set())
  const fileInputRef = useRef<HTMLInputElement>(null)

  // Extended columns with metadata
  const extendedColumns = columns as ExtendedColumnDefinition[]

  // Toggle column expansion
  const toggleColumn = (index: number) => {
    setExpandedColumns((prev) => {
      const next = new Set(prev)
      if (next.has(index)) {
        next.delete(index)
      } else {
        next.add(index)
      }
      return next
    })
  }

  // Add new column
  const addColumn = useCallback(() => {
    const newColumn: ExtendedColumnDefinition = {
      name: '',
      type: 'string',
      nullable: true,
      source: 'user_defined',
    }
    onChange([...columns, newColumn])
  }, [columns, onChange])

  // Update column
  const updateColumn = useCallback(
    (index: number, updates: Partial<ExtendedColumnDefinition>) => {
      const updated = [...columns]
      updated[index] = { ...updated[index], ...updates }
      onChange(updated)
    },
    [columns, onChange]
  )

  // Remove column
  const removeColumn = useCallback(
    (index: number) => {
      onChange(columns.filter((_, i) => i !== index))
    },
    [columns, onChange]
  )

  // Parse pasted schema
  const handleParsePaste = useCallback(() => {
    setParseError(null)
    try {
      // Try JSON first
      let parsed: any
      try {
        parsed = JSON.parse(pasteText)
      } catch {
        // Try YAML-like simple format (name: type)
        const lines = pasteText.split('\n').filter((l) => l.trim())
        parsed = lines.map((line) => {
          const parts = line.split(/[:\t,]+/).map((p) => p.trim())
          return {
            name: parts[0],
            type: parts[1] || 'string',
            nullable: parts[2] !== 'NOT NULL',
          }
        })
      }

      // Normalize parsed data
      const newColumns: ExtendedColumnDefinition[] = Array.isArray(parsed)
        ? parsed.map((col: any) => ({
            name: col.name || col.column_name || col.field || '',
            type: (col.type || col.data_type || 'string').toLowerCase() as DataType,
            nullable: col.nullable !== false,
            description: col.description || col.comment,
            source: 'uploaded' as const,
          }))
        : Object.entries(parsed).map(([name, type]) => ({
            name,
            type: (typeof type === 'string' ? type : 'string').toLowerCase() as DataType,
            nullable: true,
            source: 'uploaded' as const,
          }))

      onChange(newColumns)
      setPasteText('')
      setInputMode('manual')
    } catch (err) {
      setParseError('Failed to parse schema. Supported formats: JSON array, JSON object, or "name: type" per line.')
    }
  }, [pasteText, onChange])

  // Handle file upload
  const handleFileUpload = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const file = event.target.files?.[0]
      if (!file) return

      const reader = new FileReader()
      reader.onload = (e) => {
        const content = e.target?.result as string
        setPasteText(content)
        setInputMode('paste')
      }
      reader.readAsText(file)
    },
    []
  )

  // Simulate schema inference (would call backend in real implementation)
  const handleInferSchema = useCallback(async () => {
    setIsInferring(true)
    // Simulate API call
    await new Promise((resolve) => setTimeout(resolve, 1500))

    // Mock inferred schema based on source type
    const inferredColumns: ExtendedColumnDefinition[] = [
      { name: 'id', type: 'string', nullable: false, source: 'inferred', is_business_key: true },
      { name: 'created_at', type: 'timestamp', nullable: false, source: 'inferred' },
      { name: 'updated_at', type: 'timestamp', nullable: true, source: 'inferred' },
      { name: 'data', type: 'string', nullable: true, source: 'inferred' },
    ]

    onChange(inferredColumns)
    setIsInferring(false)
    setInputMode('manual')
  }, [onChange])

  return (
    <div className={cn('space-y-4', className)}>
      {/* Input Mode Selector */}
      <div className="flex items-center gap-2 p-1 bg-gray-800/30 rounded-lg">
        <button
          onClick={() => setInputMode('manual')}
          className={cn(
            'flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors',
            inputMode === 'manual'
              ? 'bg-blue-900/300/20 text-blue-400 border border-blue-500/30'
              : 'text-gray-400 hover:text-white hover:bg-gray-800/5'
          )}
        >
          <Plus className="w-4 h-4" />
          Manual
        </button>
        <button
          onClick={() => fileInputRef.current?.click()}
          className={cn(
            'flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors',
            inputMode === 'upload'
              ? 'bg-purple-500/20 text-purple-400 border border-purple-500/30'
              : 'text-gray-400 hover:text-white hover:bg-gray-800/5'
          )}
        >
          <Upload className="w-4 h-4" />
          Upload
        </button>
        <button
          onClick={() => setInputMode('paste')}
          className={cn(
            'flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors',
            inputMode === 'paste'
              ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
              : 'text-gray-400 hover:text-white hover:bg-gray-800/5'
          )}
        >
          <ClipboardPaste className="w-4 h-4" />
          Paste
        </button>
        <button
          onClick={handleInferSchema}
          disabled={isInferring}
          className={cn(
            'flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors',
            inputMode === 'infer'
              ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30'
              : 'text-gray-400 hover:text-white hover:bg-gray-800/5',
            isInferring && 'opacity-50 cursor-not-allowed'
          )}
        >
          <Wand2 className={cn('w-4 h-4', isInferring && 'animate-spin')} />
          {isInferring ? 'Inferring...' : 'Infer'}
        </button>
        <input
          ref={fileInputRef}
          type="file"
          accept=".json,.yaml,.yml,.csv"
          onChange={handleFileUpload}
          className="hidden"
        />
      </div>

      {/* Paste Input */}
      {inputMode === 'paste' && (
        <Card className="p-4 space-y-3 border-emerald-500/30">
          <div className="flex items-center gap-2 text-emerald-400">
            <FileJson className="w-5 h-5" />
            <span className="font-medium">Paste Schema</span>
          </div>
          <Textarea
            placeholder={`Paste schema in JSON, YAML, or simple format:

JSON: [{"name": "id", "type": "string"}, ...]
Simple:
  id: string
  name: string
  amount: decimal`}
            value={pasteText}
            onChange={(e) => setPasteText(e.target.value)}
            rows={8}
            className="font-mono text-sm"
          />
          {parseError && (
            <div className="flex items-center gap-2 text-red-400 text-sm">
              <AlertCircle className="w-4 h-4" />
              {parseError}
            </div>
          )}
          <div className="flex justify-end gap-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                setInputMode('manual')
                setPasteText('')
              }}
            >
              Cancel
            </Button>
            <Button
              variant="primary"
              size="sm"
              onClick={handleParsePaste}
              disabled={!pasteText.trim()}
            >
              <Sparkles className="w-4 h-4 mr-1" />
              Parse Schema
            </Button>
          </div>
        </Card>
      )}

      {/* Column List */}
      {inputMode === 'manual' && (
        <>
          {columns.length > 0 ? (
            <div className="space-y-2">
              {extendedColumns.map((column, index) => (
                <div
                  key={index}
                  className="border border-white/10 rounded-lg overflow-hidden"
                >
                  {/* Column Header */}
                  <div className="flex items-center gap-3 p-3 bg-gray-800/30">
                    <button
                      onClick={() => toggleColumn(index)}
                      className="text-gray-400 hover:text-white"
                    >
                      {expandedColumns.has(index) ? (
                        <ChevronDown className="w-4 h-4" />
                      ) : (
                        <ChevronRight className="w-4 h-4" />
                      )}
                    </button>

                    <Input
                      placeholder="Column name"
                      value={column.name}
                      onChange={(e) => updateColumn(index, { name: e.target.value })}
                      className="flex-1 !py-1.5"
                    />

                    <Select
                      value={column.type}
                      onChange={(e) => updateColumn(index, { type: e.target.value as DataType })}
                      options={DATA_TYPES}
                      className="w-32 !py-1.5"
                    />

                    <label className="flex items-center gap-1.5 text-sm text-gray-400">
                      <input
                        type="checkbox"
                        checked={column.nullable !== false}
                        onChange={(e) => updateColumn(index, { nullable: e.target.checked })}
                        className="rounded bg-gray-700 border-gray-600"
                      />
                      Null
                    </label>

                    {column.source && (
                      <Badge
                        variant={column.source === 'inferred' ? 'warning' : 'info'}
                        size="sm"
                      >
                        {column.source === 'inferred' ? 'Inferred' : 'Manual'}
                      </Badge>
                    )}

                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => removeColumn(index)}
                      className="text-red-400 hover:text-red-300"
                    >
                      <Trash2 className="w-4 h-4" />
                    </Button>
                  </div>

                  {/* Expanded Details */}
                  {expandedColumns.has(index) && (
                    <div className="p-3 bg-gray-900/30 border-t border-white/5 space-y-3">
                      <div className="grid grid-cols-2 gap-3">
                        <Input
                          label="Business Description"
                          placeholder="What does this column represent?"
                          value={column.description || ''}
                          onChange={(e) => updateColumn(index, { description: e.target.value })}
                        />
                        <Select
                          label="Sensitivity"
                          value={(column as ExtendedColumnDefinition).sensitivity || 'internal'}
                          onChange={(e) =>
                            updateColumn(index, { sensitivity: e.target.value } as any)
                          }
                          options={SENSITIVITY_OPTIONS}
                        />
                      </div>

                      {/* PII Classification Dropdown */}
                      {v2Mode && (
                        <Select
                          label="PII Classification"
                          value={column.pii || 'none'}
                          onChange={(e) => {
                            updateColumn(index, { pii: e.target.value as PIIClassification })
                          }}
                          options={PII_OPTIONS}
                        />
                      )}

                      <div className="flex items-center gap-4 flex-wrap">
                        <label className="flex items-center gap-2 text-sm text-gray-400">
                          <input
                            type="checkbox"
                            checked={column.pk || false}
                            onChange={(e) => updateColumn(index, { pk: e.target.checked })}
                            className="rounded bg-gray-700 border-gray-600"
                          />
                          <Key className="w-3.5 h-3.5" />
                          Primary Key
                        </label>
                        <label className="flex items-center gap-2 text-sm text-gray-400">
                          <input
                            type="checkbox"
                            checked={(column as ExtendedColumnDefinition).is_business_key || false}
                            onChange={(e) => {
                              updateColumn(index, { is_business_key: e.target.checked } as any)
                              if (v2Mode) {
                                const updated = [...columns] as ExtendedColumnDefinition[]
                                updated[index] = { ...updated[index], is_business_key: e.target.checked }
                                const bKeys = updated.filter((c) => c.is_business_key).map((c) => c.name)
                                onBusinessKeysChange?.(bKeys)
                              }
                            }}
                            className="rounded bg-gray-700 border-gray-600"
                          />
                          <Shield className="w-3.5 h-3.5" />
                          Business Key
                        </label>

                        {v2Mode && (
                          <>
                            <label className="flex items-center gap-2 text-sm text-gray-400">
                              <input
                                type="checkbox"
                                checked={(column as ExtendedColumnDefinition).is_partition_key || false}
                                onChange={(e) => {
                                  updateColumn(index, { is_partition_key: e.target.checked } as any)
                                  const updated = [...columns] as ExtendedColumnDefinition[]
                                  updated[index] = { ...updated[index], is_partition_key: e.target.checked }
                                  const pKeys = updated.filter((c) => c.is_partition_key).map((c) => c.name)
                                  onPartitionKeysChange?.(pKeys)
                                }}
                                className="rounded bg-gray-700 border-gray-600"
                              />
                              Partition Key
                            </label>
                            <label className="flex items-center gap-2 text-sm text-gray-400">
                              <input
                                type="radio"
                                name="watermark_column"
                                checked={(column as ExtendedColumnDefinition).is_watermark || false}
                                onChange={() => {
                                  // Unset all others, set this one
                                  const updated = (columns as ExtendedColumnDefinition[]).map((c, i) => ({
                                    ...c,
                                    is_watermark: i === index,
                                  }))
                                  onChange(updated)
                                  onWatermarkColumnChange?.(column.name)
                                }}
                                className="bg-gray-700 border-gray-600"
                              />
                              <Clock className="w-3.5 h-3.5" />
                              Watermark
                            </label>
                          </>
                        )}

                        {!v2Mode && (
                          <label className="flex items-center gap-2 text-sm text-gray-400">
                            <input
                              type="checkbox"
                              checked={column.pii === 'pii' || column.pii === 'phi' || column.pii === 'pci' || column.pii === 'sensitive'}
                              onChange={(e) => updateColumn(index, { pii: e.target.checked ? 'pii' : 'none' })}
                              className="rounded bg-gray-700 border-gray-600"
                            />
                            <EyeOff className="w-3.5 h-3.5" />
                            PII
                          </label>
                        )}
                      </div>

                      {/* V2 Quality Rule Builder */}
                      {v2Mode && (
                        <div className="border-t border-gray-700 pt-3 mt-2">
                          <p className="text-xs font-medium text-gray-400 mb-2">Quality Rules</p>
                          {((column as ExtendedColumnDefinition).quality_rules || []).map((rule, ri) => (
                            <div key={ri} className="flex items-center gap-2 mb-1 text-xs text-gray-300">
                              <span className="px-1.5 py-0.5 bg-gray-700 rounded">{rule.rule_type}</span>
                              <span className="text-gray-500">{JSON.stringify(rule.params)}</span>
                              <button
                                onClick={() => {
                                  const rules = [...((column as ExtendedColumnDefinition).quality_rules || [])]
                                  rules.splice(ri, 1)
                                  updateColumn(index, { quality_rules: rules } as any)
                                  // Collect all rules across columns
                                  const allRules = (columns as ExtendedColumnDefinition[]).flatMap((c, ci) =>
                                    (ci === index ? rules : (c.quality_rules || [])).map((r) => ({ ...r, column: c.name }))
                                  )
                                  onQualityRulesChange?.(allRules)
                                }}
                                className="text-red-400 hover:text-red-300"
                              >
                                <Trash2 className="w-3 h-3" />
                              </button>
                            </div>
                          ))}
                          <QualityRuleAdder
                            onAdd={(rule) => {
                              const rules = [...((column as ExtendedColumnDefinition).quality_rules || []), rule]
                              updateColumn(index, { quality_rules: rules } as any)
                              const allRules = (columns as ExtendedColumnDefinition[]).flatMap((c, ci) =>
                                (ci === index ? rules : (c.quality_rules || [])).map((r) => ({ ...r, column: c.name }))
                              )
                              onQualityRulesChange?.(allRules)
                            }}
                          />
                        </div>
                      )}
                    </div>
                  )}
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-8 border-2 border-dashed border-gray-700 rounded-lg">
              <FileText className="w-8 h-8 mx-auto text-gray-400 mb-2" />
              <p className="text-gray-400 text-sm">No columns defined</p>
              <p className="text-gray-400 text-xs mt-1">
                Add columns manually, upload a schema file, or infer from source
              </p>
            </div>
          )}

          {/* Add Column Button */}
          <Button onClick={addColumn} variant="outline" className="w-full">
            <Plus className="w-4 h-4 mr-2" />
            Add Column
          </Button>
        </>
      )}

      {/* Schema Summary */}
      {columns.length > 0 && (
        <div className="flex items-center gap-4 text-xs text-gray-400">
          <span>{columns.length} columns</span>
          <span>|</span>
          <span>{columns.filter((c) => c.pk).length} primary keys</span>
          <span>|</span>
          <span>{columns.filter((c) => c.pii && c.pii !== 'none').length} PII columns</span>
          <span>|</span>
          <span>
            {(extendedColumns.filter((c) => c.source === 'inferred').length)} inferred
          </span>
        </div>
      )}
    </div>
  )
}

export default SchemaInputPanel
