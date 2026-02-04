'use client'

/**
 * NLTransformInput - Natural Language & SQL Transformation Input Component (v2.0)
 *
 * Allows users to describe complex transformations via multiple input modes:
 * 1. Natural Language - Describe in plain English
 * 2. SQL Copy-Paste - Paste T-SQL/Spark SQL directly
 * 3. Structured Builder - Visual form-based configuration
 *
 * IMPORTANT: All inputs are ALWAYS converted to structured metadata first.
 * Natural language and SQL are never executed directly.
 *
 * Features:
 * - Natural language input with LLM-powered code generation
 * - SQL copy-paste with T-SQL to PySpark conversion
 * - Structured transform builder (joins, aggregations, windows)
 * - Real-time conversion to structured TransformConfig
 * - Confidence scoring and validation
 * - Preview of generated PySpark/SQL code
 * - Transformation templates/presets
 */

import React, { useState, useCallback } from 'react'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { cn } from '@/lib/utils'
import {
  TransformConfig,
  TransformType,
  TargetZone,
  NLTransformInput as NLTransformInputType,
  ColumnDefinition,
} from '@/types/pipeline-canonical'
import { api } from '@/lib/api'

// =============================================================================
// Props
// =============================================================================

interface NLTransformInputProps {
  zone: TargetZone
  schema: ColumnDefinition[]
  onTransformAdd: (transform: Partial<TransformConfig>) => void
  className?: string
}

interface TransformPreview {
  transform_type: TransformType
  config: Record<string, any>
  pyspark_code: string
  sql_code?: string
  confidence: number
  warnings: string[]
}

// =============================================================================
// Transform Input Modes
// =============================================================================

type TransformInputMode = 'natural_language' | 'sql_paste' | 'structured_builder'

// =============================================================================
// Transform Templates
// =============================================================================

interface TransformTemplate {
  name: string
  description: string
  transform_type: TransformType
  config: Record<string, any>
  example_nl: string
}

const TRANSFORM_TEMPLATES: TransformTemplate[] = [
  {
    name: 'Running Total',
    description: 'Calculate cumulative sum partitioned by a key',
    transform_type: 'window',
    config: {
      partition_by: [],
      order_by: [],
      functions: [{ type: 'running_sum', column: '', alias: 'running_total' }],
    },
    example_nl: 'Calculate running total of {column} partitioned by {partition_column}',
  },
  {
    name: 'Row Number',
    description: 'Add row number for deduplication',
    transform_type: 'window',
    config: {
      partition_by: [],
      order_by: [{ column: '', direction: 'desc' }],
      functions: [{ type: 'row_number', alias: 'row_num' }],
    },
    example_nl: 'Add row number partitioned by {partition_column} ordered by {order_column}',
  },
  {
    name: 'Group By Sum',
    description: 'Aggregate with sum by groups',
    transform_type: 'aggregate',
    config: {
      group_by: [],
      aggregations: [{ column: '', function: 'sum', alias: '' }],
    },
    example_nl: 'Sum {column} grouped by {group_columns}',
  },
  {
    name: 'Deduplicate',
    description: 'Remove duplicate rows',
    transform_type: 'dedup',
    config: {
      key_columns: [],
      order_column: '',
      order_direction: 'desc',
    },
    example_nl: 'Deduplicate by {key_columns} keeping latest by {order_column}',
  },
  {
    name: 'Null Handling',
    description: 'Fill or drop null values',
    transform_type: 'null_handling',
    config: {
      strategy: 'fill',
      columns: [],
      fill_value: '',
    },
    example_nl: 'Fill nulls in {columns} with {fill_value}',
  },
  {
    name: 'Derived Column',
    description: 'Create a calculated column',
    transform_type: 'derive',
    config: {
      expression: '',
      target_column: '',
    },
    example_nl: 'Create column {target_column} = {expression}',
  },
  {
    name: 'Filter Rows',
    description: 'Keep rows matching condition',
    transform_type: 'filter',
    config: {
      condition: '',
    },
    example_nl: 'Keep rows where {condition}',
  },
  {
    name: 'Type Cast',
    description: 'Convert column types',
    transform_type: 'cast',
    config: {
      types: {},
    },
    example_nl: 'Cast {column} to {type}',
  },
]

// =============================================================================
// Main Component
// =============================================================================

export function NLTransformInput({
  zone,
  schema,
  onTransformAdd,
  className,
}: NLTransformInputProps) {
  const [inputMode, setInputMode] = useState<TransformInputMode>('natural_language')
  const [nlDescription, setNlDescription] = useState('')
  const [sqlInput, setSqlInput] = useState('')
  const [isGenerating, setIsGenerating] = useState(false)
  const [preview, setPreview] = useState<TransformPreview | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Structured builder state
  const [selectedTemplate, setSelectedTemplate] = useState<TransformTemplate | null>(null)
  const [structuredConfig, setStructuredConfig] = useState<Record<string, any>>({})

  // Apply template
  const applyTemplate = useCallback((template: TransformTemplate) => {
    setSelectedTemplate(template)
    setStructuredConfig({ ...template.config })
    setInputMode('structured_builder')
  }, [])

  // Generate structured transform from natural language
  const generateFromNL = useCallback(async () => {
    if (!nlDescription.trim()) return

    setIsGenerating(true)
    setError(null)

    try {
      // IMPORTANT: This converts NL to structured metadata - NL is NEVER executed directly
      const response = await fetch('/api/v2/data-agent/nl/transform', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          description: nlDescription,
          schema: schema.map(c => ({ name: c.name, type: c.type })),
          zone,
        }),
      })

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: 'Failed to generate transform' }))
        throw new Error(errorData.error || 'Failed to generate transform')
      }

      const result = await response.json()

      setPreview({
        transform_type: result.transform_type || 'derive',
        config: result.config || {},
        pyspark_code: result.pyspark_code || '',
        sql_code: result.sql_code,
        confidence: result.confidence || 0.5,
        warnings: result.warnings || [],
      })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to generate transform')
    } finally {
      setIsGenerating(false)
    }
  }, [nlDescription, schema, zone])

  // Generate structured transform from SQL paste
  const generateFromSQL = useCallback(async () => {
    if (!sqlInput.trim()) return

    setIsGenerating(true)
    setError(null)

    try {
      // Convert SQL to structured metadata - SQL is NEVER executed directly
      const response = await fetch('/api/v2/data-agent/sql/transform', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: sqlInput,
          schema: schema.map(c => ({ name: c.name, type: c.type })),
          zone,
          source_dialect: 'tsql', // Assume T-SQL, backend will auto-detect
        }),
      })

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: 'Failed to convert SQL' }))
        throw new Error(errorData.error || 'Failed to convert SQL')
      }

      const result = await response.json()

      setPreview({
        transform_type: result.transform_type || 'sql',
        config: result.config || {},
        pyspark_code: result.pyspark_code || '',
        sql_code: result.spark_sql || sqlInput, // Converted Spark SQL
        confidence: result.confidence || 0.7,
        warnings: result.warnings || [],
      })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to convert SQL')
    } finally {
      setIsGenerating(false)
    }
  }, [sqlInput, schema, zone])

  // Generate from structured builder
  const generateFromStructured = useCallback(() => {
    if (!selectedTemplate) return

    setError(null)

    try {
      // Create preview from structured config
      const pyspark = generatePySparkFromConfig(selectedTemplate.transform_type, structuredConfig)

      setPreview({
        transform_type: selectedTemplate.transform_type,
        config: structuredConfig,
        pyspark_code: pyspark,
        confidence: 1.0, // Structured input is always 100% confidence
        warnings: [],
      })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Invalid configuration')
    }
  }, [selectedTemplate, structuredConfig])

  // Helper: Generate PySpark code from structured config
  const generatePySparkFromConfig = (
    transformType: TransformType,
    config: Record<string, any>
  ): string => {
    switch (transformType) {
      case 'window':
        const partitionBy = config.partition_by?.join(', ') || ''
        const orderBy = config.order_by?.map((o: any) =>
          `F.col("${o.column}").${o.direction === 'desc' ? 'desc' : 'asc'}()`
        ).join(', ') || ''
        const windowFuncs = config.functions?.map((f: any) => {
          if (f.type === 'row_number') return `F.row_number().over(window_spec).alias("${f.alias}")`
          if (f.type === 'rank') return `F.rank().over(window_spec).alias("${f.alias}")`
          if (f.type === 'running_sum') return `F.sum("${f.column}").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)).alias("${f.alias}")`
          return ''
        }).filter(Boolean).join('\n')
        return `window_spec = Window.partitionBy(${partitionBy}).orderBy(${orderBy})\ndf = df.withColumn(\n${windowFuncs}\n)`

      case 'aggregate':
        const groupBy = config.group_by?.map((c: string) => `"${c}"`).join(', ') || ''
        const aggs = config.aggregations?.map((a: any) =>
          `F.${a.function}("${a.column}").alias("${a.alias || a.column + '_' + a.function}")`
        ).join(',\n  ') || ''
        return `df = df.groupBy(${groupBy}).agg(\n  ${aggs}\n)`

      case 'dedup':
        const keyColumns = config.key_columns?.map((c: string) => `"${c}"`).join(', ') || ''
        if (config.order_column) {
          return `window_spec = Window.partitionBy(${keyColumns}).orderBy(F.col("${config.order_column}").${config.order_direction === 'desc' ? 'desc' : 'asc'}())\ndf = df.withColumn("_row_num", F.row_number().over(window_spec))\ndf = df.filter(F.col("_row_num") == 1).drop("_row_num")`
        }
        return `df = df.dropDuplicates([${keyColumns}])`

      case 'filter':
        return `df = df.filter("${config.condition}")`

      case 'derive':
        return `df = df.withColumn("${config.target_column}", F.expr("${config.expression}"))`

      case 'cast':
        return Object.entries(config.types || {})
          .map(([col, type]) => `df = df.withColumn("${col}", df["${col}"].cast("${type}"))`)
          .join('\n')

      case 'null_handling':
        if (config.strategy === 'drop') {
          const cols = config.columns?.length ? `subset=[${config.columns.map((c: string) => `"${c}"`).join(', ')}]` : ''
          return `df = df.dropna(${cols})`
        }
        return `df = df.fillna(${JSON.stringify(config.fill_value)}, subset=[${(config.columns || []).map((c: string) => `"${c}"`).join(', ')}])`

      default:
        return '# Custom transformation'
    }
  }

  // Add structured transform to pipeline (NOT the raw NL)
  const addTransform = useCallback(() => {
    if (!preview) return

    // Create structured transform configuration
    const transform: Partial<TransformConfig> = {
      transform_type: preview.transform_type,
      zone,
      config: preview.config,
      nl_description: nlDescription, // Store original NL for reference only
      generated_pyspark: preview.pyspark_code,
      generated_sql: preview.sql_code,
      is_active: true,
    }

    onTransformAdd(transform)

    // Reset form
    setNlDescription('')
    setPreview(null)
  }, [preview, zone, nlDescription, onTransformAdd])

  return (
    <div className={cn('space-y-4', className)}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-2xl">🔄</span>
          <h3 className="text-lg font-semibold text-white dark:text-white">
            Add Transformation
          </h3>
        </div>
        <Badge variant="info">
          {zone.toUpperCase()} Zone
        </Badge>
      </div>

      {/* Input Mode Tabs */}
      <div className="flex border-b border-gray-200 dark:border-gray-700">
        <button
          onClick={() => setInputMode('natural_language')}
          className={cn(
            'px-4 py-2 text-sm font-medium border-b-2 transition-colors',
            inputMode === 'natural_language'
              ? 'border-blue-500 text-blue-600 dark:text-blue-400'
              : 'border-transparent text-gray-400 hover:text-gray-300 dark:hover:text-gray-300'
          )}
        >
          📝 Natural Language
        </button>
        <button
          onClick={() => setInputMode('sql_paste')}
          className={cn(
            'px-4 py-2 text-sm font-medium border-b-2 transition-colors',
            inputMode === 'sql_paste'
              ? 'border-blue-500 text-blue-600 dark:text-blue-400'
              : 'border-transparent text-gray-400 hover:text-gray-300 dark:hover:text-gray-300'
          )}
        >
          💾 SQL Paste
        </button>
        <button
          onClick={() => setInputMode('structured_builder')}
          className={cn(
            'px-4 py-2 text-sm font-medium border-b-2 transition-colors',
            inputMode === 'structured_builder'
              ? 'border-blue-500 text-blue-600 dark:text-blue-400'
              : 'border-transparent text-gray-400 hover:text-gray-300 dark:hover:text-gray-300'
          )}
        >
          🔧 Structured Builder
        </button>
      </div>

      {/* Natural Language Input */}
      {inputMode === 'natural_language' && (
        <Card className="p-4 space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-1">
              Describe your transformation in plain English
            </label>
            <textarea
              value={nlDescription}
              onChange={(e) => setNlDescription(e.target.value)}
              placeholder="Example: Calculate running total of sales by customer_id, ordered by transaction_date descending, and add a rank column"
              rows={4}
              className="w-full px-3 py-2 border border-gray-600 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-400 dark:bg-gray-800 dark:text-white font-mono text-sm"
            />
            <div className="mt-2 flex items-start gap-2">
              <div className="text-blue-500 text-sm">💡</div>
              <div className="text-xs text-gray-400 space-y-1">
                <p><strong>Supported transformations:</strong></p>
                <ul className="list-disc list-inside ml-2">
                  <li>Filtering, joins, aggregations, window functions</li>
                  <li>Column derivations, type casting, renaming</li>
                  <li>Deduplication, sorting, pivots/unpivots</li>
                  <li>Data masking, hashing, encryption (for PII)</li>
                </ul>
              </div>
            </div>
          </div>

          <Button
            onClick={generateFromNL}
            disabled={!nlDescription.trim() || isGenerating}
            className="w-full"
          >
            {isGenerating ? (
              <>
                <svg className="animate-spin -ml-1 mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                Converting to structured metadata...
              </>
            ) : (
              '✨ Generate Structured Transform'
            )}
          </Button>
        </Card>
      )}

      {/* SQL Paste Input */}
      {inputMode === 'sql_paste' && (
        <Card className="p-4 space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-1">
              Paste your SQL (T-SQL, Spark SQL, or standard SQL)
            </label>
            <textarea
              value={sqlInput}
              onChange={(e) => setSqlInput(e.target.value)}
              placeholder={`-- Example T-SQL:
SELECT
    customer_id,
    SUM(amount) as total_amount,
    COUNT(*) as order_count,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_amount DESC) as rank
FROM orders
WHERE status = 'completed'
GROUP BY customer_id, region`}
              rows={10}
              className="w-full px-3 py-2 border border-gray-600 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-400 dark:bg-gray-800 dark:text-white font-mono text-sm"
            />
            <div className="mt-2 flex items-start gap-2">
              <div className="text-green-500 text-sm">💾</div>
              <div className="text-xs text-gray-400 space-y-1">
                <p><strong>Supported SQL dialects:</strong></p>
                <ul className="list-disc list-inside ml-2">
                  <li><strong>T-SQL</strong> (SQL Server/SSIS) - Auto-converted to Spark SQL</li>
                  <li><strong>Spark SQL</strong> - Used directly</li>
                  <li><strong>Standard SQL</strong> - BigQuery compatible</li>
                </ul>
                <p className="mt-2 text-yellow-600 dark:text-yellow-400">
                  <strong>Note:</strong> SQL is validated and converted to structured metadata.
                  Complex procedural logic may require manual review.
                </p>
              </div>
            </div>
          </div>

          <Button
            onClick={generateFromSQL}
            disabled={!sqlInput.trim() || isGenerating}
            className="w-full"
          >
            {isGenerating ? (
              <>
                <svg className="animate-spin -ml-1 mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                Converting SQL to structured metadata...
              </>
            ) : (
              '🔄 Convert SQL to Structured Transform'
            )}
          </Button>
        </Card>
      )}

      {/* Structured Builder */}
      {inputMode === 'structured_builder' && (
        <Card className="p-4 space-y-4">
          {/* Template Selection */}
          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">
              Select Transform Template
            </label>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              {TRANSFORM_TEMPLATES.map((template) => (
                <button
                  key={template.name}
                  onClick={() => applyTemplate(template)}
                  className={cn(
                    'p-3 text-left border rounded-lg transition-all',
                    selectedTemplate?.name === template.name
                      ? 'border-blue-500 bg-blue-900/30 dark:bg-blue-900/20'
                      : 'border-gray-200 dark:border-gray-700 hover:border-blue-300 dark:hover:border-blue-600'
                  )}
                >
                  <div className="font-medium text-sm text-white dark:text-white">
                    {template.name}
                  </div>
                  <div className="text-xs text-gray-400 dark:text-gray-400 mt-1">
                    {template.description}
                  </div>
                </button>
              ))}
            </div>
          </div>

          {/* Dynamic Config Form */}
          {selectedTemplate && (
            <div className="p-4 bg-gray-800/50 dark:bg-gray-800/50 rounded-lg space-y-4">
              <h4 className="font-medium text-white dark:text-white">
                Configure {selectedTemplate.name}
              </h4>

              {/* Column Selection for templates that need it */}
              {['window', 'aggregate', 'dedup'].includes(selectedTemplate.transform_type) && (
                <div>
                  <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-1">
                    Group/Partition By Columns
                  </label>
                  <div className="flex flex-wrap gap-2">
                    {schema.map((col) => (
                      <button
                        key={col.name}
                        onClick={() => {
                          const key = selectedTemplate.transform_type === 'aggregate' ? 'group_by' : selectedTemplate.transform_type === 'dedup' ? 'key_columns' : 'partition_by'
                          const current = structuredConfig[key] || []
                          const updated = current.includes(col.name)
                            ? current.filter((c: string) => c !== col.name)
                            : [...current, col.name]
                          setStructuredConfig({ ...structuredConfig, [key]: updated })
                        }}
                        className={cn(
                          'px-2 py-1 text-xs rounded border transition-colors',
                          (structuredConfig.partition_by || structuredConfig.group_by || structuredConfig.key_columns || []).includes(col.name)
                            ? 'bg-blue-900/300 text-white border-blue-500'
                            : 'bg-gray-800 dark:bg-gray-700 border-gray-600 dark:border-gray-600'
                        )}
                      >
                        {col.name}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Aggregation Column */}
              {selectedTemplate.transform_type === 'aggregate' && (
                <div className="grid grid-cols-3 gap-2">
                  <div>
                    <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-1">
                      Column to Aggregate
                    </label>
                    <select
                      value={structuredConfig.aggregations?.[0]?.column || ''}
                      onChange={(e) => setStructuredConfig({
                        ...structuredConfig,
                        aggregations: [{ ...structuredConfig.aggregations?.[0], column: e.target.value }]
                      })}
                      className="w-full px-2 py-1 border border-gray-600 dark:border-gray-600 rounded text-sm dark:bg-gray-800"
                    >
                      <option value="">Select column</option>
                      {schema.map((col) => (
                        <option key={col.name} value={col.name}>{col.name}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-1">
                      Function
                    </label>
                    <select
                      value={structuredConfig.aggregations?.[0]?.function || 'sum'}
                      onChange={(e) => setStructuredConfig({
                        ...structuredConfig,
                        aggregations: [{ ...structuredConfig.aggregations?.[0], function: e.target.value }]
                      })}
                      className="w-full px-2 py-1 border border-gray-600 dark:border-gray-600 rounded text-sm dark:bg-gray-800"
                    >
                      <option value="sum">SUM</option>
                      <option value="count">COUNT</option>
                      <option value="avg">AVG</option>
                      <option value="min">MIN</option>
                      <option value="max">MAX</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-1">
                      Alias
                    </label>
                    <input
                      type="text"
                      value={structuredConfig.aggregations?.[0]?.alias || ''}
                      onChange={(e) => setStructuredConfig({
                        ...structuredConfig,
                        aggregations: [{ ...structuredConfig.aggregations?.[0], alias: e.target.value }]
                      })}
                      placeholder="result_column"
                      className="w-full px-2 py-1 border border-gray-600 dark:border-gray-600 rounded text-sm dark:bg-gray-800"
                    />
                  </div>
                </div>
              )}

              {/* Filter condition */}
              {selectedTemplate.transform_type === 'filter' && (
                <div>
                  <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-1">
                    Filter Condition (SQL expression)
                  </label>
                  <input
                    type="text"
                    value={structuredConfig.condition || ''}
                    onChange={(e) => setStructuredConfig({ ...structuredConfig, condition: e.target.value })}
                    placeholder="status = 'active' AND amount > 100"
                    className="w-full px-2 py-1 border border-gray-600 dark:border-gray-600 rounded text-sm dark:bg-gray-800"
                  />
                </div>
              )}

              {/* Derived column */}
              {selectedTemplate.transform_type === 'derive' && (
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-1">
                      New Column Name
                    </label>
                    <input
                      type="text"
                      value={structuredConfig.target_column || ''}
                      onChange={(e) => setStructuredConfig({ ...structuredConfig, target_column: e.target.value })}
                      placeholder="new_column"
                      className="w-full px-2 py-1 border border-gray-600 dark:border-gray-600 rounded text-sm dark:bg-gray-800"
                    />
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-1">
                      Expression
                    </label>
                    <input
                      type="text"
                      value={structuredConfig.expression || ''}
                      onChange={(e) => setStructuredConfig({ ...structuredConfig, expression: e.target.value })}
                      placeholder="column_a + column_b"
                      className="w-full px-2 py-1 border border-gray-600 dark:border-gray-600 rounded text-sm dark:bg-gray-800"
                    />
                  </div>
                </div>
              )}

              <Button
                onClick={generateFromStructured}
                disabled={!selectedTemplate}
                className="w-full"
              >
                🔧 Generate Transform
              </Button>
            </div>
          )}
        </Card>
      )}

      {/* Error Display */}
      {error && (
        <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
          <p className="text-sm text-red-600 dark:text-red-400">❌ {error}</p>
        </div>
      )}

      {/* Preview of Generated Structured Transform */}
      {preview && (
        <Card className="p-4 space-y-4 border-2 border-blue-600/50 dark:border-blue-800">
          <div className="flex items-center justify-between">
            <h4 className="font-medium text-white dark:text-white">
              Generated Structured Transform
            </h4>
            <div className="flex items-center gap-2">
              <Badge
                variant={preview.confidence >= 0.8 ? 'success' : preview.confidence >= 0.6 ? 'warning' : 'error'}
              >
                {Math.round(preview.confidence * 100)}% confidence
              </Badge>
              <Badge variant="info">{preview.transform_type}</Badge>
            </div>
          </div>

          {/* Important Notice */}
          <div className="p-3 bg-blue-900/30 dark:bg-blue-900/20 border border-blue-600/50 dark:border-blue-800 rounded-md">
            <div className="flex gap-2">
              <div className="text-blue-600 dark:text-blue-400">ℹ️</div>
              <div className="flex-1 text-sm text-blue-200 dark:text-blue-300">
                <p className="font-medium">NL → Structured Metadata Conversion</p>
                <p className="text-xs mt-1">
                  Your natural language was converted to <strong>structured configuration</strong>.
                  This ensures deterministic, validated execution.
                </p>
              </div>
            </div>
          </div>

          {/* Warnings */}
          {preview.warnings.length > 0 && (
            <div className="p-3 bg-amber-900/30 dark:bg-yellow-900/20 border border-amber-600/50 dark:border-yellow-800 rounded-md">
              <p className="text-sm font-medium text-amber-200 dark:text-yellow-300 mb-2">
                ⚠️ Warnings:
              </p>
              <ul className="text-xs text-yellow-700 dark:text-yellow-400 space-y-1">
                {preview.warnings.map((w, i) => (
                  <li key={i}>• {w}</li>
                ))}
              </ul>
            </div>
          )}

          {/* Structured Configuration */}
          <div>
            <label className="text-xs font-medium text-gray-400 dark:text-gray-400 mb-1 block">
              Structured Configuration (This is what gets executed)
            </label>
            <pre className="p-3 bg-gray-900 text-green-400 text-xs rounded-md overflow-x-auto">
              {JSON.stringify(preview.config, null, 2)}
            </pre>
          </div>

          {/* Generated PySpark Code */}
          <div>
            <label className="text-xs font-medium text-gray-400 dark:text-gray-400 mb-1 block">
              Generated PySpark Code
            </label>
            <pre className="p-3 bg-gray-900 text-green-400 text-xs rounded-md overflow-x-auto">
              {preview.pyspark_code}
            </pre>
          </div>

          {/* Generated SQL (if available) */}
          {preview.sql_code && (
            <div>
              <label className="text-xs font-medium text-gray-400 dark:text-gray-400 mb-1 block">
                SQL Equivalent
              </label>
              <pre className="p-3 bg-gray-900 text-blue-400 text-xs rounded-md overflow-x-auto">
                {preview.sql_code}
              </pre>
            </div>
          )}

          {/* Confidence Guide */}
          <div className="text-xs text-gray-400 dark:text-gray-400">
            <p className="font-medium mb-1">Confidence Score Guide:</p>
            <ul className="space-y-1 ml-4">
              <li>• <strong>80-100%:</strong> High confidence - ready to use</li>
              <li>• <strong>60-79%:</strong> Medium confidence - review carefully</li>
              <li>• <strong>&lt;60%:</strong> Low confidence - manual review required</li>
            </ul>
          </div>

          {/* Add Transform Button */}
          <Button
            onClick={addTransform}
            variant="primary"
            className="w-full"
            disabled={preview.confidence < 0.5}
          >
            {preview.confidence < 0.5 ? (
              '⚠️ Confidence Too Low - Review Required'
            ) : (
              '✓ Add Structured Transform to Pipeline'
            )}
          </Button>
        </Card>
      )}

      {/* Available Schema Reference */}
      <Card className="p-4 bg-gray-800/50 dark:bg-gray-800/50">
        <h4 className="text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">
          Available Columns ({schema.length})
        </h4>
        <div className="flex flex-wrap gap-2">
          {schema.map((col) => (
            <Badge key={col.name} variant="default" className="text-xs">
              {col.name}: {col.type}
            </Badge>
          ))}
        </div>
      </Card>

      {/* Examples */}
      <Card className="p-4 bg-purple-50 dark:bg-purple-900/20 border-purple-200 dark:border-purple-800">
        <h4 className="text-sm font-medium text-purple-900 dark:text-purple-300 mb-2">
          💡 Example Transformations
        </h4>
        <div className="space-y-2 text-xs text-purple-800 dark:text-purple-400">
          <div className="p-2 bg-gray-800/50 dark:bg-purple-900/30 rounded">
            <strong>Window Function:</strong> "Add a row number partitioned by customer_id and ordered by order_date descending"
          </div>
          <div className="p-2 bg-gray-800/50 dark:bg-purple-900/30 rounded">
            <strong>Aggregation:</strong> "Group by product_category and calculate sum of sales, count of orders, and average order value"
          </div>
          <div className="p-2 bg-gray-800/50 dark:bg-purple-900/30 rounded">
            <strong>Derivation:</strong> "Create a new column called profit that is revenue minus cost"
          </div>
          <div className="p-2 bg-gray-800/50 dark:bg-purple-900/30 rounded">
            <strong>Filter:</strong> "Keep only rows where status equals 'active' and amount is greater than 100"
          </div>
        </div>
      </Card>
    </div>
  )
}

export default NLTransformInput
