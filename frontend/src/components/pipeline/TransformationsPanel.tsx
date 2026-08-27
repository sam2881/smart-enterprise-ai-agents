'use client'

/**
 * TransformationsPanel - Zone-Aware Transform Builder
 *
 * Features:
 * - Zone-specific transform tabs (Landing→Bronze, Bronze→Silver, Silver→Gold)
 * - Auto-suggested transforms based on zone + schema
 * - Custom transform input (NL, PySpark, SQL modes)
 * - Transform preview showing input → output schema changes
 *
 * All transforms stored as structured TransformConfig metadata.
 * NL is NEVER executed directly - always converted to structured config first.
 */

import React, { useState, useCallback, useMemo } from 'react'
import type {
  TransformConfig,
  TransformType,
  ColumnDefinition,
  SchemaConfig,
  ContractType,
  TargetZone,
} from '@/types/pipeline-canonical'

// =============================================================================
// Types
// =============================================================================

interface TransformationsPanelProps {
  schema: Partial<SchemaConfig>
  contractType?: ContractType
  transforms: Array<Partial<TransformConfig>>
  onChange: (transforms: Array<Partial<TransformConfig>>) => void
}

interface ZoneTab {
  key: ZoneTransition
  label: string
  fromZone: string
  toZone: string
  description: string
}

type ZoneTransition = 'landing_bronze' | 'bronze_silver' | 'silver_gold'

interface AutoSuggestion {
  type: TransformType
  label: string
  description: string
  zone: ZoneTransition
  config: Record<string, any>
  auto: boolean
}

// =============================================================================
// Constants
// =============================================================================

const ZONE_TABS: ZoneTab[] = [
  {
    key: 'landing_bronze',
    label: 'Landing → Bronze',
    fromZone: 'landing',
    toZone: 'bronze',
    description: 'Type casting, audit columns, basic schema enforcement',
  },
  {
    key: 'bronze_silver',
    label: 'Bronze → Silver',
    fromZone: 'bronze',
    toZone: 'silver',
    description: 'Deduplication, null handling, data standardization',
  },
  {
    key: 'silver_gold',
    label: 'Silver → Gold',
    fromZone: 'silver',
    toZone: 'gold',
    description: 'Business logic, aggregations, joins, Data Vault, hash keys, effectivity dating — Gold is the final layer',
  },
]

const ZONE_TO_TARGET: Record<ZoneTransition, TargetZone> = {
  landing_bronze: 'bronze',
  bronze_silver: 'silver',
  silver_gold: 'gold',
}

const TRANSFORM_TYPES: Array<{ value: TransformType; label: string; description: string }> = [
  { value: 'cast', label: 'Type Cast', description: 'Cast column to a different data type' },
  { value: 'rename', label: 'Rename', description: 'Rename a column' },
  { value: 'deduplicate', label: 'Deduplicate', description: 'Remove duplicate rows' },
  { value: 'null_handling', label: 'Null Handling', description: 'Fill, drop, or default null values' },
  { value: 'filter', label: 'Filter', description: 'Filter rows by condition' },
  { value: 'derive', label: 'Derive Column', description: 'Create new column from expression' },
  { value: 'aggregate', label: 'Aggregate', description: 'Group by and aggregate' },
  { value: 'window', label: 'Window Function', description: 'Running totals, ranks, lag/lead' },
  { value: 'join', label: 'Join', description: 'Join with another table' },
  { value: 'hash', label: 'Hash', description: 'Generate hash key (MD5/SHA-256)' },
  { value: 'encrypt', label: 'Encrypt (FPE)', description: 'Format-preserving encryption' },
  { value: 'mask', label: 'Mask', description: 'Mask sensitive column values' },
  { value: 'sql', label: 'SQL Expression', description: 'Custom SQL expression' },
  { value: 'pyspark', label: 'PySpark Code', description: 'Custom PySpark code' },
]

// =============================================================================
// Auto-suggestion logic
// =============================================================================

function generateAutoSuggestions(
  schema: Partial<SchemaConfig>,
  contractType?: ContractType
): AutoSuggestion[] {
  const columns = schema.columns || []
  const primaryKeys = schema.primary_keys || columns.filter((c) => c.pk).map((c) => c.name)
  const suggestions: AutoSuggestion[] = []

  // Landing → Bronze: Type casting + audit columns
  suggestions.push({
    type: 'cast',
    label: 'Type Casting',
    description: 'Cast STRING columns to proper types from schema',
    zone: 'landing_bronze',
    config: {
      columns: columns.map((c) => ({ column: c.name, cast_type: c.type })),
    },
    auto: true,
  })
  suggestions.push({
    type: 'derive',
    label: 'Add Audit Columns',
    description: 'Add ingestion_ts, batch_id, source_file columns',
    zone: 'landing_bronze',
    config: {
      expression: 'current_timestamp()',
      column: 'ingestion_ts',
      additional: [
        { column: 'batch_id', expression: "lit('{{ batch_id }}')" },
        { column: 'source_file', expression: "input_file_name()" },
      ],
    },
    auto: true,
  })

  // Bronze → Silver: Dedup + null handling
  if (primaryKeys.length > 0) {
    suggestions.push({
      type: 'deduplicate',
      label: 'Deduplicate',
      description: `Remove duplicates by ${primaryKeys.join(', ')}`,
      zone: 'bronze_silver',
      config: {
        partition_by: primaryKeys,
        order_by: [{ column: 'ingestion_ts', direction: 'desc' }],
      },
      auto: true,
    })
  }

  const nullableCols = columns.filter((c) => c.nullable !== false)
  if (nullableCols.length > 0) {
    suggestions.push({
      type: 'null_handling',
      label: 'Null Handling',
      description: `Handle nulls in ${nullableCols.length} nullable columns`,
      zone: 'bronze_silver',
      config: {
        strategy: 'drop',
        columns: nullableCols.map((c) => c.name),
      },
      auto: false,
    })
  }

  // Silver → Gold: Business logic (encryption for PII)
  const piiCols = columns.filter((c) => c.pii && c.pii !== 'none')
  if (piiCols.length > 0) {
    suggestions.push({
      type: 'encrypt',
      label: 'FPE Encryption',
      description: `Encrypt ${piiCols.length} sensitive column(s)`,
      zone: 'silver_gold',
      config: {
        columns: piiCols.map((c) => ({
          column: c.name,
          type: c.pii === 'pci' ? 'fpe' : 'hash',
          output_column: `${c.name}_encrypted`,
        })),
      },
      auto: true,
    })
  }

  // Silver → Gold: Contract-specific (Gold is the final layer)
  if (contractType === 'DATA_VAULT') {
    suggestions.push({
      type: 'hash',
      label: 'Data Vault Hash Keys',
      description: 'Generate hub keys and hash diffs for Data Vault',
      zone: 'silver_gold',
      config: {
        columns: primaryKeys.map((k) => ({
          column: k,
          type: 'fpe_hk',
          output_column: `${k}_hk`,
        })),
      },
      auto: true,
    })
  }

  if (contractType === 'SCD2') {
    suggestions.push({
      type: 'scd',
      label: 'SCD Type 2 Merge',
      description: 'Apply SCD2 with effective dating',
      zone: 'silver_gold',
      config: {
        type: 2,
        business_keys: primaryKeys,
        tracked_columns: columns.filter((c) => !c.pk).map((c) => c.name),
        effective_from: 'effective_from',
        effective_to: 'effective_to',
        current_flag: 'is_current',
      },
      auto: true,
    })
  }

  return suggestions
}

// =============================================================================
// Component
// =============================================================================

export function TransformationsPanel({
  schema,
  contractType,
  transforms,
  onChange,
}: TransformationsPanelProps) {
  const [activeZone, setActiveZone] = useState<ZoneTransition>('landing_bronze')
  const [showAddTransform, setShowAddTransform] = useState(false)
  const [newTransformType, setNewTransformType] = useState<TransformType>('cast')

  const autoSuggestions = useMemo(
    () => generateAutoSuggestions(schema, contractType),
    [schema, contractType]
  )

  // Get transforms for active zone
  const zoneTransforms = useMemo(() => {
    const targetZone = ZONE_TO_TARGET[activeZone]
    return transforms.filter((t) => t.zone === targetZone)
  }, [transforms, activeZone])

  const zoneSuggestions = useMemo(
    () => autoSuggestions.filter((s) => s.zone === activeZone),
    [autoSuggestions, activeZone]
  )

  // Check if a suggestion is already added
  const isSuggestionAdded = useCallback(
    (suggestion: AutoSuggestion) => {
      const targetZone = ZONE_TO_TARGET[suggestion.zone]
      return transforms.some(
        (t) => t.zone === targetZone && t.transform_type === suggestion.type
      )
    },
    [transforms]
  )

  const addSuggestion = useCallback(
    (suggestion: AutoSuggestion) => {
      const targetZone = ZONE_TO_TARGET[suggestion.zone]
      const newTransform: Partial<TransformConfig> = {
        transform_type: suggestion.type,
        zone: targetZone,
        transform_order: transforms.filter((t) => t.zone === targetZone).length + 1,
        config: suggestion.config,
        is_active: true,
        nl_description: suggestion.description,
      }
      onChange([...transforms, newTransform])
    },
    [transforms, onChange]
  )

  const removeSuggestion = useCallback(
    (suggestion: AutoSuggestion) => {
      const targetZone = ZONE_TO_TARGET[suggestion.zone]
      onChange(
        transforms.filter(
          (t) => !(t.zone === targetZone && t.transform_type === suggestion.type)
        )
      )
    },
    [transforms, onChange]
  )

  const addCustomTransform = useCallback(() => {
    const targetZone = ZONE_TO_TARGET[activeZone]
    const newTransform: Partial<TransformConfig> = {
      transform_type: newTransformType,
      zone: targetZone,
      transform_order: zoneTransforms.length + 1,
      config: {},
      is_active: true,
    }
    onChange([...transforms, newTransform])
    setShowAddTransform(false)
  }, [activeZone, newTransformType, zoneTransforms.length, transforms, onChange])

  const removeTransform = useCallback(
    (index: number) => {
      const targetZone = ZONE_TO_TARGET[activeZone]
      const zoneTs = transforms.filter((t) => t.zone === targetZone)
      const toRemove = zoneTs[index]
      onChange(transforms.filter((t) => t !== toRemove))
    },
    [activeZone, transforms, onChange]
  )

  const updateTransformConfig = useCallback(
    (index: number, key: string, value: any) => {
      const targetZone = ZONE_TO_TARGET[activeZone]
      let zoneIndex = 0
      const updated = transforms.map((t) => {
        if (t.zone === targetZone) {
          if (zoneIndex === index) {
            zoneIndex++
            return { ...t, config: { ...t.config, [key]: value } }
          }
          zoneIndex++
        }
        return t
      })
      onChange(updated)
    },
    [activeZone, transforms, onChange]
  )

  const toggleTransformActive = useCallback(
    (index: number) => {
      const targetZone = ZONE_TO_TARGET[activeZone]
      let zoneIndex = 0
      const updated = transforms.map((t) => {
        if (t.zone === targetZone) {
          if (zoneIndex === index) {
            zoneIndex++
            return { ...t, is_active: !t.is_active }
          }
          zoneIndex++
        }
        return t
      })
      onChange(updated)
    },
    [activeZone, transforms, onChange]
  )

  const totalTransforms = transforms.filter((t) => t.is_active !== false).length

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-gray-900">Transformations</h3>
        <span className="text-sm text-gray-500">{totalTransforms} active transform(s)</span>
      </div>

      {/* Zone tabs */}
      <div className="flex gap-1 bg-gray-100 rounded-lg p-1">
        {ZONE_TABS.map((tab) => {
          const count = transforms.filter(
            (t) => t.zone === ZONE_TO_TARGET[tab.key] && t.is_active !== false
          ).length
          return (
            <button
              key={tab.key}
              type="button"
              onClick={() => setActiveZone(tab.key)}
              className={`flex-1 py-2 px-2 text-xs rounded-md transition-colors ${
                activeZone === tab.key
                  ? 'bg-white text-gray-900 shadow-sm font-medium'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              {tab.label}
              {count > 0 && (
                <span className="ml-1 px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded-full text-[10px] font-medium">
                  {count}
                </span>
              )}
            </button>
          )
        })}
      </div>

      {/* Zone description */}
      <p className="text-xs text-gray-500">
        {ZONE_TABS.find((t) => t.key === activeZone)?.description}
      </p>

      {/* Auto-suggested transforms */}
      {zoneSuggestions.length > 0 && (
        <div className="space-y-2">
          <p className="text-xs font-medium text-gray-600 uppercase tracking-wide">
            Suggested Transforms
          </p>
          {zoneSuggestions.map((suggestion, i) => {
            const added = isSuggestionAdded(suggestion)
            return (
              <div
                key={i}
                className={`flex items-center justify-between p-3 rounded-lg border ${
                  added
                    ? 'bg-blue-50 border-blue-200'
                    : 'bg-gray-50 border-gray-200'
                }`}
              >
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium text-gray-900">
                      {suggestion.label}
                    </span>
                    {suggestion.auto && (
                      <span className="px-1.5 py-0.5 bg-green-100 text-green-700 rounded text-[10px] font-medium">
                        Auto
                      </span>
                    )}
                  </div>
                  <p className="text-xs text-gray-500 mt-0.5">{suggestion.description}</p>
                </div>
                <button
                  type="button"
                  onClick={() =>
                    added ? removeSuggestion(suggestion) : addSuggestion(suggestion)
                  }
                  className={`ml-3 px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                    added
                      ? 'bg-blue-100 text-blue-700 hover:bg-blue-200'
                      : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                  }`}
                >
                  {added ? 'Added' : '+ Add'}
                </button>
              </div>
            )
          })}
        </div>
      )}

      {/* Active transforms for zone */}
      {zoneTransforms.length > 0 && (
        <div className="space-y-2">
          <p className="text-xs font-medium text-gray-600 uppercase tracking-wide">
            Active Transforms
          </p>
          {zoneTransforms.map((t, idx) => (
            <TransformCard
              key={idx}
              transform={t}
              index={idx}
              columns={schema.columns || []}
              onToggleActive={() => toggleTransformActive(idx)}
              onUpdateConfig={(key, value) => updateTransformConfig(idx, key, value)}
              onRemove={() => removeTransform(idx)}
            />
          ))}
        </div>
      )}

      {/* Add custom transform */}
      {showAddTransform ? (
        <div className="p-3 border border-gray-200 rounded-lg bg-white space-y-3">
          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">Transform Type:</label>
            <select
              value={newTransformType}
              onChange={(e) => setNewTransformType(e.target.value as TransformType)}
              className="flex-1 px-2 py-1.5 border border-gray-300 rounded-md text-sm"
            >
              {TRANSFORM_TYPES.map((t) => (
                <option key={t.value} value={t.value}>
                  {t.label} - {t.description}
                </option>
              ))}
            </select>
          </div>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={addCustomTransform}
              className="px-4 py-2 bg-gray-900 text-white rounded-md text-sm font-medium hover:bg-gray-800"
            >
              Add Transform
            </button>
            <button
              type="button"
              onClick={() => setShowAddTransform(false)}
              className="px-4 py-2 bg-gray-100 text-gray-700 rounded-md text-sm font-medium hover:bg-gray-200"
            >
              Cancel
            </button>
          </div>
        </div>
      ) : (
        <button
          type="button"
          onClick={() => setShowAddTransform(true)}
          className="w-full py-2 border-2 border-dashed border-gray-300 rounded-lg text-sm text-gray-500 hover:border-gray-400 hover:text-gray-600 transition-colors"
        >
          + Add Custom Transform
        </button>
      )}

      {/* Empty state */}
      {transforms.length === 0 && zoneSuggestions.length === 0 && (
        <div className="text-center py-6 text-gray-400 text-sm">
          Define your schema first to see auto-suggested transforms.
        </div>
      )}
    </div>
  )
}

// =============================================================================
// TransformCard Sub-component
// =============================================================================

function TransformCard({
  transform,
  index,
  columns,
  onToggleActive,
  onUpdateConfig,
  onRemove,
}: {
  transform: Partial<TransformConfig>
  index: number
  columns: ColumnDefinition[]
  onToggleActive: () => void
  onUpdateConfig: (key: string, value: any) => void
  onRemove: () => void
}) {
  const [expanded, setExpanded] = useState(false)
  const typeInfo = TRANSFORM_TYPES.find((t) => t.value === transform.transform_type)

  return (
    <div
      className={`border rounded-lg overflow-hidden ${
        transform.is_active === false
          ? 'border-gray-200 bg-gray-50 opacity-60'
          : 'border-gray-200 bg-white'
      }`}
    >
      {/* Header */}
      <div className="flex items-center gap-3 px-3 py-2">
        <span className="text-xs text-gray-400 font-mono w-5">{index + 1}</span>
        <button
          type="button"
          onClick={() => setExpanded(!expanded)}
          className="flex-1 text-left"
        >
          <span className="text-sm font-medium text-gray-900">
            {typeInfo?.label || transform.transform_type}
          </span>
          {transform.nl_description && (
            <span className="text-xs text-gray-500 ml-2">
              - {transform.nl_description}
            </span>
          )}
        </button>
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={onToggleActive}
            className={`px-2 py-1 text-xs rounded ${
              transform.is_active !== false
                ? 'bg-green-100 text-green-700'
                : 'bg-gray-200 text-gray-500'
            }`}
          >
            {transform.is_active !== false ? 'On' : 'Off'}
          </button>
          <button
            type="button"
            onClick={onRemove}
            className="text-gray-400 hover:text-red-500 transition-colors px-1"
          >
            ✕
          </button>
        </div>
      </div>

      {/* Expanded config editor */}
      {expanded && (
        <div className="px-3 pb-3 border-t border-gray-100 pt-2 space-y-2">
          <TransformConfigEditor
            type={transform.transform_type || 'cast'}
            config={transform.config || {}}
            columns={columns}
            onUpdate={onUpdateConfig}
          />
        </div>
      )}
    </div>
  )
}

// =============================================================================
// TransformConfigEditor - Per-type config fields
// =============================================================================

function TransformConfigEditor({
  type,
  config,
  columns,
  onUpdate,
}: {
  type: TransformType
  config: Record<string, any>
  columns: ColumnDefinition[]
  onUpdate: (key: string, value: any) => void
}) {
  switch (type) {
    case 'deduplicate':
    case 'dedup':
      return (
        <div className="space-y-2">
          <label className="text-xs text-gray-600">Partition By (dedup keys):</label>
          <div className="flex flex-wrap gap-1">
            {columns.map((col) => (
              <button
                key={col.name}
                type="button"
                onClick={() => {
                  const current = config.partition_by || []
                  const updated = current.includes(col.name)
                    ? current.filter((c: string) => c !== col.name)
                    : [...current, col.name]
                  onUpdate('partition_by', updated)
                }}
                className={`px-2 py-1 text-xs rounded-full border ${
                  (config.partition_by || []).includes(col.name)
                    ? 'bg-blue-100 border-blue-300 text-blue-700'
                    : 'bg-white border-gray-200 text-gray-600'
                }`}
              >
                {col.name}
              </button>
            ))}
          </div>
        </div>
      )

    case 'null_handling':
      return (
        <div className="space-y-2">
          <label className="text-xs text-gray-600">Strategy:</label>
          <select
            value={config.strategy || 'drop'}
            onChange={(e) => onUpdate('strategy', e.target.value)}
            className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm"
          >
            <option value="drop">Drop rows with nulls</option>
            <option value="fill">Fill with default value</option>
            <option value="coalesce">Coalesce with fallback</option>
          </select>
          {config.strategy === 'fill' && (
            <input
              type="text"
              value={config.fill_value || ''}
              onChange={(e) => onUpdate('fill_value', e.target.value)}
              placeholder="Default value..."
              className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm"
            />
          )}
        </div>
      )

    case 'filter':
      return (
        <div className="space-y-2">
          <label className="text-xs text-gray-600">Filter Condition (SQL expression):</label>
          <input
            type="text"
            value={config.condition || ''}
            onChange={(e) => onUpdate('condition', e.target.value)}
            placeholder="e.g., amount > 0 AND status != 'deleted'"
            className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm font-mono"
          />
        </div>
      )

    case 'derive':
      return (
        <div className="space-y-2">
          <label className="text-xs text-gray-600">Output Column:</label>
          <input
            type="text"
            value={config.column || ''}
            onChange={(e) => onUpdate('column', e.target.value)}
            placeholder="new_column_name"
            className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm"
          />
          <label className="text-xs text-gray-600">Expression:</label>
          <input
            type="text"
            value={config.expression || ''}
            onChange={(e) => onUpdate('expression', e.target.value)}
            placeholder="e.g., col('price') * col('quantity')"
            className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm font-mono"
          />
        </div>
      )

    case 'rename':
      return (
        <div className="space-y-2">
          <label className="text-xs text-gray-600">Source Column:</label>
          <select
            value={config.source || ''}
            onChange={(e) => onUpdate('source', e.target.value)}
            className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm"
          >
            <option value="">Select column...</option>
            {columns.map((col) => (
              <option key={col.name} value={col.name}>{col.name}</option>
            ))}
          </select>
          <label className="text-xs text-gray-600">New Name:</label>
          <input
            type="text"
            value={config.target || ''}
            onChange={(e) => onUpdate('target', e.target.value)}
            placeholder="new_column_name"
            className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm"
          />
        </div>
      )

    case 'sql':
      return (
        <div className="space-y-2">
          <label className="text-xs text-gray-600">SQL Expression:</label>
          <textarea
            value={config.sql || ''}
            onChange={(e) => onUpdate('sql', e.target.value)}
            placeholder="SELECT col1, col2, col1 * col2 AS derived FROM __df__"
            rows={3}
            className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm font-mono"
          />
        </div>
      )

    case 'pyspark':
      return (
        <div className="space-y-2">
          <label className="text-xs text-gray-600">PySpark Code:</label>
          <textarea
            value={config.code || ''}
            onChange={(e) => onUpdate('code', e.target.value)}
            placeholder="df = df.withColumn('new_col', F.col('a') + F.col('b'))"
            rows={3}
            className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm font-mono"
          />
        </div>
      )

    default:
      return (
        <div className="space-y-2">
          <label className="text-xs text-gray-600">Configuration (JSON):</label>
          <textarea
            value={JSON.stringify(config, null, 2)}
            onChange={(e) => {
              try {
                const parsed = JSON.parse(e.target.value)
                Object.entries(parsed).forEach(([k, v]) => onUpdate(k, v))
              } catch {
                // Invalid JSON, ignore
              }
            }}
            rows={3}
            className="w-full px-2 py-1.5 border border-gray-200 rounded text-sm font-mono"
          />
        </div>
      )
  }
}

export default TransformationsPanel
