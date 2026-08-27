'use client'

/**
 * GoldModelingSelector - Enterprise Data Modeling Strategy Selector
 *
 * At Gold layer, user must explicitly choose a modeling pattern:
 * 1. Data Vault 2.0 - For long-term historization, audit trails, stable business keys
 * 2. Star Schema - For BI/analytics performance, reporting use cases
 * 3. Snowflake Schema - For normalized dimensions, deep hierarchies
 * 4. Flat/Reporting Table - Restricted use, only for extracts or ad-hoc reporting
 *
 * Based on Master System Prompt Section 7: GOLD ZONE - FINAL MODELING
 */

import React, { useState, useCallback } from 'react'
import { Card } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Badge } from '@/components/ui/Badge'
import { cn } from '@/lib/utils'
import {
  GoldModelingStrategy,
  GoldZoneConfig,
  DataVault2Config,
  StarSchemaConfig,
  ColumnDefinition,
  JoinDependency,
  V2GoldModelConfig,
  V2GoldModelType,
  V2HubConfig,
  V2SatelliteConfig,
  V2DimensionConfig,
  V2FactConfig,
  V2MeasureConfig,
  V2AggFunction,
  V2DimensionLookup,
  V2SCDType,
} from '@/types/pipeline-canonical'
import { JoinDependencyBuilder } from './JoinDependencyBuilder'
import {
  Database,
  Star,
  Snowflake,
  Table,
  Plus,
  Trash2,
  Info,
  AlertTriangle,
  CheckCircle,
  Link as LinkIcon,
  Layers,
  Key,
  Clock,
  Shield,
} from 'lucide-react'

// =============================================================================
// Props
// =============================================================================

interface GoldModelingSelectorProps {
  value?: Partial<GoldZoneConfig>
  onChange: (config: Partial<GoldZoneConfig>) => void
  availableColumns?: ColumnDefinition[]
  leftEntityName?: string
  className?: string
  v2Mode?: boolean
  onV2GoldModelChange?: (config: V2GoldModelConfig) => void
}

// =============================================================================
// Modeling Strategy Cards
// =============================================================================

interface ModelingStrategyOption {
  id: GoldModelingStrategy
  name: string
  icon: React.ReactNode
  description: string
  useWhen: string[]
  includes: string[]
  gradient: string
  recommended?: boolean
  restricted?: boolean
}

const MODELING_STRATEGIES: ModelingStrategyOption[] = [
  {
    id: 'data_vault_2',
    name: 'Data Vault 2.0',
    icon: <Database className="w-6 h-6" />,
    description: 'Enterprise-scale, audit-ready data warehouse architecture',
    gradient: 'from-purple-500 to-indigo-600',
    useWhen: [
      'Long-term historization is required',
      'Business keys are stable',
      'Audit & traceability are mandatory',
      'Multiple source system integration',
    ],
    includes: [
      'Hubs (business keys)',
      'Links (relationships)',
      'Satellites (descriptive attributes)',
      'Hash key generation via metadata',
      'Load dates & record sources',
    ],
  },
  {
    id: 'star_schema',
    name: 'Star Schema',
    icon: <Star className="w-6 h-6" />,
    description: 'Classic dimensional model optimized for BI and analytics',
    gradient: 'from-yellow-500 to-orange-500',
    recommended: true,
    useWhen: [
      'Analytics & BI performance is primary',
      'Reporting use cases dominate',
      'Clear fact/dimension separation',
      'Self-service analytics required',
    ],
    includes: [
      'Fact tables with measures',
      'Dimension tables with attributes',
      'Surrogate keys',
      'SCD Type 1 / Type 2 support',
      'Conformed dimensions',
    ],
  },
  {
    id: 'snowflake_schema',
    name: 'Snowflake Schema',
    icon: <Snowflake className="w-6 h-6" />,
    description: 'Normalized star schema for complex dimension hierarchies',
    gradient: 'from-cyan-500 to-blue-500',
    useWhen: [
      'Dimension normalization is required',
      'Hierarchies are deep and shared',
      'Storage optimization needed',
      'Complex dimension relationships',
    ],
    includes: [
      'Everything in Star Schema',
      'Normalized dimension tables',
      'Hierarchy tables',
      'Lookup tables',
      'Reduced data redundancy',
    ],
  },
  {
    id: 'flat_table',
    name: 'Flat/Reporting Table',
    icon: <Table className="w-6 h-6" />,
    description: 'Denormalized single table for specific use cases',
    gradient: 'from-gray-500 to-gray-600',
    restricted: true,
    useWhen: [
      'Only for extracts or ad-hoc reporting',
      'Simple aggregation outputs',
      'Data export to external systems',
      'Temporary analysis tables',
    ],
    includes: [
      'Single denormalized table',
      'Pre-computed metrics',
      'Simple refresh strategy',
      'Limited retention',
    ],
  },
]

// =============================================================================
// Sub-Components
// =============================================================================

function StrategyCard({
  strategy,
  selected,
  onClick,
}: {
  strategy: ModelingStrategyOption
  selected: boolean
  onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        'relative p-4 rounded-xl border-2 text-left transition-all duration-200',
        selected
          ? 'border-blue-500 bg-blue-900/30 dark:bg-blue-900/20'
          : 'border-gray-200 dark:border-gray-700 hover:border-blue-300 dark:hover:border-blue-700',
        strategy.restricted && !selected && 'opacity-75'
      )}
    >
      {/* Badges */}
      <div className="absolute top-2 right-2 flex gap-1">
        {strategy.recommended && (
          <span className="px-2 py-0.5 text-[10px] font-bold bg-green-500 text-white rounded-full">
            RECOMMENDED
          </span>
        )}
        {strategy.restricted && (
          <span className="px-2 py-0.5 text-[10px] font-bold bg-amber-900/300 text-black rounded-full">
            RESTRICTED
          </span>
        )}
      </div>

      {/* Icon & Name */}
      <div className="flex items-center gap-3 mb-3">
        <div
          className={cn(
            'p-2 rounded-lg bg-gradient-to-br text-white',
            strategy.gradient
          )}
        >
          {strategy.icon}
        </div>
        <div>
          <h4 className="font-semibold text-white dark:text-white">
            {strategy.name}
          </h4>
          <p className="text-xs text-gray-400 dark:text-gray-400">
            {strategy.description}
          </p>
        </div>
      </div>

      {/* Use When */}
      <div className="mb-3">
        <p className="text-xs font-medium text-gray-400 dark:text-gray-300 mb-1">
          Use when:
        </p>
        <ul className="text-xs text-gray-400 dark:text-gray-400 space-y-0.5">
          {strategy.useWhen.slice(0, 2).map((item, i) => (
            <li key={i} className="flex items-start gap-1">
              <CheckCircle className="w-3 h-3 text-green-500 mt-0.5 flex-shrink-0" />
              {item}
            </li>
          ))}
        </ul>
      </div>

      {/* Includes */}
      <div>
        <p className="text-xs font-medium text-gray-400 dark:text-gray-300 mb-1">
          Includes:
        </p>
        <div className="flex flex-wrap gap-1">
          {strategy.includes.slice(0, 3).map((item, i) => (
            <span
              key={i}
              className="px-1.5 py-0.5 text-[10px] bg-gray-700 dark:bg-gray-700 rounded"
            >
              {item}
            </span>
          ))}
          {strategy.includes.length > 3 && (
            <span className="px-1.5 py-0.5 text-[10px] text-gray-400">
              +{strategy.includes.length - 3} more
            </span>
          )}
        </div>
      </div>

      {/* Selection Indicator */}
      {selected && (
        <div className="absolute top-2 left-2">
          <CheckCircle className="w-5 h-5 text-blue-500" />
        </div>
      )}
    </button>
  )
}

// =============================================================================
// Data Vault 2.0 Configuration Form
// =============================================================================

function DataVaultConfigForm({
  config,
  onChange,
  columns,
}: {
  config?: Partial<DataVault2Config>
  onChange: (config: Partial<DataVault2Config>) => void
  columns: ColumnDefinition[]
}) {
  const [newHubName, setNewHubName] = useState('')
  const [selectedBusinessKeys, setSelectedBusinessKeys] = useState<string[]>([])

  const addHub = () => {
    if (!newHubName || selectedBusinessKeys.length === 0) return

    const newHub = {
      hub_name: `hub_${newHubName.toLowerCase().replace(/\s+/g, '_')}`,
      business_keys: selectedBusinessKeys,
      source_system: 'primary',
      hash_key_column: `hk_${newHubName.toLowerCase().replace(/\s+/g, '_')}`,
      load_date_column: 'load_date',
      record_source_column: 'record_source',
    }

    onChange({
      ...config,
      hubs: [...(config?.hubs || []), newHub],
    })

    setNewHubName('')
    setSelectedBusinessKeys([])
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 text-purple-600 dark:text-purple-400">
        <Database className="w-5 h-5" />
        <h4 className="font-semibold">Data Vault 2.0 Configuration</h4>
      </div>

      {/* Info Box */}
      <div className="p-3 bg-purple-50 dark:bg-purple-900/20 rounded-lg border border-purple-200 dark:border-purple-800">
        <div className="flex items-start gap-2">
          <Info className="w-4 h-4 text-purple-500 mt-0.5" />
          <div className="text-xs text-purple-700 dark:text-purple-300">
            <p className="font-medium mb-1">Data Vault 2.0 Components:</p>
            <ul className="list-disc list-inside space-y-0.5 text-purple-600 dark:text-purple-400">
              <li>
                <strong>Hubs</strong> - Store unique business keys (e.g., Customer ID)
              </li>
              <li>
                <strong>Links</strong> - Store relationships between hubs
              </li>
              <li>
                <strong>Satellites</strong> - Store descriptive attributes with history
              </li>
            </ul>
          </div>
        </div>
      </div>

      {/* Existing Hubs */}
      {config?.hubs && config.hubs.length > 0 && (
        <div className="space-y-2">
          <p className="text-sm font-medium text-gray-300 dark:text-gray-300">
            Configured Hubs ({config.hubs.length})
          </p>
          {config.hubs.map((hub, index) => (
            <div
              key={index}
              className="flex items-center justify-between p-3 bg-gray-800/50 dark:bg-gray-800 rounded-lg"
            >
              <div className="flex items-center gap-3">
                <Key className="w-4 h-4 text-purple-500" />
                <div>
                  <span className="font-medium text-white dark:text-white">
                    {hub.hub_name}
                  </span>
                  <span className="ml-2 text-xs text-gray-400">
                    Keys: {hub.business_keys.join(', ')}
                  </span>
                </div>
              </div>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => {
                  const newHubs = config.hubs?.filter((_, i) => i !== index)
                  onChange({ ...config, hubs: newHubs })
                }}
              >
                <Trash2 className="w-4 h-4 text-red-500" />
              </Button>
            </div>
          ))}
        </div>
      )}

      {/* Add New Hub */}
      <div className="p-4 border border-dashed border-purple-300 dark:border-purple-700 rounded-lg space-y-3">
        <p className="text-sm font-medium text-gray-300 dark:text-gray-300">
          Add New Hub
        </p>

        <Input
          label="Hub Name"
          placeholder="e.g., Customer, Product, Order"
          value={newHubName}
          onChange={(e) => setNewHubName(e.target.value)}
        />

        <div>
          <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-2">
            Select Business Keys (columns that uniquely identify the entity)
          </label>
          <div className="flex flex-wrap gap-2">
            {columns.map((col) => (
              <button
                key={col.name}
                onClick={() => {
                  setSelectedBusinessKeys((prev) =>
                    prev.includes(col.name)
                      ? prev.filter((c) => c !== col.name)
                      : [...prev, col.name]
                  )
                }}
                className={cn(
                  'px-2 py-1 text-xs rounded border transition-colors',
                  selectedBusinessKeys.includes(col.name)
                    ? 'bg-purple-500 text-white border-purple-500'
                    : 'bg-gray-800 dark:bg-gray-700 border-gray-600 dark:border-gray-600'
                )}
              >
                {col.name}
                {col.pk && <Key className="w-3 h-3 ml-1 inline" />}
              </button>
            ))}
          </div>
        </div>

        <Button
          onClick={addHub}
          disabled={!newHubName || selectedBusinessKeys.length === 0}
          className="w-full"
        >
          <Plus className="w-4 h-4 mr-2" />
          Add Hub
        </Button>
      </div>

      {/* Hash Algorithm */}
      <Select
        label="Hash Algorithm"
        value={config?.hash_algorithm || 'sha256'}
        onChange={(e) =>
          onChange({ ...config, hash_algorithm: e.target.value as 'md5' | 'sha1' | 'sha256' })
        }
        options={[
          { value: 'md5', label: 'MD5 (Fast, less secure)' },
          { value: 'sha1', label: 'SHA-1 (Balanced)' },
          { value: 'sha256', label: 'SHA-256 (Recommended)' },
        ]}
      />
    </div>
  )
}

// =============================================================================
// Star Schema Configuration Form
// =============================================================================

function StarSchemaConfigForm({
  config,
  onChange,
  columns,
}: {
  config?: Partial<StarSchemaConfig>
  onChange: (config: Partial<StarSchemaConfig>) => void
  columns: ColumnDefinition[]
}) {
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 text-yellow-600 dark:text-yellow-400">
        <Star className="w-5 h-5" />
        <h4 className="font-semibold">Star Schema Configuration</h4>
      </div>

      {/* Info Box */}
      <div className="p-3 bg-amber-900/30 dark:bg-yellow-900/20 rounded-lg border border-amber-600/50 dark:border-yellow-800">
        <div className="flex items-start gap-2">
          <Info className="w-4 h-4 text-yellow-500 mt-0.5" />
          <div className="text-xs text-yellow-700 dark:text-yellow-300">
            <p className="font-medium mb-1">Star Schema Components:</p>
            <ul className="list-disc list-inside space-y-0.5 text-yellow-600 dark:text-yellow-400">
              <li>
                <strong>Fact Table</strong> - Contains measures and foreign keys to dimensions
              </li>
              <li>
                <strong>Dimension Tables</strong> - Contains descriptive attributes
              </li>
              <li>
                <strong>SCD</strong> - Slowly Changing Dimensions for history tracking
              </li>
            </ul>
          </div>
        </div>
      </div>

      {/* Fact Table Configuration */}
      <div className="p-4 bg-gray-800/50 dark:bg-gray-800 rounded-lg space-y-3">
        <h5 className="font-medium text-white dark:text-white flex items-center gap-2">
          <Layers className="w-4 h-4" />
          Fact Table
        </h5>

        <Input
          label="Fact Table Name"
          placeholder="e.g., fact_sales, fact_orders"
          value={config?.fact_table?.table_name || ''}
          onChange={(e) =>
            onChange({
              ...config,
              fact_table: { ...config?.fact_table, table_name: e.target.value } as any,
            })
          }
        />

        <Input
          label="Grain Description"
          placeholder="e.g., One row per order line item per day"
          value={config?.fact_table?.grain_description || ''}
          onChange={(e) =>
            onChange({
              ...config,
              fact_table: { ...config?.fact_table, grain_description: e.target.value } as any,
            })
          }
        />

        <div>
          <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-2">
            Grain Columns (columns that define uniqueness)
          </label>
          <div className="flex flex-wrap gap-2">
            {columns.map((col) => (
              <button
                key={col.name}
                onClick={() => {
                  const current = config?.fact_table?.grain_columns || []
                  const updated = current.includes(col.name)
                    ? current.filter((c) => c !== col.name)
                    : [...current, col.name]
                  onChange({
                    ...config,
                    fact_table: { ...config?.fact_table, grain_columns: updated } as any,
                  })
                }}
                className={cn(
                  'px-2 py-1 text-xs rounded border transition-colors',
                  (config?.fact_table?.grain_columns || []).includes(col.name)
                    ? 'bg-amber-900/300 text-white border-yellow-500'
                    : 'bg-gray-800 dark:bg-gray-700 border-gray-600 dark:border-gray-600'
                )}
              >
                {col.name}
              </button>
            ))}
          </div>
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-400 dark:text-gray-400 mb-2">
            Measure Columns (numeric columns for aggregation)
          </label>
          <div className="flex flex-wrap gap-2">
            {columns
              .filter((c) => ['integer', 'bigint', 'float', 'double', 'decimal'].includes(c.type))
              .map((col) => (
                <button
                  key={col.name}
                  onClick={() => {
                    const current = config?.fact_table?.measures || []
                    const exists = current.some((m) => m.column_name === col.name)
                    const updated = exists
                      ? current.filter((m) => m.column_name !== col.name)
                      : [...current, { column_name: col.name, aggregation_type: 'sum' as const }]
                    onChange({
                      ...config,
                      fact_table: { ...config?.fact_table, measures: updated } as any,
                    })
                  }}
                  className={cn(
                    'px-2 py-1 text-xs rounded border transition-colors',
                    (config?.fact_table?.measures || []).some((m) => m.column_name === col.name)
                      ? 'bg-amber-900/300 text-white border-yellow-500'
                      : 'bg-gray-800 dark:bg-gray-700 border-gray-600 dark:border-gray-600'
                  )}
                >
                  {col.name}
                </button>
              ))}
          </div>
        </div>
      </div>

      {/* Dimension Tables */}
      <div className="p-4 bg-gray-800/50 dark:bg-gray-800 rounded-lg space-y-3">
        <h5 className="font-medium text-white dark:text-white flex items-center gap-2">
          <LinkIcon className="w-4 h-4" />
          Dimension Tables
        </h5>

        <div className="text-sm text-gray-400 dark:text-gray-400">
          Configure dimension tables in the full form (coming soon).
          Currently supported via transformation rules.
        </div>

        <Select
          label="Default SCD Type"
          value="2"
          onChange={() => {}}
          options={[
            { value: '1', label: 'SCD Type 1 (Overwrite)' },
            { value: '2', label: 'SCD Type 2 (History with effective dates)' },
            { value: '3', label: 'SCD Type 3 (Previous value column)' },
          ]}
        />
      </div>
    </div>
  )
}

// =============================================================================
// V2 Data Vault Configuration Form
// =============================================================================

function V2DataVaultConfigForm({
  config,
  onChange,
  columns,
}: {
  config: V2GoldModelConfig
  onChange: (config: V2GoldModelConfig) => void
  columns: ColumnDefinition[]
}) {
  const [newHubName, setNewHubName] = useState('')
  const [newHubKeys, setNewHubKeys] = useState<string[]>([])
  const [newHubTable, setNewHubTable] = useState('')
  const [newSatName, setNewSatName] = useState('')
  const [newSatHub, setNewSatHub] = useState('')
  const [newSatCols, setNewSatCols] = useState<string[]>([])
  const [newSatTable, setNewSatTable] = useState('')

  const addHub = () => {
    if (!newHubName || newHubKeys.length === 0 || !newHubTable) return
    const hub: V2HubConfig = {
      hub_name: newHubName,
      business_keys: newHubKeys,
      target_table: newHubTable,
    }
    onChange({ ...config, hubs: [...(config.hubs || []), hub] })
    setNewHubName('')
    setNewHubKeys([])
    setNewHubTable('')
  }

  const addSatellite = () => {
    if (!newSatName || !newSatHub || newSatCols.length === 0 || !newSatTable) return
    const sat: V2SatelliteConfig = {
      satellite_name: newSatName,
      hub_reference: newSatHub,
      descriptive_columns: newSatCols,
      target_table: newSatTable,
    }
    onChange({ ...config, satellites: [...(config.satellites || []), sat] })
    setNewSatName('')
    setNewSatHub('')
    setNewSatCols([])
    setNewSatTable('')
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 text-purple-600 dark:text-purple-400">
        <Database className="w-5 h-5" />
        <h4 className="font-semibold">V2 Data Vault Configuration</h4>
      </div>

      {/* Existing Hubs */}
      {(config.hubs || []).length > 0 && (
        <div className="space-y-2">
          <p className="text-sm font-medium text-gray-300">Hubs ({config.hubs!.length})</p>
          {config.hubs!.map((hub, i) => (
            <div key={i} className="flex items-center justify-between p-3 bg-gray-800/50 rounded-lg">
              <div className="flex items-center gap-3">
                <Key className="w-4 h-4 text-purple-500" />
                <div>
                  <span className="font-medium text-white">{hub.hub_name}</span>
                  <span className="ml-2 text-xs text-gray-400">Keys: {hub.business_keys.join(', ')} | Table: {hub.target_table}</span>
                </div>
              </div>
              <Button variant="ghost" size="sm" onClick={() => {
                onChange({ ...config, hubs: config.hubs!.filter((_, idx) => idx !== i) })
              }}>
                <Trash2 className="w-4 h-4 text-red-500" />
              </Button>
            </div>
          ))}
        </div>
      )}

      {/* Add Hub */}
      <div className="p-4 border border-dashed border-purple-300 dark:border-purple-700 rounded-lg space-y-3">
        <p className="text-sm font-medium text-gray-300">Add Hub</p>
        <Input label="Hub Name" placeholder="e.g., hub_customer" value={newHubName} onChange={(e) => setNewHubName(e.target.value)} />
        <Input label="Target Table" placeholder="e.g., hub_customer" value={newHubTable} onChange={(e) => setNewHubTable(e.target.value)} />
        <div>
          <label className="block text-xs font-medium text-gray-400 mb-2">Business Keys</label>
          <div className="flex flex-wrap gap-2">
            {columns.map((col) => (
              <button key={col.name} onClick={() => {
                setNewHubKeys((prev) => prev.includes(col.name) ? prev.filter((c) => c !== col.name) : [...prev, col.name])
              }} className={cn('px-2 py-1 text-xs rounded border transition-colors', newHubKeys.includes(col.name) ? 'bg-purple-500 text-white border-purple-500' : 'bg-gray-800 dark:bg-gray-700 border-gray-600')}>
                {col.name}
              </button>
            ))}
          </div>
        </div>
        <Button onClick={addHub} disabled={!newHubName || newHubKeys.length === 0 || !newHubTable} className="w-full">
          <Plus className="w-4 h-4 mr-2" />Add Hub
        </Button>
      </div>

      {/* Existing Satellites */}
      {(config.satellites || []).length > 0 && (
        <div className="space-y-2">
          <p className="text-sm font-medium text-gray-300">Satellites ({config.satellites!.length})</p>
          {config.satellites!.map((sat, i) => (
            <div key={i} className="flex items-center justify-between p-3 bg-gray-800/50 rounded-lg">
              <div>
                <span className="font-medium text-white">{sat.satellite_name}</span>
                <span className="ml-2 text-xs text-gray-400">Hub: {sat.hub_reference} | Cols: {sat.descriptive_columns.join(', ')}</span>
              </div>
              <Button variant="ghost" size="sm" onClick={() => {
                onChange({ ...config, satellites: config.satellites!.filter((_, idx) => idx !== i) })
              }}>
                <Trash2 className="w-4 h-4 text-red-500" />
              </Button>
            </div>
          ))}
        </div>
      )}

      {/* Add Satellite */}
      <div className="p-4 border border-dashed border-purple-300 dark:border-purple-700 rounded-lg space-y-3">
        <p className="text-sm font-medium text-gray-300">Add Satellite</p>
        <Input label="Satellite Name" placeholder="e.g., sat_customer_details" value={newSatName} onChange={(e) => setNewSatName(e.target.value)} />
        <Select
          label="Hub Reference"
          value={newSatHub}
          onChange={(e) => setNewSatHub(e.target.value)}
          options={[
            { value: '', label: 'Select a hub...' },
            ...(config.hubs || []).map((h) => ({ value: h.hub_name, label: h.hub_name })),
          ]}
        />
        <Input label="Target Table" placeholder="e.g., sat_customer_details" value={newSatTable} onChange={(e) => setNewSatTable(e.target.value)} />
        <div>
          <label className="block text-xs font-medium text-gray-400 mb-2">Descriptive Columns</label>
          <div className="flex flex-wrap gap-2">
            {columns.map((col) => (
              <button key={col.name} onClick={() => {
                setNewSatCols((prev) => prev.includes(col.name) ? prev.filter((c) => c !== col.name) : [...prev, col.name])
              }} className={cn('px-2 py-1 text-xs rounded border transition-colors', newSatCols.includes(col.name) ? 'bg-purple-500 text-white border-purple-500' : 'bg-gray-800 dark:bg-gray-700 border-gray-600')}>
                {col.name}
              </button>
            ))}
          </div>
        </div>
        <Button onClick={addSatellite} disabled={!newSatName || !newSatHub || newSatCols.length === 0 || !newSatTable} className="w-full">
          <Plus className="w-4 h-4 mr-2" />Add Satellite
        </Button>
      </div>
    </div>
  )
}

// =============================================================================
// V2 Star Schema Configuration Form
// =============================================================================

function V2StarSchemaConfigForm({
  config,
  onChange,
  columns,
}: {
  config: V2GoldModelConfig
  onChange: (config: V2GoldModelConfig) => void
  columns: ColumnDefinition[]
}) {
  // Dimension builder state
  const [newDimName, setNewDimName] = useState('')
  const [newDimKey, setNewDimKey] = useState('')
  const [newDimSCD, setNewDimSCD] = useState<V2SCDType>(2)
  const [newDimCols, setNewDimCols] = useState<string[]>([])
  const [newDimTable, setNewDimTable] = useState('')

  // Fact builder state
  const [newFactName, setNewFactName] = useState('')
  const [newFactGrain, setNewFactGrain] = useState<string[]>([])
  const [newFactTable, setNewFactTable] = useState('')
  const [newFactMeasures, setNewFactMeasures] = useState<V2MeasureConfig[]>([])
  const [newFactLookups, setNewFactLookups] = useState<V2DimensionLookup[]>([])

  // Temp measure state
  const [tmpMeasureName, setTmpMeasureName] = useState('')
  const [tmpMeasureCol, setTmpMeasureCol] = useState('')
  const [tmpMeasureAgg, setTmpMeasureAgg] = useState('sum')

  // Temp lookup state
  const [tmpLookupDim, setTmpLookupDim] = useState('')
  const [tmpLookupKey, setTmpLookupKey] = useState('')
  const [tmpLookupSK, setTmpLookupSK] = useState('')

  const addDimension = () => {
    if (!newDimName || !newDimKey || newDimCols.length === 0 || !newDimTable) return
    const dim: V2DimensionConfig = {
      dimension_name: newDimName,
      natural_key: newDimKey,
      scd_type: newDimSCD,
      columns: newDimCols,
      target_table: newDimTable,
    }
    onChange({ ...config, dimensions: [...(config.dimensions || []), dim] })
    setNewDimName('')
    setNewDimKey('')
    setNewDimSCD(2)
    setNewDimCols([])
    setNewDimTable('')
  }

  const addMeasure = () => {
    if (!tmpMeasureName || !tmpMeasureAgg) return
    setNewFactMeasures((prev) => [...prev, { name: tmpMeasureName, source_column: tmpMeasureCol || tmpMeasureName, agg_function: tmpMeasureAgg as V2AggFunction }])
    setTmpMeasureName('')
    setTmpMeasureCol('')
    setTmpMeasureAgg('sum')
  }

  const addLookup = () => {
    if (!tmpLookupDim || !tmpLookupKey) return
    setNewFactLookups((prev) => [...prev, { dimension_name: tmpLookupDim, join_key: tmpLookupKey, surrogate_key: tmpLookupSK || undefined }])
    setTmpLookupDim('')
    setTmpLookupKey('')
    setTmpLookupSK('')
  }

  const addFact = () => {
    if (!newFactName || newFactGrain.length === 0 || newFactMeasures.length === 0 || !newFactTable) return
    const fact: V2FactConfig = {
      fact_name: newFactName,
      grain_columns: newFactGrain,
      measures: newFactMeasures,
      dimension_lookups: newFactLookups,
      target_table: newFactTable,
    }
    onChange({ ...config, facts: [...(config.facts || []), fact] })
    setNewFactName('')
    setNewFactGrain([])
    setNewFactTable('')
    setNewFactMeasures([])
    setNewFactLookups([])
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 text-yellow-600 dark:text-yellow-400">
        <Star className="w-5 h-5" />
        <h4 className="font-semibold">V2 Star Schema Configuration</h4>
      </div>

      {/* Existing Dimensions */}
      {(config.dimensions || []).length > 0 && (
        <div className="space-y-2">
          <p className="text-sm font-medium text-gray-300">Dimensions ({config.dimensions!.length})</p>
          {config.dimensions!.map((dim, i) => (
            <div key={i} className="flex items-center justify-between p-3 bg-gray-800/50 rounded-lg">
              <div>
                <span className="font-medium text-white">{dim.dimension_name}</span>
                <span className="ml-2 text-xs text-gray-400">SCD{dim.scd_type} | Key: {dim.natural_key} | Table: {dim.target_table}</span>
              </div>
              <Button variant="ghost" size="sm" onClick={() => {
                onChange({ ...config, dimensions: config.dimensions!.filter((_, idx) => idx !== i) })
              }}>
                <Trash2 className="w-4 h-4 text-red-500" />
              </Button>
            </div>
          ))}
        </div>
      )}

      {/* Add Dimension */}
      <div className="p-4 border border-dashed border-yellow-600/50 rounded-lg space-y-3">
        <p className="text-sm font-medium text-gray-300">Add Dimension</p>
        <div className="grid grid-cols-2 gap-3">
          <Input label="Dimension Name" placeholder="e.g., dim_customer" value={newDimName} onChange={(e) => setNewDimName(e.target.value)} />
          <Input label="Target Table" placeholder="e.g., dim_customer" value={newDimTable} onChange={(e) => setNewDimTable(e.target.value)} />
        </div>
        <div className="grid grid-cols-2 gap-3">
          <Input label="Natural Key Column" placeholder="e.g., customer_id" value={newDimKey} onChange={(e) => setNewDimKey(e.target.value)} />
          <Select
            label="SCD Type"
            value={String(newDimSCD)}
            onChange={(e) => setNewDimSCD(Number(e.target.value) as V2SCDType)}
            options={[
              { value: '0', label: 'Type 0 (Retain Original)' },
              { value: '1', label: 'Type 1 (Overwrite)' },
              { value: '2', label: 'Type 2 (History Rows)' },
              { value: '3', label: 'Type 3 (Previous Column)' },
            ]}
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-400 mb-2">Columns</label>
          <div className="flex flex-wrap gap-2">
            {columns.map((col) => (
              <button key={col.name} onClick={() => {
                setNewDimCols((prev) => prev.includes(col.name) ? prev.filter((c) => c !== col.name) : [...prev, col.name])
              }} className={cn('px-2 py-1 text-xs rounded border transition-colors', newDimCols.includes(col.name) ? 'bg-yellow-600 text-white border-yellow-500' : 'bg-gray-800 dark:bg-gray-700 border-gray-600')}>
                {col.name}
              </button>
            ))}
          </div>
        </div>
        <Button onClick={addDimension} disabled={!newDimName || !newDimKey || newDimCols.length === 0 || !newDimTable} className="w-full">
          <Plus className="w-4 h-4 mr-2" />Add Dimension
        </Button>
      </div>

      {/* Existing Facts */}
      {(config.facts || []).length > 0 && (
        <div className="space-y-2">
          <p className="text-sm font-medium text-gray-300">Facts ({config.facts!.length})</p>
          {config.facts!.map((fact, i) => (
            <div key={i} className="flex items-center justify-between p-3 bg-gray-800/50 rounded-lg">
              <div>
                <span className="font-medium text-white">{fact.fact_name}</span>
                <span className="ml-2 text-xs text-gray-400">
                  Grain: {fact.grain_columns.join(', ')} | Measures: {fact.measures.length} | Lookups: {fact.dimension_lookups.length}
                </span>
              </div>
              <Button variant="ghost" size="sm" onClick={() => {
                onChange({ ...config, facts: config.facts!.filter((_, idx) => idx !== i) })
              }}>
                <Trash2 className="w-4 h-4 text-red-500" />
              </Button>
            </div>
          ))}
        </div>
      )}

      {/* Add Fact */}
      <div className="p-4 border border-dashed border-yellow-600/50 rounded-lg space-y-3">
        <p className="text-sm font-medium text-gray-300">Add Fact</p>
        <div className="grid grid-cols-2 gap-3">
          <Input label="Fact Name" placeholder="e.g., fact_sales" value={newFactName} onChange={(e) => setNewFactName(e.target.value)} />
          <Input label="Target Table" placeholder="e.g., fact_sales" value={newFactTable} onChange={(e) => setNewFactTable(e.target.value)} />
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-400 mb-2">Grain Columns</label>
          <div className="flex flex-wrap gap-2">
            {columns.map((col) => (
              <button key={col.name} onClick={() => {
                setNewFactGrain((prev) => prev.includes(col.name) ? prev.filter((c) => c !== col.name) : [...prev, col.name])
              }} className={cn('px-2 py-1 text-xs rounded border transition-colors', newFactGrain.includes(col.name) ? 'bg-yellow-600 text-white border-yellow-500' : 'bg-gray-800 dark:bg-gray-700 border-gray-600')}>
                {col.name}
              </button>
            ))}
          </div>
        </div>

        {/* Measures sub-builder */}
        <div className="p-3 bg-gray-900/30 rounded-lg space-y-2">
          <p className="text-xs font-medium text-gray-400">Measures ({newFactMeasures.length})</p>
          {newFactMeasures.map((m, i) => (
            <div key={i} className="flex items-center gap-2 text-xs text-gray-300">
              <span>{m.name} ({m.agg_function}{m.source_column ? ` on ${m.source_column}` : ''})</span>
              <button onClick={() => setNewFactMeasures((prev) => prev.filter((_, idx) => idx !== i))} className="text-red-400 hover:text-red-300">
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          ))}
          <div className="grid grid-cols-4 gap-2">
            <Input placeholder="Measure name" value={tmpMeasureName} onChange={(e) => setTmpMeasureName(e.target.value)} className="!py-1" />
            <Input placeholder="Source column" value={tmpMeasureCol} onChange={(e) => setTmpMeasureCol(e.target.value)} className="!py-1" />
            <Select value={tmpMeasureAgg} onChange={(e) => setTmpMeasureAgg(e.target.value)} options={[
              { value: 'sum', label: 'SUM' }, { value: 'count', label: 'COUNT' }, { value: 'avg', label: 'AVG' },
              { value: 'min', label: 'MIN' }, { value: 'max', label: 'MAX' }, { value: 'count_distinct', label: 'COUNT DISTINCT' },
            ]} className="!py-1" />
            <Button size="sm" onClick={addMeasure} disabled={!tmpMeasureName}>
              <Plus className="w-3 h-3" />
            </Button>
          </div>
        </div>

        {/* Dimension Lookups sub-builder */}
        <div className="p-3 bg-gray-900/30 rounded-lg space-y-2">
          <p className="text-xs font-medium text-gray-400">Dimension Lookups ({newFactLookups.length})</p>
          {newFactLookups.map((lk, i) => (
            <div key={i} className="flex items-center gap-2 text-xs text-gray-300">
              <span>{lk.dimension_name} on {lk.join_key}{lk.surrogate_key ? ` (SK: ${lk.surrogate_key})` : ''}</span>
              <button onClick={() => setNewFactLookups((prev) => prev.filter((_, idx) => idx !== i))} className="text-red-400 hover:text-red-300">
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          ))}
          <div className="grid grid-cols-4 gap-2">
            <Select value={tmpLookupDim} onChange={(e) => setTmpLookupDim(e.target.value)} options={[
              { value: '', label: 'Dimension...' },
              ...(config.dimensions || []).map((d) => ({ value: d.dimension_name, label: d.dimension_name })),
            ]} className="!py-1" />
            <Input placeholder="Join key" value={tmpLookupKey} onChange={(e) => setTmpLookupKey(e.target.value)} className="!py-1" />
            <Input placeholder="Surrogate key (opt)" value={tmpLookupSK} onChange={(e) => setTmpLookupSK(e.target.value)} className="!py-1" />
            <Button size="sm" onClick={addLookup} disabled={!tmpLookupDim || !tmpLookupKey}>
              <Plus className="w-3 h-3" />
            </Button>
          </div>
        </div>

        <Button onClick={addFact} disabled={!newFactName || newFactGrain.length === 0 || newFactMeasures.length === 0 || !newFactTable} className="w-full">
          <Plus className="w-4 h-4 mr-2" />Add Fact
        </Button>
      </div>
    </div>
  )
}

// =============================================================================
// Main Component
// =============================================================================

export function GoldModelingSelector({
  value,
  onChange,
  availableColumns = [],
  leftEntityName = '',
  className,
  v2Mode = false,
  onV2GoldModelChange,
}: GoldModelingSelectorProps) {
  const [selectedStrategy, setSelectedStrategy] = useState<GoldModelingStrategy | null>(
    value?.modeling_strategy || null
  )
  const [v2Config, setV2Config] = useState<V2GoldModelConfig>({ model_type: 'flat' })
  const [v2ModelType, setV2ModelType] = useState<V2GoldModelType>('flat')

  const handleV2ConfigChange = useCallback(
    (updated: V2GoldModelConfig) => {
      setV2Config(updated)
      onV2GoldModelChange?.(updated)
    },
    [onV2GoldModelChange]
  )

  const handleV2ModelTypeChange = useCallback(
    (modelType: V2GoldModelType) => {
      setV2ModelType(modelType)
      const updated: V2GoldModelConfig = { ...v2Config, model_type: modelType }
      setV2Config(updated)
      onV2GoldModelChange?.(updated)
    },
    [v2Config, onV2GoldModelChange]
  )

  const handleStrategySelect = useCallback(
    (strategy: GoldModelingStrategy) => {
      setSelectedStrategy(strategy)
      onChange({
        ...value,
        modeling_strategy: strategy,
      })
    },
    [value, onChange]
  )

  return (
    <div className={cn('space-y-6', className)}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-white dark:text-white">
            Gold Zone Modeling Strategy
          </h3>
          <p className="text-sm text-gray-400 dark:text-gray-400">
            Select the data modeling pattern for your Gold layer output
          </p>
        </div>
        {selectedStrategy && (
          <Badge variant="info">{selectedStrategy.replace('_', ' ').toUpperCase()}</Badge>
        )}
      </div>

      {/* Warning for restricted */}
      {selectedStrategy === 'flat_table' && (
        <div className="p-3 bg-amber-900/30 dark:bg-yellow-900/20 border border-amber-600/50 dark:border-yellow-800 rounded-lg flex items-start gap-2">
          <AlertTriangle className="w-5 h-5 text-yellow-500 flex-shrink-0" />
          <div className="text-sm text-yellow-700 dark:text-yellow-300">
            <strong>Restricted Use:</strong> Flat/Reporting tables should only be used for
            extracts or ad-hoc reporting. For enterprise data warehousing, consider
            Star Schema or Data Vault 2.0.
          </div>
        </div>
      )}

      {/* Strategy Selection Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {MODELING_STRATEGIES.map((strategy) => (
          <StrategyCard
            key={strategy.id}
            strategy={strategy}
            selected={selectedStrategy === strategy.id}
            onClick={() => handleStrategySelect(strategy.id)}
          />
        ))}
      </div>

      {/* Strategy-specific Configuration */}
      {selectedStrategy && (
        <Card className="p-4">
          {selectedStrategy === 'data_vault_2' && (
            <DataVaultConfigForm
              config={value?.data_vault_config}
              onChange={(dvConfig) =>
                onChange({ ...value, data_vault_config: dvConfig })
              }
              columns={availableColumns}
            />
          )}

          {selectedStrategy === 'star_schema' && (
            <StarSchemaConfigForm
              config={value?.star_schema_config}
              onChange={(ssConfig) =>
                onChange({ ...value, star_schema_config: ssConfig })
              }
              columns={availableColumns}
            />
          )}

          {selectedStrategy === 'snowflake_schema' && (
            <div className="text-center py-8 text-gray-400">
              <Snowflake className="w-12 h-12 mx-auto mb-4 text-cyan-400" />
              <p className="font-medium">Snowflake Schema Configuration</p>
              <p className="text-sm">
                Extends Star Schema with normalized dimensions.
                <br />
                Configure Star Schema first, then add normalization rules.
              </p>
            </div>
          )}

          {selectedStrategy === 'flat_table' && (
            <div className="space-y-4">
              <div className="flex items-center gap-2 text-gray-400 dark:text-gray-400">
                <Table className="w-5 h-5" />
                <h4 className="font-semibold">Flat Table Configuration</h4>
              </div>

              <Input
                label="Table Name"
                placeholder="e.g., rpt_daily_sales_summary"
                value={value?.flat_table_config?.table_name || ''}
                onChange={(e) =>
                  onChange({
                    ...value,
                    flat_table_config: {
                      ...value?.flat_table_config,
                      table_name: e.target.value,
                    } as any,
                  })
                }
              />

              <Input
                label="Description"
                placeholder="Purpose of this flat table..."
                value={value?.flat_table_config?.description || ''}
                onChange={(e) =>
                  onChange({
                    ...value,
                    flat_table_config: {
                      ...value?.flat_table_config,
                      description: e.target.value,
                    } as any,
                  })
                }
              />

              <Select
                label="Refresh Strategy"
                value={value?.flat_table_config?.refresh_strategy || 'full'}
                onChange={(e) =>
                  onChange({
                    ...value,
                    flat_table_config: {
                      ...value?.flat_table_config,
                      refresh_strategy: e.target.value as 'full' | 'incremental' | 'snapshot',
                    } as any,
                  })
                }
                options={[
                  { value: 'full', label: 'Full Refresh (Overwrite)' },
                  { value: 'incremental', label: 'Incremental' },
                  { value: 'snapshot', label: 'Daily Snapshot' },
                ]}
              />

              <Input
                label="Retention Days"
                type="number"
                placeholder="90"
                value={value?.flat_table_config?.retention_days?.toString() || ''}
                onChange={(e) =>
                  onChange({
                    ...value,
                    flat_table_config: {
                      ...value?.flat_table_config,
                      retention_days: parseInt(e.target.value) || undefined,
                    } as any,
                  })
                }
              />
            </div>
          )}
        </Card>
      )}

      {/* V2 Mode: Model Type Selection and Config */}
      {v2Mode && (
        <>
          <div className="border-t border-gray-700 pt-6 mt-6">
            <h3 className="text-lg font-semibold text-white mb-2">V2 Gold Model Configuration</h3>
            <p className="text-sm text-gray-400 mb-4">Configure the V2 FeedGroupConfig gold model type</p>

            <div className="grid grid-cols-4 gap-3 mb-4">
              {(['flat', 'scd2', 'data_vault', 'star_schema'] as V2GoldModelType[]).map((mt) => (
                <button
                  key={mt}
                  onClick={() => handleV2ModelTypeChange(mt)}
                  className={cn(
                    'p-3 rounded-lg border-2 text-sm font-medium transition-all',
                    v2ModelType === mt
                      ? 'border-blue-500 bg-blue-900/30 text-white'
                      : 'border-gray-700 text-gray-400 hover:border-blue-700'
                  )}
                >
                  {mt.replace('_', ' ').toUpperCase()}
                </button>
              ))}
            </div>

            {v2ModelType === 'data_vault' && (
              <Card className="p-4">
                <V2DataVaultConfigForm
                  config={v2Config}
                  onChange={handleV2ConfigChange}
                  columns={availableColumns}
                />
              </Card>
            )}

            {v2ModelType === 'star_schema' && (
              <Card className="p-4">
                <V2StarSchemaConfigForm
                  config={v2Config}
                  onChange={handleV2ConfigChange}
                  columns={availableColumns}
                />
              </Card>
            )}

            {v2ModelType === 'scd2' && (
              <Card className="p-4">
                <div className="text-sm text-gray-400">
                  SCD Type 2 is configured via the pipeline-level business keys and tracked columns.
                  No additional gold model configuration is needed.
                </div>
              </Card>
            )}

            {v2ModelType === 'flat' && (
              <Card className="p-4">
                <div className="text-sm text-gray-400">
                  Flat model produces a simple denormalized table. No additional configuration needed.
                </div>
              </Card>
            )}
          </div>
        </>
      )}

      {/* Join Dependencies - Show when any strategy is selected */}
      {selectedStrategy && (
        <Card className="p-4">
          <JoinDependencyBuilder
            dependencies={value?.join_dependencies || []}
            onChange={(deps: JoinDependency[]) =>
              onChange({ ...value, join_dependencies: deps })
            }
            leftEntityName={
              leftEntityName ||
              value?.star_schema_config?.fact_table?.table_name ||
              value?.flat_table_config?.table_name ||
              'your_table'
            }
            availableColumns={availableColumns}
          />
        </Card>
      )}
    </div>
  )
}

export default GoldModelingSelector
