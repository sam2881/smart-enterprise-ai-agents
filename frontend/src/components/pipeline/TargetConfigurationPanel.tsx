'use client'

/**
 * TargetConfigurationPanel - Intelligent Zone Configuration
 *
 * Features:
 * - Auto-generate zone configs from feed metadata (domain, feed_name)
 * - Editable zone table with intelligent defaults
 * - Advanced options: clustering, retention, Data Vault config
 * - Deterministic naming convention: {zone}_{domain} / {feed_name}_{zone}
 *
 * UI writes to metadata ONLY - generates zone_configurations rows.
 */

import React, { useState, useCallback, useMemo } from 'react'
import type {
  TargetConfig,
  SchemaConfig,
  ContractType,
  WriteMode,
  TargetZone,
  ColumnDefinition,
} from '@/types/pipeline-canonical'

// =============================================================================
// Types
// =============================================================================

interface TargetConfigurationPanelProps {
  feedName: string
  domain: string
  schema: Partial<SchemaConfig>
  contractType?: ContractType
  target: Partial<TargetConfig>
  onChange: (target: Partial<TargetConfig>) => void
}

interface ZoneConfig {
  zone: TargetZone
  label: string
  dataset: string
  table: string
  writeMode: WriteMode
  partitionField: string
  enabled: boolean
}

// =============================================================================
// Constants
// =============================================================================

const ZONES: Array<{ zone: TargetZone; label: string; description: string }> = [
  { zone: 'landing', label: 'Landing', description: 'Raw copy, zero transformation, STRING columns' },
  { zone: 'bronze', label: 'Bronze', description: 'Type casting, audit columns, basic schema' },
  { zone: 'silver', label: 'Silver', description: 'Cleaning, dedup, standardization' },
  { zone: 'gold', label: 'Gold', description: 'Final curated — business logic, Data Vault ready, analytics' },
]

const WRITE_MODES: Array<{ value: WriteMode; label: string }> = [
  { value: 'append', label: 'Append' },
  { value: 'overwrite', label: 'Overwrite' },
  { value: 'merge', label: 'Merge' },
  { value: 'scd_type_1', label: 'SCD Type 1' },
  { value: 'scd_type_2', label: 'SCD Type 2' },
]

// =============================================================================
// Helper: Deterministic naming
// =============================================================================

function sanitizeName(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '')
}

function generateZoneDefaults(
  feedName: string,
  domain: string,
  schema: Partial<SchemaConfig>,
  contractType?: ContractType
): ZoneConfig[] {
  const safeFeed = sanitizeName(feedName || 'feed')
  const safeDomain = sanitizeName(domain || 'default')
  const columns = schema.columns || []

  // Find best partition candidate
  const dateCol = columns.find(
    (c) => c.type === 'date' || c.type === 'timestamp' || c.name.includes('date')
  )
  const partitionCol = dateCol?.name || 'ingestion_date'

  return ZONES.map(({ zone, label }) => {
    const isEarly = zone === 'landing' || zone === 'bronze'

    return {
      zone,
      label,
      dataset: `${zone}_${safeDomain}`,
      table: `${safeFeed}_${zone}`,
      writeMode: isEarly ? 'append' : (contractType === 'SCD2' ? 'scd_type_2' : 'merge'),
      partitionField: zone === 'landing' ? 'batch_id' : partitionCol,
      enabled: true,
    }
  })
}

// =============================================================================
// Component
// =============================================================================

export function TargetConfigurationPanel({
  feedName,
  domain,
  schema,
  contractType,
  target,
  onChange,
}: TargetConfigurationPanelProps) {
  const [zones, setZones] = useState<ZoneConfig[]>(() =>
    generateZoneDefaults(feedName, domain, schema, contractType)
  )
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [clusteringFields, setClusteringFields] = useState<string[]>(
    target.clustering_fields || []
  )
  const [retentionDays, setRetentionDays] = useState(target.table_maintenance?.retention_days || 365)
  const [generated, setGenerated] = useState(false)

  const columns = schema.columns || []
  const primaryKeys = useMemo(
    () => schema.primary_keys || columns.filter((c) => c.pk).map((c) => c.name),
    [schema, columns]
  )

  const autoGenerate = useCallback(() => {
    const newZones = generateZoneDefaults(feedName, domain, schema, contractType)
    setZones(newZones)
    setGenerated(true)

    // Set the final zone as the target config
    const finalZone = newZones.filter((z) => z.enabled).pop()
    if (finalZone) {
      onChange({
        ...target,
        target_zone: finalZone.zone,
        bq_dataset: finalZone.dataset,
        bq_table: finalZone.table,
        write_mode: finalZone.writeMode,
        partition_field: finalZone.partitionField,
        clustering_fields: primaryKeys.slice(0, 4),
        merge_keys: primaryKeys,
      })
    }
  }, [feedName, domain, schema, contractType, target, onChange, primaryKeys])

  const updateZone = useCallback(
    (index: number, field: keyof ZoneConfig, value: any) => {
      const updated = [...zones]
      updated[index] = { ...updated[index], [field]: value }
      setZones(updated)

      // Sync the final enabled zone to target config
      const finalZone = updated.filter((z) => z.enabled).pop()
      if (finalZone) {
        onChange({
          ...target,
          target_zone: finalZone.zone,
          bq_dataset: finalZone.dataset,
          bq_table: finalZone.table,
          write_mode: finalZone.writeMode,
          partition_field: finalZone.partitionField,
        })
      }
    },
    [zones, target, onChange]
  )

  const toggleClustering = useCallback(
    (colName: string) => {
      const updated = clusteringFields.includes(colName)
        ? clusteringFields.filter((c) => c !== colName)
        : clusteringFields.length < 4
        ? [...clusteringFields, colName]
        : clusteringFields
      setClusteringFields(updated)
      onChange({ ...target, clustering_fields: updated })
    },
    [clusteringFields, target, onChange]
  )

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-gray-900">Target Configuration</h3>
        <button
          type="button"
          onClick={autoGenerate}
          className="px-4 py-2 bg-gray-900 text-white rounded-md text-sm font-medium hover:bg-gray-800 flex items-center gap-2"
        >
          Auto-Generate Zones
        </button>
      </div>

      {!generated && !feedName && (
        <div className="text-center py-6 text-gray-400 text-sm">
          Fill in Pipeline Identity (name, domain) and Schema to auto-generate zone configurations.
        </div>
      )}

      {/* Zone config table */}
      {(generated || feedName) && (
        <div className="border border-gray-200 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase w-8"></th>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Zone</th>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Dataset</th>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Table</th>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Write Mode</th>
                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Partition</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {zones.map((zone, idx) => (
                <tr
                  key={zone.zone}
                  className={zone.enabled ? 'hover:bg-gray-50' : 'bg-gray-50 opacity-50'}
                >
                  <td className="px-3 py-2">
                    <input
                      type="checkbox"
                      checked={zone.enabled}
                      onChange={(e) => updateZone(idx, 'enabled', e.target.checked)}
                      className="w-4 h-4 text-blue-600 rounded"
                    />
                  </td>
                  <td className="px-3 py-2">
                    <div>
                      <span className="font-medium text-gray-900">{zone.label}</span>
                      <p className="text-xs text-gray-400">
                        {ZONES.find((z) => z.zone === zone.zone)?.description}
                      </p>
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    <input
                      type="text"
                      value={zone.dataset}
                      onChange={(e) => updateZone(idx, 'dataset', e.target.value)}
                      disabled={!zone.enabled}
                      className="w-full px-2 py-1 border border-gray-200 rounded text-xs font-mono bg-white disabled:bg-gray-100"
                    />
                  </td>
                  <td className="px-3 py-2">
                    <input
                      type="text"
                      value={zone.table}
                      onChange={(e) => updateZone(idx, 'table', e.target.value)}
                      disabled={!zone.enabled}
                      className="w-full px-2 py-1 border border-gray-200 rounded text-xs font-mono bg-white disabled:bg-gray-100"
                    />
                  </td>
                  <td className="px-3 py-2">
                    <select
                      value={zone.writeMode}
                      onChange={(e) => updateZone(idx, 'writeMode', e.target.value)}
                      disabled={!zone.enabled}
                      className="w-full px-2 py-1 border border-gray-200 rounded text-xs bg-white disabled:bg-gray-100"
                    >
                      {WRITE_MODES.map((m) => (
                        <option key={m.value} value={m.value}>{m.label}</option>
                      ))}
                    </select>
                  </td>
                  <td className="px-3 py-2">
                    <input
                      type="text"
                      value={zone.partitionField}
                      onChange={(e) => updateZone(idx, 'partitionField', e.target.value)}
                      disabled={!zone.enabled}
                      className="w-full px-2 py-1 border border-gray-200 rounded text-xs font-mono bg-white disabled:bg-gray-100"
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Naming convention hint */}
      {generated && (
        <div className="px-3 py-2 text-xs rounded-md border bg-blue-50 border-blue-200 text-blue-700">
          Naming convention: Dataset = {'{zone}_{domain}'}, Table = {'{feed_name}_{zone}'}.
          Edit any field to override.
        </div>
      )}

      {/* Advanced options toggle */}
      <button
        type="button"
        onClick={() => setShowAdvanced(!showAdvanced)}
        className="text-sm text-gray-500 hover:text-gray-700 flex items-center gap-1"
      >
        <span className={`transform transition-transform ${showAdvanced ? 'rotate-90' : ''}`}>
          ▸
        </span>
        Advanced Options
      </button>

      {showAdvanced && (
        <div className="space-y-4 pl-4 border-l-2 border-gray-200">
          {/* Clustering fields */}
          <div className="space-y-2">
            <label className="text-sm font-medium text-gray-700">
              Clustering Fields (max 4)
            </label>
            <p className="text-xs text-gray-500">
              BigQuery clustering improves query performance. Select up to 4 columns.
            </p>
            <div className="flex flex-wrap gap-1">
              {columns.map((col) => (
                <button
                  key={col.name}
                  type="button"
                  onClick={() => toggleClustering(col.name)}
                  className={`px-2 py-1 text-xs rounded-full border ${
                    clusteringFields.includes(col.name)
                      ? 'bg-blue-100 border-blue-300 text-blue-700'
                      : 'bg-white border-gray-200 text-gray-600 hover:border-gray-300'
                  } ${
                    !clusteringFields.includes(col.name) && clusteringFields.length >= 4
                      ? 'opacity-40 cursor-not-allowed'
                      : ''
                  }`}
                  disabled={!clusteringFields.includes(col.name) && clusteringFields.length >= 4}
                >
                  {col.name}
                </button>
              ))}
            </div>
          </div>

          {/* Merge keys */}
          {(contractType === 'SCD2' || contractType === 'DATA_VAULT') && (
            <div className="space-y-2">
              <label className="text-sm font-medium text-gray-700">Merge Keys</label>
              <p className="text-xs text-gray-500">
                Primary/business keys used for merge operations.
              </p>
              <div className="flex flex-wrap gap-1">
                {columns.map((col) => (
                  <button
                    key={col.name}
                    type="button"
                    onClick={() => {
                      const current = target.merge_keys || []
                      const updated = current.includes(col.name)
                        ? current.filter((c) => c !== col.name)
                        : [...current, col.name]
                      onChange({ ...target, merge_keys: updated })
                    }}
                    className={`px-2 py-1 text-xs rounded-full border ${
                      (target.merge_keys || []).includes(col.name)
                        ? 'bg-purple-100 border-purple-300 text-purple-700'
                        : 'bg-white border-gray-200 text-gray-600 hover:border-gray-300'
                    }`}
                  >
                    {col.name}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Retention policy */}
          <div className="space-y-2">
            <label className="text-sm font-medium text-gray-700">Retention Policy</label>
            <div className="flex items-center gap-3">
              <input
                type="number"
                value={retentionDays}
                onChange={(e) => {
                  const val = parseInt(e.target.value) || 365
                  setRetentionDays(val)
                  onChange({
                    ...target,
                    table_maintenance: {
                      ...target.table_maintenance,
                      retention_days: val,
                    },
                  })
                }}
                min={1}
                max={3650}
                className="w-24 px-2 py-1.5 border border-gray-200 rounded text-sm"
              />
              <span className="text-sm text-gray-500">days</span>
              <div className="flex gap-2 ml-4">
                {[90, 180, 365, 730].map((d) => (
                  <button
                    key={d}
                    type="button"
                    onClick={() => {
                      setRetentionDays(d)
                      onChange({
                        ...target,
                        table_maintenance: {
                          ...target.table_maintenance,
                          retention_days: d,
                        },
                      })
                    }}
                    className={`px-2 py-1 text-xs rounded border ${
                      retentionDays === d
                        ? 'bg-blue-100 border-blue-300 text-blue-700'
                        : 'bg-white border-gray-200 text-gray-500'
                    }`}
                  >
                    {d}d
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* BQ Project + Location */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-sm font-medium text-gray-700">BQ Project</label>
              <input
                type="text"
                value={target.bq_project || ''}
                onChange={(e) => onChange({ ...target, bq_project: e.target.value })}
                placeholder="my-gcp-project"
                className="mt-1 w-full px-2 py-1.5 border border-gray-200 rounded text-sm"
              />
            </div>
            <div>
              <label className="text-sm font-medium text-gray-700">BQ Location</label>
              <input
                type="text"
                value={target.bq_location || ''}
                onChange={(e) => onChange({ ...target, bq_location: e.target.value })}
                placeholder="US"
                className="mt-1 w-full px-2 py-1.5 border border-gray-200 rounded text-sm"
              />
            </div>
          </div>

          {/* Table format */}
          <div>
            <label className="text-sm font-medium text-gray-700">Table Format</label>
            <div className="flex gap-2 mt-1">
              {(['parquet', 'delta', 'iceberg'] as const).map((fmt) => (
                <button
                  key={fmt}
                  type="button"
                  onClick={() => onChange({ ...target, table_format: fmt })}
                  className={`px-3 py-1.5 text-xs rounded-md border ${
                    target.table_format === fmt
                      ? 'bg-blue-100 border-blue-300 text-blue-700 font-medium'
                      : 'bg-white border-gray-200 text-gray-600 hover:border-gray-300'
                  }`}
                >
                  {fmt.charAt(0).toUpperCase() + fmt.slice(1)}
                </button>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default TargetConfigurationPanel
