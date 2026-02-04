'use client'

import React, { useState } from 'react'
import { cn } from '@/lib/utils'
import { Input } from '@/components/ui/Input'
import { Textarea } from '@/components/ui/Textarea'

// Gold zone operational configuration for enterprise data modeling
export interface GoldOperationalConfig {
  // Data Loading Strategy
  loading_strategy: LoadingStrategy

  // Slowly Changing Dimension (SCD) Configuration
  scd_config?: SCDConfig

  // Late Arriving Data Handling
  late_data_handling: LateDataConfig

  // Backfill Strategy
  backfill: BackfillConfig

  // Data Lineage & Audit
  lineage: LineageConfig

  // Output Configuration
  output: OutputConfig
}

export interface LoadingStrategy {
  mode: 'full_refresh' | 'incremental' | 'snapshot' | 'merge' | 'scd'
  incremental_key?: string  // Column for incremental loads
  watermark_column?: string  // For streaming incremental
  high_watermark_value?: string
  merge_keys?: string[]  // Columns for MERGE statement
  delete_detection: 'none' | 'soft_delete' | 'hard_delete' | 'compare_full'
  soft_delete_column?: string
}

export interface SCDConfig {
  scd_type: 1 | 2 | 3 | 4 | 6  // SCD Type
  effective_date_column: string
  end_date_column?: string  // For Type 2
  current_flag_column?: string  // For Type 2
  history_table?: string  // For Type 4
  // Type 2 specific
  track_changes_columns?: string[]  // Columns to track for changes
  hash_diff_enabled?: boolean  // Use hash comparison for changes
  // Type 3 specific
  previous_value_columns?: string[]  // Columns to keep previous values
}

export interface LateDataConfig {
  strategy: 'reject' | 'accept_all' | 'accept_within_window' | 'route_to_error' | 'reprocess'
  acceptance_window_hours?: number  // How late is acceptable
  reprocessing_trigger?: 'manual' | 'automatic' | 'scheduled'
  error_table?: string  // Where to route late data
  alert_on_late_data: boolean
  late_data_threshold_percent?: number  // Alert if > X% of batch is late
}

export interface BackfillConfig {
  enabled: boolean
  strategy: 'full_historical' | 'incremental_catchup' | 'partition_by_partition' | 'custom_date_range'
  partition_column?: string
  start_date?: string
  end_date?: string
  parallel_partitions?: number  // How many partitions to process in parallel
  checkpoint_enabled: boolean  // Resume from failure
  validation_mode: 'none' | 'row_count' | 'checksum' | 'full_compare'
}

export interface LineageConfig {
  track_source_files: boolean
  track_transformation_versions: boolean
  audit_columns: AuditColumns
  data_classification?: 'public' | 'internal' | 'confidential' | 'restricted'
  retention_days?: number
}

export interface AuditColumns {
  created_at: string
  created_by: string
  updated_at: string
  updated_by: string
  source_system?: string
  batch_id?: string
  record_hash?: string
}

export interface OutputConfig {
  format: 'bigquery' | 'delta' | 'iceberg' | 'parquet' | 'hive'
  clustering_columns?: string[]
  table_expiration_days?: number
  require_partition_filter: boolean
  enable_streaming_buffer?: boolean  // For BigQuery
  time_travel_days?: number  // For Delta/Iceberg
}

interface GoldOperationalConfigProps {
  value: Partial<GoldOperationalConfig>
  onChange: (config: GoldOperationalConfig) => void
  className?: string
}

type TabId = 'loading' | 'scd' | 'late_data' | 'backfill' | 'lineage' | 'output'

const TABS: { id: TabId; label: string; icon: string }[] = [
  { id: 'loading', label: 'Loading', icon: '📥' },
  { id: 'scd', label: 'SCD', icon: '📜' },
  { id: 'late_data', label: 'Late Data', icon: '⏰' },
  { id: 'backfill', label: 'Backfill', icon: '🔄' },
  { id: 'lineage', label: 'Lineage', icon: '🔗' },
  { id: 'output', label: 'Output', icon: '📤' },
]

export function GoldOperationalConfigPanel({ value, onChange, className }: GoldOperationalConfigProps) {
  const [activeTab, setActiveTab] = useState<TabId>('loading')

  // Initialize with defaults
  const config: GoldOperationalConfig = {
    loading_strategy: {
      mode: 'incremental',
      delete_detection: 'none',
      ...value.loading_strategy
    },
    scd_config: value.scd_config,
    late_data_handling: {
      strategy: 'accept_within_window',
      acceptance_window_hours: 24,
      alert_on_late_data: true,
      ...value.late_data_handling
    },
    backfill: {
      enabled: false,
      strategy: 'incremental_catchup',
      checkpoint_enabled: true,
      validation_mode: 'row_count',
      ...value.backfill
    },
    lineage: {
      track_source_files: true,
      track_transformation_versions: true,
      audit_columns: {
        created_at: '_created_at',
        created_by: '_created_by',
        updated_at: '_updated_at',
        updated_by: '_updated_by',
        ...value.lineage?.audit_columns
      },
      ...value.lineage
    },
    output: {
      format: 'bigquery',
      require_partition_filter: true,
      ...value.output
    }
  }

  const updateLoading = (updates: Partial<LoadingStrategy>) => {
    onChange({ ...config, loading_strategy: { ...config.loading_strategy, ...updates } })
  }

  const updateSCD = (updates: Partial<SCDConfig>) => {
    const current = config.scd_config || {
      scd_type: 2,
      effective_date_column: 'effective_date',
      end_date_column: 'end_date',
      current_flag_column: 'is_current'
    }
    onChange({ ...config, scd_config: { ...current, ...updates } })
  }

  const updateLateData = (updates: Partial<LateDataConfig>) => {
    onChange({ ...config, late_data_handling: { ...config.late_data_handling, ...updates } })
  }

  const updateBackfill = (updates: Partial<BackfillConfig>) => {
    onChange({ ...config, backfill: { ...config.backfill, ...updates } })
  }

  const updateLineage = (updates: Partial<LineageConfig>) => {
    onChange({ ...config, lineage: { ...config.lineage, ...updates } })
  }

  const updateOutput = (updates: Partial<OutputConfig>) => {
    onChange({ ...config, output: { ...config.output, ...updates } })
  }

  return (
    <div className={cn('space-y-4', className)}>
      {/* Header */}
      <div>
        <h3 className="text-lg font-semibold text-white flex items-center gap-2">
          <span className="text-yellow-400">🥇</span>
          Gold Zone Operational Config
        </h3>
        <p className="text-sm text-gray-400">
          Configure loading strategy, SCD handling, backfill, and data lineage
        </p>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 p-1 bg-gray-800/30 rounded-lg overflow-x-auto">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={cn(
              'px-3 py-2 rounded-md transition-all duration-200 whitespace-nowrap',
              'flex items-center gap-1.5 text-sm',
              activeTab === tab.id
                ? 'bg-yellow-600/20 text-yellow-400 border border-yellow-500/30'
                : 'text-gray-400 hover:text-white hover:bg-gray-700/50'
            )}
          >
            <span>{tab.icon}</span>
            <span>{tab.label}</span>
          </button>
        ))}
      </div>

      {/* Loading Strategy Tab */}
      {activeTab === 'loading' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Loading Mode *
            </label>
            <select
              value={config.loading_strategy.mode}
              onChange={(e) => updateLoading({ mode: e.target.value as LoadingStrategy['mode'] })}
              className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
              style={{ colorScheme: 'dark' }}
            >
              <option value="full_refresh" className="bg-gray-800">Full Refresh (truncate & reload)</option>
              <option value="incremental" className="bg-gray-800">Incremental (append new records)</option>
              <option value="snapshot" className="bg-gray-800">Snapshot (daily state capture)</option>
              <option value="merge" className="bg-gray-800">Merge (upsert)</option>
              <option value="scd" className="bg-gray-800">SCD (slowly changing dimensions)</option>
            </select>
          </div>

          {(config.loading_strategy.mode === 'incremental' || config.loading_strategy.mode === 'merge') && (
            <div className="grid grid-cols-2 gap-4">
              <Input
                label="Incremental Key Column"
                placeholder="updated_at"
                value={config.loading_strategy.incremental_key || ''}
                onChange={(e) => updateLoading({ incremental_key: e.target.value })}
                helperText="Column to track incremental changes"
              />
              <Input
                label="Watermark Column"
                placeholder="event_timestamp"
                value={config.loading_strategy.watermark_column || ''}
                onChange={(e) => updateLoading({ watermark_column: e.target.value })}
                helperText="For streaming incremental"
              />
            </div>
          )}

          {config.loading_strategy.mode === 'merge' && (
            <Input
              label="Merge Key Columns"
              placeholder="id, source_system"
              value={config.loading_strategy.merge_keys?.join(', ') || ''}
              onChange={(e) => updateLoading({ merge_keys: e.target.value.split(',').map(s => s.trim()).filter(Boolean) })}
              helperText="Comma-separated columns for MERGE ON clause"
            />
          )}

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Delete Detection
            </label>
            <select
              value={config.loading_strategy.delete_detection}
              onChange={(e) => updateLoading({ delete_detection: e.target.value as LoadingStrategy['delete_detection'] })}
              className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
              style={{ colorScheme: 'dark' }}
            >
              <option value="none" className="bg-gray-800">None (ignore deletes)</option>
              <option value="soft_delete" className="bg-gray-800">Soft Delete (flag deleted records)</option>
              <option value="hard_delete" className="bg-gray-800">Hard Delete (remove from target)</option>
              <option value="compare_full" className="bg-gray-800">Compare Full (detect missing records)</option>
            </select>
          </div>

          {config.loading_strategy.delete_detection === 'soft_delete' && (
            <Input
              label="Soft Delete Column"
              placeholder="is_deleted"
              value={config.loading_strategy.soft_delete_column || ''}
              onChange={(e) => updateLoading({ soft_delete_column: e.target.value })}
            />
          )}
        </div>
      )}

      {/* SCD Tab */}
      {activeTab === 'scd' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div className="p-3 bg-blue-500/10 border border-blue-500/30 rounded-lg">
            <p className="text-sm text-blue-400">
              <strong>SCD (Slowly Changing Dimensions)</strong> tracks historical changes to dimension data.
              Enable this when you need to maintain history of attribute changes over time.
            </p>
          </div>

          <div className="flex items-center gap-3 mb-4">
            <input
              type="checkbox"
              id="scd-enabled"
              checked={!!config.scd_config}
              onChange={(e) => {
                if (e.target.checked) {
                  onChange({
                    ...config,
                    scd_config: {
                      scd_type: 2,
                      effective_date_column: 'effective_date',
                      end_date_column: 'end_date',
                      current_flag_column: 'is_current'
                    }
                  })
                } else {
                  onChange({ ...config, scd_config: undefined })
                }
              }}
              className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
            />
            <label htmlFor="scd-enabled" className="text-sm font-medium text-white">
              Enable SCD Processing
            </label>
          </div>

          {config.scd_config && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-1.5">
                  SCD Type
                </label>
                <select
                  value={config.scd_config.scd_type}
                  onChange={(e) => updateSCD({ scd_type: parseInt(e.target.value) as SCDConfig['scd_type'] })}
                  className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                  style={{ colorScheme: 'dark' }}
                >
                  <option value={1} className="bg-gray-800">Type 1 - Overwrite (no history)</option>
                  <option value={2} className="bg-gray-800">Type 2 - Add Row (full history)</option>
                  <option value={3} className="bg-gray-800">Type 3 - Add Column (limited history)</option>
                  <option value={4} className="bg-gray-800">Type 4 - History Table (separate table)</option>
                  <option value={6} className="bg-gray-800">Type 6 - Hybrid (Type 1 + 2 + 3)</option>
                </select>
                <p className="mt-1.5 text-xs text-gray-400">
                  {config.scd_config.scd_type === 1 && 'Overwrites old values with new values. No history maintained.'}
                  {config.scd_config.scd_type === 2 && 'Creates new row for each change. Full history with effective dates.'}
                  {config.scd_config.scd_type === 3 && 'Adds columns for previous values. Limited history (usually 1 prior value).'}
                  {config.scd_config.scd_type === 4 && 'Maintains current values in main table, full history in separate table.'}
                  {config.scd_config.scd_type === 6 && 'Combines Type 1, 2, and 3. Current row + full history + previous value columns.'}
                </p>
              </div>

              <Input
                label="Effective Date Column"
                placeholder="effective_date"
                value={config.scd_config.effective_date_column}
                onChange={(e) => updateSCD({ effective_date_column: e.target.value })}
              />

              {(config.scd_config.scd_type === 2 || config.scd_config.scd_type === 6) && (
                <div className="grid grid-cols-2 gap-4">
                  <Input
                    label="End Date Column"
                    placeholder="end_date"
                    value={config.scd_config.end_date_column || ''}
                    onChange={(e) => updateSCD({ end_date_column: e.target.value })}
                  />
                  <Input
                    label="Current Flag Column"
                    placeholder="is_current"
                    value={config.scd_config.current_flag_column || ''}
                    onChange={(e) => updateSCD({ current_flag_column: e.target.value })}
                  />
                </div>
              )}

              {config.scd_config.scd_type === 4 && (
                <Input
                  label="History Table Name"
                  placeholder="dim_customer_history"
                  value={config.scd_config.history_table || ''}
                  onChange={(e) => updateSCD({ history_table: e.target.value })}
                />
              )}

              {(config.scd_config.scd_type === 2 || config.scd_config.scd_type === 6) && (
                <>
                  <Input
                    label="Track Changes Columns"
                    placeholder="status, tier, address"
                    value={config.scd_config.track_changes_columns?.join(', ') || ''}
                    onChange={(e) => updateSCD({ track_changes_columns: e.target.value.split(',').map(s => s.trim()).filter(Boolean) })}
                    helperText="Columns to monitor for changes (comma-separated)"
                  />

                  <div className="flex items-center gap-3">
                    <input
                      type="checkbox"
                      id="hash-diff"
                      checked={config.scd_config.hash_diff_enabled || false}
                      onChange={(e) => updateSCD({ hash_diff_enabled: e.target.checked })}
                      className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
                    />
                    <label htmlFor="hash-diff" className="text-sm text-gray-300">
                      Use hash comparison for change detection (recommended for many columns)
                    </label>
                  </div>
                </>
              )}

              {config.scd_config.scd_type === 3 && (
                <Input
                  label="Previous Value Columns"
                  placeholder="previous_status, previous_tier"
                  value={config.scd_config.previous_value_columns?.join(', ') || ''}
                  onChange={(e) => updateSCD({ previous_value_columns: e.target.value.split(',').map(s => s.trim()).filter(Boolean) })}
                  helperText="Columns to keep previous values for"
                />
              )}
            </>
          )}
        </div>
      )}

      {/* Late Data Tab */}
      {activeTab === 'late_data' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Late Arriving Data Strategy
            </label>
            <select
              value={config.late_data_handling.strategy}
              onChange={(e) => updateLateData({ strategy: e.target.value as LateDataConfig['strategy'] })}
              className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
              style={{ colorScheme: 'dark' }}
            >
              <option value="reject" className="bg-gray-800">Reject (fail if late data detected)</option>
              <option value="accept_all" className="bg-gray-800">Accept All (process regardless of lateness)</option>
              <option value="accept_within_window" className="bg-gray-800">Accept Within Window (configurable grace period)</option>
              <option value="route_to_error" className="bg-gray-800">Route to Error Table (for manual review)</option>
              <option value="reprocess" className="bg-gray-800">Trigger Reprocessing (backfill affected partitions)</option>
            </select>
          </div>

          {config.late_data_handling.strategy === 'accept_within_window' && (
            <Input
              label="Acceptance Window (hours)"
              type="number"
              value={config.late_data_handling.acceptance_window_hours || 24}
              onChange={(e) => updateLateData({ acceptance_window_hours: parseInt(e.target.value) })}
              helperText="Data arriving within this window will be processed"
            />
          )}

          {config.late_data_handling.strategy === 'route_to_error' && (
            <Input
              label="Error Table Name"
              placeholder="late_data_errors"
              value={config.late_data_handling.error_table || ''}
              onChange={(e) => updateLateData({ error_table: e.target.value })}
            />
          )}

          {config.late_data_handling.strategy === 'reprocess' && (
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Reprocessing Trigger
              </label>
              <select
                value={config.late_data_handling.reprocessing_trigger || 'manual'}
                onChange={(e) => updateLateData({ reprocessing_trigger: e.target.value as LateDataConfig['reprocessing_trigger'] })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="manual" className="bg-gray-800">Manual (operator triggers backfill)</option>
                <option value="automatic" className="bg-gray-800">Automatic (immediately reprocess)</option>
                <option value="scheduled" className="bg-gray-800">Scheduled (batch reprocessing at end of day)</option>
              </select>
            </div>
          )}

          <div className="flex items-center gap-3">
            <input
              type="checkbox"
              id="alert-late"
              checked={config.late_data_handling.alert_on_late_data}
              onChange={(e) => updateLateData({ alert_on_late_data: e.target.checked })}
              className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
            />
            <label htmlFor="alert-late" className="text-sm text-gray-300">
              Send alert when late data is detected
            </label>
          </div>

          {config.late_data_handling.alert_on_late_data && (
            <Input
              label="Alert Threshold (%)"
              type="number"
              min={0}
              max={100}
              value={config.late_data_handling.late_data_threshold_percent || 5}
              onChange={(e) => updateLateData({ late_data_threshold_percent: parseInt(e.target.value) })}
              helperText="Alert if more than X% of records in batch are late"
            />
          )}
        </div>
      )}

      {/* Backfill Tab */}
      {activeTab === 'backfill' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div className="flex items-center gap-3">
            <input
              type="checkbox"
              id="backfill-enabled"
              checked={config.backfill.enabled}
              onChange={(e) => updateBackfill({ enabled: e.target.checked })}
              className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
            />
            <label htmlFor="backfill-enabled" className="text-sm font-medium text-white">
              Enable Backfill Capability
            </label>
          </div>

          {config.backfill.enabled && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-1.5">
                  Backfill Strategy
                </label>
                <select
                  value={config.backfill.strategy}
                  onChange={(e) => updateBackfill({ strategy: e.target.value as BackfillConfig['strategy'] })}
                  className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                  style={{ colorScheme: 'dark' }}
                >
                  <option value="full_historical" className="bg-gray-800">Full Historical (reprocess all data)</option>
                  <option value="incremental_catchup" className="bg-gray-800">Incremental Catchup (from last checkpoint)</option>
                  <option value="partition_by_partition" className="bg-gray-800">Partition by Partition (parallel processing)</option>
                  <option value="custom_date_range" className="bg-gray-800">Custom Date Range</option>
                </select>
              </div>

              {(config.backfill.strategy === 'partition_by_partition' || config.backfill.strategy === 'custom_date_range') && (
                <Input
                  label="Partition Column"
                  placeholder="process_date"
                  value={config.backfill.partition_column || ''}
                  onChange={(e) => updateBackfill({ partition_column: e.target.value })}
                />
              )}

              {config.backfill.strategy === 'custom_date_range' && (
                <div className="grid grid-cols-2 gap-4">
                  <Input
                    label="Start Date"
                    type="date"
                    value={config.backfill.start_date || ''}
                    onChange={(e) => updateBackfill({ start_date: e.target.value })}
                  />
                  <Input
                    label="End Date"
                    type="date"
                    value={config.backfill.end_date || ''}
                    onChange={(e) => updateBackfill({ end_date: e.target.value })}
                  />
                </div>
              )}

              {config.backfill.strategy === 'partition_by_partition' && (
                <Input
                  label="Parallel Partitions"
                  type="number"
                  min={1}
                  max={32}
                  value={config.backfill.parallel_partitions || 4}
                  onChange={(e) => updateBackfill({ parallel_partitions: parseInt(e.target.value) })}
                  helperText="Number of partitions to process concurrently"
                />
              )}

              <div className="flex items-center gap-3">
                <input
                  type="checkbox"
                  id="checkpoint-enabled"
                  checked={config.backfill.checkpoint_enabled}
                  onChange={(e) => updateBackfill({ checkpoint_enabled: e.target.checked })}
                  className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
                />
                <label htmlFor="checkpoint-enabled" className="text-sm text-gray-300">
                  Enable checkpointing (resume from failure)
                </label>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-300 mb-1.5">
                  Validation Mode
                </label>
                <select
                  value={config.backfill.validation_mode}
                  onChange={(e) => updateBackfill({ validation_mode: e.target.value as BackfillConfig['validation_mode'] })}
                  className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                  style={{ colorScheme: 'dark' }}
                >
                  <option value="none" className="bg-gray-800">None</option>
                  <option value="row_count" className="bg-gray-800">Row Count Validation</option>
                  <option value="checksum" className="bg-gray-800">Checksum Validation</option>
                  <option value="full_compare" className="bg-gray-800">Full Data Comparison</option>
                </select>
              </div>
            </>
          )}
        </div>
      )}

      {/* Lineage Tab */}
      {activeTab === 'lineage' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div className="space-y-3">
            <div className="flex items-center gap-3">
              <input
                type="checkbox"
                id="track-source"
                checked={config.lineage.track_source_files}
                onChange={(e) => updateLineage({ track_source_files: e.target.checked })}
                className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
              />
              <label htmlFor="track-source" className="text-sm text-gray-300">
                Track source files in audit columns
              </label>
            </div>

            <div className="flex items-center gap-3">
              <input
                type="checkbox"
                id="track-version"
                checked={config.lineage.track_transformation_versions}
                onChange={(e) => updateLineage({ track_transformation_versions: e.target.checked })}
                className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
              />
              <label htmlFor="track-version" className="text-sm text-gray-300">
                Track transformation code versions
              </label>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Data Classification
              </label>
              <select
                value={config.lineage.data_classification || 'internal'}
                onChange={(e) => updateLineage({ data_classification: e.target.value as LineageConfig['data_classification'] })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="public" className="bg-gray-800">Public</option>
                <option value="internal" className="bg-gray-800">Internal</option>
                <option value="confidential" className="bg-gray-800">Confidential</option>
                <option value="restricted" className="bg-gray-800">Restricted</option>
              </select>
            </div>

            <Input
              label="Retention (days)"
              type="number"
              value={config.lineage.retention_days || 365}
              onChange={(e) => updateLineage({ retention_days: parseInt(e.target.value) })}
            />
          </div>

          <div className="border-t border-white/10 pt-4">
            <h4 className="text-sm font-medium text-white mb-3">Audit Column Names</h4>
            <div className="grid grid-cols-2 gap-4">
              <Input
                label="Created At"
                value={config.lineage.audit_columns.created_at}
                onChange={(e) => updateLineage({
                  audit_columns: { ...config.lineage.audit_columns, created_at: e.target.value }
                })}
              />
              <Input
                label="Created By"
                value={config.lineage.audit_columns.created_by}
                onChange={(e) => updateLineage({
                  audit_columns: { ...config.lineage.audit_columns, created_by: e.target.value }
                })}
              />
              <Input
                label="Updated At"
                value={config.lineage.audit_columns.updated_at}
                onChange={(e) => updateLineage({
                  audit_columns: { ...config.lineage.audit_columns, updated_at: e.target.value }
                })}
              />
              <Input
                label="Updated By"
                value={config.lineage.audit_columns.updated_by}
                onChange={(e) => updateLineage({
                  audit_columns: { ...config.lineage.audit_columns, updated_by: e.target.value }
                })}
              />
            </div>
          </div>
        </div>
      )}

      {/* Output Tab */}
      {activeTab === 'output' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Output Format
            </label>
            <select
              value={config.output.format}
              onChange={(e) => updateOutput({ format: e.target.value as OutputConfig['format'] })}
              className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
              style={{ colorScheme: 'dark' }}
            >
              <option value="bigquery" className="bg-gray-800">BigQuery (native)</option>
              <option value="delta" className="bg-gray-800">Delta Lake</option>
              <option value="iceberg" className="bg-gray-800">Apache Iceberg</option>
              <option value="parquet" className="bg-gray-800">Parquet (GCS)</option>
              <option value="hive" className="bg-gray-800">Hive (Dataproc Metastore)</option>
            </select>
          </div>

          <Input
            label="Clustering Columns"
            placeholder="customer_id, order_date"
            value={config.output.clustering_columns?.join(', ') || ''}
            onChange={(e) => updateOutput({ clustering_columns: e.target.value.split(',').map(s => s.trim()).filter(Boolean) })}
            helperText="Columns to cluster by for query optimization"
          />

          <div className="grid grid-cols-2 gap-4">
            <Input
              label="Table Expiration (days)"
              type="number"
              value={config.output.table_expiration_days || ''}
              onChange={(e) => updateOutput({ table_expiration_days: e.target.value ? parseInt(e.target.value) : undefined })}
              helperText="Optional: Auto-delete table after N days"
            />

            {(config.output.format === 'delta' || config.output.format === 'iceberg') && (
              <Input
                label="Time Travel (days)"
                type="number"
                value={config.output.time_travel_days || 7}
                onChange={(e) => updateOutput({ time_travel_days: parseInt(e.target.value) })}
                helperText="How long to retain history for time travel"
              />
            )}
          </div>

          <div className="flex items-center gap-3">
            <input
              type="checkbox"
              id="require-partition"
              checked={config.output.require_partition_filter}
              onChange={(e) => updateOutput({ require_partition_filter: e.target.checked })}
              className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
            />
            <label htmlFor="require-partition" className="text-sm text-gray-300">
              Require partition filter on queries (recommended for large tables)
            </label>
          </div>

          {config.output.format === 'bigquery' && (
            <div className="flex items-center gap-3">
              <input
                type="checkbox"
                id="streaming-buffer"
                checked={config.output.enable_streaming_buffer || false}
                onChange={(e) => updateOutput({ enable_streaming_buffer: e.target.checked })}
                className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
              />
              <label htmlFor="streaming-buffer" className="text-sm text-gray-300">
                Enable streaming buffer (for real-time inserts)
              </label>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

export default GoldOperationalConfigPanel
