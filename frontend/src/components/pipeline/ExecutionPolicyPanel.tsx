'use client'

import React, { useState } from 'react'
import { cn } from '@/lib/utils'
import { Input } from '@/components/ui/Input'
import { Textarea } from '@/components/ui/Textarea'

// Enhanced execution policy with enterprise controls
export interface ExecutionPolicy {
  // Scheduling
  schedule_interval: string  // Cron expression or preset
  schedule_timezone: string
  start_date?: string
  end_date?: string

  // SLA Configuration
  sla: SLAConfig

  // Processing Mode
  processing_mode: 'batch' | 'streaming' | 'micro-batch'
  batch_size?: number
  micro_batch_interval_seconds?: number

  // Partitioning Strategy
  partitioning: PartitionConfig

  // Idempotency & Retry
  idempotency: IdempotencyConfig

  // Failure Handling
  failure_handling: FailureConfig

  // Resource Allocation
  resources: ResourceConfig
}

export interface SLAConfig {
  enabled: boolean
  max_runtime_minutes: number
  warning_after_minutes: number
  escalation_email?: string
  escalation_slack_channel?: string
  on_breach: 'alert' | 'alert_and_retry' | 'alert_and_skip' | 'fail'
}

export interface PartitionConfig {
  strategy: 'none' | 'date' | 'hash' | 'range' | 'custom'
  partition_columns?: string[]
  partition_granularity?: 'hourly' | 'daily' | 'monthly' | 'yearly'
  num_partitions?: number  // For hash partitioning
  range_boundaries?: string[]  // For range partitioning
}

export interface IdempotencyConfig {
  enabled: boolean
  idempotency_key: string[]  // Columns that form the idempotency key
  on_duplicate: 'skip' | 'update' | 'fail' | 'upsert'
  dedup_window_hours?: number  // Look-back window for deduplication
}

export interface FailureConfig {
  retry_count: number
  retry_delay_seconds: number
  retry_exponential_backoff: boolean
  on_final_failure: 'alert' | 'alert_and_pause' | 'trigger_fallback' | 'mark_partial'
  fallback_dag_id?: string
  partial_success_threshold_percent?: number  // e.g., 95% means fail only if <95% success
}

export interface ResourceConfig {
  dataproc_cluster_size: 'small' | 'medium' | 'large' | 'xlarge' | 'custom'
  num_workers?: number
  worker_machine_type?: string
  spark_config?: Record<string, string>
  priority: 'low' | 'medium' | 'high' | 'critical'
  // Cost optimization
  cost_labels?: Record<string, string>
  preemptible_ratio?: number
  autoscaling?: {
    min_workers?: number
    max_workers?: number
    scale_up_factor?: number
    cooldown_period_seconds?: number
  }
}

interface V2ScheduleConfig {
  dev_schedule?: string
  prod_schedule?: string
  dev_notification_emails?: string
  prod_notification_emails?: string
  enable_duplicate_detection?: boolean
  enable_self_retrigger?: boolean
}

interface ExecutionPolicyPanelProps {
  value: Partial<ExecutionPolicy>
  onChange: (policy: ExecutionPolicy) => void
  className?: string
  v2Mode?: boolean
  onV2ScheduleChange?: (config: V2ScheduleConfig) => void
}

type TabId = 'scheduling' | 'sla' | 'partitioning' | 'idempotency' | 'failure' | 'resources'

const TABS: { id: TabId; label: string; icon: string }[] = [
  { id: 'scheduling', label: 'Schedule', icon: '📅' },
  { id: 'sla', label: 'SLA', icon: '⏱️' },
  { id: 'partitioning', label: 'Partitions', icon: '📊' },
  { id: 'idempotency', label: 'Idempotency', icon: '🔄' },
  { id: 'failure', label: 'Failure', icon: '⚠️' },
  { id: 'resources', label: 'Resources', icon: '💻' },
]

const SCHEDULE_PRESETS = [
  { value: '@hourly', label: 'Hourly' },
  { value: '@daily', label: 'Daily (midnight UTC)' },
  { value: '0 6 * * *', label: 'Daily at 6 AM UTC' },
  { value: '0 */4 * * *', label: 'Every 4 hours' },
  { value: '@weekly', label: 'Weekly (Sunday midnight)' },
  { value: '0 0 1 * *', label: 'Monthly (1st of month)' },
  { value: 'custom', label: 'Custom Cron Expression' },
]

export function ExecutionPolicyPanel({ value, onChange, className, v2Mode = false, onV2ScheduleChange }: ExecutionPolicyPanelProps) {
  const [activeTab, setActiveTab] = useState<TabId>('scheduling')
  const [showCustomCron, setShowCustomCron] = useState(false)
  const [v2Schedule, setV2Schedule] = useState<V2ScheduleConfig>({
    dev_schedule: '@daily',
    prod_schedule: '@daily',
    dev_notification_emails: '',
    prod_notification_emails: '',
    enable_duplicate_detection: false,
    enable_self_retrigger: false,
  })

  const updateV2Schedule = (updates: Partial<V2ScheduleConfig>) => {
    const updated = { ...v2Schedule, ...updates }
    setV2Schedule(updated)
    onV2ScheduleChange?.(updated)
  }

  // Initialize with defaults
  const policy: ExecutionPolicy = {
    schedule_interval: '@daily',
    schedule_timezone: 'UTC',
    processing_mode: 'batch',
    sla: {
      enabled: false,
      max_runtime_minutes: 60,
      warning_after_minutes: 45,
      on_breach: 'alert',
      ...value.sla
    },
    partitioning: {
      strategy: 'none',
      ...value.partitioning
    },
    idempotency: {
      enabled: true,
      idempotency_key: [],
      on_duplicate: 'upsert',
      dedup_window_hours: 24,
      ...value.idempotency
    },
    failure_handling: {
      retry_count: 2,
      retry_delay_seconds: 300,
      retry_exponential_backoff: true,
      on_final_failure: 'alert',
      partial_success_threshold_percent: 95,
      ...value.failure_handling
    },
    resources: {
      dataproc_cluster_size: 'medium',
      priority: 'medium',
      ...value.resources
    },
    ...value
  }

  const updatePolicy = (updates: Partial<ExecutionPolicy>) => {
    onChange({ ...policy, ...updates })
  }

  const updateSLA = (updates: Partial<SLAConfig>) => {
    onChange({ ...policy, sla: { ...policy.sla, ...updates } })
  }

  const updatePartitioning = (updates: Partial<PartitionConfig>) => {
    onChange({ ...policy, partitioning: { ...policy.partitioning, ...updates } })
  }

  const updateIdempotency = (updates: Partial<IdempotencyConfig>) => {
    onChange({ ...policy, idempotency: { ...policy.idempotency, ...updates } })
  }

  const updateFailure = (updates: Partial<FailureConfig>) => {
    onChange({ ...policy, failure_handling: { ...policy.failure_handling, ...updates } })
  }

  const updateResources = (updates: Partial<ResourceConfig>) => {
    onChange({ ...policy, resources: { ...policy.resources, ...updates } })
  }

  return (
    <div className={cn('space-y-4', className)}>
      {/* Header */}
      <div>
        <h3 className="text-lg font-semibold text-white">Execution Policy</h3>
        <p className="text-sm text-gray-400">
          Configure scheduling, SLA, partitioning, and failure handling
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
                ? 'bg-blue-600/20 text-blue-400 border border-blue-500/30'
                : 'text-gray-400 hover:text-white hover:bg-gray-700/50'
            )}
          >
            <span>{tab.icon}</span>
            <span>{tab.label}</span>
          </button>
        ))}
      </div>

      {/* Scheduling Tab */}
      {activeTab === 'scheduling' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Schedule Interval
            </label>
            <select
              value={SCHEDULE_PRESETS.find(p => p.value === policy.schedule_interval)?.value || 'custom'}
              onChange={(e) => {
                if (e.target.value === 'custom') {
                  setShowCustomCron(true)
                } else {
                  setShowCustomCron(false)
                  updatePolicy({ schedule_interval: e.target.value })
                }
              }}
              className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
              style={{ colorScheme: 'dark' }}
            >
              {SCHEDULE_PRESETS.map((preset) => (
                <option key={preset.value} value={preset.value} className="bg-gray-800">
                  {preset.label}
                </option>
              ))}
            </select>
          </div>

          {showCustomCron && (
            <Input
              label="Custom Cron Expression"
              placeholder="0 */6 * * *"
              value={policy.schedule_interval}
              onChange={(e) => updatePolicy({ schedule_interval: e.target.value })}
              helperText="Cron format: minute hour day month weekday"
            />
          )}

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Timezone
              </label>
              <select
                value={policy.schedule_timezone}
                onChange={(e) => updatePolicy({ schedule_timezone: e.target.value })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="UTC" className="bg-gray-800">UTC</option>
                <option value="America/New_York" className="bg-gray-800">America/New_York (EST/EDT)</option>
                <option value="America/Los_Angeles" className="bg-gray-800">America/Los_Angeles (PST/PDT)</option>
                <option value="Europe/London" className="bg-gray-800">Europe/London (GMT/BST)</option>
                <option value="Asia/Tokyo" className="bg-gray-800">Asia/Tokyo (JST)</option>
                <option value="Asia/Kolkata" className="bg-gray-800">Asia/Kolkata (IST)</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Processing Mode
              </label>
              <select
                value={policy.processing_mode}
                onChange={(e) => updatePolicy({ processing_mode: e.target.value as ExecutionPolicy['processing_mode'] })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="batch" className="bg-gray-800">Batch</option>
                <option value="micro-batch" className="bg-gray-800">Micro-Batch</option>
                <option value="streaming" className="bg-gray-800">Streaming</option>
              </select>
            </div>
          </div>

          {policy.processing_mode === 'micro-batch' && (
            <Input
              label="Micro-Batch Interval (seconds)"
              type="number"
              value={policy.micro_batch_interval_seconds || 60}
              onChange={(e) => updatePolicy({ micro_batch_interval_seconds: parseInt(e.target.value) })}
            />
          )}

          <div className="grid grid-cols-2 gap-4">
            <Input
              label="Start Date (optional)"
              type="date"
              value={policy.start_date || ''}
              onChange={(e) => updatePolicy({ start_date: e.target.value })}
            />
            <Input
              label="End Date (optional)"
              type="date"
              value={policy.end_date || ''}
              onChange={(e) => updatePolicy({ end_date: e.target.value })}
            />
          </div>

          {/* V2 Per-Environment Schedule & Controls */}
          {v2Mode && (
            <div className="border-t border-gray-700 pt-4 mt-4 space-y-4">
              <h4 className="text-sm font-medium text-gray-300">V2 Per-Environment Schedules</h4>

              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Dev Schedule (cron)"
                  placeholder="@daily"
                  value={v2Schedule.dev_schedule || ''}
                  onChange={(e) => updateV2Schedule({ dev_schedule: e.target.value })}
                  helperText="Cron expression for dev environment"
                />
                <Input
                  label="Prod Schedule (cron)"
                  placeholder="0 6 * * *"
                  value={v2Schedule.prod_schedule || ''}
                  onChange={(e) => updateV2Schedule({ prod_schedule: e.target.value })}
                  helperText="Cron expression for prod environment"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Dev Notification Emails"
                  placeholder="dev-team@company.com, dev-oncall@company.com"
                  value={v2Schedule.dev_notification_emails || ''}
                  onChange={(e) => updateV2Schedule({ dev_notification_emails: e.target.value })}
                  helperText="Comma-separated emails for dev alerts"
                />
                <Input
                  label="Prod Notification Emails"
                  placeholder="prod-oncall@company.com, data-eng@company.com"
                  value={v2Schedule.prod_notification_emails || ''}
                  onChange={(e) => updateV2Schedule({ prod_notification_emails: e.target.value })}
                  helperText="Comma-separated emails for prod alerts"
                />
              </div>

              <div className="flex items-center gap-6">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={v2Schedule.enable_duplicate_detection || false}
                    onChange={(e) => updateV2Schedule({ enable_duplicate_detection: e.target.checked })}
                    className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
                  />
                  <span className="text-sm text-gray-300">Enable Duplicate Detection</span>
                </label>

                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={v2Schedule.enable_self_retrigger || false}
                    onChange={(e) => updateV2Schedule({ enable_self_retrigger: e.target.checked })}
                    className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
                  />
                  <span className="text-sm text-gray-300">Enable Self-Retrigger</span>
                </label>
              </div>
            </div>
          )}
        </div>
      )}

      {/* SLA Tab */}
      {activeTab === 'sla' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div className="flex items-center gap-3">
            <input
              type="checkbox"
              id="sla-enabled"
              checked={policy.sla.enabled}
              onChange={(e) => updateSLA({ enabled: e.target.checked })}
              className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
            />
            <label htmlFor="sla-enabled" className="text-sm font-medium text-white">
              Enable SLA Monitoring
            </label>
          </div>

          {policy.sla.enabled && (
            <>
              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Max Runtime (minutes)"
                  type="number"
                  value={policy.sla.max_runtime_minutes}
                  onChange={(e) => updateSLA({ max_runtime_minutes: parseInt(e.target.value) })}
                  helperText="Pipeline will be flagged if exceeds this duration"
                />
                <Input
                  label="Warning After (minutes)"
                  type="number"
                  value={policy.sla.warning_after_minutes}
                  onChange={(e) => updateSLA({ warning_after_minutes: parseInt(e.target.value) })}
                  helperText="Send warning notification at this threshold"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-300 mb-1.5">
                  On SLA Breach
                </label>
                <select
                  value={policy.sla.on_breach}
                  onChange={(e) => updateSLA({ on_breach: e.target.value as SLAConfig['on_breach'] })}
                  className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                  style={{ colorScheme: 'dark' }}
                >
                  <option value="alert" className="bg-gray-800">Alert Only</option>
                  <option value="alert_and_retry" className="bg-gray-800">Alert and Retry</option>
                  <option value="alert_and_skip" className="bg-gray-800">Alert and Skip to Next</option>
                  <option value="fail" className="bg-gray-800">Mark as Failed</option>
                </select>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Escalation Email"
                  type="email"
                  placeholder="oncall@company.com"
                  value={policy.sla.escalation_email || ''}
                  onChange={(e) => updateSLA({ escalation_email: e.target.value })}
                />
                <Input
                  label="Slack Channel"
                  placeholder="#data-alerts"
                  value={policy.sla.escalation_slack_channel || ''}
                  onChange={(e) => updateSLA({ escalation_slack_channel: e.target.value })}
                />
              </div>
            </>
          )}
        </div>
      )}

      {/* Partitioning Tab */}
      {activeTab === 'partitioning' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Partitioning Strategy
            </label>
            <select
              value={policy.partitioning.strategy}
              onChange={(e) => updatePartitioning({ strategy: e.target.value as PartitionConfig['strategy'] })}
              className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
              style={{ colorScheme: 'dark' }}
            >
              <option value="none" className="bg-gray-800">No Partitioning</option>
              <option value="date" className="bg-gray-800">Date-Based Partitioning</option>
              <option value="hash" className="bg-gray-800">Hash Partitioning</option>
              <option value="range" className="bg-gray-800">Range Partitioning</option>
              <option value="custom" className="bg-gray-800">Custom</option>
            </select>
          </div>

          {policy.partitioning.strategy === 'date' && (
            <div className="grid grid-cols-2 gap-4">
              <Input
                label="Partition Column"
                placeholder="created_at"
                value={policy.partitioning.partition_columns?.[0] || ''}
                onChange={(e) => updatePartitioning({ partition_columns: [e.target.value] })}
              />
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-1.5">
                  Granularity
                </label>
                <select
                  value={policy.partitioning.partition_granularity || 'daily'}
                  onChange={(e) => updatePartitioning({ partition_granularity: e.target.value as PartitionConfig['partition_granularity'] })}
                  className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                  style={{ colorScheme: 'dark' }}
                >
                  <option value="hourly" className="bg-gray-800">Hourly</option>
                  <option value="daily" className="bg-gray-800">Daily</option>
                  <option value="monthly" className="bg-gray-800">Monthly</option>
                  <option value="yearly" className="bg-gray-800">Yearly</option>
                </select>
              </div>
            </div>
          )}

          {policy.partitioning.strategy === 'hash' && (
            <div className="grid grid-cols-2 gap-4">
              <Input
                label="Partition Column(s)"
                placeholder="customer_id"
                value={policy.partitioning.partition_columns?.join(', ') || ''}
                onChange={(e) => updatePartitioning({ partition_columns: e.target.value.split(',').map(s => s.trim()) })}
                helperText="Comma-separated column names"
              />
              <Input
                label="Number of Partitions"
                type="number"
                value={policy.partitioning.num_partitions || 16}
                onChange={(e) => updatePartitioning({ num_partitions: parseInt(e.target.value) })}
              />
            </div>
          )}

          {policy.partitioning.strategy === 'range' && (
            <Textarea
              label="Range Boundaries"
              placeholder="0, 1000, 10000, 100000"
              value={policy.partitioning.range_boundaries?.join(', ') || ''}
              onChange={(e) => updatePartitioning({ range_boundaries: e.target.value.split(',').map(s => s.trim()) })}
              helperText="Comma-separated boundary values"
            />
          )}
        </div>
      )}

      {/* Idempotency Tab */}
      {activeTab === 'idempotency' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div className="flex items-center gap-3">
            <input
              type="checkbox"
              id="idempotency-enabled"
              checked={policy.idempotency.enabled}
              onChange={(e) => updateIdempotency({ enabled: e.target.checked })}
              className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
            />
            <label htmlFor="idempotency-enabled" className="text-sm font-medium text-white">
              Enable Idempotency Control
            </label>
          </div>

          {policy.idempotency.enabled && (
            <>
              <Input
                label="Idempotency Key Columns *"
                placeholder="id, source_system, event_timestamp"
                value={policy.idempotency.idempotency_key.join(', ')}
                onChange={(e) => updateIdempotency({ idempotency_key: e.target.value.split(',').map(s => s.trim()).filter(Boolean) })}
                helperText="Comma-separated columns that uniquely identify a record"
              />

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-300 mb-1.5">
                    On Duplicate Record
                  </label>
                  <select
                    value={policy.idempotency.on_duplicate}
                    onChange={(e) => updateIdempotency({ on_duplicate: e.target.value as IdempotencyConfig['on_duplicate'] })}
                    className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                    style={{ colorScheme: 'dark' }}
                  >
                    <option value="skip" className="bg-gray-800">Skip (keep existing)</option>
                    <option value="update" className="bg-gray-800">Update (overwrite)</option>
                    <option value="upsert" className="bg-gray-800">Upsert (merge)</option>
                    <option value="fail" className="bg-gray-800">Fail pipeline</option>
                  </select>
                </div>

                <Input
                  label="Dedup Window (hours)"
                  type="number"
                  value={policy.idempotency.dedup_window_hours || 24}
                  onChange={(e) => updateIdempotency({ dedup_window_hours: parseInt(e.target.value) })}
                  helperText="Look-back window for deduplication"
                />
              </div>
            </>
          )}
        </div>
      )}

      {/* Failure Handling Tab */}
      {activeTab === 'failure' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div className="grid grid-cols-3 gap-4">
            <Input
              label="Retry Count"
              type="number"
              min={0}
              max={10}
              value={policy.failure_handling.retry_count}
              onChange={(e) => updateFailure({ retry_count: parseInt(e.target.value) })}
            />
            <Input
              label="Retry Delay (seconds)"
              type="number"
              value={policy.failure_handling.retry_delay_seconds}
              onChange={(e) => updateFailure({ retry_delay_seconds: parseInt(e.target.value) })}
            />
            <div className="flex items-end pb-2">
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={policy.failure_handling.retry_exponential_backoff}
                  onChange={(e) => updateFailure({ retry_exponential_backoff: e.target.checked })}
                  className="w-4 h-4 rounded bg-gray-800 border-gray-600 text-blue-500 focus:ring-blue-500/50"
                />
                <span className="text-sm text-gray-300">Exponential Backoff</span>
              </label>
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              On Final Failure
            </label>
            <select
              value={policy.failure_handling.on_final_failure}
              onChange={(e) => updateFailure({ on_final_failure: e.target.value as FailureConfig['on_final_failure'] })}
              className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
              style={{ colorScheme: 'dark' }}
            >
              <option value="alert" className="bg-gray-800">Alert Only</option>
              <option value="alert_and_pause" className="bg-gray-800">Alert and Pause DAG</option>
              <option value="trigger_fallback" className="bg-gray-800">Trigger Fallback DAG</option>
              <option value="mark_partial" className="bg-gray-800">Mark as Partial Success</option>
            </select>
          </div>

          {policy.failure_handling.on_final_failure === 'trigger_fallback' && (
            <Input
              label="Fallback DAG ID"
              placeholder="fallback_sales_pipeline"
              value={policy.failure_handling.fallback_dag_id || ''}
              onChange={(e) => updateFailure({ fallback_dag_id: e.target.value })}
            />
          )}

          {policy.failure_handling.on_final_failure === 'mark_partial' && (
            <Input
              label="Partial Success Threshold (%)"
              type="number"
              min={0}
              max={100}
              value={policy.failure_handling.partial_success_threshold_percent || 95}
              onChange={(e) => updateFailure({ partial_success_threshold_percent: parseInt(e.target.value) })}
              helperText="Mark as success if this % of records processed successfully"
            />
          )}
        </div>
      )}

      {/* Resources Tab */}
      {activeTab === 'resources' && (
        <div className="space-y-4 p-4 bg-gray-800/20 rounded-lg border border-white/5">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Dataproc Cluster Size
              </label>
              <select
                value={policy.resources.dataproc_cluster_size}
                onChange={(e) => updateResources({ dataproc_cluster_size: e.target.value as ResourceConfig['dataproc_cluster_size'] })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="small" className="bg-gray-800">Small (2 workers, n1-standard-4)</option>
                <option value="medium" className="bg-gray-800">Medium (4 workers, n1-standard-8)</option>
                <option value="large" className="bg-gray-800">Large (8 workers, n1-standard-16)</option>
                <option value="xlarge" className="bg-gray-800">XLarge (16 workers, n1-highmem-32)</option>
                <option value="custom" className="bg-gray-800">Custom</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Execution Priority
              </label>
              <select
                value={policy.resources.priority}
                onChange={(e) => updateResources({ priority: e.target.value as ResourceConfig['priority'] })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="low" className="bg-gray-800">Low (can be preempted)</option>
                <option value="medium" className="bg-gray-800">Medium (standard)</option>
                <option value="high" className="bg-gray-800">High (priority queue)</option>
                <option value="critical" className="bg-gray-800">Critical (dedicated resources)</option>
              </select>
            </div>
          </div>

          {policy.resources.dataproc_cluster_size === 'custom' && (
            <div className="grid grid-cols-2 gap-4">
              <Input
                label="Number of Workers"
                type="number"
                value={policy.resources.num_workers || 4}
                onChange={(e) => updateResources({ num_workers: parseInt(e.target.value) })}
              />
              <Input
                label="Worker Machine Type"
                placeholder="n1-standard-8"
                value={policy.resources.worker_machine_type || ''}
                onChange={(e) => updateResources({ worker_machine_type: e.target.value })}
              />
            </div>
          )}

          <Textarea
            label="Custom Spark Config (JSON)"
            placeholder='{"spark.executor.memory": "4g", "spark.executor.cores": "2"}'
            value={policy.resources.spark_config ? JSON.stringify(policy.resources.spark_config) : ''}
            onChange={(e) => {
              try {
                const config = e.target.value ? JSON.parse(e.target.value) : undefined
                updateResources({ spark_config: config })
              } catch {
                // Invalid JSON, ignore
              }
            }}
            rows={3}
            helperText="Optional Spark configuration overrides"
          />

          {/* Cost Optimization */}
          <div className="border-t border-gray-700 pt-4 mt-4">
            <h4 className="text-sm font-medium text-gray-300 mb-3">Cost Optimization</h4>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-1.5">
                  Preemptible Worker Ratio
                </label>
                <input
                  type="range"
                  min={0}
                  max={100}
                  step={10}
                  value={(policy.resources.preemptible_ratio ?? 60) * 100}
                  onChange={(e) => updateResources({ preemptible_ratio: parseInt(e.target.value) / 100 })}
                  className="w-full"
                />
                <p className="text-xs text-gray-500 mt-1">
                  {Math.round((policy.resources.preemptible_ratio ?? 0.6) * 100)}% preemptible workers (cheaper but can be reclaimed)
                </p>
              </div>

              <Input
                label="Cost Labels (JSON)"
                placeholder='{"domain": "sales", "team": "analytics"}'
                value={policy.resources.cost_labels ? JSON.stringify(policy.resources.cost_labels) : ''}
                onChange={(e) => {
                  try {
                    const labels = e.target.value ? JSON.parse(e.target.value) : undefined
                    updateResources({ cost_labels: labels })
                  } catch {
                    // Invalid JSON, ignore
                  }
                }}
                helperText="Labels for cost attribution in GCP billing"
              />
            </div>

            <div className="grid grid-cols-2 gap-4 mt-4">
              <Input
                label="Autoscale Min Workers"
                type="number"
                value={policy.resources.autoscaling?.min_workers ?? 2}
                onChange={(e) => updateResources({
                  autoscaling: { ...policy.resources.autoscaling, min_workers: parseInt(e.target.value) }
                })}
              />
              <Input
                label="Autoscale Max Workers"
                type="number"
                value={policy.resources.autoscaling?.max_workers ?? 10}
                onChange={(e) => updateResources({
                  autoscaling: { ...policy.resources.autoscaling, max_workers: parseInt(e.target.value) }
                })}
              />
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default ExecutionPolicyPanel
