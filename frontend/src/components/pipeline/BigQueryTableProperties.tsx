/**
 * BigQueryTableProperties - BigQuery-specific table configuration
 *
 * Features: Table expiration, partition filtering, clustering, labels
 * Part of Phase 3 - Medium Priority Enhancements
 */

import React from 'react'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'

export interface BigQueryTablePropertiesData {
  // Table Lifecycle
  expiration_days?: number  // Table expiration in days
  description?: string

  // Partitioning
  time_partitioning?: {
    type: 'HOUR' | 'DAY' | 'MONTH' | 'YEAR'
    field: string
    expiration_ms?: number
  }
  require_partition_filter?: boolean  // Force partition filter in queries

  // Clustering
  clustering_fields?: string[]  // Max 4 fields

  // Labels (tags)
  labels?: Record<string, string>

  // Access Control
  require_table_partition_filter?: boolean
  enable_refresh?: boolean
  refresh_interval_minutes?: number

  // Cost Control
  max_bytes_billed?: number  // Maximum bytes to bill for queries
}

interface BigQueryTablePropertiesProps {
  config: Partial<BigQueryTablePropertiesData>
  onChange: (config: Partial<BigQueryTablePropertiesData>) => void
}

export const BigQueryTableProperties: React.FC<BigQueryTablePropertiesProps> = ({
  config,
  onChange
}) => {
  const handleChange = (field: keyof BigQueryTablePropertiesData, value: any) => {
    onChange({ ...config, [field]: value })
  }

  const handlePartitioningChange = (field: string, value: any) => {
    onChange({
      ...config,
      time_partitioning: {
        ...(config.time_partitioning || { type: 'DAY', field: '' }),
        [field]: value
      }
    })
  }

  const addLabel = () => {
    const labels = config.labels || {}
    onChange({ ...config, labels: { ...labels, '': '' } })
  }

  const updateLabel = (oldKey: string, newKey: string, value: string) => {
    const labels = { ...(config.labels || {}) }
    if (oldKey !== newKey && oldKey in labels) {
      delete labels[oldKey]
    }
    labels[newKey] = value
    onChange({ ...config, labels })
  }

  const removeLabel = (key: string) => {
    const labels = { ...(config.labels || {}) }
    delete labels[key]
    onChange({ ...config, labels })
  }

  const addClusteringField = () => {
    const fields = config.clustering_fields || []
    if (fields.length < 4) {
      onChange({ ...config, clustering_fields: [...fields, ''] })
    }
  }

  const updateClusteringField = (index: number, value: string) => {
    const fields = [...(config.clustering_fields || [])]
    fields[index] = value
    onChange({ ...config, clustering_fields: fields })
  }

  const removeClusteringField = (index: number) => {
    const fields = [...(config.clustering_fields || [])]
    fields.splice(index, 1)
    onChange({ ...config, clustering_fields: fields })
  }

  return (
    <div className="space-y-6">
      {/* Table Lifecycle */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Table Lifecycle</h3>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Table Expiration (days)
            </label>
            <Input
              type="number"
              min="1"
              max="365"
              value={config.expiration_days || ''}
              onChange={(e) => handleChange('expiration_days', parseInt(e.target.value))}
              placeholder="90"
            />
            <p className="text-xs text-gray-500 mt-1">
              Automatically delete table after this many days (leave empty for no expiration)
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Table Description
            </label>
            <Input
              type="text"
              value={config.description || ''}
              onChange={(e) => handleChange('description', e.target.value)}
              placeholder="Brief description of the table"
            />
          </div>
        </div>
      </Card>

      {/* Time Partitioning */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Time Partitioning</h3>
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Partition Type
              </label>
              <Select
                value={config.time_partitioning?.type || 'DAY'}
                onChange={(e) => handlePartitioningChange('type', e.target.value)}
              >
                <option value="HOUR">Hourly</option>
                <option value="DAY">Daily</option>
                <option value="MONTH">Monthly</option>
                <option value="YEAR">Yearly</option>
              </Select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Partition Field
              </label>
              <Input
                type="text"
                value={config.time_partitioning?.field || ''}
                onChange={(e) => handlePartitioningChange('field', e.target.value)}
                placeholder="event_date"
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Partition Expiration (milliseconds)
            </label>
            <Input
              type="number"
              value={config.time_partitioning?.expiration_ms || ''}
              onChange={(e) => handlePartitioningChange('expiration_ms', parseInt(e.target.value))}
              placeholder="2592000000"
            />
            <p className="text-xs text-gray-500 mt-1">
              Delete partitions older than this (e.g., 2592000000 = 30 days)
            </p>
          </div>

          <div className="flex items-center space-x-3">
            <input
              type="checkbox"
              id="require_partition_filter"
              checked={config.require_partition_filter || false}
              onChange={(e) => handleChange('require_partition_filter', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="require_partition_filter" className="text-sm font-medium text-gray-700">
              Require partition filter in queries (cost optimization)
            </label>
          </div>
        </div>
      </Card>

      {/* Clustering */}
      <Card className="p-4">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-sm font-medium text-gray-900">
            Clustering Fields (Max 4)
          </h3>
          <Button
            type="button"
            onClick={addClusteringField}
            disabled={(config.clustering_fields || []).length >= 4}
            className="text-sm px-3 py-1"
          >
            + Add Field
          </Button>
        </div>

        {(config.clustering_fields || []).length === 0 ? (
          <p className="text-sm text-gray-500 italic">
            No clustering fields. Table will not be clustered.
          </p>
        ) : (
          <div className="space-y-2">
            {(config.clustering_fields || []).map((field, index) => (
              <div key={index} className="flex items-center space-x-2">
                <span className="text-sm text-gray-600 w-8">#{index + 1}</span>
                <Input
                  type="text"
                  value={field}
                  onChange={(e) => updateClusteringField(index, e.target.value)}
                  placeholder="column_name"
                  className="flex-1"
                />
                <Button
                  type="button"
                  onClick={() => removeClusteringField(index)}
                  className="text-red-600 hover:text-red-800 px-2 py-1"
                >
                  Remove
                </Button>
              </div>
            ))}
          </div>
        )}

        <p className="text-xs text-gray-500 mt-3">
          Clustering improves query performance by co-locating related data. Order matters.
        </p>
      </Card>

      {/* Labels (Tags) */}
      <Card className="p-4">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-sm font-medium text-gray-900">Labels (Tags)</h3>
          <Button
            type="button"
            onClick={addLabel}
            className="text-sm px-3 py-1"
          >
            + Add Label
          </Button>
        </div>

        {Object.keys(config.labels || {}).length === 0 ? (
          <p className="text-sm text-gray-500 italic">
            No labels defined.
          </p>
        ) : (
          <div className="space-y-2">
            {Object.entries(config.labels || {}).map(([key, value]) => (
              <div key={key} className="flex items-center space-x-2">
                <Input
                  type="text"
                  value={key}
                  onChange={(e) => updateLabel(key, e.target.value, value)}
                  placeholder="key"
                  className="flex-1"
                />
                <span className="text-gray-500">=</span>
                <Input
                  type="text"
                  value={value}
                  onChange={(e) => updateLabel(key, key, e.target.value)}
                  placeholder="value"
                  className="flex-1"
                />
                <Button
                  type="button"
                  onClick={() => removeLabel(key)}
                  className="text-red-600 hover:text-red-800 px-2 py-1"
                >
                  Remove
                </Button>
              </div>
            ))}
          </div>
        )}

        <p className="text-xs text-gray-500 mt-3">
          Labels help organize and filter tables (e.g., env=prod, team=analytics)
        </p>
      </Card>

      {/* Cost Control */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Cost Control</h3>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Max Bytes Billed
            </label>
            <Input
              type="number"
              min="0"
              value={config.max_bytes_billed || ''}
              onChange={(e) => handleChange('max_bytes_billed', parseInt(e.target.value))}
              placeholder="10737418240"
            />
            <p className="text-xs text-gray-500 mt-1">
              Maximum bytes to process per query (e.g., 10737418240 = 10 GB). Queries exceeding this will fail.
            </p>
          </div>
        </div>
      </Card>

      {/* Materialized View Options */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Materialized View Options</h3>
        <div className="space-y-4">
          <div className="flex items-center space-x-3">
            <input
              type="checkbox"
              id="enable_refresh"
              checked={config.enable_refresh || false}
              onChange={(e) => handleChange('enable_refresh', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="enable_refresh" className="text-sm font-medium text-gray-700">
              Enable automatic refresh (for materialized views)
            </label>
          </div>

          {config.enable_refresh && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Refresh Interval (minutes)
              </label>
              <Input
                type="number"
                min="1"
                max="1440"
                value={config.refresh_interval_minutes || 60}
                onChange={(e) => handleChange('refresh_interval_minutes', parseInt(e.target.value))}
              />
            </div>
          )}
        </div>
      </Card>

      {/* Help Text */}
      <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4">
        <div className="flex">
          <svg
            className="h-5 w-5 text-yellow-600 mt-0.5 mr-2 flex-shrink-0"
            fill="currentColor"
            viewBox="0 0 20 20"
          >
            <path
              fillRule="evenodd"
              d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z"
              clipRule="evenodd"
            />
          </svg>
          <div className="text-sm text-yellow-700">
            <p className="font-medium mb-1">BigQuery Optimization Tips:</p>
            <ul className="list-disc list-inside space-y-1 text-xs">
              <li>Use partitioning + clustering for large tables (&gt;1TB) to reduce query costs</li>
              <li>Require partition filters to prevent full table scans</li>
              <li>Set table expiration to automatically clean up old data</li>
              <li>Use labels to track costs by team, project, or environment</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  )
}
