'use client'

/**
 * DTSXMigrationForm - SSIS/DTSX Migration Configuration Component (Updated)
 *
 * Allows users to migrate SSIS packages to modern Airflow pipelines.
 * Uses new canonical types and API endpoints.
 *
 * Features:
 * - Upload/specify DTSX file location
 * - Parse and preview SSIS components
 * - Map SSIS connections to Airflow connections
 * - Select destination data model
 */

import React, { useState, useCallback } from 'react'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Card } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { cn } from '@/lib/utils'
import { DTSXSourceConfig } from '@/types/pipeline-canonical'
import { api } from '@/lib/api'

// =============================================================================
// Props
// =============================================================================

interface DTSXMigrationFormProps {
  onChange: (config: Partial<DTSXSourceConfig>) => void
  className?: string
}

interface ParsedDTSX {
  sources: Array<{
    name: string
    type: string
    table_name?: string
    query?: string
  }>
  transforms: Array<{
    name: string
    type: string
  }>
  destinations: Array<{
    name: string
    type: string
    table_name?: string
  }>
}

// =============================================================================
// Main Component
// =============================================================================

export function DTSXMigrationForm({ onChange, className }: DTSXMigrationFormProps) {
  const [config, setConfig] = useState<Partial<DTSXSourceConfig>>({
    dtsx_location: '',
    dtsx_package_name: '',
    original_server: '',
  })
  const [isParsing, setIsParsing] = useState(false)
  const [parsedData, setParsedData] = useState<ParsedDTSX | null>(null)
  const [parseError, setParseError] = useState<string | null>(null)

  // Update config and notify parent
  const updateConfig = useCallback((updates: Partial<DTSXSourceConfig>) => {
    const newConfig = { ...config, ...updates }
    setConfig(newConfig)
    onChange(newConfig)
  }, [config, onChange])

  // Parse DTSX file
  const parseDTSX = useCallback(async () => {
    if (!config.dtsx_location?.trim()) {
      setParseError('DTSX location is required')
      return
    }

    setIsParsing(true)
    setParseError(null)

    try {
      const result = await api.parseDTSX(config.dtsx_location)

      setParsedData({
        sources: result.sources || [],
        transforms: result.transforms || [],
        destinations: result.destinations || [],
      })

      // Update config with parsed data
      updateConfig({
        extracted_sources: result.sources || [],
        extracted_transforms: result.transforms || [],
        extracted_destinations: result.destinations || [],
      })

    } catch (err) {
      setParseError(err instanceof Error ? err.message : 'Failed to parse DTSX file')
    } finally {
      setIsParsing(false)
    }
  }, [config.dtsx_location, updateConfig])

  return (
    <div className={cn('space-y-6', className)}>
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="w-12 h-12 bg-orange-100 dark:bg-orange-900/30 rounded-lg flex items-center justify-center">
          <span className="text-2xl">📦</span>
        </div>
        <div>
          <h3 className="text-lg font-semibold text-white dark:text-white">
            SSIS/DTSX Migration
          </h3>
          <p className="text-sm text-gray-400 dark:text-gray-400">
            Migrate your SSIS packages to modern Airflow + PySpark pipelines
          </p>
        </div>
      </div>

      {/* DTSX Location Input */}
      <Card className="p-4 space-y-4">
        <h4 className="font-medium text-white dark:text-white">1. DTSX Package Location</h4>

        <div className="space-y-3">
          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-1">
              DTSX File Path (GCS) <span className="text-red-500">*</span>
            </label>
            <Input
              value={config.dtsx_location || ''}
              onChange={(e) => updateConfig({ dtsx_location: e.target.value })}
              placeholder="gs://your-bucket/ssis-packages/MyPackage.dtsx"
            />
            <p className="mt-1 text-xs text-gray-400">
              Upload your .dtsx file to GCS and provide the full path
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-1">
              Package Name <span className="text-red-500">*</span>
            </label>
            <Input
              value={config.dtsx_package_name || ''}
              onChange={(e) => updateConfig({ dtsx_package_name: e.target.value })}
              placeholder="MySSISPackage"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-1">
              Dataflow Name (Optional)
            </label>
            <Input
              value={config.dataflow_name || ''}
              onChange={(e) => updateConfig({ dataflow_name: e.target.value })}
              placeholder="Specific dataflow to extract"
            />
            <p className="mt-1 text-xs text-gray-400">
              Leave empty to migrate the entire package
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-1">
              Original SSIS Server (Optional)
            </label>
            <Input
              value={config.original_server || ''}
              onChange={(e) => updateConfig({ original_server: e.target.value })}
              placeholder="e.g., PROD-SSIS-01"
            />
          </div>

          <Button
            onClick={parseDTSX}
            disabled={!config.dtsx_location?.trim() || !config.dtsx_package_name?.trim() || isParsing}
            className="w-full"
          >
            {isParsing ? (
              <>
                <svg className="animate-spin -ml-1 mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                Parsing DTSX Package...
              </>
            ) : (
              '🔍 Parse DTSX Package'
            )}
          </Button>

          {parseError && (
            <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
              <p className="text-sm text-red-600 dark:text-red-400">❌ {parseError}</p>
            </div>
          )}
        </div>
      </Card>

      {/* Parsed Package Preview */}
      {parsedData && (
        <>
          {/* Package Summary */}
          <Card className="p-4 space-y-4">
            <div className="flex items-center justify-between">
              <h4 className="font-medium text-white dark:text-white">
                2. Parsed Package: {config.dtsx_package_name}
              </h4>
              <Badge variant="success">✓ Parsed Successfully</Badge>
            </div>

            <div className="grid grid-cols-3 gap-4">
              <div className="text-center p-3 bg-blue-900/30 dark:bg-blue-900/20 rounded-lg">
                <div className="text-2xl font-bold text-blue-600">{parsedData.sources.length}</div>
                <div className="text-xs text-gray-400">Data Sources</div>
              </div>
              <div className="text-center p-3 bg-purple-50 dark:bg-purple-900/20 rounded-lg">
                <div className="text-2xl font-bold text-purple-600">{parsedData.transforms.length}</div>
                <div className="text-xs text-gray-400">Transformations</div>
              </div>
              <div className="text-center p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
                <div className="text-2xl font-bold text-green-600">{parsedData.destinations.length}</div>
                <div className="text-xs text-gray-400">Destinations</div>
              </div>
            </div>

            {/* Sources */}
            {parsedData.sources.length > 0 && (
              <div>
                <h5 className="text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">Data Sources</h5>
                <div className="space-y-2">
                  {parsedData.sources.map((source, idx) => (
                    <div
                      key={idx}
                      className="flex items-center justify-between p-2 bg-blue-900/30 dark:bg-blue-900/20 rounded"
                    >
                      <div className="flex items-center gap-2">
                        <Badge variant="info">{source.type}</Badge>
                        <span className="text-sm font-medium">{source.name}</span>
                      </div>
                      <span className="text-xs text-gray-400">
                        {source.table_name || (source.query ? 'Custom SQL' : 'N/A')}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Transformations */}
            {parsedData.transforms.length > 0 && (
              <div>
                <h5 className="text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">Transformations</h5>
                <div className="flex flex-wrap gap-2">
                  {parsedData.transforms.map((transform, idx) => (
                    <Badge key={idx} variant="default">
                      {transform.type}: {transform.name}
                    </Badge>
                  ))}
                </div>
              </div>
            )}

            {/* Destinations */}
            {parsedData.destinations.length > 0 && (
              <div>
                <h5 className="text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">Destinations</h5>
                <div className="space-y-2">
                  {parsedData.destinations.map((dest, idx) => (
                    <div
                      key={idx}
                      className="flex items-center justify-between p-2 bg-green-50 dark:bg-green-900/20 rounded"
                    >
                      <div className="flex items-center gap-2">
                        <Badge variant="success">{dest.type}</Badge>
                        <span className="text-sm font-medium">{dest.name}</span>
                      </div>
                      <span className="text-xs text-gray-400">{dest.table_name || 'N/A'}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </Card>

          {/* Migration Info */}
          <Card className="p-4 bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/20 dark:to-purple-900/20">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 bg-gray-800 dark:bg-gray-800 rounded-full flex items-center justify-center">
                ✨
              </div>
              <div className="flex-1">
                <h4 className="font-medium text-white dark:text-white">Ready to Migrate</h4>
                <p className="text-sm text-gray-400 dark:text-gray-400">
                  Your SSIS package will be converted to a modern Airflow DAG with PySpark transformations
                </p>
              </div>
            </div>

            <div className="mt-4 grid grid-cols-2 gap-2 text-xs">
              <div className="flex items-center gap-2 text-gray-400 dark:text-gray-400">
                <span>✓</span>
                <span>SSIS → Airflow DAG</span>
              </div>
              <div className="flex items-center gap-2 text-gray-400 dark:text-gray-400">
                <span>✓</span>
                <span>T-SQL → PySpark</span>
              </div>
              <div className="flex items-center gap-2 text-gray-400 dark:text-gray-400">
                <span>✓</span>
                <span>OLEDB/ADO.NET → BigQuery</span>
              </div>
              <div className="flex items-center gap-2 text-gray-400 dark:text-gray-400">
                <span>✓</span>
                <span>SQL Server → GCS/BigQuery</span>
              </div>
            </div>
          </Card>
        </>
      )}

      {/* Information Box */}
      <Card className="p-4 bg-blue-900/30 dark:bg-blue-900/20 border-blue-600/50 dark:border-blue-800">
        <div className="flex gap-3">
          <div className="text-blue-600 dark:text-blue-400 text-xl">ℹ️</div>
          <div className="flex-1 text-sm text-gray-300 dark:text-gray-300">
            <p className="font-medium mb-1">Migration Notes:</p>
            <ul className="list-disc list-inside space-y-1 text-xs">
              <li>All data flows will be converted to PySpark jobs</li>
              <li>SQL Server connections will map to BigQuery datasets</li>
              <li>SSIS variables will become Airflow variables</li>
              <li>Execute SQL tasks will be converted to SQL operators</li>
              <li>Data types will be automatically mapped to BigQuery types</li>
            </ul>
          </div>
        </div>
      </Card>
    </div>
  )
}

export default DTSXMigrationForm
