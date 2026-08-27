'use client'

/**
 * UnifiedPipelineForm - Enhanced Pipeline Configuration Form
 *
 * Features:
 * - 72+ source types with type-specific configurations
 * - JSON/YAML/CSV schema upload and paste
 * - Bronze/Silver/Gold layer NL instruction inputs
 * - Copybook, DTSX, and schema file location inputs
 * - Comprehensive data collection for agent API
 */

import React, { useState, useCallback, useRef } from 'react'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { cn } from '@/lib/utils'
import {
  Upload, FileText, Edit3, Sparkles, Wand2, Database, Layers,
  FileCode, FolderOpen, ChevronDown, ChevronUp, Info, AlertTriangle
} from 'lucide-react'
import {
  UnifiedPipelineInput,
  PipelineConfig,
  SourceConfig,
  SchemaConfig,
  TargetConfig,
  ExecutionPolicy,
  Environment,
  SourceType,
  TargetZone,
  WriteMode,
  ProcessingMode,
  ColumnDefinition,
  DataType,
  GoldZoneConfig,
  TransformConfig,
  FileReferences,
  NLInstructions,
  TableFormat,
} from '@/types/pipeline-canonical'
import { SourceTypeSelector } from './SourceTypeSelector'
import {
  FileSourceConfigForm,
  DatabaseSourceConfigForm,
  NoSQLSourceConfigForm,
  StreamingSourceConfigForm,
  APISourceConfigForm,
  EBCDICSourceConfigForm,
  DTSXSourceConfigForm,
  LogsSourceConfigForm,
  NestedSourceConfigForm,
  SpecialSourceConfigForm,
} from './SourceConfigForms'
import { NLTransformInput } from './NLTransformInput'
import { GoldModelingSelector } from './GoldModelingSelector'
import { TransformationsPanel } from './TransformationsPanel'
import { SchemaDefinitionPanel } from './SchemaDefinitionPanel'
import { TargetConfigurationPanel } from './TargetConfigurationPanel'

// =============================================================================
// Props
// =============================================================================

interface UnifiedPipelineFormProps {
  jiraTicket?: string
  createdBy: string
  onSubmit: (input: UnifiedPipelineInput) => Promise<void>
  onCancel?: () => void
  isSubmitting?: boolean
  className?: string
}

// =============================================================================
// File References Component
// =============================================================================

// Use the FileReferences type from pipeline-canonical.ts

function FileReferencesSection({
  value,
  onChange,
  sourceType,
}: {
  value: FileReferences
  onChange: (refs: FileReferences) => void
  sourceType: SourceType | null
}) {
  const [isExpanded, setIsExpanded] = useState(true)
  const showCopybook = sourceType?.toString().includes('ebcdic') || sourceType?.toString().includes('cobol')
  const showDTSX = sourceType?.toString().includes('dtsx') || sourceType?.toString().includes('ssis')

  return (
    <div className="p-4 bg-gray-800/50 border border-gray-700 rounded-lg">
      <button
        type="button"
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between text-left"
      >
        <div className="flex items-center gap-2">
          <FolderOpen className="w-5 h-5 text-amber-400" />
          <span className="font-medium text-gray-200">File References (Optional)</span>
        </div>
        {isExpanded ? (
          <ChevronUp className="w-4 h-4 text-gray-400" />
        ) : (
          <ChevronDown className="w-4 h-4 text-gray-400" />
        )}
      </button>

      {isExpanded && (
        <div className="mt-4 space-y-4">
          <p className="text-xs text-gray-400 mb-3">
            Provide paths to external files the agent can use for schema inference and configuration.
          </p>

          {/* Copybook Path - Always show for EBCDIC, optional for others */}
          {(showCopybook || !sourceType) && (
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1">
                <FileCode className="w-4 h-4 inline mr-1 text-green-400" />
                COBOL Copybook Path (GCS)
              </label>
              <input
                type="text"
                value={value.copybook_path || ''}
                onChange={(e) => onChange({ ...value, copybook_path: e.target.value })}
                placeholder="gs://copybooks/CUSTOMER-MASTER.cpy"
                className="w-full px-3 py-2 bg-gray-900 border border-gray-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-blue-500 focus:border-blue-500"
              />
              <p className="mt-1 text-xs text-gray-400">
                Agent will parse this copybook to auto-infer schema and data types
              </p>
            </div>
          )}

          {/* DTSX Path - Always show for SSIS, optional for others */}
          {(showDTSX || !sourceType) && (
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1">
                <FileCode className="w-4 h-4 inline mr-1 text-blue-400" />
                DTSX Package Path (GCS)
              </label>
              <input
                type="text"
                value={value.dtsx_path || ''}
                onChange={(e) => onChange({ ...value, dtsx_path: e.target.value })}
                placeholder="gs://ssis-packages/InventoryDaily.dtsx"
                className="w-full px-3 py-2 bg-gray-900 border border-gray-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-blue-500 focus:border-blue-500"
              />
              <p className="mt-1 text-xs text-gray-400">
                Agent will parse SSIS package and migrate to Airflow/Spark
              </p>
            </div>
          )}

          {/* Schema File Path */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1">
              <FileText className="w-4 h-4 inline mr-1 text-purple-400" />
              Schema Definition File (GCS)
            </label>
            <input
              type="text"
              value={value.schema_file_path || ''}
              onChange={(e) => onChange({ ...value, schema_file_path: e.target.value })}
              placeholder="gs://schemas/customer_schema.json"
              className="w-full px-3 py-2 bg-gray-900 border border-gray-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-blue-500 focus:border-blue-500"
            />
            <p className="mt-1 text-xs text-gray-400">
              JSON/YAML/Avro schema file for automatic parsing
            </p>
          </div>

          {/* Sample Data Path */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1">
              <Database className="w-4 h-4 inline mr-1 text-cyan-400" />
              Sample Data File (GCS)
            </label>
            <input
              type="text"
              value={value.sample_data_path || ''}
              onChange={(e) => onChange({ ...value, sample_data_path: e.target.value })}
              placeholder="gs://samples/customer_sample.csv"
              className="w-full px-3 py-2 bg-gray-900 border border-gray-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-blue-500 focus:border-blue-500"
            />
            <p className="mt-1 text-xs text-gray-400">
              Sample data for schema inference and data quality profiling
            </p>
          </div>
        </div>
      )}
    </div>
  )
}

// =============================================================================
// Layer NL Instructions Component (Bronze/Silver/Gold)
// =============================================================================

interface LayerInstructions {
  bronze: string
  silver: string
  gold: string
}

function LayerNLInstructionsSection({
  value,
  onChange,
  targetZone,
}: {
  value: LayerInstructions
  onChange: (instructions: LayerInstructions) => void
  targetZone: TargetZone
}) {
  const [activeLayer, setActiveLayer] = useState<'bronze' | 'silver' | 'gold'>(
    targetZone === 'landing' ? 'bronze' : (targetZone as 'bronze' | 'silver' | 'gold')
  )

  const layers = [
    {
      id: 'bronze' as const,
      label: 'Bronze Layer',
      color: 'amber',
      icon: '🥉',
      description: 'Raw data ingestion - describe what data to load and how to handle errors',
      placeholder: 'Example: Load all EBCDIC files daily, handle encoding errors by quarantining bad records, add ingestion timestamp and source file name columns...',
    },
    {
      id: 'silver' as const,
      label: 'Silver Layer',
      color: 'gray',
      icon: '🥈',
      description: 'Data cleaning & validation - describe cleaning rules and transformations',
      placeholder: 'Example: Remove duplicates by customer_id keeping latest record, validate email format, mask PII fields (phone, SSN), convert dates to UTC...',
    },
    {
      id: 'gold' as const,
      label: 'Gold Layer',
      color: 'yellow',
      icon: '🥇',
      description: 'Business logic & aggregations - describe final transformations and metrics',
      placeholder: 'Example: Calculate customer lifetime value by summing all orders, create customer 360 view joining with support tickets, implement SCD Type 2 for history tracking...',
    },
  ]

  return (
    <Card className="p-0 overflow-hidden bg-gray-800/50 border-gray-700">
      <div className="p-4 border-b border-gray-700 bg-gradient-to-r from-amber-500/10 via-gray-500/10 to-yellow-500/10">
        <div className="flex items-center gap-2 mb-2">
          <Layers className="w-5 h-5 text-blue-400" />
          <h4 className="font-semibold text-gray-200">Medallion Architecture - Layer Instructions</h4>
        </div>
        <p className="text-xs text-gray-400">
          Describe what the agent should do at each layer. Natural language instructions will be converted to structured transformations.
        </p>
      </div>

      {/* Layer Tabs */}
      <div className="flex border-b border-gray-700">
        {layers.map((layer) => (
          <button
            key={layer.id}
            type="button"
            onClick={() => setActiveLayer(layer.id)}
            className={cn(
              'flex-1 flex items-center justify-center gap-2 px-4 py-3 text-sm font-medium transition-colors',
              activeLayer === layer.id
                ? 'bg-gray-700/50 text-gray-100 border-b-2 border-blue-500'
                : 'text-gray-400 hover:text-gray-300 hover:bg-gray-700/30'
            )}
          >
            <span>{layer.icon}</span>
            <span>{layer.label}</span>
            {value[layer.id] && (
              <span className="w-2 h-2 rounded-full bg-green-500" title="Has instructions" />
            )}
          </button>
        ))}
      </div>

      {/* Active Layer Content */}
      {layers.map((layer) => (
        <div
          key={layer.id}
          className={cn('p-4', activeLayer !== layer.id && 'hidden')}
        >
          <div className="flex items-start gap-2 mb-3">
            <Sparkles className="w-4 h-4 text-purple-400 mt-0.5" />
            <p className="text-sm text-gray-300">{layer.description}</p>
          </div>
          <textarea
            value={value[layer.id]}
            onChange={(e) => onChange({ ...value, [layer.id]: e.target.value })}
            placeholder={layer.placeholder}
            className="w-full px-3 py-2 bg-gray-900 border border-gray-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-purple-500 focus:border-purple-500 text-sm"
            rows={5}
          />
        </div>
      ))}

      <div className="p-3 bg-purple-900/20 border-t border-purple-800/30">
        <div className="flex items-start gap-2">
          <Info className="w-4 h-4 text-purple-400 flex-shrink-0 mt-0.5" />
          <p className="text-xs text-purple-300">
            The AI agent will convert your instructions into structured PySpark transformations, data quality rules, and schema definitions.
            You can review and modify the generated code before deployment.
          </p>
        </div>
      </div>
    </Card>
  )
}

// =============================================================================
// Main Component
// =============================================================================

export function UnifiedPipelineForm({
  jiraTicket,
  createdBy,
  onSubmit,
  onCancel,
  isSubmitting = false,
  className,
}: UnifiedPipelineFormProps) {
  // Form state
  const [pipeline, setPipeline] = useState<Partial<PipelineConfig>>({
    environment: 'dev',
    jira_ticket: jiraTicket,
  })
  const [source, setSource] = useState<Partial<SourceConfig>>({})
  const [schema, setSchema] = useState<Partial<SchemaConfig>>({ columns: [] })
  const [target, setTarget] = useState<Partial<TargetConfig>>({
    target_zone: 'bronze',
    write_mode: 'append',
  })
  const [executionPolicy, setExecutionPolicy] = useState<Partial<ExecutionPolicy>>({
    processing_mode: 'batch',
    retry_count: 2,
  })
  const [transformations, setTransformations] = useState<Partial<TransformConfig>[]>([])
  const [goldZoneConfig, setGoldZoneConfig] = useState<Partial<GoldZoneConfig>>({})

  // Source type selection
  const [selectedSourceType, setSelectedSourceType] = useState<SourceType | null>(null)

  // Schema input mode: manual, upload, paste
  const [schemaInputMode, setSchemaInputMode] = useState<'manual' | 'upload' | 'paste'>('manual')
  const [schemaText, setSchemaText] = useState('')
  const schemaFileInputRef = useRef<HTMLInputElement>(null)
  const [schemaParseMessage, setSchemaParseMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)

  // File references
  const [fileReferences, setFileReferences] = useState<FileReferences>({})

  // Layer NL instructions
  const [layerInstructions, setLayerInstructions] = useState<LayerInstructions>({
    bronze: '',
    silver: '',
    gold: '',
  })

  // NL instruction toggle states
  const [showNLInput, setShowNLInput] = useState<{
    identity: boolean
    source: boolean
    schema: boolean
    target: boolean
    execution: boolean
  }>({
    identity: false,
    source: false,
    schema: false,
    target: false,
    execution: false,
  })

  // NL instruction values
  const [nlInstructions, setNlInstructions] = useState<{
    identity: string
    source: string
    schema: string
    target: string
    execution: string
  }>({
    identity: '',
    source: '',
    schema: '',
    target: '',
    execution: '',
  })

  // Add transformation
  const handleAddTransform = useCallback((transform: Partial<TransformConfig>) => {
    setTransformations((prev) => [...prev, transform])
  }, [])

  // Remove transformation
  const removeTransform = useCallback((index: number) => {
    setTransformations((prev) => prev.filter((_, i) => i !== index))
  }, [])

  // Handle source type change
  const handleSourceTypeChange = useCallback((sourceType: SourceType) => {
    setSelectedSourceType(sourceType)
    setSource({ source_type: sourceType })
  }, [])

  // Render appropriate source config form based on source type
  const renderSourceConfigForm = () => {
    if (!selectedSourceType) return null

    const sourceTypeStr = selectedSourceType.toString()

    // File-based sources
    if (sourceTypeStr.startsWith('file_') && !sourceTypeStr.includes('ebcdic')) {
      return (
        <FileSourceConfigForm
          config={source.file_config || {}}
          onChange={(fileConfig) => setSource({ ...source, file_config: fileConfig })}
        />
      )
    }

    // Database sources (RDBMS)
    if (sourceTypeStr.startsWith('database_')) {
      return (
        <DatabaseSourceConfigForm
          config={source.database_config || {}}
          onChange={(dbConfig) => setSource({ ...source, database_config: dbConfig })}
        />
      )
    }

    // NoSQL sources
    if (sourceTypeStr.startsWith('nosql_')) {
      return (
        <NoSQLSourceConfigForm
          config={source.nosql_config || {}}
          onChange={(nosqlConfig) => setSource({ ...source, nosql_config: nosqlConfig })}
          sourceType={selectedSourceType}
        />
      )
    }

    // Streaming sources
    if (sourceTypeStr.startsWith('streaming_')) {
      return (
        <StreamingSourceConfigForm
          config={source.streaming_config || {}}
          onChange={(streamConfig) => setSource({ ...source, streaming_config: streamConfig })}
          sourceType={selectedSourceType}
        />
      )
    }

    // Logs sources
    if (sourceTypeStr.startsWith('logs_')) {
      return (
        <LogsSourceConfigForm
          config={source.logs_config || {}}
          onChange={(logsConfig) => setSource({ ...source, logs_config: logsConfig })}
        />
      )
    }

    // Nested sources
    if (sourceTypeStr.startsWith('nested_')) {
      return (
        <NestedSourceConfigForm
          config={source.nested_config || {}}
          onChange={(nestedConfig) => setSource({ ...source, nested_config: nestedConfig })}
        />
      )
    }

    // Special sources (IoT, Timeseries, Geospatial, ML Features, Open Data)
    if (sourceTypeStr.startsWith('special_')) {
      return (
        <SpecialSourceConfigForm
          config={source.special_config || {}}
          onChange={(specialConfig) => setSource({ ...source, special_config: specialConfig })}
          sourceType={selectedSourceType}
        />
      )
    }

    // API sources
    if (sourceTypeStr.startsWith('api_') || sourceTypeStr.startsWith('saas_')) {
      return (
        <APISourceConfigForm
          config={source.api_config || {}}
          onChange={(apiConfig) => setSource({ ...source, api_config: apiConfig })}
        />
      )
    }

    // EBCDIC/Legacy sources
    if (sourceTypeStr.includes('ebcdic') || sourceTypeStr.includes('cobol') || sourceTypeStr.includes('mainframe')) {
      return (
        <EBCDICSourceConfigForm
          config={source.ebcdic_config || {}}
          onChange={(ebcdicConfig) => setSource({ ...source, ebcdic_config: ebcdicConfig })}
        />
      )
    }

    // DTSX/SSIS sources
    if (sourceTypeStr.includes('dtsx') || sourceTypeStr.includes('ssis')) {
      return (
        <DTSXSourceConfigForm
          config={source.dtsx_config || {}}
          onChange={(dtsxConfig) => setSource({ ...source, dtsx_config: dtsxConfig })}
        />
      )
    }

    // Cloud storage sources
    if (sourceTypeStr.startsWith('cloud_')) {
      return (
        <FileSourceConfigForm
          config={source.file_config || {}}
          onChange={(fileConfig) => setSource({ ...source, file_config: fileConfig })}
        />
      )
    }

    // Advanced sources (CDC, Delta, Iceberg)
    if (sourceTypeStr.startsWith('advanced_')) {
      return (
        <DatabaseSourceConfigForm
          config={source.database_config || {}}
          onChange={(dbConfig) => setSource({ ...source, database_config: dbConfig })}
        />
      )
    }

    return (
      <div className="p-4 bg-gray-700/50 rounded-md text-sm text-gray-300">
        <AlertTriangle className="w-4 h-4 inline mr-2 text-amber-400" />
        Configuration form for <strong className="text-gray-100">{selectedSourceType}</strong> will use generic settings.
        The agent will infer additional configuration from your descriptions.
      </div>
    )
  }

  // Add column to schema
  const addColumn = useCallback(() => {
    const newColumn: ColumnDefinition = {
      name: '',
      type: 'string',
      nullable: true,
    }
    setSchema((prev) => ({
      ...prev,
      columns: [...(prev.columns || []), newColumn],
    }))
  }, [])

  // Update column
  const updateColumn = useCallback((index: number, updates: Partial<ColumnDefinition>) => {
    setSchema((prev) => ({
      ...prev,
      columns: (prev.columns || []).map((col, i) => (i === index ? { ...col, ...updates } : col)),
    }))
  }, [])

  // Remove column
  const removeColumn = useCallback((index: number) => {
    setSchema((prev) => ({
      ...prev,
      columns: (prev.columns || []).filter((_, i) => i !== index),
    }))
  }, [])

  // Parse schema from text (JSON or YAML-like format)
  const parseSchemaText = useCallback((text: string): { success: boolean; count?: number; error?: string } => {
    setSchemaParseMessage(null)

    try {
      // Try JSON first
      const parsed = JSON.parse(text)
      let columns: ColumnDefinition[] = []

      if (Array.isArray(parsed)) {
        columns = parsed.map((col: any) => ({
          name: col.name || col.column_name || col.field || '',
          type: (col.type || col.data_type || col.dataType || 'string').toLowerCase() as DataType,
          nullable: col.nullable !== false,
          pii: col.pii === true || col.is_pii === true ? 'pii' : (col.pii || 'none'),
        }))
      } else if (parsed.columns || parsed.fields || parsed.schema) {
        const cols = parsed.columns || parsed.fields || parsed.schema
        columns = cols.map((col: any) => ({
          name: col.name || col.column_name || col.field || '',
          type: (col.type || col.data_type || col.dataType || 'string').toLowerCase() as DataType,
          nullable: col.nullable !== false,
          pii: col.pii === true || col.is_pii === true ? 'pii' : (col.pii || 'none'),
        }))
      } else if (typeof parsed === 'object' && !Array.isArray(parsed)) {
        columns = Object.entries(parsed).map(([name, value]) => ({
          name,
          type: (typeof value === 'string' ? value : (value as any)?.type || 'string').toLowerCase() as DataType,
          nullable: true,
        }))
      }

      if (columns.length > 0) {
        setSchema((prev) => ({ ...prev, columns }))
        setSchemaInputMode('manual')
        setSchemaParseMessage({ type: 'success', text: `Successfully parsed ${columns.length} columns` })
        return { success: true, count: columns.length }
      }
      setSchemaParseMessage({ type: 'error', text: 'No columns found in schema' })
      return { success: false, error: 'No columns found in schema' }
    } catch {
      // Try YAML-like or simple "name: type" format
      const lines = text.trim().split('\n').filter((l) => l.trim() && !l.trim().startsWith('#'))
      const columns: ColumnDefinition[] = []

      for (const line of lines) {
        // Match patterns like: column_name: string, "column_name": "integer", - name: type
        const match = line.match(/^[\s\-]*["']?(\w+)["']?\s*[:=]\s*["']?(\w+)["']?/)
        if (match) {
          columns.push({
            name: match[1],
            type: match[2].toLowerCase() as DataType,
            nullable: true,
          })
        }
      }

      if (columns.length > 0) {
        setSchema((prev) => ({ ...prev, columns }))
        setSchemaInputMode('manual')
        setSchemaParseMessage({ type: 'success', text: `Successfully parsed ${columns.length} columns` })
        return { success: true, count: columns.length }
      }

      setSchemaParseMessage({ type: 'error', text: 'Could not parse schema. Use JSON array or "name: type" format per line.' })
      return { success: false, error: 'Could not parse schema' }
    }
  }, [])

  // Handle file upload for schema
  const handleSchemaFileUpload = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const file = event.target.files?.[0]
      if (!file) return

      const reader = new FileReader()
      reader.onload = (e) => {
        const content = e.target?.result as string
        parseSchemaText(content)
      }
      reader.readAsText(file)

      if (schemaFileInputRef.current) {
        schemaFileInputRef.current.value = ''
      }
    },
    [parseSchemaText]
  )

  // Handle paste schema submission
  const handlePasteSchemaSubmit = useCallback(() => {
    if (!schemaText.trim()) {
      setSchemaParseMessage({ type: 'error', text: 'Please paste your schema definition' })
      return
    }
    const result = parseSchemaText(schemaText)
    if (result.success) {
      setSchemaText('')
    }
  }, [schemaText, parseSchemaText])

  // Toggle NL input section
  const toggleNLInput = useCallback((section: keyof typeof showNLInput) => {
    setShowNLInput((prev) => ({ ...prev, [section]: !prev[section] }))
  }, [])

  // Handle submit
  const handleSubmit = useCallback(async () => {
    // Build NL instructions object
    const nlInstructionsPayload: NLInstructions = {}
    if (nlInstructions.identity) nlInstructionsPayload.identity = nlInstructions.identity
    if (nlInstructions.source) nlInstructionsPayload.source = nlInstructions.source
    if (nlInstructions.schema) nlInstructionsPayload.schema = nlInstructions.schema
    if (nlInstructions.target) nlInstructionsPayload.target = nlInstructions.target
    if (nlInstructions.execution) nlInstructionsPayload.execution = nlInstructions.execution
    if (layerInstructions.bronze) nlInstructionsPayload.layer_bronze = layerInstructions.bronze
    if (layerInstructions.silver) nlInstructionsPayload.layer_silver = layerInstructions.silver
    if (layerInstructions.gold) nlInstructionsPayload.layer_gold = layerInstructions.gold

    // Build file references object
    const fileReferencesPayload: FileReferences = {}
    if (fileReferences.copybook_path) fileReferencesPayload.copybook_path = fileReferences.copybook_path
    if (fileReferences.dtsx_path) fileReferencesPayload.dtsx_path = fileReferences.dtsx_path
    if (fileReferences.schema_file_path) fileReferencesPayload.schema_file_path = fileReferences.schema_file_path
    if (fileReferences.sample_data_path) fileReferencesPayload.sample_data_path = fileReferences.sample_data_path
    if (fileReferences.config_file_path) fileReferencesPayload.config_file_path = fileReferences.config_file_path

    const input: UnifiedPipelineInput = {
      input_type: 'ui_structured',
      created_by: createdBy,
      jira_ticket: jiraTicket,
      pipeline,
      source,
      schema,
      target,
      execution_policy: executionPolicy,
      transformations: transformations as TransformConfig[],
      gold_zone_config: target.target_zone === 'gold' ? goldZoneConfig : undefined,
      // File references as separate property
      file_references: Object.keys(fileReferencesPayload).length > 0 ? fileReferencesPayload : undefined,
      // NL instructions for agent processing
      nl_instructions: Object.keys(nlInstructionsPayload).length > 0 ? nlInstructionsPayload : undefined,
    }

    await onSubmit(input)
  }, [
    createdBy, jiraTicket, pipeline, source, schema, target, executionPolicy,
    transformations, goldZoneConfig, fileReferences, nlInstructions, layerInstructions, onSubmit
  ])

  return (
    <div className={cn('space-y-6', className)}>
      {/* Pipeline Identity */}
      <Card className="p-6 bg-gray-800/50 border-gray-700">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-100">
            1. Pipeline Identity
          </h3>
          <button
            type="button"
            onClick={() => toggleNLInput('identity')}
            className={cn(
              'flex items-center gap-1 px-3 py-1.5 rounded-md text-sm transition-colors',
              showNLInput.identity
                ? 'bg-purple-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-purple-900/50'
            )}
          >
            <Wand2 className="w-4 h-4" />
            AI Assist
          </button>
        </div>

        {showNLInput.identity && (
          <div className="mb-6 p-4 bg-purple-900/30 border border-purple-700 rounded-lg">
            <label className="block text-sm font-medium text-purple-300 mb-2">
              <Sparkles className="w-4 h-4 inline mr-1" />
              Describe your pipeline in natural language
            </label>
            <textarea
              value={nlInstructions.identity}
              onChange={(e) => setNlInstructions((prev) => ({ ...prev, identity: e.target.value }))}
              placeholder="Example: Create a customer data pipeline for the retail domain owned by the data-engineering team..."
              className="w-full px-3 py-2 bg-gray-900 border border-purple-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-purple-500 focus:border-purple-500"
              rows={3}
            />
          </div>
        )}

        <div className="grid grid-cols-2 gap-4">
          <Input
            label="DAG ID"
            placeholder="customer_daily_pipeline"
            value={pipeline.dag_id || ''}
            onChange={(e) => setPipeline({ ...pipeline, dag_id: e.target.value })}
            required
          />
          <Input
            label="Pipeline Name"
            placeholder="Customer Daily Pipeline"
            value={pipeline.pipeline_name || ''}
            onChange={(e) => setPipeline({ ...pipeline, pipeline_name: e.target.value })}
            required
          />
          <Input
            label="Domain"
            placeholder="customer"
            value={pipeline.domain || ''}
            onChange={(e) => setPipeline({ ...pipeline, domain: e.target.value })}
            required
          />
          <Input
            label="Product Code"
            placeholder="CUST-001"
            value={pipeline.product_code || ''}
            onChange={(e) => setPipeline({ ...pipeline, product_code: e.target.value })}
            required
          />
          <Input
            label="Owner Team"
            placeholder="data-engineering"
            value={pipeline.owner_team || ''}
            onChange={(e) => setPipeline({ ...pipeline, owner_team: e.target.value })}
            required
          />
          <Input
            label="Owner Email"
            type="email"
            placeholder="team@company.com"
            value={pipeline.owner_email || ''}
            onChange={(e) => setPipeline({ ...pipeline, owner_email: e.target.value })}
            required
          />
          <Select
            label="Environment"
            value={pipeline.environment || 'dev'}
            onChange={(e) => setPipeline({ ...pipeline, environment: e.target.value as Environment })}
            options={[
              { value: 'dev', label: 'Development' },
              { value: 'qa', label: 'QA' },
              { value: 'prod', label: 'Production' },
            ]}
          />
          <Input
            label="Jira Ticket"
            placeholder="SCRUM-4"
            value={pipeline.jira_ticket || ''}
            onChange={(e) => setPipeline({ ...pipeline, jira_ticket: e.target.value })}
          />
          <div className="col-span-2">
            <Input
              label="Description"
              placeholder="Brief description of this pipeline"
              value={pipeline.description || ''}
              onChange={(e) => setPipeline({ ...pipeline, description: e.target.value })}
            />
          </div>
        </div>
      </Card>

      {/* Contract Type & Pattern Selection */}
      <Card className="p-6 bg-gray-800/50 border-gray-700">
        <div className="flex items-center gap-2 mb-4">
          <Database className="w-5 h-5 text-blue-400" />
          <h3 className="text-lg font-semibold text-gray-100">
            1.5. Contract Type & Data Model
          </h3>
          <div className="ml-2 group relative">
            <Info className="w-4 h-4 text-gray-400 cursor-help" />
            <div className="absolute left-0 top-6 w-80 p-3 bg-gray-900 border border-gray-700 rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10">
              <p className="text-sm text-gray-300">
                <strong>Contract Type</strong> determines the pipeline pattern and data model:
                <br />• <strong>STANDARD</strong>: Simple ETL/ELT (P01-P03)
                <br />• <strong>SCD2</strong>: Slowly Changing Dimension Type 2 with history tracking (P07)
                <br />• <strong>DATA_VAULT</strong>: Data Vault 2.0 with Hubs, Links, Satellites (P08)
                <br />• <strong>STAR_SCHEMA</strong>: Star Schema with Facts and Dimensions (P09)
              </p>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4">
          {/* STANDARD */}
          <button
            type="button"
            onClick={() => setPipeline({ ...pipeline, contract_type: 'STANDARD' })}
            className={cn(
              'p-4 rounded-lg border-2 transition-all text-left',
              pipeline.contract_type === 'STANDARD'
                ? 'border-blue-500 bg-blue-900/30 ring-2 ring-blue-500/50'
                : 'border-gray-700 bg-gray-800/30 hover:border-gray-600'
            )}
          >
            <div className="flex items-center gap-2 mb-2">
              <Layers className={cn(
                'w-5 h-5',
                pipeline.contract_type === 'STANDARD' ? 'text-blue-400' : 'text-gray-400'
              )} />
              <span className={cn(
                'font-semibold',
                pipeline.contract_type === 'STANDARD' ? 'text-blue-300' : 'text-gray-300'
              )}>
                STANDARD
              </span>
              {pipeline.contract_type === 'STANDARD' && (
                <span className="ml-auto text-xs bg-blue-600 text-white px-2 py-0.5 rounded">
                  Selected
                </span>
              )}
            </div>
            <p className="text-sm text-gray-400">
              Standard ETL/ELT pipeline for batch or streaming data. Medallion architecture (Landing → Bronze → Silver → Gold).
            </p>
            <div className="mt-2 text-xs text-gray-500">
              Patterns: P01 (File), P03 (Database), P05 (Streaming)
            </div>
          </button>

          {/* SCD2 */}
          <button
            type="button"
            onClick={() => setPipeline({ ...pipeline, contract_type: 'SCD2' })}
            className={cn(
              'p-4 rounded-lg border-2 transition-all text-left',
              pipeline.contract_type === 'SCD2'
                ? 'border-amber-500 bg-amber-900/30 ring-2 ring-amber-500/50'
                : 'border-gray-700 bg-gray-800/30 hover:border-gray-600'
            )}
          >
            <div className="flex items-center gap-2 mb-2">
              <Database className={cn(
                'w-5 h-5',
                pipeline.contract_type === 'SCD2' ? 'text-amber-400' : 'text-gray-400'
              )} />
              <span className={cn(
                'font-semibold',
                pipeline.contract_type === 'SCD2' ? 'text-amber-300' : 'text-gray-300'
              )}>
                SCD TYPE 2
              </span>
              {pipeline.contract_type === 'SCD2' && (
                <span className="ml-auto text-xs bg-amber-600 text-white px-2 py-0.5 rounded">
                  Selected
                </span>
              )}
            </div>
            <p className="text-sm text-gray-400">
              Slowly Changing Dimension Type 2 with historical tracking. Hash-based change detection.
            </p>
            <div className="mt-2 text-xs text-gray-500">
              Pattern: P07 (SCD2 Pipeline)
            </div>
          </button>

          {/* DATA_VAULT */}
          <button
            type="button"
            onClick={() => setPipeline({ ...pipeline, contract_type: 'DATA_VAULT' })}
            className={cn(
              'p-4 rounded-lg border-2 transition-all text-left',
              pipeline.contract_type === 'DATA_VAULT'
                ? 'border-purple-500 bg-purple-900/30 ring-2 ring-purple-500/50'
                : 'border-gray-700 bg-gray-800/30 hover:border-gray-600'
            )}
          >
            <div className="flex items-center gap-2 mb-2">
              <Layers className={cn(
                'w-5 h-5',
                pipeline.contract_type === 'DATA_VAULT' ? 'text-purple-400' : 'text-gray-400'
              )} />
              <span className={cn(
                'font-semibold',
                pipeline.contract_type === 'DATA_VAULT' ? 'text-purple-300' : 'text-gray-300'
              )}>
                DATA VAULT 2.0
              </span>
              {pipeline.contract_type === 'DATA_VAULT' && (
                <span className="ml-auto text-xs bg-purple-600 text-white px-2 py-0.5 rounded">
                  Selected
                </span>
              )}
            </div>
            <p className="text-sm text-gray-400">
              Data Vault 2.0 methodology with Hubs, Links, and Satellites for enterprise data warehousing.
            </p>
            <div className="mt-2 text-xs text-gray-500">
              Pattern: P08 (Data Vault Pipeline)
            </div>
          </button>

          {/* STAR_SCHEMA */}
          <button
            type="button"
            onClick={() => setPipeline({ ...pipeline, contract_type: 'STAR_SCHEMA' })}
            className={cn(
              'p-4 rounded-lg border-2 transition-all text-left',
              pipeline.contract_type === 'STAR_SCHEMA'
                ? 'border-green-500 bg-green-900/30 ring-2 ring-green-500/50'
                : 'border-gray-700 bg-gray-800/30 hover:border-gray-600'
            )}
          >
            <div className="flex items-center gap-2 mb-2">
              <Database className={cn(
                'w-5 h-5',
                pipeline.contract_type === 'STAR_SCHEMA' ? 'text-green-400' : 'text-gray-400'
              )} />
              <span className={cn(
                'font-semibold',
                pipeline.contract_type === 'STAR_SCHEMA' ? 'text-green-300' : 'text-gray-300'
              )}>
                STAR SCHEMA
              </span>
              {pipeline.contract_type === 'STAR_SCHEMA' && (
                <span className="ml-auto text-xs bg-green-600 text-white px-2 py-0.5 rounded">
                  Selected
                </span>
              )}
            </div>
            <p className="text-sm text-gray-400">
              Star Schema with fact tables and dimensions for OLAP and business intelligence.
            </p>
            <div className="mt-2 text-xs text-gray-500">
              Pattern: P09 (Star Schema Pipeline)
            </div>
          </button>
        </div>

        {/* SCD2-specific configuration */}
        {pipeline.contract_type === 'SCD2' && (
          <div className="mt-4 p-4 bg-amber-900/20 border border-amber-700 rounded-lg">
            <h4 className="text-sm font-semibold text-amber-300 mb-3">
              SCD2 Configuration
            </h4>
            <div className="space-y-3">
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Business Keys
                  <span className="text-amber-400 ml-1">*</span>
                </label>
                <Input
                  placeholder="customer_id, account_id (comma-separated)"
                  value={pipeline.business_keys?.join(', ') || ''}
                  onChange={(e) => {
                    const keys = e.target.value.split(',').map(k => k.trim()).filter(Boolean)
                    setPipeline({ ...pipeline, business_keys: keys })
                  }}
                  helperText="Columns that uniquely identify a business entity"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">
                  Tracked Columns (Optional)
                </label>
                <Input
                  placeholder="name, email, address (comma-separated, or leave empty to track all)"
                  value={pipeline.tracked_columns?.join(', ') || ''}
                  onChange={(e) => {
                    const cols = e.target.value.split(',').map(c => c.trim()).filter(Boolean)
                    setPipeline({ ...pipeline, tracked_columns: cols })
                  }}
                  helperText="Columns to track for changes. Leave empty to track all columns."
                />
              </div>
            </div>
          </div>
        )}
      </Card>

      {/* File References */}
      <FileReferencesSection
        value={fileReferences}
        onChange={setFileReferences}
        sourceType={selectedSourceType}
      />

      {/* Source Configuration */}
      <Card className="p-6 bg-gray-800/50 border-gray-700">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-100">
            2. Source Configuration
          </h3>
          <button
            type="button"
            onClick={() => toggleNLInput('source')}
            className={cn(
              'flex items-center gap-1 px-3 py-1.5 rounded-md text-sm transition-colors',
              showNLInput.source
                ? 'bg-purple-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-purple-900/50'
            )}
          >
            <Wand2 className="w-4 h-4" />
            AI Assist
          </button>
        </div>

        {showNLInput.source && (
          <div className="mb-6 p-4 bg-purple-900/30 border border-purple-700 rounded-lg">
            <label className="block text-sm font-medium text-purple-300 mb-2">
              <Sparkles className="w-4 h-4 inline mr-1" />
              Describe your data source in natural language
            </label>
            <textarea
              value={nlInstructions.source}
              onChange={(e) => setNlInstructions((prev) => ({ ...prev, source: e.target.value }))}
              placeholder="Example: Load EBCDIC files from gs://legacy-exports/customer/*.dat using copybook at gs://copybooks/CUSTOMER.cpy, files are cp037 encoded..."
              className="w-full px-3 py-2 bg-gray-900 border border-purple-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-purple-500 focus:border-purple-500"
              rows={3}
            />
          </div>
        )}

        <SourceTypeSelector
          value={selectedSourceType}
          onChange={handleSourceTypeChange}
        />

        {selectedSourceType && (
          <div className="mt-6">
            <h4 className="text-md font-medium text-gray-200 mb-3">
              Configure {selectedSourceType}
            </h4>
            {renderSourceConfigForm()}
          </div>
        )}
      </Card>

      {/* Schema Definition */}
      <Card className="p-6 bg-gray-800/50 border-gray-700">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-100">
            3. Schema Definition
          </h3>
          <button
            type="button"
            onClick={() => toggleNLInput('schema')}
            className={cn(
              'flex items-center gap-1 px-3 py-1.5 rounded-md text-sm transition-colors',
              showNLInput.schema
                ? 'bg-purple-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-purple-900/50'
            )}
          >
            <Wand2 className="w-4 h-4" />
            AI Assist
          </button>
        </div>

        {showNLInput.schema && (
          <div className="mb-6 p-4 bg-purple-900/30 border border-purple-700 rounded-lg">
            <label className="block text-sm font-medium text-purple-300 mb-2">
              <Sparkles className="w-4 h-4 inline mr-1" />
              Describe your schema in natural language
            </label>
            <textarea
              value={nlInstructions.schema}
              onChange={(e) => setNlInstructions((prev) => ({ ...prev, schema: e.target.value }))}
              placeholder="Example: Customer data with ID (integer PK), name (string), email (string PII), balance (decimal 11,2), created_at (timestamp)..."
              className="w-full px-3 py-2 bg-gray-900 border border-purple-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-purple-500 focus:border-purple-500"
              rows={3}
            />
          </div>
        )}

        {/* Schema Input Mode Tabs */}
        <div className="flex items-center gap-1 mb-4 p-1 bg-gray-900 rounded-lg w-fit">
          <button
            type="button"
            onClick={() => setSchemaInputMode('manual')}
            className={cn(
              'flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors',
              schemaInputMode === 'manual'
                ? 'bg-gray-700 text-blue-400 shadow-sm'
                : 'text-gray-400 hover:text-gray-200'
            )}
          >
            <Edit3 className="w-4 h-4" />
            Manual Entry
          </button>
          <button
            type="button"
            onClick={() => setSchemaInputMode('upload')}
            className={cn(
              'flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors',
              schemaInputMode === 'upload'
                ? 'bg-gray-700 text-blue-400 shadow-sm'
                : 'text-gray-400 hover:text-gray-200'
            )}
          >
            <Upload className="w-4 h-4" />
            Upload File
          </button>
          <button
            type="button"
            onClick={() => setSchemaInputMode('paste')}
            className={cn(
              'flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors',
              schemaInputMode === 'paste'
                ? 'bg-gray-700 text-blue-400 shadow-sm'
                : 'text-gray-400 hover:text-gray-200'
            )}
          >
            <FileText className="w-4 h-4" />
            Paste Schema
          </button>
        </div>

        {/* Parse Message */}
        {schemaParseMessage && (
          <div className={cn(
            'mb-4 p-3 rounded-lg text-sm',
            schemaParseMessage.type === 'success'
              ? 'bg-green-900/30 border border-green-700 text-green-300'
              : 'bg-red-900/30 border border-red-700 text-red-300'
          )}>
            {schemaParseMessage.text}
          </div>
        )}

        {/* Manual Entry Mode */}
        {schemaInputMode === 'manual' && (
          <>
            <div className="flex justify-end mb-3">
              <Button type="button" onClick={addColumn} variant="outline" size="sm">
                + Add Column
              </Button>
            </div>
            {schema.columns && schema.columns.length > 0 ? (
              <div className="space-y-3">
                {schema.columns.map((column, index) => (
                  <div key={index} className="flex gap-3 items-start">
                    <Input
                      placeholder="Column name"
                      value={column.name}
                      onChange={(e) => updateColumn(index, { name: e.target.value })}
                      className="flex-1"
                    />
                    <Select
                      value={column.type}
                      onChange={(e) => updateColumn(index, { type: e.target.value as DataType })}
                      options={[
                        { value: 'string', label: 'STRING' },
                        { value: 'integer', label: 'INTEGER' },
                        { value: 'bigint', label: 'BIGINT' },
                        { value: 'float', label: 'FLOAT' },
                        { value: 'double', label: 'DOUBLE' },
                        { value: 'decimal', label: 'DECIMAL' },
                        { value: 'boolean', label: 'BOOLEAN' },
                        { value: 'date', label: 'DATE' },
                        { value: 'timestamp', label: 'TIMESTAMP' },
                      ]}
                      className="w-32"
                    />
                    <label className="flex items-center gap-1">
                      <input
                        type="checkbox"
                        checked={column.nullable !== false}
                        onChange={(e) => updateColumn(index, { nullable: e.target.checked })}
                        className="rounded bg-gray-700 border-gray-600"
                      />
                      <span className="text-sm text-gray-300">Null</span>
                    </label>
                    <label className="flex items-center gap-1">
                      <input
                        type="checkbox"
                        checked={column.pii === 'pii' || column.pii === 'phi' || column.pii === 'pci' || column.pii === 'sensitive'}
                        onChange={(e) => updateColumn(index, { pii: e.target.checked ? 'pii' : 'none' })}
                        className="rounded bg-gray-700 border-gray-600"
                      />
                      <span className="text-sm text-gray-300">PII</span>
                    </label>
                    <Button type="button" onClick={() => removeColumn(index)} variant="ghost" size="sm">
                      ✕
                    </Button>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-8 text-gray-400 border-2 border-dashed border-gray-600 rounded-lg">
                No columns defined. Click "Add Column" or switch to Upload/Paste mode.
              </div>
            )}
          </>
        )}

        {/* Upload File Mode */}
        {schemaInputMode === 'upload' && (
          <div className="space-y-4">
            <div className="border-2 border-dashed border-gray-600 rounded-lg p-8 text-center hover:border-blue-500 transition-colors">
              <Upload className="w-12 h-12 mx-auto text-gray-400 mb-4" />
              <p className="text-gray-300 mb-2">
                Upload a schema file (JSON, YAML, or CSV)
              </p>
              <p className="text-xs text-gray-400 mb-4">
                Supports: JSON array, YAML schema, Avro schema, CSV headers
              </p>
              <input
                ref={schemaFileInputRef}
                type="file"
                accept=".json,.yaml,.yml,.csv,.avsc"
                onChange={handleSchemaFileUpload}
                className="hidden"
              />
              <Button
                type="button"
                onClick={() => schemaFileInputRef.current?.click()}
                variant="outline"
              >
                Choose File
              </Button>
            </div>
            <div className="p-4 bg-gray-900 rounded-lg">
              <h4 className="text-sm font-medium text-gray-300 mb-2">
                Example JSON format:
              </h4>
              <pre className="text-xs text-gray-400 font-mono overflow-x-auto">
{`[
  { "name": "customer_id", "type": "integer", "nullable": false },
  { "name": "email", "type": "string", "pii": true },
  { "name": "balance", "type": "decimal" }
]`}
              </pre>
            </div>
          </div>
        )}

        {/* Paste Schema Mode */}
        {schemaInputMode === 'paste' && (
          <div className="space-y-4">
            <textarea
              value={schemaText}
              onChange={(e) => setSchemaText(e.target.value)}
              placeholder={`Paste your schema in JSON or simple format:

JSON:
[{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]

Simple format (one per line):
customer_id: integer
customer_name: string
email: string
balance: decimal`}
              className="w-full px-4 py-3 bg-gray-900 border border-gray-600 rounded-lg text-gray-200 font-mono text-sm placeholder-gray-500 focus:ring-blue-500 focus:border-blue-500"
              rows={12}
            />
            <div className="flex justify-end">
              <Button type="button" onClick={handlePasteSchemaSubmit} variant="primary">
                Parse & Apply Schema
              </Button>
            </div>
          </div>
        )}

        {/* Show current columns count when not in manual mode */}
        {schema.columns && schema.columns.length > 0 && schemaInputMode !== 'manual' && (
          <div className="mt-4 p-3 bg-green-900/30 border border-green-700 rounded-lg">
            <p className="text-sm text-green-300">
              ✓ {schema.columns.length} columns defined.{' '}
              <button
                type="button"
                onClick={() => setSchemaInputMode('manual')}
                className="text-green-400 underline hover:no-underline"
              >
                View & Edit
              </button>
            </p>
          </div>
        )}
      </Card>

      {/* Layer NL Instructions (Bronze/Silver/Gold) */}
      <LayerNLInstructionsSection
        value={layerInstructions}
        onChange={setLayerInstructions}
        targetZone={target.target_zone as TargetZone || 'bronze'}
      />

      {/* Transformations - Zone-Aware Builder */}
      <Card className="p-6 bg-gray-800/50 border-gray-700">
        <h3 className="text-lg font-semibold text-gray-100 mb-4">
          4. Transformations
        </h3>

        {/* Zone-Aware Transform Builder */}
        <div className="mb-6 p-4 bg-gray-900/50 rounded-lg border border-gray-700">
          <TransformationsPanel
            schema={schema}
            contractType={pipeline.contract_type}
            transforms={transformations}
            onChange={setTransformations}
          />
        </div>

        {/* NL Transform Input - Additional custom transforms */}
        <div className="border-t border-gray-700 pt-4">
          <p className="text-sm text-gray-400 mb-3">
            Or add transforms using Natural Language, SQL, or PySpark:
          </p>
          <NLTransformInput
            zone={(target.target_zone as TargetZone) || 'bronze'}
            schema={(schema.columns || []) as ColumnDefinition[]}
            onTransformAdd={handleAddTransform}
          />
        </div>
      </Card>

      {/* Gold Zone Modeling */}
      {target.target_zone === 'gold' && (
        <Card className="p-6 bg-gray-800/50 border-gray-700">
          <h3 className="text-lg font-semibold text-gray-100 mb-4">
            5. Gold Zone Modeling
          </h3>
          <p className="text-sm text-gray-400 mb-4">
            Select the data modeling strategy for your Gold zone tables.
          </p>
          <GoldModelingSelector
            value={goldZoneConfig}
            onChange={setGoldZoneConfig}
            availableColumns={(schema.columns || []) as ColumnDefinition[]}
          />
        </Card>
      )}

      {/* Target Configuration */}
      <Card className="p-6 bg-gray-800/50 border-gray-700">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-100">
            {target.target_zone === 'gold' ? '6' : '5'}. Target Configuration
          </h3>
          <button
            type="button"
            onClick={() => toggleNLInput('target')}
            className={cn(
              'flex items-center gap-1 px-3 py-1.5 rounded-md text-sm transition-colors',
              showNLInput.target
                ? 'bg-purple-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-purple-900/50'
            )}
          >
            <Wand2 className="w-4 h-4" />
            AI Assist
          </button>
        </div>

        {showNLInput.target && (
          <div className="mb-6 p-4 bg-purple-900/30 border border-purple-700 rounded-lg">
            <label className="block text-sm font-medium text-purple-300 mb-2">
              <Sparkles className="w-4 h-4 inline mr-1" />
              Describe your target destination
            </label>
            <textarea
              value={nlInstructions.target}
              onChange={(e) => setNlInstructions((prev) => ({ ...prev, target: e.target.value }))}
              placeholder="Example: Load to bronze zone in customer_data dataset, partition by ingestion_date, cluster by customer_id..."
              className="w-full px-3 py-2 bg-gray-900 border border-purple-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-purple-500 focus:border-purple-500"
              rows={3}
            />
          </div>
        )}

        <div className="grid grid-cols-2 gap-4">
          <Select
            label="Target Zone"
            value={target.target_zone || 'bronze'}
            onChange={(e) => setTarget({ ...target, target_zone: e.target.value as TargetZone })}
            options={[
              { value: 'landing', label: 'Landing' },
              { value: 'bronze', label: 'Bronze (Raw)' },
              { value: 'silver', label: 'Silver (Cleaned)' },
              { value: 'gold', label: 'Gold (Final)' },
            ]}
          />
          <Input
            label="BigQuery Dataset"
            placeholder="customer_data"
            value={target.bq_dataset || ''}
            onChange={(e) => setTarget({ ...target, bq_dataset: e.target.value })}
            required
          />
          <Input
            label="BigQuery Table"
            placeholder="customers_bronze"
            value={target.bq_table || ''}
            onChange={(e) => setTarget({ ...target, bq_table: e.target.value })}
            required
          />
          <Select
            label="Write Mode"
            value={target.write_mode || 'append'}
            onChange={(e) => setTarget({ ...target, write_mode: e.target.value as WriteMode })}
            options={[
              { value: 'append', label: 'Append' },
              { value: 'overwrite', label: 'Overwrite' },
              { value: 'merge', label: 'Merge' },
              { value: 'scd_type_1', label: 'SCD Type 1' },
              { value: 'scd_type_2', label: 'SCD Type 2' },
            ]}
          />
          <Input
            label="Partition Field"
            placeholder="ingestion_date"
            value={target.partition_field || ''}
            onChange={(e) => setTarget({ ...target, partition_field: e.target.value })}
          />
          <Input
            label="Clustering Fields"
            placeholder="customer_id, region (comma-separated)"
            value={target.clustering_fields?.join(', ') || ''}
            onChange={(e) => setTarget({ ...target, clustering_fields: e.target.value.split(',').map(s => s.trim()).filter(Boolean) })}
          />
          <Select
            label="Table Format"
            value={target.table_format || 'delta'}
            onChange={(e) => setTarget({ ...target, table_format: e.target.value as TableFormat })}
            options={[
              { value: 'delta', label: 'Delta Lake' },
              { value: 'iceberg', label: 'Apache Iceberg' },
              { value: 'parquet', label: 'Parquet' },
            ]}
          />
        </div>
      </Card>

      {/* Execution Policy */}
      <Card className="p-6 bg-gray-800/50 border-gray-700">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-100">
            {target.target_zone === 'gold' ? '7' : '6'}. Execution Policy
          </h3>
          <button
            type="button"
            onClick={() => toggleNLInput('execution')}
            className={cn(
              'flex items-center gap-1 px-3 py-1.5 rounded-md text-sm transition-colors',
              showNLInput.execution
                ? 'bg-purple-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-purple-900/50'
            )}
          >
            <Wand2 className="w-4 h-4" />
            AI Assist
          </button>
        </div>

        {showNLInput.execution && (
          <div className="mb-6 p-4 bg-purple-900/30 border border-purple-700 rounded-lg">
            <label className="block text-sm font-medium text-purple-300 mb-2">
              <Sparkles className="w-4 h-4 inline mr-1" />
              Describe your execution requirements
            </label>
            <textarea
              value={nlInstructions.execution}
              onChange={(e) => setNlInstructions((prev) => ({ ...prev, execution: e.target.value }))}
              placeholder="Example: Run daily at 2 AM EST, retry 3 times on failure, requires human approval for production..."
              className="w-full px-3 py-2 bg-gray-900 border border-purple-600 rounded-md text-gray-200 placeholder-gray-500 focus:ring-purple-500 focus:border-purple-500"
              rows={3}
            />
          </div>
        )}

        <div className="grid grid-cols-2 gap-4">
          <Input
            label="Schedule Interval"
            placeholder="@daily or 0 2 * * *"
            value={executionPolicy.schedule_interval || ''}
            onChange={(e) => setExecutionPolicy({ ...executionPolicy, schedule_interval: e.target.value })}
          />
          <Select
            label="Processing Mode"
            value={executionPolicy.processing_mode || 'batch'}
            onChange={(e) =>
              setExecutionPolicy({ ...executionPolicy, processing_mode: e.target.value as ProcessingMode })
            }
            options={[
              { value: 'batch', label: 'Batch' },
              { value: 'micro_batch', label: 'Micro Batch' },
              { value: 'streaming', label: 'Streaming' },
            ]}
          />
          <Input
            label="Retry Count"
            type="number"
            min={0}
            max={10}
            value={executionPolicy.retry_count || 2}
            onChange={(e) => setExecutionPolicy({ ...executionPolicy, retry_count: parseInt(e.target.value) })}
          />
          <Input
            label="Timeout (hours)"
            type="number"
            min={1}
            value={executionPolicy.timeout_hours || 1}
            onChange={(e) => setExecutionPolicy({ ...executionPolicy, timeout_hours: parseInt(e.target.value) })}
          />
          <Input
            label="SLA (hours)"
            type="number"
            min={1}
            value={executionPolicy.sla_hours || 4}
            onChange={(e) => setExecutionPolicy({ ...executionPolicy, sla_hours: parseInt(e.target.value) })}
          />
          <Input
            label="Alert Emails"
            type="text"
            placeholder="alerts@company.com (comma-separated)"
            value={executionPolicy.alert_emails?.join(', ') || ''}
            onChange={(e) => setExecutionPolicy({ ...executionPolicy, alert_emails: e.target.value.split(',').map(s => s.trim()).filter(Boolean) })}
          />
          <div className="col-span-2 flex items-center gap-6">
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={executionPolicy.requires_human_approval || false}
                onChange={(e) =>
                  setExecutionPolicy({ ...executionPolicy, requires_human_approval: e.target.checked })
                }
                className="h-4 w-4 rounded bg-gray-700 border-gray-600 text-blue-600"
              />
              <span className="text-sm text-gray-300">Requires human approval</span>
            </label>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={executionPolicy.catchup || false}
                onChange={(e) =>
                  setExecutionPolicy({ ...executionPolicy, catchup: e.target.checked })
                }
                className="h-4 w-4 rounded bg-gray-700 border-gray-600 text-blue-600"
              />
              <span className="text-sm text-gray-300">Enable catchup runs</span>
            </label>
          </div>
        </div>
      </Card>

      {/* Submit Actions */}
      <div className="flex justify-end gap-3 pt-4 border-t border-gray-700">
        {onCancel && (
          <Button type="button" onClick={onCancel} variant="outline" disabled={isSubmitting}>
            Cancel
          </Button>
        )}
        <Button type="button" onClick={handleSubmit} variant="primary" disabled={isSubmitting}>
          {isSubmitting ? 'Creating Pipeline...' : 'Create Pipeline & Start Agent'}
        </Button>
      </div>
    </div>
  )
}

export default UnifiedPipelineForm
