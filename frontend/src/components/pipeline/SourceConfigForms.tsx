/**
 * Source Configuration Forms
 *
 * Type-specific configuration forms for all 9 source categories.
 * Each form displays the appropriate fields based on the selected source type.
 *
 * DARK THEME - Professional dark color scheme
 */

import React from 'react'
import {
  SourceType,
  FileSourceConfig,
  DatabaseSourceConfig,
  StreamingSourceConfig,
  APISourceConfig,
  EBCDICSourceConfig,
  DTSXSourceConfig,
  ExtractionMode,
} from '@/types/pipeline-canonical'

// =============================================================================
// Shared Input Styles (Dark Theme)
// =============================================================================

const inputStyles = "w-full px-3 py-2 bg-gray-800 border border-gray-600 rounded-md text-white placeholder-gray-500 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
const labelStyles = "block text-sm font-medium text-gray-300 mb-1"
const selectStyles = "w-full px-3 py-2 bg-gray-800 border border-gray-600 rounded-md text-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-colors"
const checkboxLabelStyles = "ml-2 block text-sm text-gray-300"

// =============================================================================
// File Source Config Form
// =============================================================================

interface FileSourceConfigFormProps {
  config: Partial<FileSourceConfig>
  onChange: (config: Partial<FileSourceConfig>) => void
  disabled?: boolean
}

export const FileSourceConfigForm: React.FC<FileSourceConfigFormProps> = ({
  config,
  onChange,
  disabled = false,
}) => {
  const updateField = (field: keyof FileSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  return (
    <div className="space-y-4">
      <div>
        <label className={labelStyles}>
          Source Bucket <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.source_bucket || ''}
          onChange={(e) => updateField('source_bucket', e.target.value)}
          disabled={disabled}
          placeholder="my-data-bucket"
          className={inputStyles}
        />
      </div>

      <div>
        <label className={labelStyles}>
          Source Prefix <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.source_prefix || ''}
          onChange={(e) => updateField('source_prefix', e.target.value)}
          disabled={disabled}
          placeholder="raw/sales/"
          className={inputStyles}
        />
      </div>

      <div>
        <label className={labelStyles}>
          File Pattern
        </label>
        <input
          type="text"
          value={config.file_pattern || '*'}
          onChange={(e) => updateField('file_pattern', e.target.value)}
          disabled={disabled}
          placeholder="*.csv or sales_*.parquet"
          className={inputStyles}
        />
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelStyles}>
            Delimiter
          </label>
          <input
            type="text"
            value={config.delimiter || ','}
            onChange={(e) => updateField('delimiter', e.target.value)}
            disabled={disabled}
            maxLength={10}
            className={inputStyles}
          />
        </div>

        <div>
          <label className={labelStyles}>
            Encoding
          </label>
          <input
            type="text"
            value={config.encoding || 'utf-8'}
            onChange={(e) => updateField('encoding', e.target.value)}
            disabled={disabled}
            className={inputStyles}
          />
        </div>
      </div>

      <div className="flex items-center">
        <input
          type="checkbox"
          id="has_header"
          checked={config.has_header !== false}
          onChange={(e) => updateField('has_header', e.target.checked)}
          disabled={disabled}
          className="h-4 w-4 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500 focus:ring-offset-gray-800"
        />
        <label htmlFor="has_header" className={checkboxLabelStyles}>
          File has header row
        </label>
      </div>

      <div>
        <label className={labelStyles}>
          Compression
        </label>
        <select
          value={config.compression || ''}
          onChange={(e) => updateField('compression', e.target.value || undefined)}
          disabled={disabled}
          className={selectStyles}
        >
          <option value="">None</option>
          <option value="gzip">gzip</option>
          <option value="snappy">snappy</option>
          <option value="lz4">lz4</option>
        </select>
      </div>
    </div>
  )
}

// =============================================================================
// Database Source Config Form
// =============================================================================

interface DatabaseSourceConfigFormProps {
  config: Partial<DatabaseSourceConfig>
  onChange: (config: Partial<DatabaseSourceConfig>) => void
  disabled?: boolean
}

export const DatabaseSourceConfigForm: React.FC<DatabaseSourceConfigFormProps> = ({
  config,
  onChange,
  disabled = false,
}) => {
  const updateField = (field: keyof DatabaseSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  return (
    <div className="space-y-4">
      <div>
        <label className={labelStyles}>
          Airflow Connection ID <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.connection_id || ''}
          onChange={(e) => updateField('connection_id', e.target.value)}
          disabled={disabled}
          placeholder="postgres_prod"
          className={inputStyles}
        />
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelStyles}>
            Source Schema
          </label>
          <input
            type="text"
            value={config.source_schema || 'public'}
            onChange={(e) => updateField('source_schema', e.target.value)}
            disabled={disabled}
            className={inputStyles}
          />
        </div>

        <div>
          <label className={labelStyles}>
            Source Table <span className="text-red-400">*</span>
          </label>
          <input
            type="text"
            value={config.source_table || ''}
            onChange={(e) => updateField('source_table', e.target.value)}
            disabled={disabled}
            className={inputStyles}
          />
        </div>
      </div>

      <div>
        <label className={labelStyles}>
          Custom Query (Optional)
        </label>
        <textarea
          value={config.source_query || ''}
          onChange={(e) => updateField('source_query', e.target.value || undefined)}
          disabled={disabled}
          rows={3}
          placeholder="SELECT * FROM table WHERE ..."
          className={`${inputStyles} font-mono text-sm`}
        />
      </div>

      <div>
        <label className={labelStyles}>
          Extraction Mode
        </label>
        <select
          value={config.extraction_mode || 'full'}
          onChange={(e) => updateField('extraction_mode', e.target.value as ExtractionMode)}
          disabled={disabled}
          className={selectStyles}
        >
          <option value="full">Full Load</option>
          <option value="incremental">Incremental</option>
          <option value="cdc">Change Data Capture (CDC)</option>
        </select>
      </div>

      {config.extraction_mode === 'incremental' && (
        <div>
          <label className={labelStyles}>
            Watermark Column
          </label>
          <input
            type="text"
            value={config.watermark_column || ''}
            onChange={(e) => updateField('watermark_column', e.target.value)}
            disabled={disabled}
            placeholder="updated_at"
            className={inputStyles}
          />
        </div>
      )}

      <div>
        <label className={labelStyles}>
          Batch Size
        </label>
        <input
          type="number"
          value={config.batch_size || 10000}
          onChange={(e) => updateField('batch_size', parseInt(e.target.value))}
          disabled={disabled}
          min={1000}
          max={1000000}
          className={inputStyles}
        />
      </div>
    </div>
  )
}

// =============================================================================
// Streaming Source Config Form
// =============================================================================

interface StreamingSourceConfigFormProps {
  config: Partial<StreamingSourceConfig>
  onChange: (config: Partial<StreamingSourceConfig>) => void
  sourceType: SourceType
  disabled?: boolean
}

export const StreamingSourceConfigForm: React.FC<StreamingSourceConfigFormProps> = ({
  config,
  onChange,
  sourceType,
  disabled = false,
}) => {
  const updateField = (field: keyof StreamingSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  const isKafka = sourceType.includes('kafka') || sourceType.includes('confluent')
  const isPubSub = sourceType.includes('pubsub')

  return (
    <div className="space-y-4">
      {isKafka && (
        <>
          <div>
            <label className={labelStyles}>
              Kafka Bootstrap Servers <span className="text-red-400">*</span>
            </label>
            <input
              type="text"
              value={config.kafka_bootstrap_servers || ''}
              onChange={(e) => updateField('kafka_bootstrap_servers', e.target.value)}
              disabled={disabled}
              placeholder="localhost:9092"
              className={inputStyles}
            />
          </div>

          <div>
            <label className={labelStyles}>
              Kafka Topic <span className="text-red-400">*</span>
            </label>
            <input
              type="text"
              value={config.kafka_topic || ''}
              onChange={(e) => updateField('kafka_topic', e.target.value)}
              disabled={disabled}
              placeholder="user-events"
              className={inputStyles}
            />
          </div>

          <div>
            <label className={labelStyles}>
              Consumer Group
            </label>
            <input
              type="text"
              value={config.kafka_consumer_group || ''}
              onChange={(e) => updateField('kafka_consumer_group', e.target.value)}
              disabled={disabled}
              placeholder="pipeline-consumer-group"
              className={inputStyles}
            />
          </div>

          <div>
            <label className={labelStyles}>
              Offset Reset
            </label>
            <select
              value={config.kafka_offset_reset || 'earliest'}
              onChange={(e) => updateField('kafka_offset_reset', e.target.value)}
              disabled={disabled}
              className={selectStyles}
            >
              <option value="earliest">Earliest</option>
              <option value="latest">Latest</option>
            </select>
          </div>
        </>
      )}

      {isPubSub && (
        <>
          <div>
            <label className={labelStyles}>
              Pub/Sub Subscription <span className="text-red-400">*</span>
            </label>
            <input
              type="text"
              value={config.pubsub_subscription || ''}
              onChange={(e) => updateField('pubsub_subscription', e.target.value)}
              disabled={disabled}
              placeholder="projects/my-project/subscriptions/my-sub"
              className={inputStyles}
            />
          </div>

          <div>
            <label className={labelStyles}>
              GCP Project
            </label>
            <input
              type="text"
              value={config.pubsub_project || ''}
              onChange={(e) => updateField('pubsub_project', e.target.value)}
              disabled={disabled}
              placeholder="my-gcp-project"
              className={inputStyles}
            />
          </div>
        </>
      )}

      <div>
        <label className={labelStyles}>
          Message Format
        </label>
        <select
          value={config.message_format || 'json'}
          onChange={(e) => updateField('message_format', e.target.value)}
          disabled={disabled}
          className={selectStyles}
        >
          <option value="json">JSON</option>
          <option value="avro">Avro</option>
          <option value="protobuf">Protobuf</option>
        </select>
      </div>

      {config.message_format === 'avro' && (
        <div>
          <label className={labelStyles}>
            Schema Registry URL
          </label>
          <input
            type="text"
            value={config.schema_registry_url || ''}
            onChange={(e) => updateField('schema_registry_url', e.target.value)}
            disabled={disabled}
            placeholder="http://localhost:8081"
            className={inputStyles}
          />
        </div>
      )}

      {/* Windowing Configuration */}
      <div className="border-t border-gray-700 pt-4 mt-4">
        <h4 className="text-sm font-medium text-gray-300 mb-3">Windowing (Optional)</h4>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className={labelStyles}>Window Type</label>
            <select
              value={config.window_type || ''}
              onChange={(e) => updateField('window_type', e.target.value || undefined)}
              disabled={disabled}
              className={selectStyles}
            >
              <option value="">None</option>
              <option value="tumbling">Tumbling</option>
              <option value="sliding">Sliding</option>
              <option value="session">Session</option>
            </select>
          </div>

          <div>
            <label className={labelStyles}>Window Duration</label>
            <input
              type="text"
              value={config.window_duration || ''}
              onChange={(e) => updateField('window_duration', e.target.value)}
              disabled={disabled || !config.window_type}
              placeholder="5 minutes"
              className={inputStyles}
            />
          </div>

          {config.window_type === 'sliding' && (
            <div>
              <label className={labelStyles}>Slide Duration</label>
              <input
                type="text"
                value={config.slide_duration || ''}
                onChange={(e) => updateField('slide_duration', e.target.value)}
                disabled={disabled}
                placeholder="1 minute"
                className={inputStyles}
              />
            </div>
          )}

          <div>
            <label className={labelStyles}>Watermark Delay</label>
            <input
              type="text"
              value={config.watermark_delay || ''}
              onChange={(e) => updateField('watermark_delay', e.target.value)}
              disabled={disabled || !config.window_type}
              placeholder="10 minutes"
              className={inputStyles}
            />
          </div>

          <div>
            <label className={labelStyles}>Event Time Column</label>
            <input
              type="text"
              value={config.event_time_column || ''}
              onChange={(e) => updateField('event_time_column', e.target.value)}
              disabled={disabled || !config.window_type}
              placeholder="event_timestamp"
              className={inputStyles}
            />
          </div>
        </div>
      </div>

      {/* Dead Letter Queue */}
      <div className="border-t border-gray-700 pt-4 mt-4">
        <h4 className="text-sm font-medium text-gray-300 mb-3">Dead Letter Queue (Optional)</h4>
        <div>
          <label className={labelStyles}>DLQ Path</label>
          <input
            type="text"
            value={config.dlq_path || ''}
            onChange={(e) => updateField('dlq_path', e.target.value)}
            disabled={disabled}
            placeholder="gs://bucket/dlq/feed_id/"
            className={inputStyles}
          />
          <p className="text-xs text-gray-500 mt-1">GCS path where rejected/failed records are routed</p>
        </div>
      </div>
    </div>
  )
}

// =============================================================================
// API Source Config Form
// =============================================================================

interface APISourceConfigFormProps {
  config: Partial<APISourceConfig>
  onChange: (config: Partial<APISourceConfig>) => void
  disabled?: boolean
}

export const APISourceConfigForm: React.FC<APISourceConfigFormProps> = ({
  config,
  onChange,
  disabled = false,
}) => {
  const updateField = (field: keyof APISourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  return (
    <div className="space-y-4">
      <div>
        <label className={labelStyles}>
          API Endpoint <span className="text-red-400">*</span>
        </label>
        <input
          type="url"
          value={config.api_endpoint || ''}
          onChange={(e) => updateField('api_endpoint', e.target.value)}
          disabled={disabled}
          placeholder="https://api.example.com/v1/data"
          className={inputStyles}
        />
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelStyles}>
            HTTP Method
          </label>
          <select
            value={config.api_method || 'GET'}
            onChange={(e) => updateField('api_method', e.target.value)}
            disabled={disabled}
            className={selectStyles}
          >
            <option value="GET">GET</option>
            <option value="POST">POST</option>
          </select>
        </div>

        <div>
          <label className={labelStyles}>
            Auth Type
          </label>
          <select
            value={config.api_auth_type || 'bearer'}
            onChange={(e) => updateField('api_auth_type', e.target.value)}
            disabled={disabled}
            className={selectStyles}
          >
            <option value="bearer">Bearer Token</option>
            <option value="basic">Basic Auth</option>
            <option value="api_key">API Key</option>
            <option value="oauth2">OAuth2</option>
          </select>
        </div>
      </div>

      <div>
        <label className={labelStyles}>
          Auth Secret Name (GCP Secret Manager) <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.api_auth_secret || ''}
          onChange={(e) => updateField('api_auth_secret', e.target.value)}
          disabled={disabled}
          placeholder="api-credentials"
          className={inputStyles}
        />
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelStyles}>
            Pagination Type
          </label>
          <select
            value={config.pagination_type || ''}
            onChange={(e) => updateField('pagination_type', e.target.value || undefined)}
            disabled={disabled}
            className={selectStyles}
          >
            <option value="">None</option>
            <option value="offset">Offset-based</option>
            <option value="cursor">Cursor-based</option>
            <option value="page_number">Page Number</option>
          </select>
        </div>

        <div>
          <label className={labelStyles}>
            Rate Limit (requests/sec)
          </label>
          <input
            type="number"
            value={config.rate_limit_rps || 10}
            onChange={(e) => updateField('rate_limit_rps', parseInt(e.target.value))}
            disabled={disabled}
            min={1}
            className={inputStyles}
          />
        </div>
      </div>

      <div>
        <label className={labelStyles}>
          Response JSONPath
        </label>
        <input
          type="text"
          value={config.response_path || '$'}
          onChange={(e) => updateField('response_path', e.target.value)}
          disabled={disabled}
          placeholder="$.data or $.results"
          className={inputStyles}
        />
      </div>
    </div>
  )
}

// =============================================================================
// EBCDIC Source Config Form
// =============================================================================

interface EBCDICSourceConfigFormProps {
  config: Partial<EBCDICSourceConfig>
  onChange: (config: Partial<EBCDICSourceConfig>) => void
  disabled?: boolean
}

export const EBCDICSourceConfigForm: React.FC<EBCDICSourceConfigFormProps> = ({
  config,
  onChange,
  disabled = false,
}) => {
  const updateField = (field: keyof EBCDICSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  return (
    <div className="space-y-4">
      <div className="bg-amber-900/30 border border-amber-600/50 rounded-lg p-4">
        <p className="text-sm text-amber-200">
          <strong className="text-amber-300">EBCDIC/Copybook Configuration:</strong> This source type handles mainframe
          EBCDIC files with COBOL copybook definitions using the Cobrix library.
        </p>
      </div>

      <div>
        <label className={labelStyles}>
          Source Bucket <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.source_bucket || ''}
          onChange={(e) => updateField('source_bucket', e.target.value)}
          disabled={disabled}
          placeholder="legacy-mainframe-exports"
          className={inputStyles}
        />
      </div>

      <div>
        <label className={labelStyles}>
          Source Prefix <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.source_prefix || ''}
          onChange={(e) => updateField('source_prefix', e.target.value)}
          disabled={disabled}
          placeholder="customer/*.dat"
          className={inputStyles}
        />
      </div>

      <div>
        <label className={labelStyles}>
          Copybook Path (GCS) <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.copybook_path || ''}
          onChange={(e) => updateField('copybook_path', e.target.value)}
          disabled={disabled}
          placeholder="gs://copybooks/CUSTOMER-MASTER.cpy"
          className={inputStyles}
        />
        <p className="text-xs text-gray-500 mt-1">
          Path to the COBOL copybook that defines the record layout
        </p>
      </div>

      <div>
        <label className={labelStyles}>
          EBCDIC Encoding
        </label>
        <select
          value={config.encoding || 'cp037'}
          onChange={(e) => updateField('encoding', e.target.value)}
          disabled={disabled}
          className={selectStyles}
        >
          <option value="cp037">cp037 (US/Canada)</option>
          <option value="cp500">cp500 (International)</option>
          <option value="cp1047">cp1047 (Open Systems)</option>
          <option value="cp273">cp273 (Germany/Austria)</option>
          <option value="cp285">cp285 (UK)</option>
        </select>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelStyles}>
            Fixed Record Length
          </label>
          <input
            type="number"
            value={config.record_length || ''}
            onChange={(e) => updateField('record_length', parseInt(e.target.value) || undefined)}
            disabled={disabled}
            placeholder="Auto-detect from copybook"
            className={inputStyles}
          />
        </div>

        <div className="flex items-end">
          <div className="flex items-center h-10">
            <input
              type="checkbox"
              id="is_variable_length"
              checked={config.is_variable_length || false}
              onChange={(e) => updateField('is_variable_length', e.target.checked)}
              disabled={disabled}
              className="h-4 w-4 text-blue-500 bg-gray-700 border-gray-600 rounded focus:ring-blue-500 focus:ring-offset-gray-800"
            />
            <label htmlFor="is_variable_length" className={checkboxLabelStyles}>
              Variable Length Records
            </label>
          </div>
        </div>
      </div>

      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-4 mt-4">
        <h4 className="text-sm font-medium text-gray-300 mb-2">Supported COBOL Data Types</h4>
        <div className="text-xs text-gray-400 space-y-1">
          <p><code className="text-cyan-400">PIC 9(n)</code> - Numeric display</p>
          <p><code className="text-cyan-400">PIC X(n)</code> - Alphanumeric</p>
          <p><code className="text-cyan-400">PIC S9(n)V9(m) COMP-3</code> - Packed decimal (BCD)</p>
          <p><code className="text-cyan-400">PIC S9(n) COMP</code> - Binary integer</p>
        </div>
      </div>
    </div>
  )
}

// =============================================================================
// DTSX Source Config Form
// =============================================================================

interface DTSXSourceConfigFormProps {
  config: Partial<DTSXSourceConfig>
  onChange: (config: Partial<DTSXSourceConfig>) => void
  disabled?: boolean
}

export const DTSXSourceConfigForm: React.FC<DTSXSourceConfigFormProps> = ({
  config,
  onChange,
  disabled = false,
}) => {
  const updateField = (field: keyof DTSXSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  return (
    <div className="space-y-4">
      <div className="bg-blue-900/30 border border-blue-600/50 rounded-lg p-4">
        <p className="text-sm text-blue-200">
          <strong className="text-blue-300">DTSX Migration:</strong> Upload your SSIS package (.dtsx file) and we'll
          automatically parse and migrate it to a modern Airflow + PySpark pipeline.
        </p>
      </div>

      <div>
        <label className={labelStyles}>
          DTSX File Location (GCS) <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.dtsx_location || ''}
          onChange={(e) => updateField('dtsx_location', e.target.value)}
          disabled={disabled}
          placeholder="gs://bucket/ssis-packages/MyPackage.dtsx"
          className={inputStyles}
        />
      </div>

      <div>
        <label className={labelStyles}>
          Package Name <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.dtsx_package_name || ''}
          onChange={(e) => updateField('dtsx_package_name', e.target.value)}
          disabled={disabled}
          placeholder="MySSISPackage"
          className={inputStyles}
        />
      </div>

      <div>
        <label className={labelStyles}>
          Dataflow Name (Optional)
        </label>
        <input
          type="text"
          value={config.dataflow_name || ''}
          onChange={(e) => updateField('dataflow_name', e.target.value)}
          disabled={disabled}
          placeholder="Specific dataflow to extract"
          className={inputStyles}
        />
        <p className="text-xs text-gray-500 mt-1">
          Leave empty to migrate all dataflows in the package
        </p>
      </div>

      <div>
        <label className={labelStyles}>
          Original Server
        </label>
        <input
          type="text"
          value={config.original_server || ''}
          onChange={(e) => updateField('original_server', e.target.value)}
          disabled={disabled}
          placeholder="Original SQL Server name (for documentation)"
          className={inputStyles}
        />
      </div>

      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-4 mt-4">
        <h4 className="text-sm font-medium text-gray-300 mb-2">Supported SSIS Components</h4>
        <div className="grid grid-cols-2 gap-2 text-xs text-gray-400">
          <p>OLE DB Source/Destination</p>
          <p>Flat File Source/Destination</p>
          <p>Derived Column Transform</p>
          <p>Lookup Transform</p>
          <p>Conditional Split</p>
          <p>Sort Transform</p>
          <p>Aggregate Transform</p>
          <p>Merge Join</p>
        </div>
      </div>
    </div>
  )
}
