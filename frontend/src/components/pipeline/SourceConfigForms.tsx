/**
 * Source Configuration Forms
 *
 * Type-specific configuration forms for all 11 source categories.
 * Each form displays the appropriate fields based on the selected source type.
 *
 * DARK THEME - Professional dark color scheme
 */

import React from 'react'
import {
  SourceType,
  FileSourceConfig,
  DatabaseSourceConfig,
  NoSQLSourceConfig,
  StreamingSourceConfig,
  APISourceConfig,
  EBCDICSourceConfig,
  DTSXSourceConfig,
  LogsSourceConfig,
  NestedSourceConfig,
  SpecialSourceConfig,
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

// =============================================================================
// NoSQL Source Config Form
// =============================================================================

interface NoSQLSourceConfigFormProps {
  config: Partial<NoSQLSourceConfig>
  onChange: (config: Partial<NoSQLSourceConfig>) => void
  sourceType?: SourceType
  disabled?: boolean
}

export const NoSQLSourceConfigForm: React.FC<NoSQLSourceConfigFormProps> = ({
  config,
  onChange,
  sourceType,
  disabled = false,
}) => {
  const updateField = (field: keyof NoSQLSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  const sourceTypeStr = sourceType?.toString() || ''
  const isMongoDB = sourceTypeStr.includes('mongodb')
  const isCassandra = sourceTypeStr.includes('cassandra')
  const isDynamoDB = sourceTypeStr.includes('dynamodb')
  const isElasticSearch = sourceTypeStr.includes('elasticsearch') || sourceTypeStr.includes('solr')
  const isNeo4j = sourceTypeStr.includes('neo4j')
  const isRedis = sourceTypeStr.includes('redis')
  const isHBase = sourceTypeStr.includes('hbase')
  const isCouchbase = sourceTypeStr.includes('couchbase')

  return (
    <div className="space-y-4">
      <div>
        <label className={labelStyles}>
          Connection String <span className="text-red-400">*</span>
        </label>
        <input
          type="text"
          value={config.connection_string || ''}
          onChange={(e) => updateField('connection_string', e.target.value)}
          disabled={disabled}
          placeholder={
            isMongoDB ? 'mongodb://user:pass@host:27017/database' :
            isCassandra ? 'cassandra://host:9042/keyspace' :
            isDynamoDB ? 'dynamodb://access_key:secret@region' :
            isElasticSearch ? 'https://user:pass@host:9200' :
            isNeo4j ? 'neo4j://host:7687' :
            isRedis ? 'redis://host:6379' :
            'connection-string'
          }
          className={inputStyles}
        />
      </div>

      {/* MongoDB / Couchbase specific */}
      {(isMongoDB || isCouchbase) && (
        <>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className={labelStyles}>Database Name</label>
              <input
                type="text"
                value={config.database_name || ''}
                onChange={(e) => updateField('database_name', e.target.value)}
                disabled={disabled}
                placeholder="my_database"
                className={inputStyles}
              />
            </div>
            <div>
              <label className={labelStyles}>Collection Name</label>
              <input
                type="text"
                value={config.collection_name || ''}
                onChange={(e) => updateField('collection_name', e.target.value)}
                disabled={disabled}
                placeholder="my_collection"
                className={inputStyles}
              />
            </div>
          </div>
          <div>
            <label className={labelStyles}>Query Filter (JSON)</label>
            <textarea
              value={config.query_filter || ''}
              onChange={(e) => updateField('query_filter', e.target.value)}
              disabled={disabled}
              placeholder='{"status": "active", "created_at": {"$gte": "2024-01-01"}}'
              rows={3}
              className={inputStyles}
            />
          </div>
          <div>
            <label className={labelStyles}>Read Preference</label>
            <select
              value={config.read_preference || 'primary'}
              onChange={(e) => updateField('read_preference', e.target.value)}
              disabled={disabled}
              className={selectStyles}
            >
              <option value="primary">Primary</option>
              <option value="primaryPreferred">Primary Preferred</option>
              <option value="secondary">Secondary</option>
              <option value="secondaryPreferred">Secondary Preferred</option>
              <option value="nearest">Nearest</option>
            </select>
          </div>
        </>
      )}

      {/* Cassandra / HBase specific */}
      {(isCassandra || isHBase) && (
        <>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className={labelStyles}>Keyspace</label>
              <input
                type="text"
                value={config.keyspace || ''}
                onChange={(e) => updateField('keyspace', e.target.value)}
                disabled={disabled}
                placeholder="my_keyspace"
                className={inputStyles}
              />
            </div>
            <div>
              <label className={labelStyles}>Table Name</label>
              <input
                type="text"
                value={config.table_name || ''}
                onChange={(e) => updateField('table_name', e.target.value)}
                disabled={disabled}
                placeholder="my_table"
                className={inputStyles}
              />
            </div>
          </div>
        </>
      )}

      {/* DynamoDB specific */}
      {isDynamoDB && (
        <>
          <div>
            <label className={labelStyles}>Table Name</label>
            <input
              type="text"
              value={config.table_name || ''}
              onChange={(e) => updateField('table_name', e.target.value)}
              disabled={disabled}
              placeholder="my_dynamodb_table"
              className={inputStyles}
            />
          </div>
          <div>
            <label className={labelStyles}>Query Filter (JSON)</label>
            <textarea
              value={config.query_filter || ''}
              onChange={(e) => updateField('query_filter', e.target.value)}
              disabled={disabled}
              placeholder='{"KeyConditionExpression": "pk = :pk", "ExpressionAttributeValues": {":pk": {"S": "value"}}}'
              rows={3}
              className={inputStyles}
            />
          </div>
        </>
      )}

      {/* ElasticSearch / Solr specific */}
      {isElasticSearch && (
        <>
          <div>
            <label className={labelStyles}>Index Name</label>
            <input
              type="text"
              value={config.index_name || ''}
              onChange={(e) => updateField('index_name', e.target.value)}
              disabled={disabled}
              placeholder="my_index"
              className={inputStyles}
            />
          </div>
          <div>
            <label className={labelStyles}>Search Query (JSON)</label>
            <textarea
              value={config.search_query || ''}
              onChange={(e) => updateField('search_query', e.target.value)}
              disabled={disabled}
              placeholder='{"query": {"match_all": {}}, "size": 10000}'
              rows={3}
              className={inputStyles}
            />
          </div>
        </>
      )}

      {/* Neo4j specific */}
      {isNeo4j && (
        <div>
          <label className={labelStyles}>Cypher Query</label>
          <textarea
            value={config.cypher_query || ''}
            onChange={(e) => updateField('cypher_query', e.target.value)}
            disabled={disabled}
            placeholder="MATCH (n:Person) RETURN n.name, n.age LIMIT 1000"
            rows={4}
            className={inputStyles}
          />
        </div>
      )}

      {/* Common options */}
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelStyles}>Extraction Mode</label>
          <select
            value={config.extraction_mode || 'full'}
            onChange={(e) => updateField('extraction_mode', e.target.value as ExtractionMode)}
            disabled={disabled}
            className={selectStyles}
          >
            <option value="full">Full Extract</option>
            <option value="incremental">Incremental</option>
            <option value="cdc">Change Data Capture</option>
          </select>
        </div>
        <div>
          <label className={labelStyles}>Batch Size</label>
          <input
            type="number"
            value={config.batch_size || 1000}
            onChange={(e) => updateField('batch_size', parseInt(e.target.value))}
            disabled={disabled}
            min={100}
            max={100000}
            className={inputStyles}
          />
        </div>
      </div>
    </div>
  )
}

// =============================================================================
// Logs Source Config Form
// =============================================================================

interface LogsSourceConfigFormProps {
  config: Partial<LogsSourceConfig>
  onChange: (config: Partial<LogsSourceConfig>) => void
  disabled?: boolean
}

export const LogsSourceConfigForm: React.FC<LogsSourceConfigFormProps> = ({
  config,
  onChange,
  disabled = false,
}) => {
  const updateField = (field: keyof LogsSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  const isGCS = config.log_source === 'gcs'
  const isStackdriver = config.log_source === 'stackdriver'
  const isElasticsearch = config.log_source === 'elasticsearch'
  const isSplunk = config.log_source === 'splunk'

  return (
    <div className="space-y-4">
      <div>
        <label className={labelStyles}>
          Log Source <span className="text-red-400">*</span>
        </label>
        <select
          value={config.log_source || 'gcs'}
          onChange={(e) => updateField('log_source', e.target.value)}
          disabled={disabled}
          className={selectStyles}
        >
          <option value="gcs">GCS Log Files</option>
          <option value="stackdriver">Google Cloud Logging (Stackdriver)</option>
          <option value="elasticsearch">Elasticsearch</option>
          <option value="splunk">Splunk</option>
          <option value="datadog">Datadog</option>
        </select>
      </div>

      {isGCS && (
        <>
          <div>
            <label className={labelStyles}>Log Path</label>
            <input
              type="text"
              value={config.log_path || ''}
              onChange={(e) => updateField('log_path', e.target.value)}
              disabled={disabled}
              placeholder="gs://my-bucket/logs/application/"
              className={inputStyles}
            />
          </div>
          <div>
            <label className={labelStyles}>File Pattern</label>
            <input
              type="text"
              value={config.log_pattern || ''}
              onChange={(e) => updateField('log_pattern', e.target.value)}
              disabled={disabled}
              placeholder="*.log or app-*.log.gz"
              className={inputStyles}
            />
          </div>
        </>
      )}

      {isStackdriver && (
        <>
          <div>
            <label className={labelStyles}>Project ID</label>
            <input
              type="text"
              value={config.project_id || ''}
              onChange={(e) => updateField('project_id', e.target.value)}
              disabled={disabled}
              placeholder="my-gcp-project"
              className={inputStyles}
            />
          </div>
          <div>
            <label className={labelStyles}>Log Name</label>
            <input
              type="text"
              value={config.log_name || ''}
              onChange={(e) => updateField('log_name', e.target.value)}
              disabled={disabled}
              placeholder="projects/my-project/logs/my-log"
              className={inputStyles}
            />
          </div>
        </>
      )}

      {(isElasticsearch || isSplunk) && (
        <div>
          <label className={labelStyles}>Query</label>
          <textarea
            value={config.query || ''}
            onChange={(e) => updateField('query', e.target.value)}
            disabled={disabled}
            placeholder={isElasticsearch ? '{"query": {"match_all": {}}}' : 'index=main sourcetype=application'}
            rows={3}
            className={inputStyles}
          />
        </div>
      )}

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelStyles}>Start Time</label>
          <input
            type="datetime-local"
            value={config.start_time || ''}
            onChange={(e) => updateField('start_time', e.target.value)}
            disabled={disabled}
            className={inputStyles}
          />
        </div>
        <div>
          <label className={labelStyles}>End Time</label>
          <input
            type="datetime-local"
            value={config.end_time || ''}
            onChange={(e) => updateField('end_time', e.target.value)}
            disabled={disabled}
            className={inputStyles}
          />
        </div>
      </div>

      <div>
        <label className={labelStyles}>Parse Format</label>
        <select
          value={config.parse_format || 'json'}
          onChange={(e) => updateField('parse_format', e.target.value)}
          disabled={disabled}
          className={selectStyles}
        >
          <option value="json">JSON</option>
          <option value="regex">Regex</option>
          <option value="grok">Grok Pattern</option>
          <option value="unstructured">Unstructured</option>
        </select>
      </div>

      {config.parse_format === 'regex' && (
        <div>
          <label className={labelStyles}>Regex Pattern</label>
          <input
            type="text"
            value={config.log_pattern || ''}
            onChange={(e) => updateField('log_pattern', e.target.value)}
            disabled={disabled}
            placeholder="^(?<timestamp>\d{4}-\d{2}-\d{2}) (?<level>\w+) (?<message>.*)"
            className={inputStyles}
          />
        </div>
      )}
    </div>
  )
}

// =============================================================================
// Nested Source Config Form
// =============================================================================

interface NestedSourceConfigFormProps {
  config: Partial<NestedSourceConfig>
  onChange: (config: Partial<NestedSourceConfig>) => void
  disabled?: boolean
}

export const NestedSourceConfigForm: React.FC<NestedSourceConfigFormProps> = ({
  config,
  onChange,
  disabled = false,
}) => {
  const updateField = (field: keyof NestedSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelStyles}>
            Flatten Strategy <span className="text-red-400">*</span>
          </label>
          <select
            value={config.flatten_strategy || 'explode'}
            onChange={(e) => updateField('flatten_strategy', e.target.value)}
            disabled={disabled}
            className={selectStyles}
          >
            <option value="explode">Explode (flatten to rows)</option>
            <option value="struct">Keep as Struct</option>
            <option value="json_string">Store as JSON String</option>
          </select>
          <p className="text-xs text-gray-500 mt-1">
            How to handle nested objects
          </p>
        </div>
        <div>
          <label className={labelStyles}>
            Array Handling <span className="text-red-400">*</span>
          </label>
          <select
            value={config.array_handling || 'explode'}
            onChange={(e) => updateField('array_handling', e.target.value)}
            disabled={disabled}
            className={selectStyles}
          >
            <option value="explode">Explode (one row per element)</option>
            <option value="collect">Collect (array column)</option>
            <option value="first">First Element Only</option>
            <option value="json_array">Store as JSON Array</option>
          </select>
          <p className="text-xs text-gray-500 mt-1">
            How to handle arrays in data
          </p>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className={labelStyles}>Max Depth</label>
          <input
            type="number"
            value={config.max_depth || 10}
            onChange={(e) => updateField('max_depth', parseInt(e.target.value))}
            disabled={disabled}
            min={1}
            max={100}
            className={inputStyles}
          />
          <p className="text-xs text-gray-500 mt-1">
            Maximum nesting depth to process
          </p>
        </div>
        <div>
          <label className={labelStyles}>Null Handling</label>
          <select
            value={config.null_handling || 'keep'}
            onChange={(e) => updateField('null_handling', e.target.value)}
            disabled={disabled}
            className={selectStyles}
          >
            <option value="keep">Keep Nulls</option>
            <option value="remove">Remove Null Fields</option>
            <option value="default">Use Default Values</option>
          </select>
        </div>
      </div>

      <div className="flex items-center gap-4">
        <label className="flex items-center">
          <input
            type="checkbox"
            checked={config.schema_inference ?? true}
            onChange={(e) => updateField('schema_inference', e.target.checked)}
            disabled={disabled}
            className="h-4 w-4 text-blue-600 bg-gray-800 border-gray-600 rounded focus:ring-blue-500"
          />
          <span className={checkboxLabelStyles}>Enable Schema Inference</span>
        </label>
        <label className="flex items-center">
          <input
            type="checkbox"
            checked={config.preserve_paths ?? false}
            onChange={(e) => updateField('preserve_paths', e.target.checked)}
            disabled={disabled}
            className="h-4 w-4 text-blue-600 bg-gray-800 border-gray-600 rounded focus:ring-blue-500"
          />
          <span className={checkboxLabelStyles}>Preserve JSON Paths</span>
        </label>
      </div>

      {config.schema_inference && (
        <div>
          <label className={labelStyles}>Schema Sample Size</label>
          <input
            type="number"
            value={config.schema_sample_size || 100}
            onChange={(e) => updateField('schema_sample_size', parseInt(e.target.value))}
            disabled={disabled}
            min={10}
            max={10000}
            className={inputStyles}
          />
          <p className="text-xs text-gray-500 mt-1">
            Number of records to sample for schema inference
          </p>
        </div>
      )}
    </div>
  )
}

// =============================================================================
// Special Source Config Form
// =============================================================================

interface SpecialSourceConfigFormProps {
  config: Partial<SpecialSourceConfig>
  onChange: (config: Partial<SpecialSourceConfig>) => void
  sourceType?: SourceType
  disabled?: boolean
}

export const SpecialSourceConfigForm: React.FC<SpecialSourceConfigFormProps> = ({
  config,
  onChange,
  sourceType,
  disabled = false,
}) => {
  const updateField = (field: keyof SpecialSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  const sourceTypeStr = sourceType?.toString() || ''
  const isIoT = sourceTypeStr.includes('iot')
  const isTimeseries = sourceTypeStr.includes('timeseries')
  const isGeospatial = sourceTypeStr.includes('geospatial')
  const isMLFeatures = sourceTypeStr.includes('ml_features')
  const isOpenData = sourceTypeStr.includes('open_data')

  return (
    <div className="space-y-4">
      {/* IoT-specific */}
      {isIoT && (
        <>
          <div>
            <label className={labelStyles}>Device Registry</label>
            <input
              type="text"
              value={config.device_registry || ''}
              onChange={(e) => updateField('device_registry', e.target.value)}
              disabled={disabled}
              placeholder="projects/my-project/locations/us-central1/registries/my-registry"
              className={inputStyles}
            />
          </div>
          <div>
            <label className={labelStyles}>Protocol</label>
            <select
              value={config.protocol || 'mqtt'}
              onChange={(e) => updateField('protocol', e.target.value)}
              disabled={disabled}
              className={selectStyles}
            >
              <option value="mqtt">MQTT</option>
              <option value="http">HTTP</option>
              <option value="coap">CoAP</option>
            </select>
          </div>
        </>
      )}

      {/* Time-series-specific */}
      {isTimeseries && (
        <>
          <div>
            <label className={labelStyles}>Metric Name</label>
            <input
              type="text"
              value={config.metric_name || ''}
              onChange={(e) => updateField('metric_name', e.target.value)}
              disabled={disabled}
              placeholder="cpu_usage, memory_percent, request_count"
              className={inputStyles}
            />
          </div>
          <div>
            <label className={labelStyles}>Aggregation Window</label>
            <select
              value={config.aggregation_window || '1h'}
              onChange={(e) => updateField('aggregation_window', e.target.value)}
              disabled={disabled}
              className={selectStyles}
            >
              <option value="1m">1 Minute</option>
              <option value="5m">5 Minutes</option>
              <option value="15m">15 Minutes</option>
              <option value="1h">1 Hour</option>
              <option value="1d">1 Day</option>
            </select>
          </div>
        </>
      )}

      {/* Geospatial-specific */}
      {isGeospatial && (
        <>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className={labelStyles}>Geometry Column</label>
              <input
                type="text"
                value={config.geometry_column || ''}
                onChange={(e) => updateField('geometry_column', e.target.value)}
                disabled={disabled}
                placeholder="geometry"
                className={inputStyles}
              />
            </div>
            <div>
              <label className={labelStyles}>Geometry Type</label>
              <select
                value={config.geometry_type || 'point'}
                onChange={(e) => updateField('geometry_type', e.target.value)}
                disabled={disabled}
                className={selectStyles}
              >
                <option value="point">Point</option>
                <option value="linestring">LineString</option>
                <option value="polygon">Polygon</option>
                <option value="multipolygon">MultiPolygon</option>
              </select>
            </div>
          </div>
          <div>
            <label className={labelStyles}>SRID (Spatial Reference ID)</label>
            <input
              type="number"
              value={config.srid || 4326}
              onChange={(e) => updateField('srid', parseInt(e.target.value))}
              disabled={disabled}
              placeholder="4326 (WGS 84)"
              className={inputStyles}
            />
            <p className="text-xs text-gray-500 mt-1">
              4326 = WGS 84 (GPS), 3857 = Web Mercator
            </p>
          </div>
        </>
      )}

      {/* ML Features-specific */}
      {isMLFeatures && (
        <>
          <div>
            <label className={labelStyles}>Feature Group</label>
            <input
              type="text"
              value={config.feature_group || ''}
              onChange={(e) => updateField('feature_group', e.target.value)}
              disabled={disabled}
              placeholder="customer_features"
              className={inputStyles}
            />
          </div>
          <div>
            <label className={labelStyles}>Entity Columns</label>
            <input
              type="text"
              value={config.entity_columns?.join(', ') || ''}
              onChange={(e) => updateField('entity_columns', e.target.value.split(',').map(s => s.trim()).filter(Boolean))}
              disabled={disabled}
              placeholder="customer_id, timestamp"
              className={inputStyles}
            />
            <p className="text-xs text-gray-500 mt-1">
              Comma-separated list of entity/key columns
            </p>
          </div>
        </>
      )}

      {/* Open Data-specific */}
      {isOpenData && (
        <>
          <div>
            <label className={labelStyles}>Dataset URL</label>
            <input
              type="url"
              value={config.dataset_url || ''}
              onChange={(e) => updateField('dataset_url', e.target.value)}
              disabled={disabled}
              placeholder="https://data.gov/api/dataset/..."
              className={inputStyles}
            />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className={labelStyles}>API Key (if required)</label>
              <input
                type="password"
                value={config.api_key || ''}
                onChange={(e) => updateField('api_key', e.target.value)}
                disabled={disabled}
                placeholder="Optional API key"
                className={inputStyles}
              />
            </div>
            <div>
              <label className={labelStyles}>Refresh Interval</label>
              <select
                value={config.refresh_interval || '@daily'}
                onChange={(e) => updateField('refresh_interval', e.target.value)}
                disabled={disabled}
                className={selectStyles}
              >
                <option value="@hourly">Hourly</option>
                <option value="@daily">Daily</option>
                <option value="@weekly">Weekly</option>
                <option value="@monthly">Monthly</option>
              </select>
            </div>
          </div>
        </>
      )}

      {/* Default fallback for unspecified types */}
      {!isIoT && !isTimeseries && !isGeospatial && !isMLFeatures && !isOpenData && (
        <div className="p-4 bg-gray-800/50 border border-gray-700 rounded-lg text-gray-400 text-sm">
          Select a specific source type to see configuration options.
        </div>
      )}
    </div>
  )
}
