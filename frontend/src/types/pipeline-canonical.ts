/**
 * Canonical Pipeline Types - TypeScript definitions matching backend canonical models
 *
 * These types mirror the Pydantic models in agents/data_agent/src/models/
 * 
 * IMPORTANT: These are the UNIFIED internal representations that:
 * 1. All UI input gets normalized to
 * 2. All NL input gets converted to  
 * 3. PostgreSQL stores and retrieves
 * 4. DAGs and Spark jobs read at runtime
 */

// =============================================================================
// Environment & Status Types
// =============================================================================

export type Environment = 'dev' | 'qa' | 'prod'

export type PipelineStatus = 
  | 'draft' 
  | 'pending_review' 
  | 'approved' 
  | 'active' 
  | 'paused' 
  | 'deprecated' 
  | 'archived'

export type PipelinePhase = 
  | 'init' 
  | 'planning' 
  | 'generating' 
  | 'validating' 
  | 'awaiting_approval' 
  | 'deploying' 
  | 'complete' 
  | 'failed'

export type InputType = 
  | 'ui_structured' 
  | 'natural_language' 
  | 'jira_ticket' 
  | 'dtsx_migration'

// =============================================================================
// Source Types - All 9 Categories (70+ types)
// =============================================================================

export type SourceType =
  // A. File-Based Sources
  | 'file_csv'
  | 'file_tsv'
  | 'file_txt'
  | 'file_json'
  | 'file_ndjson'
  | 'file_xml'
  | 'file_parquet'
  | 'file_avro'
  | 'file_orc'
  | 'file_excel'
  | 'file_fixed_width'
  | 'file_ebcdic'
  | 'file_zip'
  | 'file_gzip'
  // B. Database (RDBMS)
  | 'database_postgres'
  | 'database_mysql'
  | 'database_oracle'
  | 'database_sqlserver'
  | 'database_db2'
  | 'database_teradata'
  | 'database_snowflake'
  | 'database_bigquery'
  | 'database_sap'
  // C. Streaming Sources
  | 'streaming_kafka'
  | 'streaming_confluent'
  | 'streaming_kinesis'
  | 'streaming_pubsub'
  | 'streaming_eventhubs'
  // D. API / SaaS Sources
  | 'api_rest'
  | 'api_graphql'
  | 'api_soap'
  | 'saas_salesforce'
  | 'saas_servicenow'
  | 'saas_jira'
  | 'saas_workday'
  | 'saas_google_analytics'
  // E. Legacy / Enterprise Systems
  | 'legacy_dtsx'
  | 'legacy_cobol'
  | 'legacy_vsam'
  | 'legacy_as400'
  | 'legacy_mainframe'
  // F. NoSQL Databases
  | 'nosql_mongodb'
  | 'nosql_cassandra'
  | 'nosql_dynamodb'
  | 'nosql_redis'
  | 'nosql_couchbase'
  | 'nosql_neo4j'
  | 'nosql_elasticsearch'
  | 'nosql_solr'
  | 'nosql_hbase'
  // G. Semi-Structured / Nested
  | 'nested_json'
  | 'nested_xml'
  | 'nested_avro'
  | 'nested_parquet'
  // G. Unstructured / Observability
  | 'logs_application'
  | 'logs_airflow'
  | 'logs_spark'
  | 'logs_audit'
  | 'logs_metrics'
  // H. Cloud Storage / External Lakes
  | 'storage_s3'
  | 'storage_gcs'
  | 'storage_adls'
  | 'storage_external'
  // I. Special / Advanced
  | 'special_iot'
  | 'special_timeseries'
  | 'special_geospatial'
  | 'special_ml_features'
  | 'special_open_data'
  // Backward compatibility
  | 'dtsx'

export type SourceFormat =
  | 'csv'
  | 'json'
  | 'jsonl'
  | 'xml'
  | 'parquet'
  | 'avro'
  | 'orc'
  | 'excel'
  | 'ebcdic'
  | 'fixed_width'

export type ExtractionMode = 'full' | 'incremental' | 'cdc'

// =============================================================================
// Medallion Architecture
// =============================================================================

export type TargetZone = 'landing' | 'bronze' | 'silver' | 'gold'

export type WriteMode = 
  | 'append' 
  | 'overwrite' 
  | 'merge' 
  | 'scd_type_1' 
  | 'scd_type_2'

export type ProcessingMode = 'batch' | 'micro_batch' | 'streaming'

export type DestinationModel = 'flat' | 'dimensional' | 'data_vault' | 'obt'

export type TableFormat = 'parquet' | 'delta' | 'iceberg'

export type WindowType = 'tumbling' | 'sliding' | 'session'

// =============================================================================
// DAG Pattern & Contract Types (APEX Registry)
// =============================================================================

export type ContractType = 'STANDARD' | 'SCD2' | 'DATA_VAULT' | 'STAR_SCHEMA'

export type PatternCode =
  | 'P01'  // File Medallion Pipeline (STANDARD)
  | 'P02'  // Big Data File Pipeline (STANDARD)
  | 'P03'  // Database Lakehouse Pipeline (STANDARD)
  | 'P04'  // Legacy Migration Pipeline (STANDARD)
  | 'P05'  // Streaming Batch Pipeline (STANDARD)
  | 'P06'  // API SaaS Pipeline (STANDARD)
  | 'P07'  // SCD Type 2 Pipeline (SCD2)
  | 'P08'  // Data Vault Pipeline (DATA_VAULT)
  | 'P09'  // Star Schema Pipeline (STAR_SCHEMA)

export type FeedType = 'BATCH' | 'STREAMING' | 'HYBRID'

export interface PatternInfo {
  pattern_code: PatternCode
  pattern_name: string
  contract_type: ContractType
  description: string
  source_types_supported: SourceType[]
  load_types_supported: string[]
  zones: TargetZone[]
  selection_priority: number
  required_variables: string[]
  spark_jobs_used: string[]
}

// =============================================================================
// Data Types & Classifications
// =============================================================================

export type DataType = 
  | 'string' 
  | 'integer' 
  | 'bigint' 
  | 'float' 
  | 'double' 
  | 'decimal' 
  | 'boolean' 
  | 'date' 
  | 'timestamp' 
  | 'binary' 
  | 'array' 
  | 'struct' 
  | 'map'

export type PIIClassification = 'none' | 'pii' | 'phi' | 'pci' | 'sensitive'

// =============================================================================
// Transformation Types
// =============================================================================

export type TransformType =
  | 'rename'
  | 'cast'
  | 'derive'
  | 'filter'
  | 'aggregate'
  | 'join'
  | 'window'
  | 'scd'
  | 'deduplicate'
  | 'dedup'        // Alias for deduplicate
  | 'null_handling'
  | 'pivot'
  | 'unpivot'
  | 'mask'
  | 'hash'
  | 'encrypt'
  | 'sql'          // SQL expression transform
  | 'pyspark'      // PySpark code transform
  | 'nl_transform'

// =============================================================================
// Quality Rules
// =============================================================================

export type QualityRuleType =
  | 'not_null'
  | 'unique'
  | 'range'
  | 'regex'
  | 'referential'
  | 'freshness'
  | 'custom'
  | 'row_count'
  | 'completeness'

export type Severity = 'warning' | 'error' | 'critical'

// =============================================================================
// Core Configuration Interfaces
// =============================================================================

export interface PipelineConfig {
  dag_id: string
  pipeline_name: string
  domain: string
  product_code: string
  owner_team: string
  owner_email: string
  environment: Environment
  jira_ticket?: string
  description?: string

  // APEX Registry Integration (Phase 1 - Gap Analysis Remediation)
  feed_id?: string                    // APEX feed identifier
  contract_id?: string                // APEX contract identifier
  contract_type?: ContractType        // STANDARD | SCD2 | DATA_VAULT | STAR_SCHEMA
  pattern_code?: PatternCode          // DAG pattern selection (P01-P09)
  feed_type?: FeedType                // BATCH | STREAMING | HYBRID
  business_keys?: string[]            // For SCD2 and Data Vault patterns
  tracked_columns?: string[]          // For SCD2 history tracking
}

// =============================================================================
// Source Configurations
// =============================================================================

export interface FileSourceConfig {
  source_bucket: string
  source_prefix: string
  file_pattern?: string
  delimiter?: string
  has_header?: boolean
  encoding?: string
  quote_char?: string
  escape_char?: string
  null_value?: string
  compression?: string
}

export interface DatabaseSourceConfig {
  connection_id: string
  source_schema?: string
  source_table: string
  source_query?: string
  extraction_mode?: ExtractionMode
  watermark_column?: string
  batch_size?: number
}

export interface StreamingSourceConfig {
  kafka_bootstrap_servers?: string
  kafka_topic?: string
  kafka_consumer_group?: string
  kafka_offset_reset?: string
  pubsub_subscription?: string
  pubsub_project?: string
  message_format?: string
  schema_registry_url?: string
  // Windowing support
  window_type?: WindowType
  window_duration?: string
  slide_duration?: string
  watermark_delay?: string
  event_time_column?: string
  // Dead Letter Queue
  dlq_path?: string
}

export interface APISourceConfig {
  api_endpoint: string
  api_method?: string
  api_headers?: Record<string, string>
  api_auth_type?: string
  api_auth_secret: string
  pagination_type?: string
  pagination_key?: string
  rate_limit_rps?: number
  response_path?: string
}

export interface EBCDICSourceConfig {
  source_bucket: string
  source_prefix: string
  file_pattern?: string
  copybook_content?: string
  copybook_path?: string
  encoding?: string
  record_length?: number
  is_variable_length?: boolean
  rdw_format?: string
  field_specs?: Array<Record<string, any>>
}

export interface DTSXSourceConfig {
  dtsx_location: string
  dtsx_package_name: string
  dataflow_name?: string
  original_server?: string
  extracted_sources?: Array<Record<string, any>>
  extracted_transforms?: Array<Record<string, any>>
  extracted_destinations?: Array<Record<string, any>>
  column_mappings?: Array<Record<string, any>>
}

export interface NoSQLSourceConfig {
  connection_string: string
  // MongoDB-specific
  database_name?: string
  collection_name?: string
  query_filter?: string  // JSON query filter
  // Cassandra-specific
  keyspace?: string
  table_name?: string
  // ElasticSearch/Solr-specific
  index_name?: string
  search_query?: string
  // Neo4j-specific
  cypher_query?: string
  // Common
  extraction_mode?: ExtractionMode
  batch_size?: number
  read_preference?: string
}

export interface LogsSourceConfig {
  log_source: 'gcs' | 'stackdriver' | 'elasticsearch' | 'splunk' | 'datadog'
  log_path?: string
  log_pattern?: string  // Regex or grok pattern
  start_time?: string
  end_time?: string
  query?: string  // For log aggregation services
  project_id?: string  // For Stackdriver
  log_name?: string
  severity_filter?: string[]  // ERROR, WARNING, INFO, DEBUG
  parse_format?: 'json' | 'regex' | 'grok' | 'unstructured'
}

export interface NestedSourceConfig {
  max_depth?: number
  flatten_strategy: 'explode' | 'struct' | 'json_string'
  array_handling: 'explode' | 'collect' | 'first' | 'json_array'
  null_handling: 'keep' | 'remove' | 'default'
  schema_inference?: boolean
  schema_sample_size?: number
  preserve_paths?: boolean
}

export interface SpecialSourceConfig {
  source_category: 'iot' | 'timeseries' | 'geospatial' | 'ml_features' | 'open_data'
  // IoT-specific
  device_registry?: string
  protocol?: 'mqtt' | 'http' | 'coap'
  // Time-series-specific
  metric_name?: string
  aggregation_window?: string
  // Geospatial-specific
  srid?: number  // Spatial Reference ID
  geometry_column?: string
  geometry_type?: 'point' | 'polygon' | 'linestring' | 'multipolygon'
  // ML Features-specific
  feature_group?: string
  entity_columns?: string[]
  // Open Data-specific
  dataset_url?: string
  api_key?: string
  refresh_interval?: string
}

export interface SourceConfig {
  source_id?: string
  pipeline_id?: string
  source_type: SourceType
  source_format?: SourceFormat
  file_config?: Partial<FileSourceConfig>
  database_config?: Partial<DatabaseSourceConfig>
  nosql_config?: Partial<NoSQLSourceConfig>
  streaming_config?: Partial<StreamingSourceConfig>
  api_config?: Partial<APISourceConfig>
  ebcdic_config?: Partial<EBCDICSourceConfig>
  dtsx_config?: Partial<DTSXSourceConfig>
  logs_config?: Partial<LogsSourceConfig>
  nested_config?: Partial<NestedSourceConfig>
  special_config?: Partial<SpecialSourceConfig>
  field_specs?: Array<Record<string, any>>
  copybook?: Record<string, any>
  record_length?: number
}

// =============================================================================
// Schema Configuration
// =============================================================================

export interface ColumnDefinition {
  name: string
  type: DataType
  nullable?: boolean
  pii?: PIIClassification
  pk?: boolean
}

export interface SchemaConfig {
  columns: ColumnDefinition[]
  primary_keys?: string[]
}

// =============================================================================
// Target Configuration
// =============================================================================

export interface TargetConfig {
  target_id?: string
  pipeline_id?: string
  target_zone: TargetZone
  bq_project?: string
  bq_dataset: string
  bq_table: string
  bq_location?: string
  write_mode: WriteMode
  merge_keys?: string[]
  destination_model?: DestinationModel
  hub_table?: string
  sat_table?: string
  link_tables?: string[]
  scd_effective_from?: string
  scd_effective_to?: string
  scd_current_flag?: string
  scd_tracked_columns?: string[]
  partition_field?: string
  partition_type?: string
  clustering_fields?: string[]
  // Table format
  table_format?: TableFormat
  table_maintenance?: {
    vacuum_hours?: number
    optimize_schedule?: string
    z_order_columns?: string[]
    retention_days?: number
  }
  additional_targets?: Array<Record<string, any>>
}

// =============================================================================
// Transformation Configurations
// =============================================================================

export interface RenameConfig {
  source: string
  target: string
}

export interface CastConfig {
  column: string
  cast_type: string
  format?: string
}

export interface DeriveConfig {
  column: string
  expression: string
  dependencies?: string[]
}

export interface FilterConfig {
  condition: string
}

export interface AggregateConfig {
  group_by: string[]
  aggregations: Array<{ column: string; function: string; alias: string }>
}

export interface JoinConfig {
  left_table: string
  right_table: string
  join_type?: string
  join_keys: Array<{ left: string; right: string }>
  select_columns?: string[]
}

export interface WindowConfig {
  partition_by?: string[]
  order_by: Array<{ column: string; direction: string }>
  functions: Array<{ type: string; alias: string }>
}

export interface SCDConfig {
  type?: number
  tracked_columns: string[]
  effective_from?: string
  effective_to?: string
  current_flag?: string
  business_keys: string[]
}

export interface DeduplicateConfig {
  partition_by: string[]
  order_by: Array<{ column: string; direction: string }>
}

export interface MaskConfig {
  column: string
  mask_type?: string
  visible_chars?: number
  mask_char?: string
}

export interface NLTransformInput {
  description: string
  source_tables?: string[]
  target_columns?: string[]
  generated_code?: string
  generated_config?: Record<string, any>
  confidence_score?: number
}

export interface TransformConfig {
  transform_id?: string
  pipeline_id?: string
  transform_type: TransformType
  transform_order?: number
  zone?: TargetZone
  config: Record<string, any>
  nl_description?: string
  generated_pyspark?: string
  generated_sql?: string
  is_active?: boolean
  created_at?: string
  updated_at?: string
}

// =============================================================================
// Quality Rules
// =============================================================================

export interface QualityRule {
  rule_id?: string
  pipeline_id?: string
  rule_name: string
  rule_type: QualityRuleType
  column_name?: string
  config: Record<string, any>
  severity: Severity
  threshold_pct?: number
  is_active?: boolean
}

// =============================================================================
// Execution Policy
// =============================================================================

export interface ExecutionPolicy {
  policy_id?: string
  pipeline_id?: string
  schedule_interval?: string
  start_date?: string
  end_date?: string
  catchup?: boolean
  processing_mode: ProcessingMode
  max_active_runs?: number
  retry_count?: number
  retry_delay_minutes?: number
  timeout_hours?: number
  sla_hours?: number
  requires_human_approval?: boolean
  approval_groups?: string[]
  alert_emails?: string[]
  slack_webhook?: string
  // Cost optimization
  cost_labels?: Record<string, string>
  preemptible_ratio?: number
  autoscaling_policy?: {
    min_workers?: number
    max_workers?: number
    scale_up_factor?: number
    cooldown_period_seconds?: number
  }
  created_at?: string
  updated_at?: string
}

// =============================================================================
// Unified Pipeline Input
// =============================================================================

// =============================================================================
// File References (for external schema/config files)
// =============================================================================

export interface FileReferences {
  copybook_path?: string        // GCS path to COBOL copybook
  dtsx_path?: string            // GCS path to SSIS package
  schema_file_path?: string     // GCS path to schema file (JSON/YAML/Avro)
  sample_data_path?: string     // GCS path to sample data
  config_file_path?: string     // GCS path to config file
}

// =============================================================================
// NL Instructions (for medallion layer natural language inputs)
// =============================================================================

export interface NLInstructions {
  identity?: string             // NL for pipeline identity
  source?: string               // NL for source configuration
  schema?: string               // NL for schema definition
  target?: string               // NL for target configuration
  execution?: string            // NL for execution policy
  layer_bronze?: string         // NL for bronze layer processing
  layer_silver?: string         // NL for silver layer processing
  layer_gold?: string           // NL for gold layer processing
}

export interface UnifiedPipelineInput {
  input_type?: InputType
  request_id?: string
  created_by: string
  jira_ticket?: string

  // Option 1: Structured UI Input (preferred)
  pipeline?: Partial<PipelineConfig>
  source?: Partial<SourceConfig>
  schema?: Partial<SchemaConfig>
  target?: Partial<TargetConfig>
  transformations?: Array<Partial<TransformConfig>>
  quality_rules?: Array<Partial<QualityRule>>
  execution_policy?: Partial<ExecutionPolicy>

  // Gold Zone Modeling (Section 7 of Master System Prompt)
  gold_zone_config?: Partial<GoldZoneConfig>

  // File References (external files for agent to process)
  file_references?: FileReferences

  // Natural Language Instructions (per section and medallion layer)
  nl_instructions?: NLInstructions

  // Option 2: Natural Language Input (single field for entire pipeline)
  natural_language?: string

  // Option 3: DTSX Migration Input
  dtsx_path?: string
  dtsx_package_name?: string
}

// =============================================================================
// Canonical Pipeline Metadata
// =============================================================================

export interface PipelineMetadata {
  pipeline: PipelineConfig
  source: SourceConfig
  schema: SchemaConfig
  target: TargetConfig
  transformations: TransformConfig[]
  quality_rules: QualityRule[]
  execution_policy: ExecutionPolicy
  metadata_version?: string
  created_at?: string
  updated_at?: string
  nl_input?: string
  nl_processing_log?: Array<Record<string, any>>
}

// =============================================================================
// Runtime & Execution
// =============================================================================

export interface RuntimeConfig {
  dag_id: string
  environment: string
  pipeline: Record<string, any>
  source: Record<string, any>
  schema: Record<string, any>
  target: Record<string, any>
  transformations: Array<Record<string, any>>
  quality_rules: Array<Record<string, any>>
  execution_policy: Record<string, any>
}

export interface ExecutionRecord {
  execution_id?: string
  pipeline_id?: string
  dag_run_id: string
  execution_date: string
  start_time: string
  end_time?: string
  duration_seconds?: number
  status: string
  records_read?: number
  records_written?: number
  records_failed?: number
  bytes_processed?: number
  quality_score?: number
  quality_passed?: boolean
  error_message?: string
  error_task?: string
  triggered_by?: string
  airflow_log_url?: string
}

export interface PipelineEvent {
  event_id?: string
  dag_id: string
  execution_date?: string
  event_type: string
  event_data?: Record<string, any>
  jira_ticket?: string
  environment?: string
  created_at?: string
}

// =============================================================================
// Source Type Categories (for UI)
// =============================================================================

export interface SourceTypeCategory {
  id: string
  label: string
  icon: string
  description: string
  types: Array<{
    value: SourceType
    label: string
    description: string
  }>
}

export const SOURCE_TYPE_CATEGORIES: SourceTypeCategory[] = [
  {
    id: 'file',
    label: 'File-Based Sources',
    icon: '📄',
    description: 'CSV, JSON, Parquet, Excel, EBCDIC, and other file formats',
    types: [
      { value: 'file_csv', label: 'CSV', description: 'Comma-separated values' },
      { value: 'file_tsv', label: 'TSV', description: 'Tab-separated values' },
      { value: 'file_json', label: 'JSON', description: 'JavaScript Object Notation' },
      { value: 'file_ndjson', label: 'NDJSON', description: 'Newline-delimited JSON' },
      { value: 'file_parquet', label: 'Parquet', description: 'Columnar storage format' },
      { value: 'file_avro', label: 'Avro', description: 'Binary serialization format' },
      { value: 'file_orc', label: 'ORC', description: 'Optimized Row Columnar' },
      { value: 'file_excel', label: 'Excel', description: 'Microsoft Excel files' },
      { value: 'file_xml', label: 'XML', description: 'Extensible Markup Language' },
      { value: 'file_fixed_width', label: 'Fixed Width', description: 'Fixed-width format files' },
      { value: 'file_ebcdic', label: 'EBCDIC', description: 'Mainframe EBCDIC files' },
    ],
  },
  {
    id: 'database',
    label: 'Database (RDBMS)',
    icon: '🗄️',
    description: 'PostgreSQL, MySQL, Oracle, Snowflake, SAP, and other databases',
    types: [
      { value: 'database_postgres', label: 'PostgreSQL', description: 'PostgreSQL database' },
      { value: 'database_mysql', label: 'MySQL', description: 'MySQL database' },
      { value: 'database_oracle', label: 'Oracle', description: 'Oracle database' },
      { value: 'database_sqlserver', label: 'SQL Server', description: 'Microsoft SQL Server' },
      { value: 'database_snowflake', label: 'Snowflake', description: 'Snowflake data warehouse' },
      { value: 'database_bigquery', label: 'BigQuery', description: 'Google BigQuery' },
      { value: 'database_teradata', label: 'Teradata', description: 'Teradata database' },
      { value: 'database_db2', label: 'DB2', description: 'IBM DB2 database' },
      { value: 'database_sap', label: 'SAP', description: 'SAP systems' },
    ],
  },
  {
    id: 'streaming',
    label: 'Streaming Sources',
    icon: '🌊',
    description: 'Kafka, Pub/Sub, Kinesis, and other streaming platforms',
    types: [
      { value: 'streaming_kafka', label: 'Kafka', description: 'Apache Kafka' },
      { value: 'streaming_confluent', label: 'Confluent', description: 'Confluent Platform' },
      { value: 'streaming_pubsub', label: 'Pub/Sub', description: 'Google Cloud Pub/Sub' },
      { value: 'streaming_kinesis', label: 'Kinesis', description: 'AWS Kinesis' },
      { value: 'streaming_eventhubs', label: 'Event Hubs', description: 'Azure Event Hubs' },
    ],
  },
  {
    id: 'api',
    label: 'API / SaaS Sources',
    icon: '🔌',
    description: 'REST APIs, Salesforce, Jira, Workday, and other SaaS platforms',
    types: [
      { value: 'api_rest', label: 'REST API', description: 'RESTful API endpoints' },
      { value: 'api_graphql', label: 'GraphQL', description: 'GraphQL API' },
      { value: 'api_soap', label: 'SOAP', description: 'SOAP web services' },
      { value: 'saas_salesforce', label: 'Salesforce', description: 'Salesforce CRM' },
      { value: 'saas_servicenow', label: 'ServiceNow', description: 'ServiceNow platform' },
      { value: 'saas_jira', label: 'Jira', description: 'Atlassian Jira' },
      { value: 'saas_workday', label: 'Workday', description: 'Workday HCM/Finance' },
      { value: 'saas_google_analytics', label: 'Google Analytics', description: 'Google Analytics' },
    ],
  },
  {
    id: 'legacy',
    label: 'Legacy / Enterprise',
    icon: '🏛️',
    description: 'SSIS/DTSX, COBOL, VSAM, AS/400, and mainframe systems',
    types: [
      { value: 'legacy_dtsx', label: 'SSIS/DTSX', description: 'SQL Server Integration Services' },
      { value: 'legacy_cobol', label: 'COBOL', description: 'COBOL applications' },
      { value: 'legacy_vsam', label: 'VSAM', description: 'Virtual Storage Access Method' },
      { value: 'legacy_as400', label: 'AS/400', description: 'IBM AS/400 systems' },
      { value: 'legacy_mainframe', label: 'Mainframe', description: 'Mainframe systems' },
    ],
  },
  {
    id: 'nosql',
    label: 'NoSQL Databases',
    icon: '🗄️',
    description: 'Document, key-value, graph, and search databases',
    types: [
      { value: 'nosql_mongodb', label: 'MongoDB', description: 'Document database' },
      { value: 'nosql_cassandra', label: 'Cassandra', description: 'Wide-column store' },
      { value: 'nosql_dynamodb', label: 'DynamoDB', description: 'AWS key-value store' },
      { value: 'nosql_redis', label: 'Redis', description: 'In-memory data store' },
      { value: 'nosql_couchbase', label: 'Couchbase', description: 'JSON document database' },
      { value: 'nosql_neo4j', label: 'Neo4j', description: 'Graph database' },
      { value: 'nosql_elasticsearch', label: 'Elasticsearch', description: 'Search & analytics engine' },
      { value: 'nosql_solr', label: 'Solr', description: 'Apache Solr search' },
      { value: 'nosql_hbase', label: 'HBase', description: 'Hadoop database' },
    ],
  },
  {
    id: 'nested',
    label: 'Semi-Structured / Nested',
    icon: '📦',
    description: 'Nested JSON, XML, Avro with complex schemas',
    types: [
      { value: 'nested_json', label: 'Nested JSON', description: 'Hierarchical JSON structures' },
      { value: 'nested_xml', label: 'Nested XML', description: 'Complex XML documents' },
      { value: 'nested_avro', label: 'Nested Avro', description: 'Avro with nested schemas' },
      { value: 'nested_parquet', label: 'Nested Parquet', description: 'Parquet with nested types' },
    ],
  },
  {
    id: 'logs',
    label: 'Logs / Observability',
    icon: '📊',
    description: 'Application logs, Airflow logs, Spark logs, audit trails',
    types: [
      { value: 'logs_application', label: 'Application Logs', description: 'Application log files' },
      { value: 'logs_airflow', label: 'Airflow Logs', description: 'Apache Airflow logs' },
      { value: 'logs_spark', label: 'Spark Logs', description: 'Apache Spark logs' },
      { value: 'logs_audit', label: 'Audit Logs', description: 'System audit trails' },
      { value: 'logs_metrics', label: 'Metrics', description: 'System metrics data' },
    ],
  },
  {
    id: 'storage',
    label: 'Cloud Storage',
    icon: '☁️',
    description: 'S3, GCS, ADLS, and external data lakes',
    types: [
      { value: 'storage_s3', label: 'Amazon S3', description: 'AWS S3 buckets' },
      { value: 'storage_gcs', label: 'Google Cloud Storage', description: 'GCS buckets' },
      { value: 'storage_adls', label: 'Azure Data Lake', description: 'Azure Data Lake Storage' },
      { value: 'storage_external', label: 'External Storage', description: 'Other cloud storage' },
    ],
  },
  {
    id: 'special',
    label: 'Special / Advanced',
    icon: '⚡',
    description: 'IoT, Time-series, Geospatial, ML features, Open Data',
    types: [
      { value: 'special_iot', label: 'IoT Devices', description: 'Internet of Things data' },
      { value: 'special_timeseries', label: 'Time-series', description: 'Time-series databases' },
      { value: 'special_geospatial', label: 'Geospatial', description: 'Geographic/spatial data' },
      { value: 'special_ml_features', label: 'ML Features', description: 'Machine learning features' },
      { value: 'special_open_data', label: 'Open Data', description: 'Public open datasets' },
    ],
  },
]

// =============================================================================
// Gold Zone Modeling - Enterprise Data Modeling Patterns
// =============================================================================

/**
 * Gold Zone Modeling Strategy
 *
 * At Gold layer, user must explicitly choose a modeling pattern:
 * - data_vault_2: For long-term historization, audit trails, business key stability
 * - star_schema: For BI/analytics performance, reporting use cases
 * - snowflake_schema: For normalized dimensions, deep hierarchies
 * - flat_table: Restricted use - only for extracts or ad-hoc reporting
 */
export type GoldModelingStrategy =
  | 'data_vault_2'
  | 'star_schema'
  | 'snowflake_schema'
  | 'flat_table'

/**
 * Data Vault 2.0 Configuration
 *
 * Use when:
 * - Long-term historization is required
 * - Business keys are stable
 * - Audit & traceability are mandatory
 */
export interface DataVault2Config {
  // Hub Configuration (business keys)
  hubs: Array<{
    hub_name: string
    business_keys: string[]
    source_system: string
    load_date_column?: string
    record_source_column?: string
    hash_key_column?: string
  }>

  // Link Configuration (relationships)
  links: Array<{
    link_name: string
    hub_references: Array<{
      hub_name: string
      foreign_key: string
    }>
    link_hash_key_column?: string
    load_date_column?: string
    record_source_column?: string
  }>

  // Satellite Configuration (descriptive attributes)
  satellites: Array<{
    satellite_name: string
    parent_hub_or_link: string
    descriptive_attributes: string[]
    hash_diff_column?: string
    load_date_column?: string
    load_end_date_column?: string
    record_source_column?: string
    is_current_flag_column?: string
  }>

  // Hash key generation
  hash_algorithm?: 'md5' | 'sha1' | 'sha256'
  concatenation_delimiter?: string
}

/**
 * Star Schema Configuration
 *
 * Use when:
 * - Analytics & BI performance is primary
 * - Reporting use cases dominate
 */
export interface StarSchemaConfig {
  // Fact Table Configuration
  fact_table: {
    table_name: string
    grain_description: string
    grain_columns: string[]
    measures: Array<{
      column_name: string
      aggregation_type: 'sum' | 'count' | 'avg' | 'min' | 'max' | 'distinct_count'
      description?: string
    }>
    degenerate_dimensions?: string[]
    foreign_keys: Array<{
      column_name: string
      dimension_table: string
    }>
    partition_column?: string
    partition_type?: 'DAY' | 'MONTH' | 'YEAR'
  }

  // Dimension Tables Configuration
  dimensions: Array<{
    table_name: string
    dimension_type: 'conformed' | 'role_playing' | 'junk' | 'degenerate' | 'outrigger'
    scd_type: 1 | 2 | 3 | 4 | 6
    natural_key: string[]
    surrogate_key_column?: string
    attributes: Array<{
      column_name: string
      description?: string
      is_hierarchy?: boolean
      hierarchy_level?: number
    }>
    // SCD Type 2 specific
    effective_start_column?: string
    effective_end_column?: string
    current_flag_column?: string
    // SCD Type 3 specific
    previous_value_columns?: string[]
  }>

  // Conformed Dimensions (shared across fact tables)
  conformed_dimension_refs?: string[]
}

/**
 * Snowflake Schema Configuration
 *
 * Use when:
 * - Dimension normalization is required
 * - Hierarchies are deep and shared
 */
export interface SnowflakeSchemaConfig extends StarSchemaConfig {
  // Normalized dimension tables
  normalized_dimensions: Array<{
    parent_dimension: string
    child_table_name: string
    relationship_key: string
    attributes: string[]
    hierarchy_path?: string[]
  }>
}

/**
 * Flat/Reporting Table Configuration
 *
 * Restricted use:
 * - Only for extracts or ad-hoc reporting
 */
export interface FlatTableConfig {
  table_name: string
  description: string
  columns: string[]
  denormalized_from?: string[]
  refresh_strategy: 'full' | 'incremental' | 'snapshot'
  retention_days?: number
}

/**
 * Join Dependency Configuration
 *
 * Supports:
 * A. Join with Existing Tables - resolved from metadata
 * B. Join with Future Tables - placeholder contracts
 */
export interface JoinDependency {
  dependency_id?: string
  join_type: 'inner' | 'left' | 'right' | 'full' | 'cross'

  // Left side (current pipeline)
  left_entity: string
  left_keys: string[]

  // Right side (dependency)
  right_entity: {
    table_name: string
    // If existing table
    existing?: {
      dataset: string
      table: string
      resolved_from_metadata: boolean
    }
    // If future table (common in migration)
    future?: {
      expected_pipeline_id: string
      expected_availability_date?: string
      placeholder_contract: boolean
    }
  }
  right_keys: string[]

  // Join metadata
  expected_grain: string
  cardinality: '1-1' | '1-N' | 'N-1' | 'N-N'
  data_freshness_requirement?: 'real-time' | 'hourly' | 'daily' | 'weekly'

  // Select columns from right side
  select_columns?: string[]

  // Validation
  grain_validated?: boolean
  fanout_risk?: boolean
}

/**
 * Complete Gold Zone Configuration
 */
export interface GoldZoneConfig {
  modeling_strategy: GoldModelingStrategy

  // Strategy-specific configurations (Partial to support incremental form building)
  data_vault_config?: Partial<DataVault2Config>
  star_schema_config?: Partial<StarSchemaConfig>
  snowflake_schema_config?: Partial<SnowflakeSchemaConfig>
  flat_table_config?: Partial<FlatTableConfig>

  // Join dependencies
  join_dependencies?: JoinDependency[]

  // Grain definition
  grain_definition?: {
    description: string
    grain_columns: string[]
    uniqueness_enforced: boolean
  }

  // Lineage tracking
  source_pipelines?: string[]
  dependent_pipelines?: string[]
}

// =============================================================================
// Jira Ticket Metadata (for Jira-Driven Pipeline Creation)
// =============================================================================

export type JiraTicketType =
  | 'migration'
  | 'enhancement'
  | 'new_feed'
  | 'fix'
  | 'refactor'
  | 'deprecation'

export type JiraTicketStatus =
  | 'open'
  | 'in_progress'
  | 'in_review'
  | 'approved'
  | 'deployed'
  | 'closed'
  | 'blocked'

export interface JiraTicketMetadata {
  ticket_id: string
  ticket_type: JiraTicketType
  summary: string
  detailed_description: string
  source_systems: string[]
  target_domain: string
  priority: 'low' | 'medium' | 'high' | 'critical'
  status: JiraTicketStatus

  // Pipeline context
  pipeline_ids?: string[]
  migration_scope?: {
    legacy_artifacts: string[]
    estimated_tables: number
    estimated_transforms: number
  }

  // Audit
  created_by: string
  assigned_to?: string
  created_timestamp: string
  updated_timestamp: string

  // Business context
  business_justification?: string
  stakeholders?: string[]
  target_go_live_date?: string
}

// =============================================================================
// V2 Feed Group Config (mirrors agents/data_agent/src/models/feed_config.py)
// =============================================================================

export type V2SourceType = 'gcs_file' | 'jdbc' | 'api' | 'streaming' | 'legacy_ebcdic'
export type V2FileFormat = 'csv' | 'json' | 'parquet' | 'avro' | 'ebcdic' | 'fixed_width' | 'orc' | 'xml'
export type V2LoadType = 'full' | 'incremental' | 'merge' | 'append' | 'cdc'
export type V2SqlLoadType = 'insert' | 'merge' | 'delete_insert'
export type V2GoldModelType = 'flat' | 'scd2' | 'data_vault' | 'star_schema'
export type V2SCDType = 0 | 1 | 2 | 3

export type V2DataType =
  | 'STRING' | 'INTEGER' | 'BIGINT' | 'FLOAT' | 'DOUBLE' | 'DECIMAL'
  | 'BOOLEAN' | 'DATE' | 'TIMESTAMP' | 'BINARY' | 'ARRAY' | 'STRUCT' | 'MAP'

export type V2PiiClassification = 'PII' | 'PHI' | 'PCI' | 'SENSITIVE'

export interface V2ColumnDef {
  name: string
  data_type: V2DataType
  nullable?: boolean
  precision?: number
  scale?: number
  pii_classification?: V2PiiClassification
  description?: string
  default_value?: string
}

export type V2TransformType =
  | 'rename' | 'cast' | 'derive' | 'filter' | 'dedup' | 'null_fill'
  | 'mask' | 'hash' | 'trim' | 'standardize_date' | 'regex_replace'
  | 'window' | 'aggregate' | 'join' | 'pivot' | 'unpivot'

export interface V2TransformRule {
  type: V2TransformType
  config: Record<string, any>
}

export type V2QualityRuleType =
  | 'not_null' | 'unique' | 'range' | 'regex' | 'in_set'
  | 'row_count' | 'freshness' | 'custom_sql' | 'compound_unique'
  | 'column_pair_comparison' | 'completeness'

export type V2QualitySeverity = 'INFO' | 'WARNING' | 'ERROR' | 'CRITICAL'

export interface V2QualityRule {
  column?: string
  rule_type: V2QualityRuleType
  params: Record<string, any>
  severity?: V2QualitySeverity
}

export interface V2SnowflakeTarget {
  account: string
  database: string
  schema_name: string
  table: string
  warehouse: string
  role?: string
}

export type V2PartitionType = 'DAY' | 'MONTH' | 'YEAR' | 'HOUR'

export interface V2BigQueryTarget {
  project?: string
  dataset: string
  table: string
  partition_field?: string
  partition_type?: V2PartitionType
  clustering_fields?: string[]
}

export interface V2HubConfig {
  hub_name: string
  business_keys: string[]
  target_table: string
}

export interface V2SatelliteConfig {
  satellite_name: string
  hub_reference: string
  descriptive_columns: string[]
  target_table: string
}

export interface V2LinkConfig {
  link_name: string
  hub_references: string[]
  target_table: string
}

export interface V2DimensionConfig {
  dimension_name: string
  natural_key: string
  scd_type: V2SCDType
  columns: string[]
  target_table: string
}

export type V2AggFunction = 'sum' | 'count' | 'avg' | 'min' | 'max' | 'count_distinct'

export interface V2MeasureConfig {
  name: string
  source_column: string
  agg_function?: V2AggFunction
  is_additive?: boolean
}

export interface V2DimensionLookup {
  dimension_name: string
  join_key: string
  surrogate_key?: string
}

export interface V2FactConfig {
  fact_name: string
  grain_columns: string[]
  measures: V2MeasureConfig[]
  dimension_lookups: V2DimensionLookup[]
  target_table: string
  date_dimension_key?: string
}

export interface V2GoldModelConfig {
  model_type: V2GoldModelType
  hubs?: V2HubConfig[]
  satellites?: V2SatelliteConfig[]
  links?: V2LinkConfig[]
  dimensions?: V2DimensionConfig[]
  facts?: V2FactConfig[]
}

export interface V2DagDependency {
  upstream_dag_id: string
  upstream_task_id?: string
  timeout_seconds?: number
}

export interface V2FeedConfig {
  feed_id: string
  feed_name: string
  domain: string
  source_type: V2SourceType
  source_connection_id: string
  source_config: Record<string, any>
  file_format?: V2FileFormat
  copybook_path?: string
  header_rows?: number
  footer_rows?: number
  encoding?: string
  schema_version?: number
  columns: V2ColumnDef[]
  business_keys: string[]
  partition_keys?: string[]
  watermark_column?: string
  load_type: V2LoadType
  target_dataset: string
  target_table: string
  target_project?: string
  write_disposition?: string
  snowflake_target?: V2SnowflakeTarget
  zones?: string[]
  zone_transforms?: Record<string, V2TransformRule[]>
  gold_model?: V2GoldModelConfig
  quality_rules?: V2QualityRule[]
  schedule?: Record<string, string>
  notification_emails?: Record<string, string[]>
  retry_count?: number
  timeout_hours?: number
  sql_load_type?: V2SqlLoadType
}

export interface V2FeedGroupConfig {
  feed_group_id: string
  dag_id: string
  domain: string
  owner?: string
  description?: string
  feeds: V2FeedConfig[]
  consumption_feeds?: V2FeedConfig[]
  enable_duplicate_detection?: boolean
  enable_self_retrigger?: boolean
  dependencies?: V2DagDependency[]
  schedule?: Record<string, string>
  notification_emails?: Record<string, string[]>
  retry_count?: number
  retry_delay_minutes?: number
  timeout_hours?: number
  max_active_runs?: number
  catchup?: boolean
  start_year?: number
  start_month?: number
  start_day?: number
  tags?: string[]
}

// =============================================================================
// Legacy Type Aliases (for backward compatibility)
// =============================================================================

export type PipelineIdentity = PipelineConfig
export type PipelineIntent = UnifiedPipelineInput
