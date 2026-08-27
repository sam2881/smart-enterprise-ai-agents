/**
 * Pipeline Components - Barrel exports
 *
 * Only exports components that are actively imported and used.
 *
 * Usage:
 *   import { UnifiedPipelineForm, PipelineProgress } from '@/components/pipeline'
 */

// Core form + progress
export { UnifiedPipelineForm } from './UnifiedPipelineForm'
export { PipelineProgress } from './PipelineProgress'

// Source selection & config
export { SourceTypeSelector } from './SourceTypeSelector'
export {
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

// Schema, transforms, target
export { SchemaDefinitionPanel } from './SchemaDefinitionPanel'
export { TransformationsPanel } from './TransformationsPanel'
export { TargetConfigurationPanel } from './TargetConfigurationPanel'

// NL + Gold zone
export { default as NLTransformInput } from './NLTransformInput'
export { GoldModelingSelector } from './GoldModelingSelector'
export { JoinDependencyBuilder } from './JoinDependencyBuilder'

// Re-export canonical types
export type {
  UnifiedPipelineInput,
  PipelineConfig,
  PipelineMetadata,
  SourceConfig,
  SchemaConfig,
  TargetConfig,
  TransformConfig,
  ExecutionPolicy,
  QualityRule,
  ExecutionRecord,
  SourceType,
  TargetZone,
  WriteMode,
  ProcessingMode,
  TransformType,
  Environment,
  InputType,
  ColumnDefinition,
  DataType,
  ContractType,
  PatternCode,
  PatternInfo,
  FeedType,
  GoldModelingStrategy,
  GoldZoneConfig,
  DataVault2Config,
  StarSchemaConfig,
  SnowflakeSchemaConfig,
  FlatTableConfig,
  JoinDependency,
  JiraTicketMetadata,
  V2FeedGroupConfig,
  V2FeedConfig,
  V2GoldModelConfig,
  V2HubConfig,
  V2SatelliteConfig,
  V2LinkConfig,
  V2DimensionConfig,
  V2FactConfig,
  V2ColumnDef,
  V2TransformRule,
  V2QualityRule,
  V2SourceType,
  V2GoldModelType,
} from '@/types/pipeline-canonical'

// Re-export constants
export { SOURCE_TYPE_CATEGORIES } from '@/types/pipeline-canonical'

// Re-export legacy types for backward compatibility (to be removed)
export type {
  PipelinePhase,
  PipelineListItem,
  CreatePipelineResponse,
} from '@/types/pipeline'
