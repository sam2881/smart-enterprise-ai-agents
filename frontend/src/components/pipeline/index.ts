/**
 * Pipeline Components - Barrel exports
 *
 * This file exports all pipeline-related components for easy importing.
 *
 * Usage:
 *   import { UnifiedPipelineForm, PipelineProgress, NLTransformInput } from '@/components/pipeline'
 */

export { UnifiedPipelineForm } from './UnifiedPipelineForm'
export { PipelineProgress } from './PipelineProgress'
export { default as NLTransformInput } from './NLTransformInput'
export { default as DTSXMigrationForm } from './DTSXMigrationForm'
export { SourceTypeSelector } from './SourceTypeSelector'
export {
  FileSourceConfigForm,
  DatabaseSourceConfigForm,
  StreamingSourceConfigForm,
  APISourceConfigForm,
  EBCDICSourceConfigForm,
  DTSXSourceConfigForm,
} from './SourceConfigForms'
export { GoldModelingSelector } from './GoldModelingSelector'
export { JoinDependencyBuilder } from './JoinDependencyBuilder'
export { SchemaInputPanel } from './SchemaInputPanel'
export { ZoneIntentPanel } from './ZoneIntentPanel'
export { ExecutionPolicyPanel } from './ExecutionPolicyPanel'
export { GoldOperationalConfigPanel } from './GoldOperationalConfig'
export { PatternSelector } from './PatternSelector'
export { NestedSourceConfigForm } from './NestedSourceConfigForm'
export { SpecialSourceConfigForm } from './SpecialSourceConfigForm'

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
  // Pattern & Contract types (Phase 1 - Gap Analysis)
  ContractType,
  PatternCode,
  PatternInfo,
  FeedType,
  // Gold Zone types
  GoldModelingStrategy,
  GoldZoneConfig,
  DataVault2Config,
  StarSchemaConfig,
  SnowflakeSchemaConfig,
  FlatTableConfig,
  JoinDependency,
  JiraTicketMetadata,
  // V2 Feed Group Config types
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
