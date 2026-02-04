/**
 * Pattern Validation Utilities
 *
 * Validates pipeline configuration against selected DAG pattern requirements.
 * Part of Phase 2 - UI Backend Gap Analysis Remediation
 */

import {
  PatternCode,
  ContractType,
  UnifiedPipelineInput,
  type PipelineConfig,
  type SourceConfig,
  type GoldZoneConfig,
} from '@/types/pipeline-canonical'

export interface ValidationResult {
  valid: boolean
  errors: string[]
  warnings: string[]
}

/**
 * Get contract type for a given pattern code
 */
export function getContractTypeForPattern(patternCode: PatternCode): ContractType {
  const mapping: Record<PatternCode, ContractType> = {
    P01: 'STANDARD',
    P02: 'STANDARD',
    P03: 'STANDARD',
    P04: 'STANDARD',
    P05: 'STANDARD',
    P06: 'STANDARD',
    P07: 'SCD2',
    P08: 'DATA_VAULT',
    P09: 'STAR_SCHEMA',
  }
  return mapping[patternCode]
}

/**
 * Validate pattern-specific requirements
 */
export function validatePatternConfig(input: UnifiedPipelineInput): ValidationResult {
  const errors: string[] = []
  const warnings: string[]  = []

  const { pipeline, source, target, gold_zone_config } = input
  const patternCode = pipeline?.pattern_code

  if (!patternCode) {
    errors.push('Pattern code is required')
    return { valid: false, errors, warnings }
  }

  // Validate based on pattern
  switch (patternCode) {
    case 'P01':
      // File Medallion - No special requirements
      break

    case 'P02':
      // Big Data File - Should use high-memory Spark config
      if (source?.file_config && !source.file_config.compression) {
        warnings.push('Consider enabling compression for large files')
      }
      break

    case 'P03':
      // Database Lakehouse - Requires connection and optional watermark
      if (source?.database_config && !source.database_config.connection_id) {
        errors.push('Database Lakehouse pattern requires connection_id')
      }
      if (source?.database_config?.extraction_mode === 'incremental' && !source.database_config.watermark_column) {
        warnings.push('Incremental extraction recommended to have watermark_column')
      }
      break

    case 'P04':
      // Legacy Migration - Requires legacy source type
      if (!source?.dtsx_config && !source?.ebcdic_config) {
        errors.push('Legacy Migration pattern requires dtsx_config or ebcdic_config')
      }
      break

    case 'P05':
      // Streaming Batch - Requires streaming config
      if (!source?.streaming_config) {
        errors.push('Streaming Batch pattern requires streaming_config')
      }
      if (source?.streaming_config && !source.streaming_config.kafka_topic && !source.streaming_config.pubsub_subscription) {
        errors.push('Streaming sources require kafka_topic or pubsub_subscription')
      }
      break

    case 'P06':
      // API SaaS - Requires API config
      if (!source?.api_config) {
        errors.push('API SaaS pattern requires api_config')
      }
      if (source?.api_config && !source.api_config.api_endpoint) {
        errors.push('API sources require api_endpoint')
      }
      break

    case 'P07':
      // SCD Type 2 - Requires business keys and tracked columns
      if (!pipeline?.business_keys || pipeline.business_keys.length === 0) {
        errors.push('SCD2 pattern requires business_keys (natural key columns)')
      }
      if (!pipeline?.tracked_columns || pipeline.tracked_columns.length === 0) {
        warnings.push('SCD2 pattern recommended to have tracked_columns (attributes to track changes)')
      }
      if (target?.write_mode && target.write_mode !== 'scd_type_2' && target.write_mode !== 'merge') {
        errors.push('SCD2 pattern requires write_mode to be "scd_type_2" or "merge"')
      }
      break

    case 'P08':
      // Data Vault - Requires Data Vault config
      if (!gold_zone_config || gold_zone_config.modeling_strategy !== 'data_vault_2') {
        errors.push('Data Vault pattern requires gold_zone_config with modeling_strategy="data_vault_2"')
      }
      if (gold_zone_config?.data_vault_config) {
        const { hubs, satellites } = gold_zone_config.data_vault_config
        if (!hubs || hubs.length === 0) {
          errors.push('Data Vault requires at least one Hub definition')
        }
        if (!satellites || satellites.length === 0) {
          warnings.push('Data Vault typically requires Satellite tables for descriptive attributes')
        }
      } else {
        errors.push('Data Vault pattern requires data_vault_config with hubs, links, and satellites')
      }
      break

    case 'P09':
      // Star Schema - Requires star schema config
      if (!gold_zone_config || gold_zone_config.modeling_strategy !== 'star_schema') {
        errors.push('Star Schema pattern requires gold_zone_config with modeling_strategy="star_schema"')
      }
      if (gold_zone_config?.star_schema_config) {
        const { fact_table, dimensions } = gold_zone_config.star_schema_config
        if (!fact_table) {
          errors.push('Star Schema requires fact table definition')
        }
        if (!dimensions || dimensions.length === 0) {
          errors.push('Star Schema requires at least one dimension table')
        }
        if (fact_table && (!fact_table.grain_columns || fact_table.grain_columns.length === 0)) {
          warnings.push('Fact table should define grain_columns for proper aggregation')
        }
      } else {
        errors.push('Star Schema pattern requires star_schema_config with fact_table and dimensions')
      }
      break

    default:
      warnings.push(`Unknown pattern code: ${patternCode}`)
  }

  // Validate feed_id and contract_id for APEX integration
  if (!pipeline?.feed_id) {
    warnings.push('feed_id is recommended for APEX registry tracking')
  }
  if (!pipeline?.contract_id) {
    warnings.push('contract_id is recommended for APEX execution tracking')
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  }
}

/**
 * Get required fields for a pattern
 */
export function getRequiredFieldsForPattern(patternCode: PatternCode): string[] {
  const requirements: Record<PatternCode, string[]> = {
    P01: ['feed_id', 'contract_id', 'dag_id', 'domain'],
    P02: ['feed_id', 'contract_id', 'dag_id', 'domain'],
    P03: ['feed_id', 'contract_id', 'dag_id', 'domain', 'source_connection_id'],
    P04: ['feed_id', 'contract_id', 'dag_id', 'domain', 'legacy_source_type'],
    P05: ['feed_id', 'contract_id', 'dag_id', 'domain', 'checkpoint_location'],
    P06: ['feed_id', 'contract_id', 'dag_id', 'domain', 'api_connection_id'],
    P07: ['feed_id', 'contract_id', 'dag_id', 'domain', 'business_keys', 'tracked_columns'],
    P08: ['feed_id', 'contract_id', 'dag_id', 'domain', 'hubs', 'links', 'satellites'],
    P09: ['feed_id', 'contract_id', 'dag_id', 'domain', 'fact_table', 'dimensions'],
  }
  return requirements[patternCode] || []
}

/**
 * Check if source type is compatible with pattern
 */
export function isSourceTypeCompatibleWithPattern(
  sourceType: string,
  patternCode: PatternCode
): boolean {
  const compatibility: Record<PatternCode, string[]> = {
    P01: ['file_'],  // All file types
    P02: ['file_parquet', 'file_orc', 'file_avro'],  // Big data formats
    P03: ['database_', 'nosql_'],  // All database types
    P04: ['legacy_', 'file_ebcdic'],  // Legacy types
    P05: ['streaming_'],  // All streaming types
    P06: ['api_', 'saas_'],  // All API types
    P07: ['file_', 'database_'],  // File or database
    P08: ['file_', 'database_'],  // File or database
    P09: ['file_', 'database_'],  // File or database
  }

  const patterns = compatibility[patternCode] || []
  return patterns.some(pattern => sourceType.startsWith(pattern))
}

/**
 * Get pattern recommendations based on source type
 */
export function recommendPatternForSource(sourceType: string): PatternCode[] {
  const recommendations: PatternCode[] = []

  if (sourceType.startsWith('file_')) {
    if (sourceType.includes('parquet') || sourceType.includes('orc')) {
      recommendations.push('P02')  // Big data file
    }
    recommendations.push('P01')  // File medallion
  }

  if (sourceType.startsWith('database_') || sourceType.startsWith('nosql_')) {
    recommendations.push('P03')  // Database lakehouse
  }

  if (sourceType.startsWith('legacy_') || sourceType === 'file_ebcdic') {
    recommendations.push('P04')  // Legacy migration
  }

  if (sourceType.startsWith('streaming_')) {
    recommendations.push('P05')  // Streaming batch
  }

  if (sourceType.startsWith('api_') || sourceType.startsWith('saas_')) {
    recommendations.push('P06')  // API SaaS
  }

  return recommendations
}
