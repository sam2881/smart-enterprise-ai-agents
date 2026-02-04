/**
 * PatternSelector Component
 *
 * Allows users to explicitly select DAG pattern (P01-P09) and contract type.
 * Filters available patterns based on selected source type and provides recommendations.
 *
 * Part of Phase 1 - UI Backend Gap Analysis Remediation
 */

import React, { useState, useEffect } from 'react'
import { PatternCode, PatternInfo, SourceType, ContractType } from '@/types/pipeline-canonical'
import { Badge } from '@/components/ui/Badge'
import { Card } from '@/components/ui/Card'

interface PatternSelectorProps {
  sourceType?: SourceType
  selectedPattern?: PatternCode
  onChange: (pattern: PatternCode, contractType: ContractType) => void
  className?: string
}

// Mock pattern data - replace with API call in production
const PATTERN_REGISTRY: PatternInfo[] = [
  {
    pattern_code: 'P01',
    pattern_name: 'File Medallion Pipeline',
    contract_type: 'STANDARD',
    description: 'Standard file ingestion with full medallion architecture (Bronze → Silver → Gold)',
    source_types_supported: ['file_csv', 'file_parquet', 'file_json', 'file_avro', 'file_orc', 'file_xml', 'file_excel', 'file_fixed_width'],
    load_types_supported: ['FULL', 'APPEND'],
    zones: ['bronze', 'silver', 'gold'],
    selection_priority: 100,
    required_variables: ['feed_id', 'contract_id', 'dag_id', 'domain', 'start_date'],
    spark_jobs_used: ['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_SEMANTIC_VALIDATION', 'SILVER_TO_GOLD']
  },
  {
    pattern_code: 'P02',
    pattern_name: 'Big Data File Pipeline',
    contract_type: 'STANDARD',
    description: 'Large file processing (>10GB) with partitioned reads and dynamic cluster scaling',
    source_types_supported: ['file_parquet', 'file_orc', 'file_avro'],
    load_types_supported: ['FULL', 'APPEND'],
    zones: ['bronze', 'silver', 'gold'],
    selection_priority: 90,
    required_variables: ['feed_id', 'contract_id', 'dag_id', 'domain', 'start_date'],
    spark_jobs_used: ['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD']
  },
  {
    pattern_code: 'P03',
    pattern_name: 'Database Lakehouse Pipeline',
    contract_type: 'STANDARD',
    description: 'Database to lakehouse with JDBC extraction, CDC, and incremental merge using Delta Lake',
    source_types_supported: ['database_postgres', 'database_mysql', 'database_oracle', 'database_sqlserver', 'database_snowflake'],
    load_types_supported: ['FULL', 'INCREMENTAL', 'CDC'],
    zones: ['bronze', 'silver', 'gold'],
    selection_priority: 100,
    required_variables: ['feed_id', 'contract_id', 'dag_id', 'domain', 'source_connection_id', 'start_date'],
    spark_jobs_used: ['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_SEMANTIC_VALIDATION', 'SILVER_TO_GOLD']
  },
  {
    pattern_code: 'P04',
    pattern_name: 'Legacy Migration Pipeline',
    contract_type: 'STANDARD',
    description: 'Legacy system migration: DTSX, COBOL copybook, AS400, EBCDIC, Mainframe with Cobrix',
    source_types_supported: ['legacy_dtsx', 'legacy_cobol', 'legacy_as400', 'file_ebcdic', 'legacy_mainframe'],
    load_types_supported: ['FULL'],
    zones: ['bronze', 'silver', 'gold'],
    selection_priority: 100,
    required_variables: ['feed_id', 'contract_id', 'dag_id', 'domain', 'legacy_source_type', 'start_date'],
    spark_jobs_used: ['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD']
  },
  {
    pattern_code: 'P05',
    pattern_name: 'Streaming Batch Pipeline',
    contract_type: 'STANDARD',
    description: 'Micro-batch from Kafka/Pub/Sub/Kinesis/EventHub with checkpoint offset management and late data handling',
    source_types_supported: ['streaming_kafka', 'streaming_pubsub', 'streaming_kinesis', 'streaming_eventhubs'],
    load_types_supported: ['APPEND'],
    zones: ['bronze', 'silver', 'gold'],
    selection_priority: 100,
    required_variables: ['feed_id', 'contract_id', 'dag_id', 'domain', 'streaming_source', 'topic_name', 'checkpoint_location', 'start_date'],
    spark_jobs_used: ['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD']
  },
  {
    pattern_code: 'P06',
    pattern_name: 'API SaaS Pipeline',
    contract_type: 'STANDARD',
    description: 'REST API and SaaS ingestion (Salesforce, SAP, Workday) with pagination, rate limiting, and OAuth authentication',
    source_types_supported: ['api_rest', 'api_graphql', 'saas_salesforce', 'saas_servicenow', 'saas_workday'],
    load_types_supported: ['FULL', 'INCREMENTAL'],
    zones: ['bronze', 'silver', 'gold'],
    selection_priority: 100,
    required_variables: ['feed_id', 'contract_id', 'dag_id', 'domain', 'api_type', 'api_connection_id', 'api_endpoint', 'start_date'],
    spark_jobs_used: ['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD']
  },
  {
    pattern_code: 'P07',
    pattern_name: 'SCD Type 2 Pipeline',
    contract_type: 'SCD2',
    description: 'Slowly Changing Dimensions Type 2 with hash-based change detection, tracking historical changes to dimension attributes',
    source_types_supported: ['file_csv', 'file_parquet', 'database_postgres', 'database_mysql', 'database_oracle'],
    load_types_supported: ['FULL', 'INCREMENTAL'],
    zones: ['bronze', 'silver', 'gold'],
    selection_priority: 80,
    required_variables: ['feed_id', 'contract_id', 'dag_id', 'domain', 'business_keys', 'tracked_columns', 'start_date'],
    spark_jobs_used: ['RAW_TO_BRONZE', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD']
  },
  {
    pattern_code: 'P08',
    pattern_name: 'Data Vault Pipeline',
    contract_type: 'DATA_VAULT',
    description: 'Data Vault 2.0 with Hub, Link, and Satellite loading for enterprise data warehouse modeling',
    source_types_supported: ['file_csv', 'file_parquet', 'database_postgres', 'database_mysql', 'database_oracle'],
    load_types_supported: ['FULL', 'INCREMENTAL'],
    zones: ['bronze', 'silver', 'gold'],
    selection_priority: 80,
    required_variables: ['feed_id', 'contract_id', 'dag_id', 'domain', 'record_source', 'hubs', 'links', 'satellites', 'start_date'],
    spark_jobs_used: ['RAW_TO_BRONZE', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD']
  },
  {
    pattern_code: 'P09',
    pattern_name: 'Star Schema Pipeline',
    contract_type: 'STAR_SCHEMA',
    description: 'Star schema dimensional modeling with fact and dimension loading, including date dimension and aggregate tables',
    source_types_supported: ['file_csv', 'file_parquet', 'database_postgres', 'database_mysql', 'database_oracle'],
    load_types_supported: ['FULL', 'INCREMENTAL'],
    zones: ['bronze', 'silver', 'gold'],
    selection_priority: 80,
    required_variables: ['feed_id', 'contract_id', 'dag_id', 'domain', 'dimensions', 'fact_table', 'start_date'],
    spark_jobs_used: ['RAW_TO_BRONZE', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD']
  }
]

export const PatternSelector: React.FC<PatternSelectorProps> = ({
  sourceType,
  selectedPattern,
  onChange,
  className = ''
}) => {
  const [patterns, setPatterns] = useState<PatternInfo[]>([])
  const [recommendedPattern, setRecommendedPattern] = useState<PatternCode | null>(null)
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    // Filter patterns compatible with source type
    if (sourceType) {
      const compatible = PATTERN_REGISTRY.filter((p) =>
        p.source_types_supported.some(st => sourceType.startsWith(st) || st === sourceType)
      )

      setPatterns(compatible.length > 0 ? compatible : PATTERN_REGISTRY)

      // Recommend highest priority pattern
      if (compatible.length > 0) {
        const recommended = compatible.sort((a, b) =>
          b.selection_priority - a.selection_priority
        )[0]
        setRecommendedPattern(recommended.pattern_code)
      }
    } else {
      setPatterns(PATTERN_REGISTRY)
    }
  }, [sourceType])

  const handlePatternSelect = (pattern: PatternInfo) => {
    onChange(pattern.pattern_code, pattern.contract_type)
  }

  const getContractTypeBadgeColor = (contractType: ContractType): string => {
    switch (contractType) {
      case 'STANDARD':
        return 'bg-blue-100 text-blue-800'
      case 'SCD2':
        return 'bg-purple-100 text-purple-800'
      case 'DATA_VAULT':
        return 'bg-green-100 text-green-800'
      case 'STAR_SCHEMA':
        return 'bg-orange-100 text-orange-800'
      default:
        return 'bg-gray-100 text-gray-800'
    }
  }

  if (isLoading) {
    return (
      <div className={`flex items-center justify-center p-8 ${className}`}>
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        <span className="ml-3 text-gray-600">Loading patterns...</span>
      </div>
    )
  }

  return (
    <div className={`space-y-4 ${className}`}>
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Select DAG Pattern (P01-P09)
        </label>
        <p className="text-sm text-gray-500 mb-4">
          Choose the pipeline architecture that best fits your data flow. The pattern determines which Airflow DAG template and Spark jobs will be generated.
        </p>
      </div>

      {recommendedPattern && (
        <div className="bg-blue-50 border border-blue-200 rounded-md p-3 flex items-start">
          <svg
            className="h-5 w-5 text-blue-600 mt-0.5 mr-2 flex-shrink-0"
            fill="currentColor"
            viewBox="0 0 20 20"
          >
            <path
              fillRule="evenodd"
              d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z"
              clipRule="evenodd"
            />
          </svg>
          <div>
            <p className="text-sm font-medium text-blue-800">
              Recommended: {recommendedPattern}
            </p>
            <p className="text-xs text-blue-600 mt-1">
              Based on your selected source type, this pattern has the highest priority match.
            </p>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {patterns.map((pattern) => (
          <Card
            key={pattern.pattern_code}
            onClick={() => handlePatternSelect(pattern)}
            className={`
              cursor-pointer transition-all duration-200 hover:shadow-lg
              ${selectedPattern === pattern.pattern_code
                ? 'ring-2 ring-blue-500 bg-blue-50 border-blue-300'
                : 'border-gray-200 hover:border-blue-300'}
              ${recommendedPattern === pattern.pattern_code && selectedPattern !== pattern.pattern_code
                ? 'ring-1 ring-blue-300'
                : ''}
            `}
          >
            <div className="p-4 space-y-3">
              {/* Pattern Code and Name */}
              <div className="flex items-start justify-between">
                <div>
                  <div className="text-lg font-bold text-gray-900">
                    {pattern.pattern_code}
                  </div>
                  <div className="text-sm font-medium text-gray-700 mt-1">
                    {pattern.pattern_name}
                  </div>
                </div>
                {selectedPattern === pattern.pattern_code && (
                  <svg
                    className="h-6 w-6 text-blue-600 flex-shrink-0"
                    fill="currentColor"
                    viewBox="0 0 20 20"
                  >
                    <path
                      fillRule="evenodd"
                      d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                      clipRule="evenodd"
                    />
                  </svg>
                )}
              </div>

              {/* Contract Type Badge */}
              <div>
                <Badge className={`${getContractTypeBadgeColor(pattern.contract_type)} text-xs px-2 py-1`}>
                  {pattern.contract_type}
                </Badge>
              </div>

              {/* Description */}
              <p className="text-xs text-gray-600 line-clamp-3">
                {pattern.description}
              </p>

              {/* Zones */}
              <div className="flex flex-wrap gap-1">
                {pattern.zones.map((zone) => (
                  <span
                    key={zone}
                    className="text-xs bg-gray-100 text-gray-700 px-2 py-0.5 rounded"
                  >
                    {zone}
                  </span>
                ))}
              </div>

              {/* Priority Indicator */}
              {recommendedPattern === pattern.pattern_code && (
                <div className="text-xs text-blue-600 font-medium flex items-center">
                  <svg
                    className="h-4 w-4 mr-1"
                    fill="currentColor"
                    viewBox="0 0 20 20"
                  >
                    <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                  </svg>
                  Recommended
                </div>
              )}
            </div>
          </Card>
        ))}
      </div>

      {patterns.length === 0 && (
        <div className="text-center py-8 text-gray-500">
          <p>No compatible patterns found for the selected source type.</p>
          <p className="text-sm mt-2">Try selecting a different source type.</p>
        </div>
      )}

      {selectedPattern && (
        <div className="mt-4 p-4 bg-gray-50 rounded-md border border-gray-200">
          <div className="text-sm font-medium text-gray-700 mb-2">
            Selected Pattern Details:
          </div>
          {patterns.find((p) => p.pattern_code === selectedPattern) && (
            <div className="text-xs text-gray-600 space-y-1">
              <div>
                <span className="font-medium">Required Variables:</span>{' '}
                {patterns
                  .find((p) => p.pattern_code === selectedPattern)
                  ?.required_variables.join(', ')}
              </div>
              <div>
                <span className="font-medium">Spark Jobs:</span>{' '}
                {patterns
                  .find((p) => p.pattern_code === selectedPattern)
                  ?.spark_jobs_used.join(', ')}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
