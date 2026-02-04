/**
 * API Types
 *
 * Types for API requests and responses that don't fit elsewhere.
 */

export interface TaskRequest {
  description: string
  priority?: string
  metadata?: Record<string, any>
}

export interface TaskResponse {
  task_id: string
  status: string
  message?: string
  created_at?: string
}

// =============================================================================
// Data Catalog API Types
// =============================================================================

export interface DataAsset {
  asset_id: string
  asset_name: string
  asset_type: string
  zone_level: string
  feed_id?: string
  domain?: string
  owner?: string
  row_count?: number
  size_bytes?: number
  description?: string
  tags?: string[]
  last_refreshed_at?: string
  is_active: boolean
}

export interface BusinessTerm {
  term_id: string
  term_name: string
  definition: string
  domain?: string
  owner?: string
  synonyms?: string[]
}

export interface CatalogSearchParams {
  query?: string
  zone?: string
  domain?: string
}

// =============================================================================
// Data Products API Types
// =============================================================================

export interface DataProduct {
  product_id: string
  product_name: string
  domain: string
  owner: string
  description?: string
  sla_freshness_hours?: number
  sla_quality_score?: number
  version: string
  status: 'DRAFT' | 'PUBLISHED' | 'DEPRECATED'
  output_assets?: string[]
  input_feeds?: string[]
  published_at?: string
  subscriber_count?: number
}

export interface DataProductSubscription {
  subscription_id: string
  product_id: string
  product_name?: string
  subscriber: string
  status: 'PENDING' | 'APPROVED' | 'REJECTED'
  requested_at: string
  approved_by?: string
  approved_at?: string
}

export interface SubscribeRequest {
  subscriber: string
}

// =============================================================================
// Data Classification API Types
// =============================================================================

export interface DataClassification {
  classification_id: string
  asset_id: string
  column_name: string
  classification_type: string
  pii_type?: string
  confidence?: number
  masking_strategy?: string
  policy_tag?: string
}

// =============================================================================
// V2 Pipeline API Types (FeedGroupConfig-based generation)
// =============================================================================

export interface V2CreatePipelineRequest {
  feed_group_config: import('@/types/pipeline-canonical').V2FeedGroupConfig
}

export interface V2CreatePipelineResponse {
  dag_id: string
  dag_path: string
  template_used: string
  sql_scripts: Record<string, string>
  ge_suites: Record<string, string>
  spark_jobs: string[]
  validation_passed: boolean
  validation_messages: string[]
  pull_request_url?: string
}

export interface V2ArtifactBundle {
  dag_id: string
  dag_code: string
  template_used: string
  sql_scripts: Record<string, string>
  ge_suites: Record<string, string>
  spark_jobs: string[]
}

export interface V2ValidationResult {
  feed_id: string
  execution_id: string
  suite_name: string
  validation_type: 'SCHEMA' | 'SEMANTIC'
  total_expectations: number
  passed: number
  failed: number
  quality_score: number
  results: Array<{
    rule_name: string
    expectation_type: string
    column: string
    success: boolean
    observed_value: string
    severity: string
  }>
}
