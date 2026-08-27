// Migration types — mirrors migration_repository.py Pydantic structures

export type MigrationStatus =
  | 'PENDING'
  | 'EXTRACTING'
  | 'GRAPH_BUILDING'
  | 'LLM_GENERATING'
  | 'COMPLETE'
  | 'FAILED'

export type ExtractionSource = 'LIVE_DB' | 'STATIC_FILES' | 'HYBRID'

export type DbPlatform = 'SQLSERVER' | 'ORACLE' | 'POSTGRESQL' | 'UNKNOWN'

export type ObjectType =
  | 'PROCEDURE'
  | 'FUNCTION'
  | 'VIEW'
  | 'TRIGGER'
  | 'PACKAGE'
  | 'PACKAGE_BODY'

export type ExtractionStatus =
  | 'PENDING'
  | 'EXTRACTED'
  | 'ENCRYPTED'
  | 'NOT_FOUND'
  | 'FAILED'

export type ArtifactType =
  | 'PYSPARK_JOB'
  | 'AIRFLOW_OPERATOR'
  | 'SQL_DDL'
  | 'GREAT_EXPECTATIONS'
  | 'SPARK_SUBMIT_CONFIG'

export type ValidationStatus = 'PENDING' | 'PASSED' | 'FAILED' | 'SKIPPED'

export interface MigrationObject {
  object_id: string
  object_schema: string
  object_name: string
  object_type: ObjectType
  db_platform: DbPlatform
  extraction_status: ExtractionStatus
  char_count?: number
  is_encrypted: boolean
  error_message?: string
  created_at: string
}

export interface MigrationArtifact {
  artifact_id: string
  object_id: string
  artifact_type: ArtifactType
  artifact_content?: string
  artifact_path?: string
  llm_model?: string
  confidence_score?: number
  validation_status: ValidationStatus
  generation_ms?: number
  created_at: string
  object_schema?: string
  object_name?: string
}

export interface GraphNode {
  id: string           // fqn: schema.name
  schema: string
  name: string
  object_type: ObjectType
  db_platform: DbPlatform
  is_encrypted: boolean
  topo_level: number
  param_count: number
  char_count: number
}

export interface GraphEdge {
  source: string       // parent fqn
  target: string       // child fqn
  type: 'CALLS' | 'SELECTS_FROM' | 'INSERTS_INTO' | 'UPDATES' | 'DELETES_FROM'
}

export interface DependencyGraph {
  nodes: GraphNode[]
  edges: GraphEdge[]
  execution_order: string[]
  execution_batches: Record<string, string[]>   // topo_level → fqn[]
  node_count: number
  edge_count: number
  max_depth: number
  has_cycles: boolean
}

export interface MigrationJob {
  job_id: string
  status: MigrationStatus
  extraction_source: ExtractionSource
  total_objects: number
  extracted_objects: number
  failed_objects: number
  skipped_objects: number
  started_at?: string
  completed_at?: string
  created_by?: string
  objects?: MigrationObject[]
  artifacts?: MigrationArtifact[]
  dependency_graph?: DependencyGraph
  error_message?: string
}

export interface MigrationJobRequest {
  connection_code: string
  schema_filter?: string
  proc_name_pattern?: string
  dtsx_source_path?: string
  target_feed_group_id?: string
  created_by?: string
}
