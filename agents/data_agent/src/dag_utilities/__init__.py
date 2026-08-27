"""
DAG Utilities Package

Core utilities for APEX DAG patterns. These utilities provide the interface
between the Control Plane (Airflow DAGs) and Data Plane (Spark jobs).

Architecture follows the 7-step Agent Workflow:
1. Validate Metadata (MetadataValidator)
2. Generate SQL (SQLGenerator)
3. Build Execution Plan (ExecutionPlanBuilder)
4. Generate DAG (Templates)
5. Process Zones (ZoneProcessor)
6. GE Validation (GEValidator)
7. GCP Execution (Dataproc, BigQuery)

Modules:
- core: MetadataClient, ExecutionContext, APEXConfig, MetadataValidator, ExecutionPlanBuilder
- generators: SQLGenerator, DeterministicDAGGenerator, TemplateManager
- processors: ZoneProcessor (single script for ALL zones)
- transforms: TransformDispatcher (metadata → PySpark)
- validation: GEValidator, ResultWriter, schema validators, semantic validators
- encryption: FPEEncryptor (fpe, fpe_hk, fpe_col_concat_hk, hash)
- sources: Source-specific utilities (file, database, streaming, legacy)
- common: DAG tasks (39+ callable functions), Spark wrappers, zone transitions, SQL executors
- spark_scripts: Standalone PySpark scripts for Dataproc (GE validator, consumption zone)
- notification: Email, Slack, PagerDuty integration
- logging: Audit logging
- remediation: Self-healing, retry handling
- governance: Contract gates, health score calculators
- pipeline: Common pipeline tasks
"""

# Core (Control Plane)
from .core import MetadataClient, ExecutionContext, APEXConfig, ExecutionTracker
from .core.metadata_validator import MetadataValidator, validate_metadata
from .core.execution_plan import ExecutionPlanBuilder, ExecutionPlan

# Generators (Control Plane)
from .generators.sql_generator import SQLGenerator, generate_metadata_sql
from .generators.dag_generator import DeterministicDAGGenerator, GeneratedArtifacts, generate_dag
from .generators.template_manager import TemplateManager

# Processors (Data Plane)
from .processors.zone_processor import ZoneProcessor, process_zone, submit_zone_job

# Transforms (Data Plane)
from .transforms.transform_dispatcher import TransformDispatcher

# Validation (Data Plane)
from .validation import GEValidator, ValidationResult, run_great_expectations
from .validation.result_writer import ValidationResultWriter

# Encryption (Data Plane)
from .encryption.fpe_encryptor import FPEEncryptor, apply_encryption

# Promotion (Control Plane - Dev → QA → Prod)
from .promotion import EnvironmentPromoter, PromotionRequest, PromotionResult

# Common - Production DAG Tasks & Spark Wrappers
from .common.dag_tasks import (
    PgConnInfo, TransientError, PermanentError,
    generate_pg_conn_info, get_pg_connection,
    get_file_run_id, audit_log, update_ingestion_status,
    check_ge_status, get_feed_config, get_feed_group_config,
    check_is_posting_date, get_feed_target_details, get_cz_schema_version,
    move_to_rejected, send_notification_file_count,
    get_group_run_id, group_audit_log, get_environment_recipients,
    # Production-aligned functions
    load_conn_info, check_trusted_ge_status, get_reporting_date_for_cz,
    get_table_run_id, move_s3_multiple_objects, move_if_duplicate_Dir,
    snowflake_check_result, rejected_file_email,
    extract_reporting_date_apps, source_table_load_success,
    source_table_load_failure, get_target_details,
    # DAG-level callables
    dag_init, dag_dup_check, dag_source_to_transient,
    dag_archive, dag_final_audit, dag_check_remaining,
    dag_handle_failure, dag_is_posting_date,
    dag_move_to_rejected, dag_rejection_notify,
    dag_group_init, dag_group_audit, dag_group_failure,
)
from .common.spark_wrapper import (
    get_spark_tg_wrapper_v2, get_spark_tg_wrapper_simple,
    build_spark_args_with_pg_conn,
    get_dataproc_pvc_config, build_zone_gcs_paths,
    get_pod_template_cstonedfs, get_samba_unlock_file_sensor_tg,
)

# Governance & Operations
from .governance import ContractGate, HealthScoreCalculator
from .logging import AuditLogger
from .remediation import RetryHandler, SelfHealer

__all__ = [
    # Core (Control Plane)
    "MetadataClient",
    "ExecutionContext",
    "APEXConfig",
    "ExecutionTracker",
    # Step 1: Metadata Validation
    "MetadataValidator",
    "validate_metadata",
    # Step 2: SQL Generation
    "SQLGenerator",
    "generate_metadata_sql",
    # Step 3: Execution Plan
    "ExecutionPlanBuilder",
    "ExecutionPlan",
    # Step 4: DAG Generation
    "DeterministicDAGGenerator",
    "GeneratedArtifacts",
    "generate_dag",
    "TemplateManager",
    # Step 5: Zone Processing + Transforms
    "ZoneProcessor",
    "process_zone",
    "submit_zone_job",
    "TransformDispatcher",
    # Step 5: Encryption
    "FPEEncryptor",
    "apply_encryption",
    # Step 6: GE Validation
    "GEValidator",
    "ValidationResult",
    "ValidationResultWriter",
    "run_great_expectations",
    # Promotion (Dev → QA → Prod)
    "EnvironmentPromoter",
    "PromotionRequest",
    "PromotionResult",
    # Production DAG Tasks (common)
    "PgConnInfo",
    "TransientError",
    "PermanentError",
    "generate_pg_conn_info",
    "get_pg_connection",
    "get_file_run_id",
    "audit_log",
    "update_ingestion_status",
    "check_ge_status",
    "get_feed_config",
    "get_feed_group_config",
    # Business Date / Target / Rejection / Group
    "check_is_posting_date",
    "get_feed_target_details",
    "get_cz_schema_version",
    "move_to_rejected",
    "send_notification_file_count",
    "get_group_run_id",
    "group_audit_log",
    "get_environment_recipients",
    # Production-aligned functions
    "load_conn_info",
    "check_trusted_ge_status",
    "get_reporting_date_for_cz",
    "get_table_run_id",
    "move_s3_multiple_objects",
    "move_if_duplicate_Dir",
    "snowflake_check_result",
    "rejected_file_email",
    "extract_reporting_date_apps",
    "source_table_load_success",
    "source_table_load_failure",
    "get_target_details",
    # Spark Wrappers
    "get_spark_tg_wrapper_v2",
    "get_spark_tg_wrapper_simple",
    "build_spark_args_with_pg_conn",
    "get_dataproc_pvc_config",
    "build_zone_gcs_paths",
    "get_pod_template_cstonedfs",
    "get_samba_unlock_file_sensor_tg",
    # DAG-level callables (PythonOperator op_kwargs)
    "dag_init",
    "dag_dup_check",
    "dag_source_to_transient",
    "dag_archive",
    "dag_final_audit",
    "dag_check_remaining",
    "dag_handle_failure",
    "dag_is_posting_date",
    "dag_move_to_rejected",
    "dag_rejection_notify",
    "dag_group_init",
    "dag_group_audit",
    "dag_group_failure",
    # Governance & Operations
    "ContractGate",
    "HealthScoreCalculator",
    "AuditLogger",
    "RetryHandler",
    "SelfHealer",
]
