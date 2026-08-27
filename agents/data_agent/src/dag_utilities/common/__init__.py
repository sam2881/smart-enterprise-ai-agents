"""
DAG Utilities Common Module

Common utilities shared across APEX DAG patterns:
- dag_tasks: 39+ production callable functions (PgConnInfo, audit, file ops)
- spark_wrapper: TaskGroup wrappers for Dataproc + GE branching
- zone_transition: Dataproc job submission, zone table paths
- sql_executor: BigQuery SQL execution
"""

from .zone_transition import submit_dataproc_job, get_zone_table_path
from .sql_executor import run_bq_query, create_or_replace_view

# Production DAG tasks (39+ callable functions)
from .dag_tasks import (
    PgConnInfo,
    TransientError,
    PermanentError,
    get_pg_connection,
    generate_pg_conn_info,
    extract_reporting_date,
    get_file_run_id,
    get_or_reuse_file_run_id,
    platform_audit_log,
    update_ingestion_status,
    get_schema_version5,
    get_spark_submit_config,
    get_feed_config,
    get_feed_group_config,
    check_flag,
    set_flag,
    source_to_transient,
    transient_to_datalake,
    move_gcs_objects,
    archive_files,
    check_dup_file,
    is_file_remaining,
    check_ge_status,
    send_notification,
    load_snowflake_table,
    get_samba_unlock_file_sensor,
    check_is_posting_date,
    get_feed_target_details,
    get_cz_schema_version,
    move_to_rejected,
    send_notification_file_count,
    get_group_run_id,
    group_audit_log,
    get_environment_recipients,
    # Production-aligned functions (from common.dag_utilities import *)
    load_conn_info,
    check_trusted_ge_status,
    get_reporting_date_for_cz,
    get_table_run_id,
    move_s3_multiple_objects,
    move_if_duplicate_Dir,
    snowflake_check_result,
    rejected_file_email,
    extract_reporting_date_apps,
    source_table_load_success,
    source_table_load_failure,
    get_target_details,
    # DAG-level callables (used by PythonOperator op_kwargs)
    dag_init,
    dag_dup_check,
    dag_source_to_transient,
    dag_archive,
    dag_final_audit,
    dag_check_remaining,
    dag_handle_failure,
    dag_is_posting_date,
    dag_move_to_rejected,
    dag_rejection_notify,
    dag_group_init,
    dag_group_audit,
    dag_group_failure,
)

# Spark TaskGroup wrappers
from .spark_wrapper import (
    build_spark_args_with_pg_conn,
    get_spark_tg_wrapper_v2,
    get_spark_tg_wrapper_simple,
    get_dataproc_pvc_config,
    build_zone_gcs_paths,
    get_pod_template_cstonedfs,
    get_samba_unlock_file_sensor_tg,
)

__all__ = [
    # Zone transitions
    "submit_dataproc_job",
    "get_zone_table_path",
    # SQL execution
    "run_bq_query",
    "create_or_replace_view",
    # Connection management
    "PgConnInfo",
    "TransientError",
    "PermanentError",
    "get_pg_connection",
    "generate_pg_conn_info",
    # Reporting Date
    "extract_reporting_date",
    # Run ID & Audit (with re-run lineage)
    "get_file_run_id",
    "get_or_reuse_file_run_id",
    "platform_audit_log",
    "update_ingestion_status",
    # Schema & Configuration
    "get_schema_version5",
    "get_spark_submit_config",
    "get_feed_config",
    "get_feed_group_config",
    "check_flag",
    "set_flag",
    # File Operations
    "source_to_transient",
    "transient_to_datalake",
    "move_gcs_objects",
    "archive_files",
    "check_dup_file",
    "is_file_remaining",
    # GE / Notifications / External
    "check_ge_status",
    "send_notification",
    "load_snowflake_table",
    "get_samba_unlock_file_sensor",
    # Business Date / Target / Consumption / Rejection / Group
    "check_is_posting_date",
    "get_feed_target_details",
    "get_cz_schema_version",
    "move_to_rejected",
    "send_notification_file_count",
    "get_group_run_id",
    "group_audit_log",
    "get_environment_recipients",
    # Production-aligned functions (from common.dag_utilities import *)
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
    # Spark wrappers
    "build_spark_args_with_pg_conn",
    "get_spark_tg_wrapper_v2",
    "get_spark_tg_wrapper_simple",
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
]
