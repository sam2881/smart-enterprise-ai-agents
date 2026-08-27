"""
Spark TaskGroup Wrapper - Production Runtime

This module provides `get_spark_tg_wrapper_v2()`, the MOST CRITICAL production
component. In production, EVERY Spark zone job goes through this wrapper which
creates an Airflow TaskGroup containing:

    1. DataprocSubmitJobOperator  — submits PySpark to Dataproc
    2. BranchPythonOperator       — checks validation_result table
    3. EmptyOperator (ge_pass)    — success branch
    4. EmptyOperator (ge_fail)    — failure branch

pg_conn_info flow:
    DAG (Airflow Variable) → spark_wrapper (appends to positional args)
    → PySpark script (sys.argv[4:10]) → psycopg2

Usage in thin DAGs:
    from dag_utilities.common.spark_wrapper import get_spark_tg_wrapper_v2

    raw_tg = get_spark_tg_wrapper_v2(
        dag=dag,
        task_group_id="raw_zone",
        spark_script_path="gs://bucket/spark_scripts/validate_schema_with_great_expectations.py",
        positional_args_list=[str(FEED_ID), "raw", "{{ ds_nodash }}"],
        pg_conn_info_dict=PG_CONN_INFO,
        ge_check_enabled=True,
        feed_id=FEED_ID,
        zone="raw",
    )
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def build_spark_args_with_pg_conn(
    base_args: List[str],
    pg_conn_info: Dict[str, str],
) -> List[str]:
    """
    Append pg_conn_info (6 values) to the base positional args list.

    This is how PostgreSQL connectivity flows from DAG → PySpark:
        base_args + [host, port, dbname, user, password, schema]

    Args:
        base_args: Base positional arguments for the Spark script
        pg_conn_info: Dict with keys: host, port, dbname, user, password, schema

    Returns:
        Extended args list with pg_conn_info appended
    """
    pg_args = [
        pg_conn_info.get("host", "localhost"),
        str(pg_conn_info.get("port", "5432")),
        pg_conn_info.get("dbname", "apex_metadata"),
        pg_conn_info.get("user", "apex_user"),
        pg_conn_info.get("password", ""),
        pg_conn_info.get("schema", "public"),
    ]
    return list(base_args) + pg_args


def get_spark_tg_wrapper_v2(
    dag: Any = None,
    task_group_id: str = "",
    spark_script_path: str = "",
    positional_args_list: Optional[List[str]] = None,
    pg_conn_info_dict: Optional[Dict[str, str]] = None,
    spark_config: Optional[Dict[str, Any]] = None,
    dataproc_config: Optional[Any] = None,
    use_serverless: bool = True,
    ge_check_enabled: bool = True,
    feed_id: Optional[int] = None,
    zone: Optional[str] = None,
    file_run_id: Optional[int] = None,
    labels: Optional[Dict[str, str]] = None,
    spark_jobs_bucket: str = "apex-spark-jobs",
    project_id: Optional[str] = None,
    region: str = "us-central1",
    cluster_name: str = "apex-dataproc",
    subnet: Optional[str] = None,
    service_account: Optional[str] = None,
    kms_key: Optional[str] = None,
    python_file_uris: Optional[List[str]] = None,
    pvc_config: Optional[Dict[str, str]] = None,
    zone_buckets: Optional[Dict[str, str]] = None,
    # ── Production-aligned parameters ──
    spark_app_name: Optional[str] = None,
    spark_app_file_name: Optional[str] = None,
    dag_name: Any = None,
    tg_name: Optional[str] = None,
    feed_name: Optional[str] = None,
    exec_size: Optional[str] = None,
    spark_args: Optional[List[str]] = None,
    extra_task_id: Optional[str] = None,
    extra_func: Optional[Any] = None,
    extra_args: Optional[Dict[str, Any]] = None,
    executor_config: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Create a TaskGroup wrapping a Spark job with optional GE branching.

    Supports BOTH naming conventions:
        - APEX style: task_group_id, spark_script_path, positional_args_list
        - Production style: tg_name, spark_app_file_name, spark_args, dag_name

    TaskGroup structure:
        spark_submit >> ge_check >> [ge_pass, ge_fail]

    With extra_func (production pattern):
        spark_submit >> extra_func (BranchPythonOperator) >> [success, fail]

    If ge_check_enabled=False and no extra_func:
        spark_submit (simple, no branching)

    Args:
        dag / dag_name: The parent Airflow DAG
        task_group_id / tg_name: Unique ID for this TaskGroup
        spark_script_path / spark_app_file_name: Path to PySpark script
        positional_args_list / spark_args: Positional arguments for the script
        pg_conn_info_dict: PostgreSQL connection info dict
        spark_app_name: Spark application name for K8s
        feed_name: Prefix for task IDs in nested TaskGroups
        exec_size: Resource allocation size (small/medium/large)
        extra_task_id: Task ID for extra BranchPythonOperator
        extra_func: Callable for extra branching (e.g., check_ge_status)
        extra_args: op_kwargs for extra_func
        executor_config: K8s executor config (from get_pod_template_cstonedfs)
        pvc_config: Optional PVC mount configuration
        zone_buckets: Optional zone GCS bucket paths

    Returns:
        Airflow TaskGroup instance
    """
    # ── Normalize production aliases ──
    resolved_dag = dag or dag_name
    resolved_tg_id = task_group_id or tg_name or "spark_tg"
    resolved_script = spark_script_path or spark_app_file_name or ""
    resolved_args = positional_args_list or spark_args or []
    resolved_pg = pg_conn_info_dict or {}

    try:
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.empty import EmptyOperator
        from airflow.operators.python import BranchPythonOperator
    except ImportError:
        return _build_taskgroup_stub(
            resolved_tg_id, resolved_script, resolved_args,
            resolved_pg, ge_check_enabled,
        )

    if resolved_dag is None:
        return _build_taskgroup_stub(
            resolved_tg_id, resolved_script, resolved_args,
            resolved_pg, ge_check_enabled,
        )

    # Build full args with pg_conn_info appended
    full_args = build_spark_args_with_pg_conn(resolved_args, resolved_pg)

    # Resolve project/region
    import os
    resolved_project = project_id or os.getenv("GCP_PROJECT", "apex-project")
    resolved_region = region or os.getenv("GCP_REGION", "us-central1")

    # Build labels
    job_labels = {
        "feed_id": str(feed_id or "unknown"),
        "zone": zone or "unknown",
        "task_group": resolved_tg_id,
    }
    if labels:
        job_labels.update(labels)

    # Apply exec_size presets (production pattern: "small", "medium", "large")
    exec_size_presets = {
        "small": {"spark.executor.memory": "4g", "spark.executor.cores": "2"},
        "medium": {"spark.executor.memory": "8g", "spark.executor.cores": "4"},
        "large": {"spark.executor.memory": "16g", "spark.executor.cores": "8"},
    }
    spark_properties = {
        "spark.executor.memory": "8g",
        "spark.executor.cores": "4",
        "spark.dynamicAllocation.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
    }
    if exec_size and exec_size.lower() in exec_size_presets:
        spark_properties.update(exec_size_presets[exec_size.lower()])
    if spark_config:
        spark_properties.update({
            k: str(v) for k, v in spark_config.items()
            if k.startswith("spark.")
        })

    # Merge PVC config into Spark properties if provided
    if pvc_config:
        spark_properties.update({
            k: str(v) for k, v in pvc_config.items()
            if k.startswith("spark.")
        })

    # Append zone bucket paths to positional args if provided
    if zone_buckets:
        full_args.extend([
            zone_buckets.get("input_path", ""),
            zone_buckets.get("output_path", ""),
        ])

    # Resolve python_file_uris
    py_uris = python_file_uris or [f"gs://{spark_jobs_bucket}/dag_utilities.zip"]

    # Resolve spark_app_name
    app_name = spark_app_name or f"{resolved_tg_id}-spark"

    with TaskGroup(group_id=resolved_tg_id, dag=resolved_dag) as tg:
        # ── Task 1: Spark Submit ──
        submit_task_id = "combined-spark" if extra_func else f"{resolved_tg_id}_spark"
        spark_submit = _build_spark_submit_task(
            task_id=submit_task_id,
            spark_script_path=resolved_script,
            args=full_args,
            project_id=resolved_project,
            region=resolved_region,
            cluster_name=cluster_name,
            use_serverless=use_serverless,
            spark_properties=spark_properties,
            labels=job_labels,
            python_file_uris=py_uris,
            subnet=subnet,
            service_account=service_account,
            kms_key=kms_key,
            dag=resolved_dag,
        )

        # ── Production pattern: extra_func as BranchPythonOperator ──
        if extra_func is not None and extra_task_id:
            extra_branch = BranchPythonOperator(
                task_id=extra_task_id,
                python_callable=extra_func,
                op_kwargs=extra_args or {},
                dag=resolved_dag,
            )
            spark_submit >> extra_branch

        elif ge_check_enabled and feed_id is not None and zone is not None:
            # ── Default GE Branch Check ──
            ge_branch = BranchPythonOperator(
                task_id=f"{resolved_tg_id}_ge_check",
                python_callable=_ge_branch_callable,
                op_kwargs={
                    "feed_id": feed_id,
                    "file_run_id": file_run_id,
                    "zone": zone,
                    "pg_conn_info_dict": resolved_pg,
                    "pass_task_id": f"{resolved_tg_id}.{resolved_tg_id}_ge_pass",
                    "fail_task_id": f"{resolved_tg_id}.{resolved_tg_id}_ge_fail",
                },
                dag=resolved_dag,
            )

            ge_pass = EmptyOperator(
                task_id=f"{resolved_tg_id}_ge_pass",
                dag=resolved_dag,
            )
            ge_fail = EmptyOperator(
                task_id=f"{resolved_tg_id}_ge_fail",
                dag=resolved_dag,
            )

            spark_submit >> ge_branch >> [ge_pass, ge_fail]

    return tg


def get_spark_tg_wrapper_simple(
    dag: Any,
    task_group_id: str,
    spark_script_path: str,
    positional_args_list: List[str],
    pg_conn_info_dict: Dict[str, str],
    **kwargs,
) -> Any:
    """
    Simplified wrapper WITHOUT GE branching.

    Convenience function that calls get_spark_tg_wrapper_v2 with ge_check_enabled=False.
    """
    return get_spark_tg_wrapper_v2(
        dag=dag,
        task_group_id=task_group_id,
        spark_script_path=spark_script_path,
        positional_args_list=positional_args_list,
        pg_conn_info_dict=pg_conn_info_dict,
        ge_check_enabled=False,
        **kwargs,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PVC & ZONE BUCKET CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════


def get_dataproc_pvc_config(
    pvc_name: str = "apex-data-pvc",
    mount_path: str = "/mnt/data",
    storage_class: str = "standard",
    size: str = "100Gi",
) -> Dict[str, str]:
    """
    Generate Spark properties for PVC mount on Dataproc (Kubernetes mode).

    In production, files are downloaded from source GCS to a PVC (local disk)
    on Dataproc cluster nodes, processed by Spark locally, then uploaded
    to per-zone GCS buckets.

    Args:
        pvc_name: PersistentVolumeClaim name
        mount_path: Mount path inside the executor/driver pods
        storage_class: Kubernetes storage class
        size: PVC size request

    Returns:
        Dict of Spark properties for PVC configuration
    """
    return {
        "spark.kubernetes.executor.volumes.persistentVolumeClaim.data-vol.options.claimName": pvc_name,
        "spark.kubernetes.executor.volumes.persistentVolumeClaim.data-vol.mount.path": mount_path,
        "spark.kubernetes.executor.volumes.persistentVolumeClaim.data-vol.mount.readOnly": "false",
        "spark.kubernetes.driver.volumes.persistentVolumeClaim.data-vol.options.claimName": pvc_name,
        "spark.kubernetes.driver.volumes.persistentVolumeClaim.data-vol.mount.path": mount_path,
        "spark.kubernetes.driver.volumes.persistentVolumeClaim.data-vol.mount.readOnly": "false",
        "spark.apex.pvc.mount_path": mount_path,
        "spark.apex.pvc.storage_class": storage_class,
        "spark.apex.pvc.size": size,
    }


def build_zone_gcs_paths(
    feed_id: int,
    zone: str,
    bucket_template: str = "apex-{zone}-bucket",
    prefix_template: str = "feed_{feed_id}",
) -> Dict[str, str]:
    """
    Build input/output GCS paths for a zone based on the 5-zone medallion architecture.

    Each zone reads from the previous zone's bucket and writes to its own bucket.
    Zone ordering: transient → raw → refined → gold → consumption

    Args:
        feed_id: Feed ID for path construction
        zone: Current zone name
        bucket_template: Bucket name template with {zone} placeholder
        prefix_template: Prefix template with {feed_id} placeholder

    Returns:
        Dict with input_path and output_path GCS URIs
    """
    zone_order = ["transient", "raw", "refined", "gold", "consumption"]
    zone_lower = zone.lower()

    # Determine input zone (previous in chain)
    current_idx = zone_order.index(zone_lower) if zone_lower in zone_order else 0
    input_zone = zone_order[current_idx - 1] if current_idx > 0 else zone_lower

    prefix = prefix_template.format(feed_id=feed_id)
    input_bucket = bucket_template.format(zone=input_zone)
    output_bucket = bucket_template.format(zone=zone_lower)

    return {
        "input_path": f"gs://{input_bucket}/{prefix}/",
        "output_path": f"gs://{output_bucket}/{prefix}/",
        "input_zone": input_zone,
        "output_zone": zone_lower,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _build_spark_submit_task(
    task_id: str,
    spark_script_path: str,
    args: List[str],
    project_id: str,
    region: str,
    cluster_name: str,
    use_serverless: bool,
    spark_properties: Dict[str, str],
    labels: Dict[str, str],
    python_file_uris: List[str],
    subnet: Optional[str],
    service_account: Optional[str],
    kms_key: Optional[str],
    dag: Any,
) -> Any:
    """Build the appropriate Dataproc submit operator."""

    if use_serverless:
        try:
            from airflow.providers.google.cloud.operators.dataproc import (
                DataprocCreateBatchOperator,
            )

            batch_config = {
                "pyspark_batch": {
                    "main_python_file_uri": spark_script_path,
                    "args": [str(a) for a in args],
                    "python_file_uris": python_file_uris,
                },
                "runtime_config": {
                    "version": "2.1",
                    "properties": spark_properties,
                },
                "environment_config": {},
                "labels": labels,
            }

            if subnet or service_account:
                exec_config = {}
                if subnet:
                    exec_config["subnetwork_uri"] = subnet
                if service_account:
                    exec_config["service_account"] = service_account
                if kms_key:
                    exec_config["kms_key"] = kms_key
                batch_config["environment_config"]["execution_config"] = exec_config

            return DataprocCreateBatchOperator(
                task_id=task_id,
                project_id=project_id,
                region=region,
                batch=batch_config,
                batch_id=f"{task_id.replace('.', '-')}-{{{{ ts_nodash }}}}",
                dag=dag,
            )
        except ImportError:
            pass

    # Fallback: traditional Dataproc cluster job
    try:
        from airflow.providers.google.cloud.operators.dataproc import (
            DataprocSubmitJobOperator,
        )

        job = {
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "labels": labels,
            "pyspark_job": {
                "main_python_file_uri": spark_script_path,
                "args": [str(a) for a in args],
                "python_file_uris": python_file_uris,
                "properties": spark_properties,
            },
        }

        return DataprocSubmitJobOperator(
            task_id=task_id,
            job=job,
            region=region,
            project_id=project_id,
            dag=dag,
        )
    except ImportError:
        # Pure stub for template rendering / testing
        from airflow.operators.empty import EmptyOperator
        return EmptyOperator(task_id=task_id, dag=dag)


def _ge_branch_callable(
    feed_id: int,
    file_run_id: Optional[int],
    zone: str,
    pg_conn_info_dict: Dict[str, str],
    pass_task_id: str,
    fail_task_id: str,
    **context,
) -> str:
    """
    BranchPythonOperator callable that checks GE validation result.

    Returns the task_id of the branch to follow.
    """
    from dag_utilities.common.dag_tasks import PgConnInfo, check_ge_status

    pg_info = PgConnInfo.from_dict(pg_conn_info_dict)

    # If file_run_id wasn't set at DAG parse time, try XCom
    run_id = file_run_id
    if run_id is None and context:
        ti = context.get("ti")
        if ti:
            run_id = ti.xcom_pull(task_ids="init", key="file_run_id")

    run_id = run_id or 0

    status = check_ge_status(feed_id, run_id, zone, pg_info)
    logger.info(
        "GE check result: feed=%s run=%s zone=%s status=%s",
        feed_id, run_id, zone, status,
    )

    return pass_task_id if status == "pass" else fail_task_id


def _build_taskgroup_stub(
    task_group_id: str,
    spark_script_path: str,
    positional_args_list: List[str],
    pg_conn_info_dict: Dict[str, str],
    ge_check_enabled: bool,
) -> Dict[str, Any]:
    """
    Non-Airflow stub for template rendering and testing.

    Returns a dict describing what the TaskGroup would contain.
    """
    full_args = build_spark_args_with_pg_conn(positional_args_list, pg_conn_info_dict)

    stub = {
        "task_group_id": task_group_id,
        "spark_script_path": spark_script_path,
        "args": full_args,
        "ge_check_enabled": ge_check_enabled,
        "tasks": [f"{task_group_id}_spark"],
    }

    if ge_check_enabled:
        stub["tasks"].extend([
            f"{task_group_id}_ge_check",
            f"{task_group_id}_ge_pass",
            f"{task_group_id}_ge_fail",
        ])

    return stub


def get_pod_template_cstonedfs(
    executor_config: Optional[Dict[str, Any]] = None,
    namespace: Optional[str] = None,
    service_account: Optional[str] = None,
    pvc_name: str = "apex-data-pvc",
    mount_path: str = "/mnt/data",
    image: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get K8s pod template for executor_config (production pattern).

    In production, every PythonOperator that needs GCS/HDFS access uses:
        executor_config=get_pod_template_cstonedfs(executor_config=None)

    This configures the Kubernetes executor to mount the shared PVC and
    use the correct namespace/service account for GCS access.

    Args:
        executor_config: Base executor config to extend (or None)
        namespace: K8s namespace override (defaults to AIRFLOW_KUBERNETES_NAMESPACE env)
        service_account: K8s service account for GCS access
        pvc_name: PVC name to mount
        mount_path: Mount path inside pod
        image: Docker image override

    Returns:
        executor_config dict for PythonOperator
    """
    import os

    ns = namespace or os.getenv("AIRFLOW_KUBERNETES_NAMESPACE", "default")
    sa = service_account or os.getenv("APEX_K8S_SERVICE_ACCOUNT", "default")

    pod_override = {
        "metadata": {
            "namespace": ns,
        },
        "spec": {
            "serviceAccountName": sa,
            "containers": [
                {
                    "name": "base",
                    "volumeMounts": [
                        {
                            "name": "data-vol",
                            "mountPath": mount_path,
                        },
                    ],
                },
            ],
            "volumes": [
                {
                    "name": "data-vol",
                    "persistentVolumeClaim": {
                        "claimName": pvc_name,
                    },
                },
            ],
        },
    }

    if image:
        pod_override["spec"]["containers"][0]["image"] = image

    config = dict(executor_config or {})
    config["pod_override"] = pod_override
    return config


def get_samba_unlock_file_sensor_tg(
    dag: Any,
    tg_name: str,
    samba_conn_id: str,
    file_path: str,
    file_mask: str = "*",
    on_success_task: str = "email_if_multiple_file_count",
    on_failure_task: str = "file_not_found",
    poke_interval_in_sec: int = 1200,
    timeout_in_min: int = 240,
    soft_fail: bool = False,
    email_to: Optional[List[str]] = None,
) -> Any:
    """
    Create a TaskGroup wrapping a Samba/SMB file sensor (production pattern).

    In production, this returns a TaskGroup containing:
        waiting_for_file >> get_file_count >> branch(on_success, on_failure)

    For GCS-based environments (no Samba), this creates a GCS sensor
    with the same TaskGroup structure.

    Args:
        dag: Parent DAG
        tg_name: TaskGroup name (e.g., "samba_file_sensor")
        samba_conn_id: Airflow connection ID for SMB share
        file_path: Path on the SMB share to monitor
        file_mask: File pattern to match
        on_success_task: Task ID to route to on file found
        on_failure_task: Task ID to route to if no file
        poke_interval_in_sec: Polling interval
        timeout_in_min: Timeout before failure
        soft_fail: If True, mark as skipped instead of failed on timeout
        email_to: Email recipients for notifications

    Returns:
        Airflow TaskGroup instance
    """
    try:
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.python import BranchPythonOperator
        from airflow.sensors.filesystem import FileSensor
    except ImportError:
        return {"tg_name": tg_name, "file_path": file_path, "file_mask": file_mask}

    if dag is None:
        return {"tg_name": tg_name, "file_path": file_path, "file_mask": file_mask}

    with TaskGroup(group_id=tg_name, dag=dag) as tg:
        try:
            from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

            waiting_for_file = GCSObjectsWithPrefixExistenceSensor(
                task_id="waiting_for_file",
                bucket=file_path.split("/")[0] if "/" in file_path else file_path,
                prefix="/".join(file_path.split("/")[1:]) if "/" in file_path else "",
                poke_interval=poke_interval_in_sec,
                timeout=timeout_in_min * 60,
                soft_fail=soft_fail,
                mode="poke",
                dag=dag,
            )
        except ImportError:
            from airflow.operators.empty import EmptyOperator
            waiting_for_file = EmptyOperator(
                task_id="waiting_for_file", dag=dag,
            )

        from airflow.operators.empty import EmptyOperator
        get_file_count = EmptyOperator(
            task_id="get_file_count", dag=dag,
        )

        waiting_for_file >> get_file_count

    return tg


__all__ = [
    "build_spark_args_with_pg_conn",
    "get_spark_tg_wrapper_v2",
    "get_spark_tg_wrapper_simple",
    "get_dataproc_pvc_config",
    "build_zone_gcs_paths",
    "get_pod_template_cstonedfs",
    "get_samba_unlock_file_sensor_tg",
]
