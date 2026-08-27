"""
File Source Utilities

Utilities for file-based sources (GCS, S3, local).
"""

from typing import Any, List, Optional
from datetime import datetime


def get_gcs_file_sensor(
    bucket: str,
    prefix: str,
    pattern: str = "*",
    timeout: int = 3600
) -> dict:
    """
    Create GCS file sensor configuration.

    Args:
        bucket: GCS bucket name
        prefix: Object prefix
        pattern: File pattern to match
        timeout: Sensor timeout in seconds

    Returns:
        Sensor configuration dictionary
    """
    return {
        "bucket": bucket,
        "prefix": prefix,
        "pattern": pattern,
        "timeout": timeout,
    }


def list_source_files(
    bucket: str,
    prefix: str,
    pattern: str = "*"
) -> List[str]:
    """
    List files in GCS matching pattern.

    Args:
        bucket: GCS bucket name
        prefix: Object prefix
        pattern: Glob pattern to match

    Returns:
        List of matching file paths
    """
    # In production, this would use google.cloud.storage
    # Simulated implementation for reference

    try:
        from google.cloud import storage
        import fnmatch

        client = storage.Client()
        bucket_obj = client.bucket(bucket)
        blobs = bucket_obj.list_blobs(prefix=prefix)

        files = []
        for blob in blobs:
            if fnmatch.fnmatch(blob.name, pattern):
                files.append(f"gs://{bucket}/{blob.name}")

        return files

    except ImportError:
        # Fallback for environments without GCS client
        return []


def archive_files(
    source_bucket: str,
    source_prefix: str,
    archive_bucket: str,
    batch_id: str
) -> int:
    """
    Archive processed files to archive bucket.

    Args:
        source_bucket: Source GCS bucket
        source_prefix: Source prefix
        archive_bucket: Archive GCS bucket
        batch_id: Batch ID for archive organization

    Returns:
        Number of files archived
    """
    # In production, this would move files to archive
    # Simulated implementation for reference

    archived_count = 0

    try:
        from google.cloud import storage

        client = storage.Client()
        source_bucket_obj = client.bucket(source_bucket)
        archive_bucket_obj = client.bucket(archive_bucket)

        blobs = source_bucket_obj.list_blobs(prefix=source_prefix)

        for blob in blobs:
            # Create archive path
            archive_path = f"{batch_id}/{blob.name}"

            # Copy to archive
            source_bucket_obj.copy_blob(
                blob,
                archive_bucket_obj,
                archive_path
            )

            # Delete original
            blob.delete()

            archived_count += 1

    except ImportError:
        pass

    return archived_count


def create_gcs_file_sensor(
    dag: Any = None,
    task_id: str = "wait_for_file",
    bucket: str = "",
    object_name: str = "",
    timeout: int = 3600,
    poke_interval: int = 300,
    mode: str = "poke",
) -> Any:
    """
    Create a real GCSObjectExistenceSensor operator for Airflow DAGs.

    Falls back to a config dict if Airflow GCS provider is not available.

    Args:
        dag: Parent Airflow DAG
        task_id: Task ID for the sensor
        bucket: GCS bucket name
        object_name: Object path/prefix to watch for
        timeout: Sensor timeout in seconds
        poke_interval: Seconds between checks
        mode: Sensor mode ('poke' or 'reschedule')

    Returns:
        GCSObjectExistenceSensor or config dict
    """
    try:
        from airflow.providers.google.cloud.sensors.gcs import (
            GCSObjectExistenceSensor,
        )

        return GCSObjectExistenceSensor(
            task_id=task_id,
            bucket=bucket,
            object=object_name,
            timeout=timeout,
            poke_interval=poke_interval,
            mode=mode,
            dag=dag,
        )
    except ImportError:
        return {
            "sensor_type": "gcs_object_existence",
            "task_id": task_id,
            "bucket": bucket,
            "object": object_name,
            "timeout": timeout,
            "poke_interval": poke_interval,
            "mode": mode,
        }


def count_source_files(
    bucket: str,
    prefix: str,
    pattern: str = "*",
) -> int:
    """
    Count files matching pattern in a GCS prefix.

    Used by send_notification_file_count for file processing summaries.

    Args:
        bucket: GCS bucket name
        prefix: Object prefix
        pattern: Glob pattern to match

    Returns:
        Number of matching files
    """
    try:
        from google.cloud import storage
        import fnmatch

        client = storage.Client()
        bucket_obj = client.bucket(bucket)
        blobs = bucket_obj.list_blobs(prefix=prefix)

        count = 0
        for blob in blobs:
            file_name = blob.name.split("/")[-1]
            if file_name and fnmatch.fnmatch(file_name, pattern):
                count += 1
        return count

    except ImportError:
        return 0


def download_to_pvc(
    bucket: str,
    prefix: str,
    local_path: str = "/mnt/data",
    pattern: str = "*",
) -> List[str]:
    """
    Download files from GCS to local Dataproc PVC (Persistent Volume Claim).

    In production, ALL files land on PVC first before Spark processes them.
    This is the GCS → PVC step in the flow:
        Source GCS → PVC (local disk) → Spark processing → Zone GCS bucket

    Args:
        bucket: Source GCS bucket name
        prefix: GCS object prefix
        local_path: Local PVC mount path on Dataproc node
        pattern: Glob pattern to filter files

    Returns:
        List of local file paths downloaded to PVC
    """
    downloaded = []

    try:
        from google.cloud import storage
        import fnmatch
        import os

        client = storage.Client()
        bucket_obj = client.bucket(bucket)
        blobs = bucket_obj.list_blobs(prefix=prefix)

        os.makedirs(local_path, exist_ok=True)

        for blob in blobs:
            file_name = blob.name.split("/")[-1]
            if not file_name or not fnmatch.fnmatch(file_name, pattern):
                continue

            local_file = os.path.join(local_path, file_name)
            blob.download_to_filename(local_file)
            downloaded.append(local_file)

    except ImportError:
        pass

    return downloaded


def upload_from_pvc(
    local_path: str,
    dest_bucket: str,
    dest_prefix: str,
    pattern: str = "*",
    delete_local: bool = False,
) -> List[str]:
    """
    Upload files from local Dataproc PVC to a GCS zone bucket.

    This is the PVC → Zone GCS step in the flow:
        Source GCS → PVC (local disk) → Spark processing → Zone GCS bucket

    Args:
        local_path: Local PVC directory path
        dest_bucket: Destination GCS bucket name
        dest_prefix: Destination GCS prefix
        pattern: Glob pattern to filter files
        delete_local: Whether to delete local files after upload

    Returns:
        List of GCS URIs uploaded
    """
    uploaded = []

    try:
        from google.cloud import storage
        import fnmatch
        import os

        client = storage.Client()
        bucket_obj = client.bucket(dest_bucket)

        if not os.path.isdir(local_path):
            return uploaded

        for file_name in os.listdir(local_path):
            if not fnmatch.fnmatch(file_name, pattern):
                continue

            local_file = os.path.join(local_path, file_name)
            if not os.path.isfile(local_file):
                continue

            gcs_path = f"{dest_prefix}/{file_name}" if dest_prefix else file_name
            blob = bucket_obj.blob(gcs_path)
            blob.upload_from_filename(local_file)
            uploaded.append(f"gs://{dest_bucket}/{gcs_path}")

            if delete_local:
                os.remove(local_file)

    except ImportError:
        pass

    return uploaded


__all__ = [
    "get_gcs_file_sensor",
    "list_source_files",
    "archive_files",
    "create_gcs_file_sensor",
    "count_source_files",
    "download_to_pvc",
    "upload_from_pvc",
]
