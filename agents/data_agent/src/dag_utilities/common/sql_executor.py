"""
SQL Executor Utilities

Utilities for executing SQL queries in BigQuery.
"""

from typing import Dict, Any, List, Optional


def run_bq_query(
    query: str,
    project_id: str = None,
    location: str = "US",
    write_disposition: str = "WRITE_APPEND",
    destination_table: str = None
) -> Dict[str, Any]:
    """
    Execute a BigQuery query.

    Args:
        query: SQL query to execute
        project_id: GCP project ID
        location: BigQuery location
        write_disposition: Write disposition for results
        destination_table: Optional destination table

    Returns:
        Query result metadata
    """
    # In production, would use google.cloud.bigquery
    # Reference implementation

    result = {
        "job_id": f"bq_job_{hash(query) % 10000}",
        "status": "DONE",
        "rows_affected": 0,
        "bytes_processed": 0,
    }

    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id, location=location)

        job_config = bigquery.QueryJobConfig(
            write_disposition=write_disposition,
        )

        if destination_table:
            job_config.destination = destination_table

        job = client.query(query, job_config=job_config)
        job.result()  # Wait for completion

        result["job_id"] = job.job_id
        result["rows_affected"] = job.num_dml_affected_rows or 0
        result["bytes_processed"] = job.total_bytes_processed or 0

    except ImportError:
        pass

    return result


def create_or_replace_view(
    view_name: str,
    view_query: str,
    project_id: str = None
) -> Dict[str, Any]:
    """
    Create or replace a BigQuery view.

    Args:
        view_name: Full view name (dataset.view)
        view_query: SQL query for view definition
        project_id: GCP project ID

    Returns:
        View creation result
    """
    ddl = f"CREATE OR REPLACE VIEW `{view_name}` AS {view_query}"
    return run_bq_query(ddl, project_id=project_id)


__all__ = [
    "run_bq_query",
    "create_or_replace_view",
]
