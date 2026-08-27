"""
Dataproc Serverless Configuration

Configures Dataproc Serverless Spark batch jobs with:
- Auto-terminate after job completion
- Spot/preemptible VM support
- Autoscaling policies
- Network and subnet configuration
- Runtime version management
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DataprocConfig:
    """Configuration for Dataproc Serverless batch jobs."""
    project_id: str = ""
    region: str = "us-central1"
    subnet: str = ""
    service_account: str = ""
    runtime_version: str = "2.1"
    spark_version: str = "3.4"

    # Resource allocation
    executor_memory: str = "8g"
    executor_cores: int = 4
    driver_memory: str = "4g"
    driver_cores: int = 2
    min_executors: int = 2
    max_executors: int = 20

    # Network
    network: str = ""
    kms_key: str = ""

    # Labels
    labels: Dict[str, str] = field(default_factory=dict)

    # Spark properties
    spark_properties: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_environment(cls) -> "DataprocConfig":
        """Create config from environment variables."""
        return cls(
            project_id=os.getenv("GCP_PROJECT", ""),
            region=os.getenv("GCP_REGION", "us-central1"),
            subnet=os.getenv("DATAPROC_SUBNET", ""),
            service_account=os.getenv("DATAPROC_SERVICE_ACCOUNT", ""),
            runtime_version=os.getenv("DATAPROC_RUNTIME_VERSION", "2.1"),
            network=os.getenv("DATAPROC_NETWORK", ""),
            kms_key=os.getenv("DATAPROC_KMS_KEY", ""),
        )

    @classmethod
    def from_metadata(
        cls,
        feed_config: Dict[str, Any],
        environment: str = "dev"
    ) -> "DataprocConfig":
        """Create config from feed metadata."""
        config = cls.from_environment()

        config.executor_memory = feed_config.get("spark_executor_memory", "8g")
        config.executor_cores = feed_config.get("spark_executor_cores", 4)
        config.labels = {
            "feed_id": str(feed_config.get("feed_id", "")),
            "domain": feed_config.get("domain", ""),
            "environment": environment,
            "feed_name": feed_config.get("feed_name", ""),
        }

        return config


def build_serverless_batch(
    feed_id: int,
    zone: str,
    batch_id: str,
    metadata_url: str,
    config: Optional[DataprocConfig] = None,
    spark_jobs_bucket: str = "apex-spark-jobs",
) -> Dict[str, Any]:
    """
    Build Dataproc Serverless batch job configuration.

    Args:
        feed_id: Numeric feed ID
        zone: Zone to process
        batch_id: Batch identifier
        metadata_url: PostgreSQL connection URL
        config: Dataproc configuration
        spark_jobs_bucket: GCS bucket containing Spark jobs

    Returns:
        Batch job configuration dict for Dataproc Serverless API
    """
    config = config or DataprocConfig.from_environment()

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": (
                f"gs://{spark_jobs_bucket}/dag_utilities/processors/zone_processor.py"
            ),
            "args": [
                "--feed-id", str(feed_id),
                "--zone", zone,
                "--batch-id", batch_id,
                "--metadata-url", metadata_url,
            ],
            "python_file_uris": [
                f"gs://{spark_jobs_bucket}/dag_utilities.zip",
            ],
        },
        "runtime_config": {
            "version": config.runtime_version,
            "properties": {
                "spark.executor.memory": config.executor_memory,
                "spark.executor.cores": str(config.executor_cores),
                "spark.driver.memory": config.driver_memory,
                "spark.driver.cores": str(config.driver_cores),
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.minExecutors": str(config.min_executors),
                "spark.dynamicAllocation.maxExecutors": str(config.max_executors),
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                **config.spark_properties,
            },
        },
        "environment_config": {},
        "labels": {
            **config.labels,
            "zone": zone,
            "batch_id": batch_id,
        },
    }

    # Add network config
    if config.subnet:
        batch["environment_config"]["execution_config"] = {
            "subnetwork_uri": config.subnet,
            "service_account": config.service_account,
        }

    # Add CMEK config
    if config.kms_key:
        batch["environment_config"]["execution_config"] = batch["environment_config"].get(
            "execution_config", {}
        )
        batch["environment_config"]["execution_config"]["kms_key"] = config.kms_key

    return batch


def build_dataproc_job(
    feed_id: int,
    zone: str,
    batch_id: str,
    metadata_url: str,
    config: Optional[DataprocConfig] = None,
    cluster_name: str = "apex-dataproc",
    spark_jobs_bucket: str = "apex-spark-jobs",
) -> Dict[str, Any]:
    """
    Build traditional Dataproc cluster job configuration.

    Fallback for when Serverless is not available.
    """
    config = config or DataprocConfig.from_environment()

    return {
        "reference": {"project_id": config.project_id},
        "placement": {"cluster_name": cluster_name},
        "labels": {
            **config.labels,
            "zone": zone,
            "batch_id": batch_id,
        },
        "pyspark_job": {
            "main_python_file_uri": (
                f"gs://{spark_jobs_bucket}/dag_utilities/processors/zone_processor.py"
            ),
            "args": [
                "--feed-id", str(feed_id),
                "--zone", zone,
                "--batch-id", batch_id,
                "--metadata-url", metadata_url,
            ],
            "python_file_uris": [
                f"gs://{spark_jobs_bucket}/dag_utilities.zip",
            ],
            "properties": {
                "spark.executor.memory": config.executor_memory,
                "spark.executor.cores": str(config.executor_cores),
                "spark.dynamicAllocation.enabled": "true",
                "spark.sql.adaptive.enabled": "true",
            },
        },
    }


__all__ = [
    "DataprocConfig",
    "build_serverless_batch",
    "build_dataproc_job",
]
