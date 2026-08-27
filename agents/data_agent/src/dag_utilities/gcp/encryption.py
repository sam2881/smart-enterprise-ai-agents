"""
GCP CMEK (Customer-Managed Encryption Keys) Module

All data at rest encrypted with CMEK:
- GCS buckets: Encrypted with KMS key
- BigQuery datasets: Encrypted with KMS key
- Dataproc: Boot disk and data disk encryption

Key URI format:
    projects/{project}/locations/{location}/keyRings/{ring}/cryptoKeys/{key}
"""

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class CMEKConfig:
    """CMEK encryption configuration."""
    project_id: str = ""
    location: str = "us-central1"
    key_ring: str = "apex-keyring"
    key_name: str = "apex-data-key"

    @property
    def key_uri(self) -> str:
        """Full KMS key URI."""
        if not self.project_id:
            return ""
        return (
            f"projects/{self.project_id}/locations/{self.location}"
            f"/keyRings/{self.key_ring}/cryptoKeys/{self.key_name}"
        )

    @classmethod
    def from_environment(cls) -> "CMEKConfig":
        """Create config from environment variables."""
        return cls(
            project_id=os.getenv("GCP_PROJECT", ""),
            location=os.getenv("GCP_REGION", "us-central1"),
            key_ring=os.getenv("KMS_KEY_RING", "apex-keyring"),
            key_name=os.getenv("KMS_KEY_NAME", "apex-data-key"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "project_id": self.project_id,
            "location": self.location,
            "key_ring": self.key_ring,
            "key_name": self.key_name,
            "key_uri": self.key_uri,
        }


def build_cmek_config(
    project_id: Optional[str] = None,
    location: Optional[str] = None,
    key_ring: Optional[str] = None,
    key_name: Optional[str] = None,
) -> CMEKConfig:
    """Build CMEK config from parameters or environment."""
    config = CMEKConfig.from_environment()

    if project_id:
        config.project_id = project_id
    if location:
        config.location = location
    if key_ring:
        config.key_ring = key_ring
    if key_name:
        config.key_name = key_name

    return config


def get_gcs_encryption_config(cmek: CMEKConfig) -> Dict[str, Any]:
    """Get GCS bucket encryption configuration."""
    if not cmek.key_uri:
        return {}
    return {
        "encryption": {
            "default_kms_key_name": cmek.key_uri,
        }
    }


def get_bigquery_encryption_config(cmek: CMEKConfig) -> Dict[str, Any]:
    """Get BigQuery dataset/table encryption configuration."""
    if not cmek.key_uri:
        return {}
    return {
        "encryption_configuration": {
            "kms_key_name": cmek.key_uri,
        }
    }


def get_dataproc_encryption_config(cmek: CMEKConfig) -> Dict[str, Any]:
    """Get Dataproc cluster encryption configuration."""
    if not cmek.key_uri:
        return {}
    return {
        "encryption_config": {
            "gce_pd_kms_key_name": cmek.key_uri,
        }
    }


__all__ = [
    "CMEKConfig",
    "build_cmek_config",
    "get_gcs_encryption_config",
    "get_bigquery_encryption_config",
    "get_dataproc_encryption_config",
]
