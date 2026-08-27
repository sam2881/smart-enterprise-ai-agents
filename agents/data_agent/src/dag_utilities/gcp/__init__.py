"""
APEX GCP-Native Integration Module (Step 7)

Provides GCP-specific configuration and optimization:
- Dataproc Serverless: Auto-terminate, spot VMs, autoscaling
- Cost Labels: Per feed/domain/environment tagging
- CMEK: Customer-Managed Encryption Keys
- Networking: VPC-SC private networking
"""

from .dataproc import DataprocConfig, build_serverless_batch
from .labels import build_cost_labels
from .encryption import CMEKConfig, build_cmek_config

__all__ = [
    "DataprocConfig",
    "build_serverless_batch",
    "build_cost_labels",
    "CMEKConfig",
    "build_cmek_config",
]
