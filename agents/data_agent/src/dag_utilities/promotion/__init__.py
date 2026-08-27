"""
DAG Utilities Promotion Module

Part of the APEX Control Plane for pipeline lifecycle management.
Provides environment promotion capabilities for promoting data pipelines
across environments (dev -> qa -> prod) with validation gates, human
approval enforcement for production, and compliance audit trails.

Usage:
    from dag_utilities.promotion import EnvironmentPromoter, PromotionRequest

    promoter = EnvironmentPromoter(metadata_client)
    request = PromotionRequest(
        feed_id=42,
        source_environment="dev",
        target_environment="qa",
        promoted_by="user@company.com",
        jira_ticket="DATA-1234",
    )
    result = promoter.promote(request)
"""

from .environment_promoter import (
    EnvironmentPromoter,
    PromotionRequest,
    PromotionResult,
)

__all__ = [
    "EnvironmentPromoter",
    "PromotionRequest",
    "PromotionResult",
]
