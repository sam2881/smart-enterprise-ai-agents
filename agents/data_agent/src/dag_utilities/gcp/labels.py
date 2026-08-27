"""
GCP Cost Labels Module

All resources tagged with feed_id, domain, environment for cost tracking.

Label format rules (GCP):
- Keys: lowercase letters, digits, underscores, hyphens (max 63 chars)
- Values: lowercase letters, digits, underscores, hyphens (max 63 chars)
- Max 64 labels per resource
"""

from typing import Any, Dict, Optional


def build_cost_labels(
    feed_id: int,
    domain: str,
    environment: str = "dev",
    feed_name: str = "",
    pattern_code: str = "",
    owner: str = "",
    extra_labels: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """
    Build GCP cost labels from platform_feed metadata.

    Args:
        feed_id: Numeric feed ID
        domain: Business domain
        environment: dev/qa/prod
        feed_name: Feed name
        pattern_code: APEX pattern code (P01-P09)
        owner: Owner team
        extra_labels: Additional custom labels

    Returns:
        Dict of sanitized GCP labels
    """
    labels = {
        "apex_feed_id": _sanitize_label(str(feed_id)),
        "apex_domain": _sanitize_label(domain),
        "apex_environment": _sanitize_label(environment),
        "apex_managed": "true",
    }

    if feed_name:
        labels["apex_feed_name"] = _sanitize_label(feed_name)
    if pattern_code:
        labels["apex_pattern"] = _sanitize_label(pattern_code)
    if owner:
        labels["apex_owner"] = _sanitize_label(owner)

    if extra_labels:
        for key, value in extra_labels.items():
            labels[_sanitize_label(key)] = _sanitize_label(value)

    return labels


def build_labels_from_metadata(
    feed_config: Dict[str, Any],
    environment: str = "dev"
) -> Dict[str, str]:
    """Build labels from platform_feed metadata dict."""
    return build_cost_labels(
        feed_id=feed_config.get("feed_id", 0),
        domain=feed_config.get("domain", ""),
        environment=environment,
        feed_name=feed_config.get("feed_name", ""),
        pattern_code=feed_config.get("pattern_code", ""),
        owner=feed_config.get("owner", ""),
    )


def _sanitize_label(value: str) -> str:
    """
    Sanitize a label value per GCP requirements.

    - Lowercase
    - Only letters, digits, underscores, hyphens
    - Max 63 characters
    """
    sanitized = value.lower().strip()
    sanitized = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in sanitized)
    return sanitized[:63]


__all__ = [
    "build_cost_labels",
    "build_labels_from_metadata",
]
