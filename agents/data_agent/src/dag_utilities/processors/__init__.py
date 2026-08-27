"""
APEX Data Plane - Zone Processors

This module contains the DATA PLANE components that EXECUTE (never decide).

Key Principle: Same zone_processor.py handles ALL zones (Transient, Raw, Refined, Gold, Consumption).
Behavior changes ONLY via metadata, never code edits.

Zone Processing Pattern (UNIFORM FOR ALL ZONES):
1. Read Metadata      → MetadataClient.get_zone_config(feed_id, zone)
2. Read Source        → SparkSession.read from previous zone/source
3. GE Schema Validate → validate_schema(df, schema_expectation_suite)
4. Transform          → Apply zone-specific transforms from metadata
5. GE Semantic Valid  → validate_semantic(df, semantic_rules)
6. Write Target       → df.write with partition/clustering from metadata
7. Update Lineage     → Record zone metrics in feed_ingestion_details
"""

from .zone_processor import (
    process_zone,
    submit_zone_job,
    ZoneProcessorConfig,
)

__all__ = [
    "process_zone",
    "submit_zone_job",
    "ZoneProcessorConfig",
]
