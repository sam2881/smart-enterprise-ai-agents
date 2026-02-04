"""
APEX - Autonomous Pipeline EXecution Engine (Data Agent)

This is the main entry point for the APEX Data Agent package. It provides
a deterministic, metadata-driven pipeline generation system that:
- Accepts validated JSON intent from UI, NL, or DTSX migration
- Selects appropriate pipeline pattern (9 patterns available)
- Generates Airflow DAGs and Spark jobs from templates
- Validates generated artifacts
- Deploys with human approval for production

Key Modules:
    - agents: LangGraph agent implementations
    - graphs: LangGraph workflow definitions (including apex_workflow)
    - state: Pydantic state models
    - models: Canonical Pydantic models (apex_models)
    - generators: Jinja2-based code generators (APEX template selector/generator)
    - dag_utilities: Runtime utilities for generated DAGs (8 submodules)
    - spark_jobs: 5 canonical PySpark jobs
    - templates/patterns: 9 pipeline pattern templates
    - config: Environment configuration
    - utils: Logging and exception utilities

APEX Patterns:
    P01 - FILE_MEDALLION: Standard file ingestion
    P02 - BIGDATA_FILE: Large file with partitioned processing
    P03 - DATABASE_LAKEHOUSE: Database CDC to lakehouse
    P04 - LEGACY_MIGRATION: DTSX/COBOL/AS400/EBCDIC migration
    P05 - STREAMING_BATCH: Kafka/Pub/Sub micro-batch
    P06 - API_SAAS: REST API/SaaS ingestion
    P07 - SCD2: Slowly Changing Dimensions Type 2
    P08 - DATA_VAULT: Data Vault 2.0
    P09 - STAR_SCHEMA: Star schema dimensional modeling
"""

__version__ = "2.0.0"  # APEX version
__author__ = "Enterprise Data Platform Team"
__codename__ = "APEX"

# Re-export commonly used items at package level
from src.config import get_settings, Settings
from src.utils import get_logger, DataAgentError
from src.agents import BaseAgent

# APEX exports
from src.generators import APEXDAGGenerator
from src.repository.registry_manager import RegistryManager
from src.models.apex_models import (
    PatternCode,
    APEXPipelineConfig,
    APEXGenerationRequest,
    APEXGenerationResponse,
    Feed,
    DataContract,
    SourceRegistry,
)

__all__ = [
    # Package metadata
    "__version__",
    "__author__",
    "__codename__",
    # Configuration
    "get_settings",
    "Settings",
    # Logging
    "get_logger",
    "DataAgentError",
    # Agents
    "BaseAgent",
    # APEX Generators & Registry
    "RegistryManager",
    "APEXDAGGenerator",
    # APEX Models
    "PatternCode",
    "APEXPipelineConfig",
    "APEXGenerationRequest",
    "APEXGenerationResponse",
    "Feed",
    "DataContract",
    "SourceRegistry",
]
