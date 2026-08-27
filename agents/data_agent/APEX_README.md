# APEX Data Agent - Architecture Overview

**APEX** = **A**utonomous **P**ipeline **EX**ecution Engine

This document describes the redesigned data agent architecture following the APEX specification.

## Architecture Components

### 1. PostgreSQL Metadata Schema (`ddl/apex/`)

17+ tables organized across 5 DDL files:

| File | Tables |
|------|--------|
| `01_extensions_and_types.sql` | Extensions, ENUM types |
| `02_core_tables.sql` | platform_connection_registry, platform_domain_registry, platform_source_registry, platform_dag_template, platform_feed_group, feed, platform_spark_config, platform_notification_config |
| `03_contract_and_schema.sql` | platform_data_contract, platform_schema_version, platform_view_definition, platform_transformation_rule, platform_contract_transformation |
| `04_validation_and_quality.sql` | platform_validation_rule, platform_quality_expectation, platform_sla_definition, platform_pipeline_dependency |
| `05_execution_and_logging.sql` | platform_pipeline_execution, platform_task_execution, platform_audit_log, platform_data_lineage, platform_validation_log, platform_error_log, platform_metadata_audit_log, platform_agent_decision_log |

### 2. dag_utilities Module (`src/dag_utilities/`)

8 submodules providing runtime utilities for generated DAGs:

| Submodule | Purpose |
|-----------|---------|
| `core/` | MetadataClient, ExecutionContext, APEXConfig, Exceptions |
| `spark/` | SparkJobSubmitter, ClusterManager, SparkConfigBuilder |
| `storage/` | FileOperations, GCSClient |
| `validation/` | SchemaValidator, SemanticValidator, QualityChecker |
| `notification/` | EmailNotifier, SlackNotifier |
| `logging/` | AuditLogger, MetricsCollector, LineageTracker |
| `remediation/` | SelfHealer, RetryHandler, CircuitBreaker, IncidentManager |

### 3. Core PySpark Jobs (`src/spark_jobs/`)

5 canonical Spark jobs for the medallion architecture:

| Job | Purpose |
|-----|---------|
| `raw_to_bronze.py` | Raw file ingestion to Bronze zone |
| `bronze_schema_validation.py` | Schema validation on Bronze data |
| `promote_bronze_to_silver.py` | Bronze to Silver transformation with view SQL |
| `silver_semantic_validation.py` | Business rule validation on Silver |
| `build_gold_layer.py` | Silver to Gold transformation, SCD2, aggregations |

### 4. Pipeline Pattern Templates (`src/templates/patterns/`)

9 pipeline patterns for different use cases:

| Pattern | Code | Description |
|---------|------|-------------|
| FILE_MEDALLION | P01 | Standard file → Bronze → Silver → Gold |
| BIGDATA_FILE | P02 | Large file with partitioned processing |
| DATABASE_LAKEHOUSE | P03 | Database CDC to lakehouse |
| LEGACY_MIGRATION | P04 | DTSX/COBOL/AS400/EBCDIC migration |
| STREAMING_BATCH | P05 | Kafka/Pub/Sub micro-batch |
| API_SAAS | P06 | REST API/SaaS ingestion |
| SCD2 | P07 | Slowly Changing Dimensions Type 2 |
| DATA_VAULT | P08 | Data Vault 2.0 (Hub, Link, Satellite) |
| STAR_SCHEMA | P09 | Star schema dimensional modeling |

### 5. Canonical Pydantic Models (`src/models/apex_models.py`)

Models mirroring the PostgreSQL schema:

- **Enums**: SourceType, FileFormat, LoadType, ZoneLevel, ValidationType, Severity, ExecutionStatus, PatternCode
- **Registry Models**: ConnectionRegistry, DomainRegistry, SourceRegistry, DAGTemplate, SparkConfig
- **Feed/Contract Models**: FeedGroup, Feed, DataContract
- **Schema/View Models**: SchemaVersion, ViewDefinition, TransformationRule
- **Validation Models**: ValidationRule, QualityExpectation, SLADefinition
- **Execution Models**: PipelineExecution, TaskExecution, AuditLog, ValidationLog, ErrorLog
- **Composite Models**: APEXPipelineConfig, APEXGenerationRequest, APEXGenerationResponse

### 6. Template Selector & Generator (`src/generators/`)

| Component | Purpose |
|-----------|---------|
| `apex_template_selector.py` | Pattern selection based on contract/source/feed configuration |
| `apex_dag_generator.py` | Jinja2 rendering of pattern templates |

### 7. LangGraph Workflow (`src/graphs/apex_workflow.py`)

APEX-specific workflow with phases:

1. `normalize_input` - Convert UI/NL/DTSX to structured
2. `resolve_pattern` - Select pipeline pattern
3. `load_metadata` - Load from PostgreSQL
4. `generate_artifacts` - Generate DAG, Spark, SQL
5. `validate_artifacts` - Syntax, security, schema validation
6. `persist_metadata` - Save to PostgreSQL
7. `await_approval` - Human approval (PROD only)
8. `deploy_artifacts` - Git commit, PR, CI/CD

## Usage

### Pattern Selection

```python
from src.generators.apex_template_selector import APEXTemplateSelector
from src.models.apex_models import DataContract, SourceRegistry, Feed

selector = APEXTemplateSelector()
pattern = selector.select_pattern(
    contract=my_contract,
    source=my_source,
    feed=my_feed,
)
```

### DAG Generation

```python
from src.generators.apex_dag_generator import APEXDAGGenerator
from src.models.apex_models import APEXPipelineConfig

generator = APEXDAGGenerator()
response = generator.generate(config)
print(f"Generated: {response.generated_dag_path}")
```

### Workflow Execution

```python
from src.graphs.apex_workflow import run_apex_workflow_sync

result = run_apex_workflow_sync(
    raw_input=my_input,
    request_id="req-123",
    input_type="ui_structured",
)
```

## Legacy Components

The following components are from the previous architecture and may be phased out:

| Component | Status | Replacement |
|-----------|--------|-------------|
| `src/generators/dag_generator.py` | Active (legacy) | `apex_dag_generator.py` |
| `src/graphs/main_graph.py` | Active (legacy) | `apex_workflow.py` |
| `src/templates/dag/enterprise_pipeline_dag.py.jinja2` | Active (legacy) | Pattern templates in `patterns/` |

## Medallion Architecture

```
┌────────────┐    ┌────────────┐    ┌────────────┐    ┌────────────┐
│    RAW     │───▶│   BRONZE   │───▶│   SILVER   │───▶│    GOLD    │
│ (Landing)  │    │  (Schema)  │    │  (Clean)   │    │ (Business) │
└────────────┘    └────────────┘    └────────────┘    └────────────┘
       │                 │                 │                 │
       │                 │                 │                 │
    STRING           Typed           Validated         Aggregated
    columns         columns          & deduped         & modeled
```

## Key Principles

1. **Metadata-Driven**: All configuration from PostgreSQL, not hardcoded
2. **Pattern-Based**: 9 canonical patterns cover all use cases
3. **Self-Healing**: Automatic remediation for common failures
4. **Observable**: Audit logging, metrics, and lineage tracking
5. **Deterministic**: Same input → Same output (no LLM randomness in generation)
6. **Explicit State**: LangGraph with typed state, no implicit memory
