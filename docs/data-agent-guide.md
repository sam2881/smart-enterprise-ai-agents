# APEX Data Engineering Agent — End-to-End Guide

How the Data Agent takes instructions from the UI and generates production-ready Airflow DAGs.

---

## High-Level Flow

```
 User (Browser)          Frontend (Next.js)           API (FastAPI)            LangGraph Workflow           Jinja2 Engine            Git / Airflow
 ============           ================           =============           ====================           =============           ==============
      |                       |                          |                          |                          |                       |
      |  Fill pipeline form   |                          |                          |                          |                       |
      |  (source, schema,     |                          |                          |                          |                       |
      |   target, pattern)    |                          |                          |                          |                       |
      | --------------------> |                          |                          |                          |                       |
      |                       |  POST /pipelines         |                          |                          |                       |
      |                       |  UnifiedPipelineInput    |                          |                          |                       |
      |                       | -----------------------> |                          |                          |                       |
      |                       |                          |  Start background task   |                          |                       |
      |                       |  { request_id, pending } |                          |                          |                       |
      |                       | <----------------------- |                          |                          |                       |
      |                       |                          |  Run 9-node workflow     |                          |                       |
      |                       |                          | -----------------------> |                          |                       |
      |                       |                          |                          |  Render DAG template     |                       |
      |                       |                          |                          | -----------------------> |                       |
      |                       |                          |                          |  Generated Python DAG    |                       |
      |                       |                          |                          | <----------------------- |                       |
      |                       |                          |                          |  Commit + PR             |                       |
      |                       |                          |                          | --------------------------------------------->  |
      |                       |                          |  { status: completed }   |                          |                       |
      |                       |                          | <----------------------- |                          |                       |
      |                       |  Poll GET /pipelines/id  |                          |                          |                       |
      |                       | -----------------------> |                          |                          |                       |
      |  Pipeline ready!      |                          |                          |                          |  Airflow picks up DAG |
      | <-------------------- |                          |                          |                          |                       |
```

---

## The 10 Steps

### Step 1 — User Fills the Form

The user opens `http://localhost:3000/pipelines` and sees `UnifiedPipelineForm` with 3 input modes:

| Mode | How It Works |
|------|-------------|
| **UI Structured** | Fill sections: source type, schema, target, execution policy |
| **Natural Language** | Type a description; LLM converts it to structured config; user previews and approves |
| **DTSX Migration** | Upload an SSIS `.dtsx` file; parser extracts sources, transforms, destinations |

For UI Structured mode, the form has these sections:

1. **Pipeline Identity** — DAG ID, domain, environment, schedule
2. **Source Type** — Pick from 70+ sources across 9 categories using `SourceTypeSelector`
3. **Source Config** — Type-specific form (file path, DB connection, API endpoint, etc.)
4. **DAG Pattern** — Pick P01-P09 using `PatternSelector` (or let the system recommend)
5. **Schema** — Define columns, data types, nullable flags using `SchemaInputPanel`
6. **Target** — BigQuery dataset, table, write mode, target zone using `ZoneIntentPanel`
7. **Gold Modeling** — Star Schema, Data Vault, SCD2, or Flat Table via `GoldModelingSelector`
8. **Joins** — Multi-table join definitions via `JoinDependencyBuilder` (left/right table, keys, join type)
9. **Execution Policy** — Schedule interval, retry count, processing mode via `ExecutionPolicyPanel`

### Step 2 — Frontend Builds the Payload

On submit, the form constructs a `UnifiedPipelineInput` JSON object:

```json
{
  "input_type": "ui_structured",
  "created_by": "user@company.com",
  "jira_ticket": "DATA-1234",
  "pipeline": {
    "dag_id": "sales_daily_pipeline",
    "domain": "sales",
    "environment": "dev",
    "pattern_code": "P01",
    "feed_id": "FEED_SALES_CSV",
    "contract_id": "CTR_SALES_001"
  },
  "source": {
    "source_type": "file_csv",
    "file_config": {
      "gcs_path": "gs://data-lake/raw/sales/",
      "delimiter": ",",
      "header": true
    }
  },
  "schema": {
    "columns": [
      { "name": "customer_id", "type": "string", "nullable": false },
      { "name": "amount", "type": "decimal", "nullable": false }
    ]
  },
  "target": {
    "target_zone": "gold",
    "bq_dataset": "sales_data",
    "bq_table": "daily_sales",
    "write_mode": "append"
  },
  "joins": [
    {
      "right_table": "silver.customer_master",
      "join_type": "left",
      "join_keys": [{"left": "customer_id", "right": "customer_id"}],
      "join_order": 1
    }
  ],
  "execution_policy": {
    "schedule_interval": "@daily",
    "retry_count": 2
  }
}
```

TypeScript types in `pipeline-canonical.ts` mirror the backend Pydantic models exactly.

### Step 3 — API Receives the Request

`POST /pipelines` on the Data Agent API (FastAPI, port 8001):

1. Generates a UUID `request_id`
2. Stores initial state as `"pending"`
3. Kicks off a **background task** to run the LangGraph workflow
4. Returns `{ request_id, status: "pending" }` immediately

The frontend polls `GET /pipelines/{request_id}` to track progress.

### Step 4 — Input Normalization

`InputDispatcher` routes the input to the right normalizer:

| Input Mode | Normalizer | What It Does |
|------------|-----------|--------------|
| `ui_structured` | `UIInputNormalizer` | Direct 1:1 field mapping |
| `natural_language` | `NLInputNormalizer` | LLM converts NL to structured metadata (including join extraction) |
| `dtsx_migration` | `DTSXNormalizer` | XML parser extracts SSIS components |

All 3 normalizers produce the same output: a canonical `PipelineMetadata` object.

**NL Join Extraction**: When a user writes "join orders with customers on customer_id, keep all orders", the `NLTransformProcessor._extract_join_config()` detects the join intent, extracts the table name (`customers`), join key (`customer_id`), and join type (`left` — inferred from "keep all orders"). The LLM also outputs structured `joins` in its response, which gets passed through `UnifiedPipelineInput.joins`.

### Step 5 — LangGraph Workflow (9 Nodes)

The workflow is a **LangGraph StateGraph** — an explicit state machine, not a ReAct loop:

```
normalize_input
      |
      V
resolve_pattern -----> Selects P01-P09 from registry based on source type + contract type
      |
      V
load_metadata -------> Loads enterprise defaults from PostgreSQL
      |
      V
generate_artifacts --> Calls APEXDAGGenerator to render Jinja2 template
      |
      V
validate_artifacts --> Syntax check, import check, security check
      |
      V
persist_metadata ----> Saves pipeline config + join dependencies to PostgreSQL
      |
      +--- PROD? ---> await_approval (pauses for human approval)
      |                     |
      V                     V
deploy_artifacts ----> Git commit, push, create PR
```

If any node fails, the workflow routes to `handle_error_node` (cleanup + log).

**State Definition** (`APEXWorkflowState`):

| Field | Type | Purpose |
|-------|------|---------|
| `request_id` | str | Unique request identifier |
| `current_phase` | str | init → normalized → pattern_resolved → metadata_loaded → artifacts_generated → validated → metadata_persisted → awaiting_approval → deployed |
| `raw_input` | Dict | Original user input |
| `input_type` | str | ui_structured, natural_language, dtsx_migration |
| `generation_request` | Dict | Normalized pipeline config |
| `selected_pattern` | str | P01-P09 |
| `validation_passed` | bool | Did artifact validation pass? |
| `requires_approval` | bool | PROD requires human approval |
| `error_message` | str | Set on failure, routes to error handler |
| `audit_trail` | list | All decisions logged |

### Step 6 — Pattern Selection

The registry (`registry.json`, APEX v2.1.0) defines 9 DAG patterns. `RegistryManager` selects by:

1. **Explicit** — If `pattern_code` is set in the request, use it directly
2. **Contract type** mapping: SCD2 → P07, DATA_VAULT → P08, STAR_SCHEMA → P09
3. **Source type** mapping: FILE → P01 (or P02 if >10GB), DATABASE → P03, STREAMING → P05, API → P06, LEGACY → P04
4. **Default** → P01 (File Medallion)

| Pattern | Name | When Selected |
|---------|------|---------------|
| P01 | File Medallion | CSV/JSON/Parquet with STANDARD contract |
| P02 | Big Data File | Files > 10 GB (auto-scaling, partitioned reads) |
| P03 | Database Lakehouse | JDBC sources with CDC/incremental/watermark |
| P04 | Legacy Migration | DTSX, COBOL, EBCDIC, AS400 |
| P05 | Streaming Batch | Kafka, Pub/Sub, Kinesis (offset management) |
| P06 | API SaaS | REST, GraphQL, Salesforce (pagination, OAuth) |
| P07 | SCD Type 2 | Any source with SCD2 contract (hash-based change detection) |
| P08 | Data Vault 2.0 | Hub + Link + Satellite loading |
| P09 | Star Schema | Fact + Dimension loading with surrogate keys |

### Step 7 — DAG Generation

`APEXDAGGenerator` in `apex_dag_generator.py`:

1. Resolves the selected pattern to a Jinja2 template file (e.g., `p01_file_medallion.py.jinja2`)
2. Builds a **context dict** with all template variables (feed_id, source config, schema, target, dependencies, etc.)
3. Renders the template into a Python DAG file
4. Writes to `dags/generated/{dag_id}.py`

The generated DAG is **metadata-driven** — it contains identity (feed_id, contract_id) but fetches all business logic from PostgreSQL at runtime via `MetadataClient`.

**Template Features** (all 9 patterns):

| Feature | How It Works |
|---------|-------------|
| **Dependency sensors** | `{% if dependencies %}` block generates `ExternalTaskSensor` for cross-DAG dependencies |
| **GE validation** | `pipeline_tasks.run_ge_validation()` as `BranchPythonOperator` (pass → next zone, fail → handle_validation_failure) |
| **Self-healing** | `SelfHealer().on_task_failure` callback on every task |
| **Retry handler** | `RetryHandler(max_retries=N).get_airflow_retry_args()` for exponential backoff |
| **Lineage tracking** | `LineageTracker().track_pipeline_lineage()` in finalize task |
| **Notifications** | `pipeline_tasks.send_notifications()` on ALL_DONE |

### Step 8 — Validation

Before deployment, the generated code is validated:

- **Syntax** — Python compiles without errors
- **Imports** — Required airflow and dag_utilities imports present
- **DAG ID** — DAG ID string appears in the code
- **Security** — No hardcoded secrets, no `os.system()` calls
- **Pattern** — All required variables for the selected pattern are present

### Step 9 — Approval (PROD Only)

For production deployments, the workflow pauses at `await_approval`. A notification is sent, and the approver reviews the generated DAG on the Approvals page (`/approvals`). Once approved via `POST /pipelines/{id}/approve`, the workflow resumes.

DEV and QA environments skip this step.

### Step 10 — Deployment

`GitClient` handles deployment:

1. Creates a feature branch: `feature/{dag_id}_{timestamp}`
2. Commits the generated DAG + metadata SQL
3. Pushes to remote (`enterprise-data-pipelines` repo)
4. Creates a Pull Request via GitHub API
5. CI/CD validates (DagBag import test)
6. After merge, `dag-sync` sidecar (alpine/git, polling every 60s) syncs DAGs + spark_jobs to Airflow

---

## What the Generated DAG Does at Runtime

Once deployed, the DAG runs this task flow in Airflow:

```
start
  |
  +--> [wait_upstream_*] ........... ExternalTaskSensor (if cross-pipeline dependencies exist)
  |
  +--> initialize_execution ........ Create execution record in PostgreSQL (with auto-incrementing sequence)
  |-->  check_source_file .......... Verify source data exists in GCS/DB/API
  |-->  raw_to_bronze .............. PySpark: read raw, schema evolution check, type cast, audit fields, write Delta
  |-->  bronze_schema_validation ... PySpark + Great Expectations: validate types, nullability, PKs
  |         |
  |         +--[PASS]--> bronze_to_silver .......... PySpark: apply Silver view SQL, deduplicate, business keys
  |         +--[FAIL]--> handle_validation_failure
  |
  |-->  silver_semantic_validation . PySpark + Great Expectations: business rules, referential integrity
  |         |
  |         +--[PASS]--> silver_to_gold ............ PySpark: resolve joins, aggregations, SCD2, Gold view SQL
  |         +--[FAIL]--> handle_validation_failure
  |
  |-->  run_quality_checks ......... Final quality gate (weighted score, 0-100)
  |-->  finalize_execution ......... Update status, lineage tracking, OpenLineage emit
  |-->  send_notifications ......... Email/Slack on success or failure
  |-->  end
```

### Idempotent Re-Runs

If the same `execution_date` is re-run, each Spark job **deletes existing data** for that date before writing:

```python
# In each zone job (raw_to_bronze, bronze_to_silver, silver_to_gold):
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, contract_config["bronze_table"])
dt.delete(f"_execution_date = '{execution_date}'")
```

The `platform_pipeline_execution` table tracks re-runs with an auto-incrementing `sequence` column per `(feed_id, execution_date)`. The `_run_id` (UUID) stays in all data tables; the sequence is PostgreSQL metadata only.

### Multi-Table Joins in Gold Zone

When `platform_join_dependency` entries exist for a contract, `build_gold_layer.py` calls `join_executor.py` before Gold processing:

```python
# Step 0: Resolve join dependencies
from spark_jobs.join_executor import execute_joins, load_join_dependencies

join_configs = load_join_dependencies(spark, metadata_db, contract_id)
if join_configs:
    gold_df = execute_joins(spark, base_df, join_configs, execution_id)
```

The join executor supports INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI joins with **grain verification** (fanout detection) after each join. Join definitions come from the `platform_join_dependency` metadata table.

### Schema Evolution

`raw_to_bronze.py` validates incoming data schema against the registered schema before writing:

| Policy | Behavior |
|--------|----------|
| `STRICT` | Fail on any column added, removed, or type-changed |
| `ADDITIVE` | Allow new columns, fail on removed or type-changed |
| `FLEXIBLE` | Allow all changes, log warnings |

### Audit Fields

Every record gets these mandatory audit fields:

| Field | Added By | Purpose |
|-------|----------|---------|
| `_run_id` | All zones | UUID of the execution run |
| `_record_uuid` | All zones | Unique UUID per row |
| `_reporting_date` | All zones | Business reporting date |
| `_load_timestamp` | All zones | When data was loaded |
| `_execution_date` | Bronze | Partition key for idempotent re-runs |
| `_source_filename` | Bronze | Original source file name |
| `_silver_execution_id` | Silver | Links back to execution |
| `_business_key` | Silver | MD5 hash of primary key columns |
| `_is_valid` | Silver | Validation pass flag |
| `_gold_execution_id` | Gold | Links back to execution |

Corrupt records are separated into a `{table}_rejects` path automatically.

---

## The 5 Canonical Spark Jobs

All Spark jobs are **metadata-driven** — they fetch config from PostgreSQL at runtime via JDBC.

### 1. raw_to_bronze.py

| Aspect | Detail |
|--------|--------|
| **Source** | Raw zone (any file format: CSV, JSON, Parquet, Avro, ORC, XML, Excel, EBCDIC, Fixed-Width) |
| **Target** | Bronze (strongly typed, Delta Lake) |
| **Operations** | Schema evolution check → Type casting → Trim whitespace → Corrupt record separation → Audit columns → Idempotent delete → DLQ routing → Write (Delta/Iceberg/Parquet) |
| **Table Formats** | Delta (default), Iceberg, Parquet — controlled by `contract_config.table_format` |
| **DLQ** | Rejected records routed to `dlq_path` in GCS if configured |
| **Partitioned By** | `_execution_date` |
| **Validation** | Optional `--validate` flag runs post-ingest GE row-count check |

### 2. bronze_schema_validation.py

| Aspect | Detail |
|--------|--------|
| **Source** | Bronze zone |
| **Operations** | Column presence → Not-null → Primary key uniqueness → Data type validation → Custom GE expectations |
| **Output** | Quality score (0-100), validation results to `platform_validation_result` table |
| **Branch** | Pass → bronze_to_silver; Fail → handle_validation_failure |

### 3. promote_bronze_to_silver.py

| Aspect | Detail |
|--------|--------|
| **Source** | Bronze zone |
| **Target** | Silver (cleaned, deduplicated, Delta Lake) |
| **Operations** | Apply Silver view SQL → Transformation rules (CLEANSING, DERIVATION, LOOKUP) → **PII detection + masking enforcement** (Step 2.5) → Deduplicate by primary keys → Generate MD5 business keys → Audit columns → Idempotent delete → Write Delta |
| **PII Step** | Detects PII via `pii_detection.detect_pii()`, persists to `platform_data_classification`, applies masking via `GovernanceEnforcer` (non-blocking) |
| **Partitioned By** | `_silver_execution_date` |

### 4. silver_semantic_validation.py

| Aspect | Detail |
|--------|--------|
| **Source** | Silver zone |
| **Operations** | Business rules → Referential integrity → Cross-field validation → Range checks → Custom GE expectations |
| **Output** | Quality score (0-100), validation results to `platform_validation_result` table |
| **Branch** | Pass → silver_to_gold; Fail → handle_validation_failure |

### 5. build_gold_layer.py

| Aspect | Detail |
|--------|--------|
| **Source** | Silver zone (+ joined tables via join_executor) |
| **Target** | Gold (business-ready, Delta Lake) |
| **Operations** | Resolve join dependencies → Apply Gold view SQL → Aggregations → SCD Type 2 (hash-based change detection) → Surrogate key generation → **PII masking enforcement** (Step 4.5) → Audit columns → Idempotent delete (skip for SCD2) → Write Delta |
| **PII Step** | Applies masking for classified columns (carries forward from Silver classifications) via `GovernanceEnforcer` (non-blocking) |
| **Partitioned By** | `_gold_execution_date` (non-SCD2) |
| **Write Mode** | SCD2: overwrite (full merge); FULL: overwrite; APPEND: append |

### 6. join_executor.py (Support Module)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Generic multi-table join engine used by build_gold_layer.py |
| **Join Types** | INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI |
| **Features** | Chain joins (A→B→C), null-safe keys, broadcast hints for small tables |
| **Safety** | Grain verification after each join — raises `ValueError` if fanout ratio > 2.0x |
| **Config Source** | `platform_join_dependency` metadata table |

---

## dag_utilities Package

Shared library imported by all generated DAGs at runtime. Installed via pip in the Airflow Docker image.

### Core (`dag_utilities/core/`)

| Module | Key Classes/Functions | Purpose |
|--------|----------------------|---------|
| `metadata_client.py` | `MetadataClient` | Central PostgreSQL client: feeds, contracts, schemas, views, validation rules, join dependencies, pipeline dependencies, execution tracking, data catalog search, data product CRUD, classification queries |
| `execution_context.py` | `ExecutionContext` | XCom-based state sharing between Airflow tasks |
| `config_loader.py` | `APEXConfig`, `ConfigLoader` | Environment config from env vars + multi-environment support (`get_environment_config()` for dev/staging/prod) |
| `exceptions.py` | `MetadataError`, `ValidationError` | Custom exception hierarchy |

### Pipeline Tasks (`dag_utilities/pipeline/`)

| Function | Used By | Purpose |
|----------|---------|---------|
| `initialize_execution()` | All patterns | Create `platform_pipeline_execution` record with auto-incrementing sequence |
| `check_source_file()` | P01, P02, P04 | Verify source file exists in GCS |
| `submit_zone_job()` | All patterns | Submit Spark job to Dataproc/local with correct args |
| `run_ge_validation()` | All patterns | Run GE validation as BranchPythonOperator (pass/fail routing) |
| `run_quality_checks()` | All patterns | Final quality gate |
| `finalize_execution()` | All patterns | Update status, record metrics, track lineage |
| `handle_validation_failure()` | All patterns | Log failure, quarantine data |
| `send_notifications()` | All patterns | Email/Slack alerts |

### Validation (`dag_utilities/validation/`)

| Module | Purpose |
|--------|---------|
| `ge_helper.py` | Great Expectations integration: build suite, validate DataFrame, return results |
| `ge_configs.py` | GE expectation builder: maps validation_rules to GE expectations |
| `ge_result_writer.py` | Persist GE results to `platform_validation_result` table |
| `schema_validator.py` | Bronze column presence + type checks |
| `semantic_validator.py` | Silver business rule + referential integrity checks |
| `quality_checker.py` | Completeness, freshness, accuracy scoring |

### Logging (`dag_utilities/logging/`)

| Module | Purpose |
|--------|---------|
| `audit_logger.py` | Write events to `platform_audit_log` table |
| `lineage_tracker.py` | Track data lineage + emit OpenLineage events |
| `metrics_collector.py` | Record pipeline metrics |
| `openlineage_emitter.py` | Convert APEX lineage to OpenLineage spec JSON, emit to HTTP endpoint or file |

### Remediation (`dag_utilities/remediation/`)

| Module | Purpose |
|--------|---------|
| `self_healer.py` | VIGIL pattern: classify errors, apply auto-remediation (8 actions) |
| `retry_handler.py` | Exponential backoff with circuit breaker |
| `incident_manager.py` | Create incidents + track SLA |

### Spark (`dag_utilities/spark/`)

| Module | Purpose |
|--------|---------|
| `config_builder.py` | Fluent Spark config builder |
| `cluster_manager.py` | Dynamic Dataproc scaling |
| `job_submitter.py` | Submit jobs to Dataproc/EMR/local |

---

## Quality Framework

### Great Expectations Integration

Validation rules from the `platform_validation_rule` and `platform_quality_expectation` metadata tables are converted to GE expectations:

```
metadata platform_validation_rule
    → GEConfigBuilder.build_expectations()
    → GEHelper.validate_dataframe()
    → GEResultWriter.write_results()
    → platform_validation_result table
```

Quality scoring uses Airbnb Midas pattern: weighted 0-100 score based on rule severity.

### Schema Evolution

`schema_evolution.py` in `quality/` enforces schema policies:

```
Incoming DataFrame schema
    → detect_schema_changes(registered, incoming)
    → validate_schema_evolution(policy=STRICT|ADDITIVE|FLEXIBLE)
    → Pass or raise ValueError
```

### Data Drift Detection

`data_drift_detector.py` in `quality/` detects 4 drift types:

| Drift Type | What It Detects | Severity Logic |
|-----------|----------------|----------------|
| Schema Drift | Columns added/removed/type changed | CRITICAL if removed, WARNING if added |
| Statistical Drift | Distribution changes (mean, stddev shift) | CRITICAL if >3σ change |
| Volume Drift | Row count anomaly | CRITICAL if >50% change, WARNING if >threshold |
| Freshness Drift | Late-arriving data | CRITICAL if >3h late, WARNING if >threshold |

Results persist to `platform_observability_metrics` table with a 30-day rolling baseline view (`v_observability_baseline`).

### OpenLineage

`openlineage_emitter.py` converts APEX lineage to [OpenLineage spec](https://openlineage.io/spec/2-0-0/OpenLineage.json):

- Emits `RunEvent` JSON for every zone transition (Bronze→Silver, Silver→Gold)
- Configurable endpoint: HTTP (Marquez, DataHub) or file fallback
- Includes APEX-specific facets: `records_read`, `records_written`, `feed_id`

---

## Cross-Pipeline Dependencies

When a Gold table depends on Silver tables from multiple feeds, the `platform_pipeline_dependency` table defines upstream relationships:

```sql
INSERT INTO platform_pipeline_dependency (upstream_feed_id, upstream_dag_id, downstream_feed_id)
VALUES ('FEED_CUSTOMERS', 'customers_daily', 'FEED_SALES_GOLD');
```

The DAG template generates `ExternalTaskSensor` tasks:

```
start
  |--> wait_customers_daily (ExternalTaskSensor, timeout=3600, poke_interval=300)
  |--> init_exec
  |--> ...rest of pipeline...
```

All 9 patterns (P01-P09) support the `{% if dependencies %}` block.

---

## Parsers

### DTSX Parser (`parsers/dtsx_parser.py`)

Parses SSIS `.dtsx` XML packages:

| Component | Extracted As |
|-----------|-------------|
| Connections | OLEDB/ADO.NET → JDBC connection config |
| Data Flow Sources | Source tables/files → SourceConfig |
| Transformations | T-SQL expressions → PySpark code |
| Destinations | Target tables → TargetConfig |
| Variables | SSIS variables → Airflow variables |

### Copybook Parser (`parsers/copybook_parser.py`)

Parses COBOL copybook files for EBCDIC/fixed-width sources:

- Extracts record layout (PIC clauses, COMP-3 packed decimal)
- Generates schema for `raw_to_bronze.py`

### NL Transform Processor (`parsers/nl_transform_processor.py`)

Converts natural language to PySpark code:

| NL Intent | Detection Pattern | Output |
|-----------|-------------------|--------|
| FILTER | "filter where amount > 100" | `df.filter("amount > 100")` |
| AGGREGATE | "sum amount group by customer" | Aggregation PySpark code |
| JOIN | "join orders with customers on customer_id" | Structured `JoinConfig` |
| WINDOW | "running total of sales by customer" | Window function PySpark code |
| DEDUPLICATE | "remove duplicates by order_id" | `df.dropDuplicates(["order_id"])` |
| SCD | "track history of customer changes" | SCD2 PySpark code |

Join extraction (`_extract_join_config`) detects table names, join keys, and join type from NL text with fuzzy matching against known catalog tables.

### PII Detection (`security/pii_detection.py`)

Scans columns for PII indicators:

| PII Type | Detection Method | Masking Strategy |
|----------|-----------------|------------------|
| SSN | Regex `\d{3}-\d{2}-\d{4}` | Hash |
| Credit Card | Luhn check | Partial mask (last 4) |
| Email | Regex | Tokenize |
| Phone | Regex | Partial mask |
| DOB | Column name + date type | Generalize to year |

---

## PostgreSQL Metadata Tables (10 DDL Files)

All pipeline configuration, execution history, and component registry lives in PostgreSQL. The generated DAGs fetch config from these tables at runtime — no business logic is hardcoded in DAG files.

### DDL File Layout

```
agents/data_agent/ddl/apex/
  01_extensions_and_types.sql     -- ENUMs and extensions
  02_core_tables.sql              -- Connections, domains, sources, feeds, templates
  03_contract_and_schema.sql      -- Data contracts, schema versions, views, transforms
  04_validation_and_quality.sql   -- Validation rules, quality expectations, SLAs
  05_execution_and_logging.sql    -- Execution logs (with sequence), audit, lineage, errors
  06_component_registry.sql       -- Template registry, utility registry, Spark job registry
  07_ge_validation.sql            -- Great Expectations checkpoints + results
  08_join_dependency.sql          -- Multi-table join definitions for Gold zone
  09_pipeline_dependency.sql      -- Cross-DAG orchestration dependencies
  10_observability_metrics.sql    -- Historical metrics + 30-day baseline view
```

### Entity-Relationship Overview

```
platform_connection_registry
       |
       V
platform_domain_registry ----> platform_source_registry ----> platform_feed_group ----> feed
                                                |               |
                                          platform_dag_template     platform_data_contract ----> platform_schema_version
                                                               |                    |
                                                          platform_view_definition      platform_validation_rule
                                                               |                    |
                                                    platform_transformation_rule     platform_quality_expectation
                                                               |
                                                   platform_contract_transformation
                                                               |
                                               platform_pipeline_execution ----> platform_task_execution
                                                       |     |              |
                                                 platform_audit_log  platform_data_lineage  platform_error_log
                                                       |
                                              platform_validation_log
                                              platform_sla_breach_log
                                              platform_execution_cost_log

platform_join_dependency -----> (contract_id FK to platform_data_contract)
platform_pipeline_dependency -> (feed_id references to feed)
platform_observability_metrics  (historical baselines)

platform_template_registry
platform_utility_registry
platform_spark_job_registry       (Component catalogs — queried by the agent before generating code)
platform_component_change_log
platform_agent_decision_log
```

### Key Tables

| DDL | Table | Purpose |
|-----|-------|---------|
| 02 | `feed` | One row per Airflow DAG. Links to platform_feed_group, template, schedule. |
| 03 | `platform_data_contract` | Defines file format, zone paths, load type, primary keys. |
| 03 | `platform_schema_version` | Versioned column definitions (supports schema evolution). |
| 03 | `platform_view_definition` | SQL view definitions for Bronze→Silver and Silver→Gold transitions. |
| 04 | `platform_validation_rule` | SQL boolean expressions, with severity and blocking flag. |
| 05 | `platform_pipeline_execution` | One row per DAG run. Includes `sequence` (auto-incrementing per feed+date). |
| 07 | `platform_validation_result` | Great Expectations validation results per checkpoint. |
| 08 | `platform_join_dependency` | Multi-table join definitions (table, keys, type, order). |
| 09 | `platform_pipeline_dependency` | Cross-DAG dependencies for ExternalTaskSensor generation. |
| 10 | `platform_observability_metrics` | Historical metrics for anomaly detection baselines. |

### How DAGs Use These Tables at Runtime

```
DAG starts
  |
  V
MetadataClient queries PostgreSQL:
  +---> feed, platform_data_contract, platform_schema_version (is_current=true)
  +---> platform_validation_rule, platform_quality_expectation (for this contract + zone)
  +---> platform_view_definition (SQL for zone transitions)
  +---> platform_join_dependency (multi-table joins for Gold)
  +---> platform_spark_config (cluster sizing)
  +---> platform_notification_config (alert routing)
  +---> platform_watermark_tracking (for incremental loads)
  |
  V
DAG tasks execute using this config (no hardcoded logic)
  |
  V
After execution:
  +---> Writes: platform_pipeline_execution (run status, metrics, sequence)
  +---> Writes: platform_task_execution (per-task metrics)
  +---> Writes: platform_audit_log (zone-level actions)
  +---> Writes: platform_data_lineage (source-to-target mapping)
  +---> Writes: platform_validation_result (GE validation outcomes)
  +---> Writes: platform_observability_metrics (for baseline computation)
  +---> Emits: OpenLineage JSON (to Marquez/DataHub/file)
  +---> Updates: platform_watermark_tracking (new bookmark value)
```

---

## Source Types (70+, 9 Categories)

| Category | Count | Examples | Config Form |
|----------|-------|---------|-------------|
| A. File-Based | 14 | CSV, Parquet, JSON, Avro, ORC, EBCDIC, Excel, Fixed-Width | `FileSourceConfigForm` |
| B. Database | 9 | PostgreSQL, MySQL, Oracle, SQL Server, Snowflake, BigQuery | `DatabaseSourceConfigForm` |
| C. Streaming | 8 | Kafka, Pub/Sub, Kinesis, Event Hubs | `StreamingSourceConfigForm` |
| D. API & SaaS | 12 | REST, GraphQL, Salesforce, ServiceNow, Workday, SAP | `APISourceConfigForm` |
| E. Legacy | 7 | DTSX/SSIS, COBOL, VSAM, AS400, Mainframe | `DTSXSourceConfigForm` |
| F. NoSQL | 9 | MongoDB, Cassandra, DynamoDB, Firestore | `DatabaseSourceConfigForm` |
| G. Logs | 5 | Splunk, Datadog, CloudWatch, Application Logs | `StreamingSourceConfigForm` |
| H. Cloud Storage | 4 | GCS, S3, Azure Blob, HDFS | `FileSourceConfigForm` |
| I. Advanced | 6 | CDC, Delta Lake, Iceberg, IoT, Time-Series | `EBCDICSourceConfigForm` |

Each category has a dedicated config form component in the frontend.

---

## Infrastructure

### Docker Services

| Service | Purpose |
|---------|---------|
| `airflow-scheduler` | Airflow scheduler with DAG parsing |
| `airflow-webserver` | Airflow UI (port 8080) |
| `airflow-postgres` | Airflow metadata database |
| `apex-postgres` | APEX metadata database (DDL 01-10) |
| `dag-sync` | Alpine/git sidecar — polls GitHub every 60s, syncs DAGs + spark_jobs |
| `backend` | FastAPI incident management (port 8000) |
| `data-agent` | FastAPI data agent API (port 8001) |
| `frontend` | Next.js UI (port 3000) |
| `kafka` | Event streaming |
| `redis` | Caching |

### dag-sync Flow

```
enterprise-data-pipelines (GitHub repo)
  ├── dags/           ──┐
  ├── spark_jobs/     ──┤── dag-sync polls every 60s
  └── dag_utilities/  ──┘       |
                                V
                    /opt/airflow/dags/       (mounted in Airflow containers)
                    /opt/airflow/dags/spark_jobs/
```

`dag_utilities` is pip-installed in the Airflow Docker image via `requirements.txt`.

---

## Key Files

| Layer | File | Purpose |
|-------|------|---------|
| Frontend | `components/pipeline/UnifiedPipelineForm.tsx` | Main form (3 input modes) |
| Frontend | `components/pipeline/SourceTypeSelector.tsx` | 70+ source picker (9 categories) |
| Frontend | `components/pipeline/PatternSelector.tsx` | P01-P09 selector |
| Frontend | `components/pipeline/JoinDependencyBuilder.tsx` | Multi-table join definition UI |
| Frontend | `components/pipeline/GoldModelingSelector.tsx` | Star Schema / Data Vault / SCD2 / Flat |
| Frontend | `types/pipeline-canonical.ts` | TypeScript types (mirrors Pydantic) |
| API | `agents/data_agent/src/api/main.py` | FastAPI endpoints (port 8001) |
| Models | `agents/data_agent/src/models/canonical.py` | UnifiedPipelineInput, PipelineMetadata |
| Models | `agents/data_agent/src/models/source.py` | 70+ SourceType enum, StreamingSourceConfig (windowing, DLQ) |
| Models | `agents/data_agent/src/models/transformation.py` | JoinConfig, WindowConfig, SCDConfig, 18 TransformTypes |
| Models | `agents/data_agent/src/models/target.py` | TargetZone, TableFormat (delta/iceberg/parquet), table_maintenance config |
| Normalizer | `agents/data_agent/src/normalizers/dispatcher.py` | Routes input to correct normalizer |
| Normalizer | `agents/data_agent/src/normalizers/nl_normalizer.py` | NL → structured (LLM-powered, with join extraction) |
| Workflow | `agents/data_agent/src/graphs/apex_workflow.py` | 9-node LangGraph state machine |
| Generator | `agents/data_agent/src/generators/apex_dag_generator.py` | Jinja2 template renderer + env context injection + BQ security DDL generation |
| Registry | `agents/data_agent/src/repository/registry_manager.py` | Pattern selection logic |
| Templates | `agents/data_agent/src/templates/patterns/*.jinja2` | 9 DAG pattern templates |
| Spark | `agents/data_agent/src/spark_jobs/*.py` | 5 canonical PySpark jobs + join_executor |
| Quality | `agents/data_agent/src/quality/schema_evolution.py` | Schema evolution enforcement |
| Quality | `agents/data_agent/src/quality/data_drift_detector.py` | 4-type drift detection + metrics persistence |
| Utilities | `agents/data_agent/src/dag_utilities/` | Shared runtime library (core, validation, logging, remediation) |
| Registry | `agents/data_agent/registry.json` | APEX Component Registry v2.1.0 |
| Catalog | `agents/data_agent/src/repository/catalog_repository.py` | Data asset search & registration |
| Governance | `agents/data_agent/src/security/governance_enforcer.py` | PII masking enforcement, BQ policy tags |
| Maintenance | `agents/data_agent/src/spark_jobs/table_maintenance.py` | Delta/Iceberg VACUUM, OPTIMIZE, compact |
| Monitoring | `agents/data_agent/src/dag_utilities/logging/cloud_monitoring.py` | GCP Cloud Monitoring custom metrics |
| Promotion | `agents/data_agent/scripts/promote_pipeline.py` | Multi-env pipeline promotion CLI |

---

## Cloud Standards Features (v2.2)

These capabilities close the gaps identified against GCP, AWS, and Azure big data engineering standards.

### Data Catalog & Discovery

Every dataset across medallion zones is auto-registered in the `platform_data_asset` table after zone transitions. Provides:

| Feature | Implementation |
|---------|---------------|
| Asset Registry | `platform_data_asset` table — auto-populated by `AuditLogger.register_zone_asset()` |
| Full-Text Search | PostgreSQL GIN index on name + description + domain |
| Business Glossary | `platform_business_term` table — maps technical columns to business definitions |
| Tag Taxonomy | `platform_tag_taxonomy` table — hierarchical classification (SENSITIVITY, DOMAIN, COMPLIANCE) |
| Catalog CRUD | `catalog_repository.py` — register, search, get_lineage, get_by_path |
| MetadataClient | `metadata_client.py` — `search_data_assets()`, `get_asset_by_path()`, `register_data_asset()` |

**DDL**: `ddl/apex/11_data_catalog.sql`
**Code**: `src/repository/catalog_repository.py`

### Data Governance & RBAC

PII detection results are now persisted and enforced, not just detected:

```
PIIDetector.detect_pii_in_dataframe()
    → persist_classifications(results, asset_id)
        → platform_data_classification table
            → GovernanceEnforcer.enforce_pii_masking(df, asset_id)
                → Masked DataFrame written to Silver/Gold
```

| Feature | Implementation |
|---------|---------------|
| Access Policies | `platform_access_policy` table — resource-level RBAC (READ/WRITE/ADMIN/MASKED_READ) |
| Data Classification | `platform_data_classification` table — links PII detection to assets |
| Masking Enforcement | `governance_enforcer.py` — applies masking transforms from classification |
| BQ Policy Tags | `governance_enforcer.generate_bq_policy_tags()` — BigQuery column-level security |
| Access Requests | `platform_access_request` table — self-service access with approval workflow |

**DDL**: `ddl/apex/12_governance.sql`
**Code**: `src/security/governance_enforcer.py`, `src/security/pii_detection.py` (persist_classifications)

### Cost Optimization

Generated Dataproc jobs now include cost labels, preemptible workers, and autoscaling:

| Feature | Implementation |
|---------|---------------|
| Cost Labels | `SparkJobConfig.cost_labels` — feed_id, domain, environment on every job |
| Preemptible Workers | `SparkJobConfig.preemptible_ratio` — default 60% preemptible for cost savings |
| Autoscaling | `AutoscalingPolicy` — min/max workers, scale-up factor, cooldown |
| Serverless Spark | `SparkJobSubmitter._submit_dataproc_serverless()` — no cluster management |
| Cost Tracking | `MetricsCollector.estimate_dataproc_cost()` — preemptible-aware estimation |
| CMEK Encryption | `SparkJobConfig.encryption_key_name` — customer-managed encryption keys |
| Network Isolation | `NetworkConfig.no_public_ip` — private subnet, no public IPs |

**Code**: `src/dag_utilities/spark/config_builder.py`, `src/dag_utilities/spark/job_submitter.py`

### Table Format Support

Pipelines now support Delta Lake and Apache Iceberg with automatic maintenance:

| Feature | Implementation |
|---------|---------------|
| Delta VACUUM | `table_maintenance.vacuum_delta_table()` — remove old files |
| Delta OPTIMIZE | `table_maintenance.optimize_delta_table()` — compact + Z-ORDER |
| Iceberg Expire | `table_maintenance.expire_iceberg_snapshots()` — free storage |
| Iceberg Compact | `table_maintenance.rewrite_iceberg_data_files()` — target file size |
| Parquet Compact | `table_maintenance.compact_small_files()` — repartition small files |

**Code**: `src/spark_jobs/table_maintenance.py`

### Data Mesh / Data Products

Enterprise data mesh support with product registry and subscription workflow:

| Feature | Implementation |
|---------|---------------|
| Product Registry | `platform_data_product` table — name, domain, owner, SLAs, version, status |
| Subscriptions | `platform_data_product_subscription` table — PENDING → APPROVED → REVOKED |
| Product Catalog | `v_data_product_catalog` view — with active subscriber count |
| MetadataClient | `metadata_client.py` — `get_data_products()`, `publish_data_product()`, `subscribe_to_product()` |
| Classification | `metadata_client.py` — `get_classifications(asset_id)` |

**DDL**: `ddl/apex/13_data_products.sql`

### Cloud Monitoring Integration

Custom metrics emitted to GCP Cloud Monitoring after each pipeline run:

| Metric | Type |
|--------|------|
| `apex/pipeline/records_processed` | Counter |
| `apex/pipeline/quality_score` | Gauge |
| `apex/pipeline/duration_seconds` | Timer |
| `apex/pipeline/cost_dollars` | Gauge |
| `apex/pipeline/sla_breach` | Counter |

**Code**: `src/dag_utilities/logging/cloud_monitoring.py`

### Streaming Enhancements

`StreamingSourceConfig` in `source.py` now supports windowing and dead letter queues:

| Feature | Field | Example |
|---------|-------|---------|
| Window Type | `window_type` | `tumbling`, `sliding`, `session` |
| Window Duration | `window_duration` | `5 minutes`, `1 hour` |
| Slide Duration | `slide_duration` | `1 minute` (for sliding windows) |
| Watermark Delay | `watermark_delay` | `10 minutes` (late data tolerance) |
| Event Time Column | `event_time_column` | `event_timestamp` |
| Dead Letter Queue | `dlq_path` | `gs://bucket/dlq/feed_id/` |

`raw_to_bronze.py` routes rejected records to the DLQ path when configured.

### Multi-Format Table Support

`TargetConfig` in `target.py` now supports configurable table formats:

| Field | Values | Purpose |
|-------|--------|---------|
| `table_format` | `delta` (default), `iceberg`, `parquet` | Storage format for zone writes |
| `table_maintenance` | `{vacuum_hours, optimize_schedule, z_order_columns, retention_days}` | Automatic maintenance config |

`raw_to_bronze.py` uses the configured `table_format` for writes and idempotent deletes (Delta API vs Iceberg SQL).

### Multi-Environment Configuration

`ConfigLoader` now provides environment-specific configuration:

| Environment | GCS Prefix | BQ Project | Metadata DB |
|-------------|-----------|------------|-------------|
| `dev` | `gs://dev-datalake` | `enterprise-data-lake-dev` | `apex_metadata_dev` |
| `staging` | `gs://staging-datalake` | `enterprise-data-lake-staging` | `apex_metadata_staging` |
| `prod` | `gs://prod-datalake` | `enterprise-data-lake` | `apex_metadata` |

Set via `APEX_ENVIRONMENT` env var. `APEXDAGGenerator` injects environment context into all generated DAGs.

### BQ Security DDL Generation

After DAG generation, `APEXDAGGenerator` also generates BigQuery policy tag DDL for PII columns:

```
generate() → DAG file + {dag_id}_bq_security.sql (if PII classifications exist)
```

### CI/CD Multi-Environment Promotion

```
python scripts/promote_pipeline.py --feed-id sales_daily --from dev --to staging
```

| Feature | Implementation |
|---------|---------------|
| Forward-only promotion | dev → staging → prod (validated) |
| Artifact regeneration | Environment-specific GCS/BQ paths |
| PR creation | Auto-creates Git PR with promotion metadata |
| Prod approval | `--require-approval` flag for production |

**Code**: `scripts/promote_pipeline.py`

---

## Updated DDL File List (13 files)

| # | File | Tables |
|---|------|--------|
| 01 | `01_extensions_and_types.sql` | ENUMs and extensions |
| 02 | `02_core_tables.sql` | connection, domain, source, feed, platform_spark_config |
| 03 | `03_contract_and_schema.sql` | platform_data_contract, platform_schema_version, platform_view_definition, platform_transformation_rule |
| 04 | `04_validation_and_quality.sql` | platform_validation_rule, platform_quality_expectation, platform_sla_definition |
| 05 | `05_execution_and_logging.sql` | platform_pipeline_execution, platform_audit_log, platform_error_log, cost_log (11 tables) |
| 06 | `06_component_registry.sql` | platform_template_registry, platform_utility_registry, platform_spark_job_registry |
| 07 | `07_ge_validation.sql` | platform_validation_result, platform_validation_summary |
| 08 | `08_join_dependency.sql` | platform_join_dependency |
| 09 | `09_pipeline_dependency.sql` | platform_pipeline_dependency |
| 10 | `10_observability_metrics.sql` | platform_observability_metrics, v_observability_baseline |
| 11 | `11_data_catalog.sql` | platform_data_asset, platform_business_term, platform_tag_taxonomy |
| 12 | `12_governance.sql` | platform_access_policy, platform_data_classification, platform_access_request |
| 13 | `13_data_products.sql` | platform_data_product, platform_data_product_subscription |

---

## One-Line Summary

**User picks source + pattern in the UI (or describes it in natural language), the agent normalizes the input, selects a template from the registry, renders a metadata-driven Airflow DAG via Jinja2 with multi-table joins, GE validation, schema evolution, cross-DAG dependencies, OpenLineage, data catalog registration, PII detection + masking enforcement (Bronze→Silver→Gold), cost-optimized Dataproc submission with preemptible workers, Cloud Monitoring custom metrics, multi-format table support (Delta/Iceberg/Parquet), streaming windowing + DLQ, BQ policy tag generation, and data product registry — validates it, generates BQ security DDL, and deploys through Git with multi-environment promotion (dev→staging→prod) — all orchestrated by a 9-node LangGraph state machine.**
