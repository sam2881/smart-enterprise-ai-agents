# Changelog — agents/data_agent/

Data Engineering Agent — 70+ source types, APEX 8-phase workflow, Jinja2 code generation.

---

## [Unreleased] — 2026-06-22

### Added
- `src/agents/connection_test_agent.py` — Pre-deployment source validation agent
  - Validates TCP connectivity + driver-level connectivity per source type
  - Discovers live schema (columns, types, row count estimate)
  - Diffs live schema against requested schema (missing/extra columns, type mismatches)
  - Detects PII fields in discovered schema (triggers governance review)
  - Recommends partitioning strategy based on row count heuristics
  - Auto-skips legacy source types (cobol, vsam, as400, mainframe, ebcdic, fixed_width)
  - Sets `can_proceed: false` and `error_message` if connectivity or schema check fails
  - Returns structured `ConnectionTestResult` in workflow state

- `src/agents/pipeline_monitoring_agent.py` — Post-deployment Airflow watcher
  - Polls Airflow REST API every 30s for first 5 DAG runs
  - Classifies failures: OOM, schema_mismatch, source_unavailable, permission_denied, timeout, data_quality_fail, unknown
  - Auto-remediates: OOM (increase Spark executor memory 50%), timeout (increase task timeout 2x)
  - Escalates non-auto-remediable failures via `pipeline.failed` Kafka event
  - Publishes SLA miss at 80% of expected runtime
  - Generates `PipelineHealthReport` after 5 runs with recommendations
  - New Kafka topics: `pipeline.healthy`, `pipeline.failed`, `pipeline.sla_missed`, `pipeline.config_update`, `pipeline.health_report`

### Fixed
- Missing `__init__.py` in 6 directories:
  - `src/api/__init__.py`
  - `src/deployers/__init__.py`
  - `src/parsers/__init__.py`
  - `src/quality/__init__.py`
  - `src/security/__init__.py`
  - `src/triggers/__init__.py`
  - `tests/__init__.py`
  These files were missing, causing `pytest tests/` to fail with ImportError on all test files.

### Architecture Decisions
- `ConnectionTestAgent` is positioned **between `validator` and `deployer`** in the APEX workflow.
  It acts as a final gate: if the source is unreachable or schema diverges significantly,
  the workflow halts and waits for human intervention rather than deploying a broken pipeline.
- `PipelineMonitoringAgent` is **post-deployment** — it's not a workflow node but a long-running
  background task started by `DeployerAgent` after successful deployment. It runs for 5 DAG
  runs (approximately 5 days for a daily pipeline) then exits.
- Both agents follow the established `SKIP_SOURCE_TYPES` pattern for legacy sources.

---

## [1.0.0] — 2026-06-21

### Initial
- APEX 8-phase workflow: normalize → resolve_pattern → load_metadata → generate_artifacts → validate → persist_metadata → await_approval → deploy
- 5-agent LangGraph: supervisor → planner → generator → validator → deployer
- 70+ source types across 9 categories (file, database, streaming, api, legacy, nosql, logs, cloud, cdc)
- Pydantic v2 canonical models for all source types (`src/models/source.py`, `src/models/canonical.py`)
- Jinja2 code generation for Airflow DAGs + Spark jobs (`src/generators/apex_dag_generator.py`)
- 42 DAG utility building blocks across 11 sub-packages (`src/dag_utilities/`)
- Zone Spark processors for Landing→Bronze→Silver→Gold (`src/spark_jobs/v2/`)
- Data drift detection (schema, statistical, volume, freshness) (`src/quality/data_drift_detector.py`)
- Schema evolution policies (STRICT/ADDITIVE/FLEXIBLE) (`src/quality/schema_evolution.py`)
- PII detection (13 types, 7 strategies) (`src/security/pii_detection.py`)
- Input normalizers for UI/NL/DTSX inputs (`src/normalizers/`)
- FastAPI on port 8001 (`src/api/main.py`)
- 13 PostgreSQL DDL files (`ddl/apex/`)
