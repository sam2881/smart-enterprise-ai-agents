# Data Engineering Agent

Automated data pipeline generation for 70+ source types. Accepts natural language, UI form, or SSIS `.dtsx` package — produces production Airflow DAGs + Spark jobs deployed via GitHub PR.

## Entry Points

| What | Where |
|------|-------|
| FastAPI (port 8001) | `src/api/main.py` |
| APEX workflow (8-phase LangGraph) | `src/graphs/apex_workflow.py` |
| Supervisor (routes to 7 agents) | `src/agents/supervisor_agent.py` |

## APEX Workflow Phases

```
normalize → resolve_pattern → load_metadata → generate_artifacts
  → connection_test → validate → persist_metadata → await_approval → deploy → monitor
```

| Agent | File | Does |
|-------|------|------|
| PlannerAgent | `src/agents/planner_agent.py` | Template selection, schema comparison |
| GeneratorAgent | `src/agents/generator_agent.py` | Jinja2 DAG + Spark job generation |
| ConnectionTestAgent | `src/agents/connection_test_agent.py` | TCP + schema validation pre-deploy |
| ValidatorAgent | `src/agents/validator_agent.py` | Great Expectations quality gate |
| DeployerAgent | `src/agents/deployer_agent.py` | GitHub PR → CI/CD → Airflow sync |
| PipelineMonitoringAgent | `src/agents/pipeline_monitoring_agent.py` | Watches first 5 DAG runs post-deploy |

## Source Types (70+ across 9 categories)

| Prefix | Types |
|--------|-------|
| `file_` | csv, parquet, excel, json, avro, orc, ebcdic, fixed_width, xml, pdf, delta, iceberg, hudi, cobol |
| `database_` | postgres, mysql, snowflake, oracle, db2, mssql, bigquery, redshift, teradata |
| `streaming_` | kafka, pubsub, kinesis, eventhub, rabbitmq, nats, mqtt, redis_stream |
| `api_` | rest, graphql, salesforce, sap, hubspot, stripe, zendesk, servicenow, jira, dynamics, marketo, workday |
| `legacy_` | dtsx, as400, mainframe, cobol_file, idms, vsam, natural |
| `nosql_` | mongodb, cassandra, dynamodb, couchdb, elasticsearch, hbase, redis, neo4j, influxdb |
| `logs_` | splunk, datadog, cloudwatch, elk, grafana |
| `cloud_` | s3, gcs, azure_blob, adls |
| `cdc_` | debezium, oracle_goldengate, aws_dms, striim, qlik, delta_lake, iceberg_cdc |

Source type Pydantic models: `src/models/source.py` | Form dispatch: `frontend/src/components/pipeline/`

## Key Directories

```
src/
  agents/          ← 7 LangGraph agents
  models/          ← Pydantic v2 canonical models (source.py, canonical.py, pipeline.py)
  generators/      ← Jinja2 DAG + SQL generation (apex_dag_generator.py)
  dag_utilities/   ← 42 DAG building blocks (see dag_utilities/README.md)
  spark_jobs/v2/   ← Zone Spark processors (landing_to_bronze, bronze_to_silver, etc.)
  normalizers/     ← Input normalization (UI/NL/DTSX → UnifiedPipelineInput)
  parsers/         ← Raw input parsers (dtsx_parser.py, nl_transform_processor.py)
  quality/         ← Data drift detection + schema evolution
  security/        ← PII detection (13 types) + governance enforcer
  repository/      ← Database access (pipeline, catalog, feed, registry)
  templates/       ← Jinja2 templates for generated DAGs
  api/             ← FastAPI routes (main.py)
  config/          ← Settings (settings.py) — owns data_agent env vars
ddl/apex/          ← 13 PostgreSQL DDL files (CANONICAL — do not edit sql/ddl/)
prompts/           ← LLM system prompts (.md files)
tests/             ← Integration tests (require live infra)
```

## Medallion Architecture

`Landing` (raw, immutable) → `Bronze` (schema enforced) → `Silver` (cleaned/deduped) → `Gold` (business logic, analytics-ready — final layer)

Zone Spark jobs: `src/spark_jobs/v2/promote_landing_to_bronze.py`, `promote_bronze_to_silver.py`, `build_gold_layer.py`, `load_fact_table.py`, `load_data_vault_hub.py`

## Adding a New Source Type

1. Add `SourceType` enum value + `[Name]Config(BaseModel)` in `src/models/source.py`
2. Add to `UnifiedSourceConfig` union type in same file
3. Add Jinja2 template in `src/templates/[prefix]_ingest_dag.j2`
4. Add frontend form component in `frontend/src/components/pipeline/[Name]SourceConfigForm.tsx`
5. Wire in `frontend/src/components/pipeline/SourceConfigSection.tsx`

## Running

```bash
cd agents/data_agent
uvicorn src.api.main:app --reload --port 8001
# API docs: http://localhost:8001/docs

# Tests (E2E — requires live infra)
pytest tests/ -v
```
