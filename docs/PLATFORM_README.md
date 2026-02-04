# Enterprise Agentic Platform - Unified Documentation

## Overview

An event-driven platform with two autonomous agent systems:

1. **Incident Management** - ServiceNow incident detection, RAG-based script matching, LangGraph remediation workflow, human approval, auto-execution via GitHub Actions or Airflow MCP
2. **Data Engineering Agent (APEX)** - UI/NL/DTSX input, 9 pipeline patterns (P01-P09), Jinja2 DAG generation, Spark job orchestration, metadata-driven medallion architecture

**Architecture**: `MCPs sense → Kafka remembers → Orchestrator routes → LangGraph reasons → FastAPI governs`

---

## System Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ ServiceNow  │    │   Airflow   │    │    Jira     │    │   GitHub    │
│    MCP      │    │    MCP      │    │    MCP      │    │    MCP      │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │ poll/push        │                   │                   │
       ▼                  ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          KAFKA (System of Record)                       │
│  incident.* │ remediation.* │ pipeline.* │ airflow.* │ gcp.alerts      │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │  Event Orchestrator   │
                    │  (routes to workflows)│
                    └───────────┬───────────┘
                                │
              ┌─────────────────┼─────────────────┐
              ▼                                   ▼
    ┌─────────────────┐                 ┌─────────────────┐
    │  LangGraph      │                 │  LangGraph      │
    │  Incident (12)  │                 │  APEX (8 nodes) │
    └─────────────────┘                 └─────────────────┘
              │                                   │
              ▼                                   ▼
    ┌─────────────────┐                 ┌─────────────────┐
    │   FastAPI        │                │  Generated DAGs │
    │   Control Plane  │                │  + Spark Jobs   │
    └─────────────────┘                 └─────────────────┘
              │
              ▼
    ┌─────────────────┐
    │  Next.js 14 UI  │
    │  (React Query)  │
    └─────────────────┘
```

---

## 1. Incident Management System

### 12-Node LangGraph Workflow

```
ingest → parse → classify → swarm_rag → generate_plan → judge →
control_plane → await_approval → execute → verify → close_ticket → feedback_loop
```

| Node | Purpose | Key File |
|------|---------|----------|
| ingest | Receive incident from Kafka | `langgraph_workflow.py:747` |
| parse | Extract entities, service, zone | `langgraph_workflow.py:781` |
| classify | Determine incident category | `langgraph_workflow.py:809` |
| swarm_rag | 4-agent RAG search (Vector, Keyword, Graph, Metadata) | `langgraph_workflow.py:874` |
| generate_plan | Create remediation plan with script selection | `langgraph_workflow.py:976` |
| judge | Evaluate plan quality (LLM judge) | `langgraph_workflow.py:1099` |
| control_plane | Determine if approval needed | `langgraph_workflow.py:1174` |
| await_approval | Pause for human approval (Kafka event) | `langgraph_workflow.py:1228` |
| execute | Run script via GitHub Actions or Airflow MCP | `langgraph_workflow.py:1393` |
| verify | Check fix was successful | `langgraph_workflow.py:1549` |
| close_ticket | Close ServiceNow ticket via MCP | `langgraph_workflow.py:1582` |
| feedback_loop | Update RAG (Weaviate + Neo4j) with resolution | `langgraph_workflow.py:1658` |

### RAG System (Swarm v5.0)

4 agents search in parallel, fused with Reciprocal Rank Fusion (RRF, k=60):

| Agent | Source | Purpose |
|-------|--------|---------|
| Vector | Weaviate | Semantic similarity via embeddings |
| Keyword | TF-IDF | Exact term matching |
| Graph | Neo4j | FIXED_BY historical relationships |
| Metadata | Fields | Category, service, environment matching |

### Monitoring → Incident Flow

1. **GCP VM Monitor** (`backend/streaming/gcp_vm_monitor.py`): Polls GCP Compute API → detects STOPPED VMs → creates ServiceNow incident → publishes to Kafka
2. **Airflow DAG Monitor** (`backend/streaming/airflow_dag_monitor.py`): Polls Airflow REST API → detects failed DAGs → creates ServiceNow incident → publishes to Kafka
3. **Auto-Remediation**: If incident is Airflow-related, `node_execute` uses Airflow MCP to retrigger the DAG directly

---

## 2. Data Engineering Agent (APEX)

### 3 Input Modes

| Mode | Description | Component |
|------|-------------|-----------|
| UI Structured | Form-based with 70+ source types across 9 categories | `UnifiedPipelineForm.tsx` |
| Natural Language | NL → structured metadata → preview → execute | `NLTransformInput.tsx` |
| DTSX Migration | Upload SSIS package → parse → map to Airflow | `DTSXMigrationForm.tsx` |

### 9 Pipeline Patterns

| Code | Pattern | Use Case |
|------|---------|----------|
| P01 | FILE_MEDALLION | Standard file ingestion (CSV, JSON, Parquet) |
| P02 | BIGDATA_FILE | Large files with partitioned processing |
| P03 | DATABASE_LAKEHOUSE | Database CDC to lakehouse |
| P04 | LEGACY_MIGRATION | DTSX/COBOL/AS400/EBCDIC migration |
| P05 | STREAMING_BATCH | Kafka/Pub/Sub micro-batch |
| P06 | API_SAAS | REST API/SaaS ingestion |
| P07 | SCD2 | Slowly Changing Dimensions Type 2 |
| P08 | DATA_VAULT | Data Vault 2.0 (Hubs, Links, Satellites) |
| P09 | STAR_SCHEMA | Star schema dimensional modeling |

### Medallion Architecture

```
Landing → Raw STRING columns (no schema)
  ↓
Bronze → Schema enforcement, basic types, audit columns
  ↓
Silver → Dedup, validation, cleansing, business keys
  ↓
Gold → Aggregation, joins, business logic
  ↓
Trusted → Curated datasets for analytics
```

### 8-Node APEX Workflow

```
normalize_input → select_pattern → generate_dag → generate_spark →
generate_sql → validate → approval_gate → deploy
```

### 5 Canonical Spark Jobs

| Job | Zone Transition | Purpose |
|-----|----------------|---------|
| `raw_to_bronze.py` | Raw → Bronze | Schema enforcement, type casting |
| `bronze_to_silver.py` | Bronze → Silver | Dedup, validation, cleansing |
| `silver_to_gold.py` | Silver → Gold | Business logic, aggregations |
| `cdc_merge.py` | Any | Change data capture merge |
| `scd2_apply.py` | Silver → Gold | SCD Type 2 processing |

---

## 3. Metadata Database (28 Tables)

### Table Groups

| Group | Tables | DDL File |
|-------|--------|----------|
| Core Infrastructure | connection_registry, domain_registry, source_registry, dag_template, feed_group, feed, spark_config, notification_config, watermark_tracking | `02_core_tables.sql` |
| Data Contracts | data_contract, schema_version, view_definition, transformation_rule, contract_transformation | `03_contract_and_schema.sql` |
| Validation & Quality | validation_rule, quality_expectation, sla_definition, pipeline_dependency | `04_validation_and_quality.sql` |
| Execution & Logging | pipeline_execution, task_execution, audit_log, data_lineage, validation_log, error_log, sla_breach_log, execution_cost_log, metadata_audit_log, agent_decision_log, template_change_log | `05_execution_and_logging.sql` |
| Component Registry | template_registry, utility_registry, spark_job_registry, component_change_log | `06_component_registry.sql` |

See `docs/METADATA_ERD.md` for full ERD diagram.

---

## 4. Kafka Topics

| Topic | System | Purpose |
|-------|--------|---------|
| `incident.created` | Incident | New incident detected |
| `incident.requires_approval` | Incident | Pending human approval |
| `incident.approved` | Incident | Human approved |
| `incident.closed` | Incident | Workflow complete |
| `remediation.started` | Incident | Execution begins |
| `remediation.executed` | Incident | Execution complete |
| `airflow.failures` | Monitor | DAG failures detected |
| `gcp.alerts` | Monitor | GCP VM alerts |
| `pipeline.requested` | Data Agent | New pipeline request |
| `pipeline.completed` | Data Agent | Pipeline generated |

---

## 5. MCP Servers

| Server | Purpose | Location |
|--------|---------|----------|
| ServiceNow MCP | Poll incidents, close tickets | `mcp-servers/servicenow-mcp/` |
| Jira MCP | Poll pipeline requests, update tickets | `mcp-servers/jira-mcp/` |
| Airflow MCP | Trigger/monitor DAGs | `mcp-servers/airflow-mcp/` + `backend/mcp/servers/airflow_server.py` |
| GitHub MCP | Trigger workflows, manage PRs | `mcp-servers/github-mcp/` |
| RAG MCP | Semantic/hybrid search | `backend/mcp/servers/rag_server.py` |
| GCS MCP | Bucket operations | `backend/mcp/servers/gcs_server.py` |

---

## 6. Frontend (Next.js 14)

### Key Pages

| URL | Purpose |
|-----|---------|
| `/incidents` | View/manage IT incidents |
| `/approvals` | Approve/reject remediation plans |
| `/workflows` | Monitor LangGraph execution |
| `/pipelines` | Create/view data pipelines (70+ sources) |
| `/jira/[id]` | Jira-integrated pipeline creation |

### Key Components

| Component | Purpose |
|-----------|---------|
| `UnifiedPipelineForm` | Main form with 3 input modes |
| `SourceTypeSelector` | 9-category source picker |
| `SourceConfigForms` | 6 type-specific config forms |
| `NLTransformInput` | NL → structured metadata |
| `IncidentChat` | AI chat with RAG + similar incidents |
| `FloatingChat` | Global floating chat widget |
| `PatternSelector` | Pipeline pattern picker |

---

## 7. Running the Platform

### Docker Compose

```bash
# Build all services
sudo docker compose build --no-cache

# Start everything
sudo docker compose up -d

# Check status
sudo docker compose ps

# View logs
sudo docker compose logs -f orchestrator
```

### Service Ports

| Service | Port | URL |
|---------|------|-----|
| Frontend | 3000 | http://localhost:3000 |
| Backend API | 8000 | http://localhost:8000/docs |
| Data Agent API | 8001 | http://localhost:8001/docs |
| Airflow | 8080 | http://localhost:8080 |
| Kafka | 29092 | localhost:29092 |
| PostgreSQL | 5432 | localhost:5432 |
| Weaviate | 8081 | http://localhost:8081 |
| Neo4j | 7474/7687 | http://localhost:7474 |
| Grafana | 3001 | http://localhost:3001 |
| Prometheus | 9090 | http://localhost:9090 |

---

## 8. Testing

```bash
# E2E Data Agent tests
python scripts/e2e_data_agent_test.py --url http://localhost:8001

# E2E Incident lifecycle tests
python scripts/e2e_incident_lifecycle_test.py --url http://localhost:8000

# Master validator (health + unit + integration)
python scripts/e2e_validator.py --all

# Frontend TypeScript check
cd frontend && npx tsc --noEmit

# UI E2E script
./test_ui_e2e.sh
```

---

## 9. Key Files Reference

| Area | File | Purpose |
|------|------|---------|
| Incident workflow | `backend/orchestrator/langgraph_workflow.py` | 12-node LangGraph |
| APEX workflow | `agents/data_agent/src/graphs/apex_workflow.py` | 8-node DAG generation |
| DAG generator | `agents/data_agent/src/generators/apex_dag_generator.py` | Jinja2 template rendering |
| RAG search | `backend/rag/hybrid_search_engine.py` | RRF fusion search |
| RAG updater | `backend/rag/rag_updater.py` | Post-resolution indexing |
| Event publisher | `backend/streaming/event_publisher.py` | Kafka event schema |
| Event orchestrator | `backend/streaming/consumers/event_orchestrator.py` | Central event router |
| GCP monitor | `backend/streaming/gcp_vm_monitor.py` | VM status polling |
| Airflow monitor | `backend/streaming/airflow_dag_monitor.py` | DAG failure detection |
| Frontend types | `frontend/src/types/pipeline-canonical.ts` | TypeScript canonical models |
| Main API | `backend/app.py` | FastAPI control plane |
| Docker | `docker-compose.yml` | All service definitions |
| DDL | `agents/data_agent/ddl/apex/` | 6 SQL files, 28 tables |
| ERD | `docs/METADATA_ERD.md` | Mermaid ERD diagrams |
