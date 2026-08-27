# Project Context — Enterprise Agentic Platform
**Version:** 2.0 | **Last Updated:** 2026-06-22 | **Primary Reference for AI Agents**

> **This document is the single source of truth for any AI agent working on this codebase.**  
> Read this before reading any other file. Everything else is implementation detail.

---

## 1. What This Platform Does

**Enterprise Agentic Platform** is a production-grade AI system that automates two enterprise workflows:

1. **IT Incident Resolution** — Detects incidents from ServiceNow, analyzes root cause, generates remediation plans, gets human approval, executes fixes, verifies recovery, closes tickets, and learns from outcomes. Fully autonomous for low-risk incidents; human-gated for high-risk.

2. **Data Pipeline Generation** — Takes a data engineering request (natural language, UI form, or SSIS package), validates the source connection, generates production-ready Airflow DAGs + Spark jobs, deploys via Git/PR, monitors the first 5 runs, and escalates failures to the incident management system.

**The systems are connected:** Data pipeline failures automatically create incidents. Incident resolutions feed back into pipeline recommendations.

---

## 2. The Mental Model (Memorize This)

```
MCPs sense → Kafka remembers → Orchestrator routes → LangGraph reasons → FastAPI governs
```

- **MCP Servers** = eyes and hands (poll external systems, execute actions)
- **Kafka** = the system of record (all state is an immutable event)
- **EventOrchestrator** = the brain stem (routes events to the right workflow)
- **LangGraph StateGraph** = the decision maker (NOT ReAct — always deterministic StateGraph)
- **FastAPI** = the governance layer (CQRS reads, human approval API, policy engine)
- **Frontend** = the human interface (approvals, monitoring, pipeline creation)

---

## 3. Architecture at a Glance

### Two Agent Systems

**System 1 — Incident Management**
```
ServiceNow MCP → Kafka (incident.created) → EventOrchestrator → Governor (FAST 9 agents)
  → IncidentIntelligenceAgent (RCA, dedup)
  → RiskAgent (blast radius, SLA)              } parallel
  → ChangeManagementAgent (CHG record)         }
  → LLM plan generation + Judge evaluation
  → ApprovalAgent (4-level: auto/standard/senior/executive)
  → ExecutionAgent (GitHub Actions / Airflow / GCP)
  → VerificationAgent (health checks, stabilization window)
  → LearningAgent (Weaviate index, Neo4j graph, RRF weights)
  → PostMortemAgent (blameless post-mortem, runbook update)
  → ServiceNow MCP → ticket closed
```

**System 2 — Data Engineering Agent**
```
Jira MCP / UI / Slack → FastAPI → Kafka (pipeline.requested) → Supervisor
  → PlannerAgent (template selection, schema comparison)
  → GeneratorAgent (Jinja2 DAG + Spark job generation)
  → [NEW] ConnectionTestAgent (source connectivity + schema validation)
  → ValidatorAgent (Great Expectations, syntax checks)
  → Human approval gate (PROD only)
  → DeployerAgent (GitHub PR → CI/CD → Airflow sync)
  → [NEW] PipelineMonitoringAgent (watches first 5 runs, auto-remediates)
  → [NEW] DataPipelineIncidentBridge (failures → incident.created → FAST workflow)
```

**Proactive Layer (always-on)**
```
[NEW] ProactiveMonitoringAgent → polls Prometheus every 60s → anomaly detection
  → publishes incident.created (source: proactive_monitoring)
  → Governor processes identically to ServiceNow incidents
```

### Kafka is the System of Record

Every state transition is a Kafka event. REST is NEVER used for internal communication. This is non-negotiable.

**Key topics:**
```
incident.created → received → enriched → plan_generated → requires_approval
  → approved/rejected → executed → verified → close_execute → closed
  → postmortem_ready

pipeline.requested → planned → generated → validated → requires_approval
  → approved → deploy_execute → deployed → failed → healthy → health_report
  → sla_missed → config_update → reconfigure
```

### CQRS Pattern
- **Write path:** Commands go to Kafka
- **Read path:** FastAPI reads from Redis (latest state) + PostgreSQL (history)
- **Result:** UI always reads from fast cache; agents publish to Kafka

---

## 4. Directory Map

```
d:\projects\ai_agent_app\
├── agents\
│   ├── data_agent\         ← Data Engineering Agent (System 2)
│   │   ├── src\agents\     ← 7 agents (planner, generator, connection_test, validator, deployer, monitoring + supervisor)
│   │   ├── src\models\     ← Pydantic canonical models (70+ source types)
│   │   ├── src\generators\ ← Jinja2 code generation
│   │   ├── src\spark_jobs\ ← Zone-level Spark processors
│   │   ├── src\security\   ← PII detection + governance
│   │   ├── src\quality\    ← Data drift detection, schema evolution
│   │   ├── src\api\        ← FastAPI (port 8001)
│   │   └── ddl\apex\       ← 13 PostgreSQL DDL files
│   └── servicenow_agent\   ← Incident Management Agent (System 1 - agent layer)
│       ├── src\agents\     ← 4 agents (proactive monitoring, post mortem + existing)
│       ├── src\rag\        ← 4-agent swarm RAG (vector/graph/keyword/metadata)
│       ├── src\streaming\  ← Kafka consumers (event_orchestrator + incident_consumer + bridge)
│       └── src\governance\ ← Audit, EU AI Act compliance
├── backend\                ← FastAPI control plane (port 8000) + LangGraph orchestration
│   ├── app.py              ← FastAPI entry point
│   ├── orchestrator\       ← 12-node LangGraph incident workflow
│   ├── streaming\          ← Kafka producer + event schemas
│   └── rag\                ← Swarm RAG (mirrors servicenow_agent)
├── frontend\               ← Next.js 14 UI (port 3000)
│   └── src\
│       ├── app\            ← Pages: incidents, approvals, pipelines, observability...
│       ├── components\     ← React components
│       └── types\          ← TypeScript types (use pipeline-canonical.ts, NOT pipeline.ts)
├── mcp-servers\            ← MCP protocol implementations
│   ├── servicenow-mcp\     ← Polls ServiceNow, publishes incident.created
│   ├── jira-mcp\           ← Polls Jira, publishes pipeline.requested
│   ├── github-mcp\         ← Triggers GitHub Actions
│   └── airflow-mcp\        ← Manages Airflow DAGs
├── dags\                   ← Generated Airflow DAGs (output of DeployerAgent)
├── sql\ddl\apex\           ← Canonical PostgreSQL schema (13 DDL files)
├── infrastructure\         ← Docker configs (Prometheus, Grafana, Tempo, Loki, Airflow)
├── tests\unit\             ← Python unit tests (pytest)
├── docs\                   ← Architecture, compliance, audit reports
├── docker-compose.yml      ← All 14 services
├── CLAUDE.md               ← Architecture rules and patterns (always read)
└── .env.example            ← Required environment variables
```

---

## 5. The 10 Patterns You Must Know

### Pattern 1: LangGraph StateGraph (not ReAct)
```python
# CORRECT: Deterministic graph with explicit edges
def node_name(state: AgentState) -> Dict[str, Any]:
    try:
        return {"result_key": value}
    except Exception as e:
        return {"error_message": str(e), "error_agent": "node_name"}

def should_continue(state: AgentState) -> Literal["next", "error"]:
    return "error" if state.get("error_message") else "next"
```

### Pattern 2: All state flows through Kafka
```python
# CORRECT
await producer.publish_event(topic="incident.enriched", event=payload, key=incident_id)
# WRONG — never use REST for internal state transitions
requests.post("http://backend/internal/update-state", ...)
```

### Pattern 3: FastAPI is control plane only
```python
# CORRECT — FastAPI publishes to Kafka, never runs LangGraph
@app.post("/api/v1/approve")
async def approve(req: ApprovalRequest):
    await producer.publish_event(topic="incident.approved", event=req.dict(), key=req.incident_id)
    return {"status": "queued"}
```

### Pattern 4: Never execute natural language directly
```typescript
// WRONG
await api.createPipeline({ description: "load CSV to BigQuery" })
// CORRECT — NL → structured → execute
const structured = await api.transformNL({ description: "load CSV to BigQuery" })
await api.createPipelineUnified(structured)
```

### Pattern 5: Pydantic v2 for all agent contracts
```python
from pydantic import BaseModel, model_validator
class UnifiedPipelineInput(BaseModel):
    model_config = ConfigDict(extra="forbid")  # Fail on unknown fields
    input_type: Literal["ui_structured", "natural_language", "dtsx_migration"]
```

### Pattern 6: Human approval gate before PROD
```python
# In ApprovalAgent — auto-approve conditions
if (risk_score <= 0.3 and judge_quality >= 7.0 
    and confidence >= 0.7 and environment != "production"):
    return ApprovalDecision.AUTO_APPROVE
# PROD always requires human
```

### Pattern 7: Jinja2 templates for code generation
```python
# CORRECT — templates are frozen, never runtime-modified
template = template_manager.get_template("file_ingest_dag.j2")
dag_code = template.render(feed_id=..., source_config=..., ...)
# WRONG — hardcoding business logic
dag_code = f"with DAG('{name}') as dag: ..."  # Never
```

### Pattern 8: Idempotency keys everywhere
```python
idempotency_key = f"{incident_id}:{event_type}:{timestamp}"
# Redis deduplication
if redis.set(f"idempotent:{agent_name}:{key}", "1", nx=True, ex=604800):
    # Process — first time seen
else:
    return None  # Already processed
```

### Pattern 9: Error never silently swallowed
```python
# CORRECT — errors become state, Governor routes to escalation
return {"error_message": str(e), "error_agent": "risk_agent", "escalate": True}
# WRONG
try: ...
except: pass  # Never swallow errors silently
```

### Pattern 10: Frontend types from canonical file only
```typescript
// CORRECT
import { UnifiedPipelineInput } from '@/types/pipeline-canonical'
// WRONG — pipeline.ts is deprecated
import { Pipeline } from '@/types/pipeline'
```

---

## 6. The Critical Rules (Non-Negotiable)

| DO | NEVER |
|----|-------|
| LangGraph `StateGraph` with explicit edges | ReAct / ad-hoc agent loop |
| Kafka for all state transitions | REST for internal events |
| Explicit Pydantic/TypedDict state | Implicit LLM memory |
| Jinja2 templates for code generation | Hardcoded business logic |
| Human approval gate for PROD | Auto-deploy to production |
| `pipeline-canonical.ts` types | `pipeline.ts` (deprecated) |
| NL → structured metadata → execute | Execute natural language directly |
| FastAPI as control plane only | FastAPI running LangGraph workflows |
| React Query for API state | Manual fetch + useState |
| Idempotency keys on all Kafka messages | Processing messages without dedup |

---

## 7. Infrastructure Services

All services started with `.\scripts\start-dev.ps1` or `docker compose up -d`.

| Service | URL | Port | Purpose |
|---------|-----|------|---------|
| Frontend | http://localhost:3000 | 3000 | Next.js 14 UI |
| Backend API | http://localhost:8000/docs | 8000 | FastAPI control plane |
| Data Agent API | http://localhost:8001/docs | 8001 | Pipeline generation API |
| Kafka UI | http://localhost:8090 | 8090 | Topic browser |
| Grafana | http://localhost:3001 | 3001 | Dashboards (anon access) |
| Prometheus | http://localhost:9090 | 9090 | Metrics |
| Langfuse | http://localhost:3002 | 3002 | LLM observability |
| Tempo | http://localhost:3200 | 3200 | Distributed tracing |
| Airflow | http://localhost:8083 | 8083 | DAG management (admin/admin123) |
| Weaviate | http://localhost:8080 | 8080 | Vector store |
| PostgreSQL | localhost:5432 | 5432 | Primary DB (admin/admin123) |
| Redis | localhost:6379 | 6379 | Cache + state |
| Neo4j | http://localhost:7474 | 7474 | Graph DB (incident knowledge) |
| Kafka | localhost:29092 | 29092 | Event streaming |

---

## 8. Source Types (70+ across 9 categories)

| Prefix | Types | Form Component |
|--------|-------|---------------|
| `file_` | csv, parquet, excel, json, avro, orc, ebcdic, fixed_width, xml, pdf, delta, iceberg, hudi, cobol | `FileSourceConfigForm` |
| `database_` | postgres, mysql, snowflake, oracle, db2, mssql, bigquery, redshift, teradata | `DatabaseSourceConfigForm` |
| `streaming_` | kafka, pubsub, kinesis, eventhub, rabbitmq, nats, mqtt, redis_stream | `StreamingSourceConfigForm` |
| `api_` | rest, graphql, salesforce, sap, hubspot, stripe, zendesk, servicenow, jira, dynamics, marketo, workday | `APISourceConfigForm` |
| `legacy_` | dtsx, as400, mainframe, cobol_file, idms, vsam, natural | `LegacySourceConfigForm` |
| `nosql_` | mongodb, cassandra, dynamodb, couchdb, elasticsearch, hbase, redis, neo4j, influxdb | `DatabaseSourceConfigForm` |
| `logs_` | splunk, datadog, cloudwatch, elk, grafana | `LogsSourceConfigForm` |
| `cloud_` | s3, gcs, azure_blob, adls | `FileSourceConfigForm` |
| `cdc_` | debezium, oracle_goldengate, aws_dms, striim, qlik, delta_lake, iceberg_cdc | `StreamingSourceConfigForm` |

**In frontend:** `if (sourceType.startsWith('file_')) return <FileSourceConfigForm />`

---

## 9. Medallion Architecture

| Zone | Description | Schema Enforcement | PII |
|------|------------|-------------------|-----|
| Landing | Raw strings, immutable | None | Encrypt |
| Bronze | Schema enforced, typed | ADDITIVE | Tokenize |
| Silver | Cleaned, deduped | STRICT | Partial Mask |
| Gold | Business logic applied — analytics-ready (final layer) | STRICT | None (de-identified) |

Promotion between zones is a Kafka event. Zone Spark jobs are in `agents/data_agent/src/spark_jobs/v2/`.

---

## 10. Current Limitations (Know Before Working)

| Limitation | Impact | Target Fix |
|-----------|--------|-----------|
| LangGraph MemorySaver (in-memory) | Pod restart loses paused workflow state | Replace with PostgresSaver (Q3 2026) |
| JWT uses HMAC-SHA256 (shared secret) | Any service with key can mint admin tokens | Upgrade to RS256 + OIDC (Q3 2026) |
| Single EventOrchestrator process | Kafka consumer lag under high load | Kubernetes Deployment 3 replicas (Q3 2026) |
| No LLM fallback routing | Claude API outage stops all workflows | LiteLLM router (Q3 2026) |
| Single-node Redis | State machine lost on Redis restart | Redis Sentinel (Q3 2026) |
| No SSO/OIDC integration | Enterprise teams can't use corporate IdP | Okta/AAD integration (Q1 2027) |
| No multi-tenancy | Team A can see Team B's data | Row-Level Security + tenant_id (Q1 2027) |
| servicenow_agent mirrors backend | Duplicate code, maintenance burden | Shared library extraction (Q4 2026) |
| ~5% test coverage | Regressions not caught automatically | 80-test suite target (Q3 2026) |

---

## 11. Development Workflows

### Starting the Platform
```powershell
# Everything
.\scripts\start-dev.ps1

# Infrastructure only (Kafka, Postgres, Redis + all 14 services)
.\scripts\start-dev.ps1 -InfraOnly

# Status check
.\scripts\start-dev.ps1 -Status

# Stop everything
.\scripts\start-dev.ps1 -StopAll
```

### Running Tests
```bash
# Backend unit tests
pytest tests/unit -v

# Data agent tests
cd agents/data_agent && pytest tests/ -v

# Frontend type check
cd frontend && npx tsc --noEmit

# E2E health check (infrastructure must be running)
python scripts/e2e_csv_pipeline_test.py
```

### Creating a Pipeline (API)
```python
import httpx
response = httpx.post("http://localhost:8001/api/v2/data-agent/pipelines", json={
    "input_type": "ui_structured",
    "created_by": "engineer@company.com",
    "jira_ticket": "DATA-1234",
    "source": {
        "source_type": "file_csv",
        "file_config": {"gcs_path": "gs://bucket/data.csv", "delimiter": ",", "header": True}
    },
    "target": {"target_zone": "gold", "bq_dataset": "sales", "bq_table": "daily", "write_mode": "append"},
    "execution_policy": {"schedule_interval": "@daily", "processing_mode": "batch"}
})
```

---

## 12. Future Roadmap

| Quarter | Focus | Key Deliverables |
|---------|-------|-----------------|
| Q3 2026 | Stability + Security | PostgresSaver, RS256 JWT, LiteLLM router, EventOrchestrator HA, DLQ |
| Q4 2026 | Data Quality + GitOps | Great Expectations in pipelines, Terraform, ArgoCD, HashiCorp Vault |
| Q1 2027 | Enterprise Features | SSO/OIDC, multi-tenancy, SLO dashboards, Slack ChatOps |
| Q2 2027 | Autonomous Operations | BackfillAgent, CapacityPlanningAgent, ChangeCorrelationAgent |

---

*This document is maintained by the Platform Architecture Team. Update it whenever the architecture changes — it's what every future AI agent reads first.*
