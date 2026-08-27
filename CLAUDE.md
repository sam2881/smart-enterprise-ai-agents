# Enterprise Agentic Platform

## Architecture

Two agent systems sharing a Next.js UI, all state flowing through Kafka:

```
MCP Servers → Kafka (system of record) → EventOrchestrator → LangGraph → FastAPI (control plane only)
```

**System 1 — Incident Management** (`backend/` + `agents/servicenow_agent/`): Automated IT incident resolution from ServiceNow. 12-node LangGraph workflow.

**System 2 — Data Engineering Agent** (`agents/data_agent/`): Automated data pipeline generation. 5-agent LangGraph. 70+ source types across 9 categories.

**Frontend** (`frontend/`): Next.js 14 + React Query + Tailwind. Unified UI for both systems.

> Deep architecture context: `docs/project-context.md` | Full spec: `docs/spec.md`

---

## System Map

| What | Where |
|------|-------|
| Incident LangGraph (12 nodes) | `backend/orchestrator/langgraph_workflow.py` |
| Data Agent LangGraph (APEX 8-phase) | `agents/data_agent/src/graphs/apex_workflow.py` |
| Data Agent FastAPI (port 8001) | `agents/data_agent/src/api/main.py` |
| Backend FastAPI (port 8000, control plane) | `backend/app.py` |
| Kafka EventOrchestrator | `agents/servicenow_agent/src/streaming/consumers/event_orchestrator.py` |
| Pydantic models (canonical) | `agents/data_agent/src/models/` |
| TypeScript types (mirrors Pydantic) | `frontend/src/types/pipeline-canonical.ts` |
| Jinja2 code gen templates | `agents/data_agent/src/templates/` |
| 70+ source type definitions | `agents/data_agent/src/models/source.py` |
| Agent LLM prompts | `agents/data_agent/prompts/` |
| Database DDL (13 files, canonical) | `agents/data_agent/ddl/apex/` |
| Swarm RAG — incident (canonical) | `agents/servicenow_agent/src/rag/` |
| MCP servers | `mcp-servers/` |
| Frontend pages | `frontend/src/app/` |
| Pipeline UI components | `frontend/src/components/pipeline/` |
| Module READMEs | `agents/data_agent/README.md` · `agents/servicenow_agent/README.md` · `frontend/README.md` |

---

## Dev Start

Copy `.env.example` → `.env` and fill in values, then:

```bash
# Everything (recommended)
.\scripts\start-dev.ps1

# Or manually:
docker compose up -d                                          # infra
cd backend && uvicorn app:app --reload                        # port 8000
cd agents/data_agent && uvicorn src.api.main:app --port 8001 # port 8001
cd frontend && npm run dev                                    # port 3000
```

Services: `http://localhost:3000` (UI) | `http://localhost:8000/docs` | `http://localhost:8001/docs`
Full service list → `docs/project-context.md`

---

## LangGraph Patterns

**Node (always this shape):**
```python
def node_name(state: AgentState) -> Dict[str, Any]:
    try:
        return {"result_key": value}
    except Exception as e:
        return {"error_message": str(e), "error_agent": "node_name"}
```

**Conditional edge:**
```python
def should_continue(state: AgentState) -> Literal["next", "error"]:
    return "error" if state.get("error_message") else "next"
```

**Incident workflow nodes (order):**
`ingest → parse → classify → swarm_rag → generate_plan → judge → control_plane → await_approval → execute → verify → close_ticket → feedback_loop`

**Data agent nodes (order):**
`supervisor → planner → connection_test → generator → validator → deployer → monitoring`

---

## Frontend Patterns

**All pipeline creation uses `UnifiedPipelineInput`:**
```typescript
import { UnifiedPipelineInput } from '@/types/pipeline-canonical'
const input: UnifiedPipelineInput = {
  input_type: 'ui_structured',
  created_by: 'user@company.com',
  source: { source_type: 'file_csv', file_config: { gcs_path: 'gs://...', delimiter: ',', header: true } },
  target: { target_zone: 'gold', bq_dataset: 'sales', bq_table: 'daily', write_mode: 'append' },
  execution_policy: { schedule_interval: '@daily', processing_mode: 'batch' }
}
```

**Dynamic source form by type prefix:**
```typescript
if (sourceType.startsWith('file_'))      return <FileSourceConfigForm ... />
if (sourceType.startsWith('database_'))  return <DatabaseSourceConfigForm ... />
if (sourceType.startsWith('streaming_')) return <StreamingSourceConfigForm ... />
// api_ | legacy_ | nosql_ | logs_ | cloud_ | cdc_
```
Source types (70+) → `agents/data_agent/README.md` | Routes → `frontend/README.md`

---

## Kafka Topics

| Topic | Trigger |
|-------|---------|
| `incident.created` | MCP or ProactiveMonitoringAgent detected incident |
| `incident.enriched` | Classification complete |
| `incident.plan_generated` | Remediation plan ready |
| `incident.requires_approval` | Awaiting human approval |
| `incident.approved` | Human approved via UI |
| `incident.closed` | Workflow complete |
| `incident.postmortem_ready` | Post-mortem generated |
| `pipeline.failed` | DAG failure → DataPipelineIncidentBridge creates incident |
| `pipeline.deployed` | Pipeline live in Airflow |

Full topic reference → `docs/architecture.md`

---

## Critical Rules

| DO | NEVER |
|----|-------|
| LangGraph `StateGraph` with explicit edges | ReAct / ad-hoc agent loop |
| Kafka for all state transitions | REST for internal events |
| Explicit Pydantic / TypedDict state | Implicit LLM memory |
| `pipeline-canonical.ts` types | `pipeline.ts` (deprecated) |
| NL → structured metadata → execute | Execute natural language directly |
| Human approval gate for PROD | Auto-deploy to production |
| Jinja2 templates for code gen | Hard-coded business logic |
| React Query for API state | Manual fetch + useState |
| FastAPI as control plane only | FastAPI running LangGraph workflows |
| `agents/data_agent/ddl/apex/` (canonical DDL) | `sql/ddl/apex/` (deleted, was duplicate) |
| `agents/servicenow_agent/src/rag/` (canonical RAG) | `backend/rag/` (legacy, imports from canonical) |

---

## Testing

```bash
pytest tests/unit -v                          # 102 unit tests
cd agents/data_agent && pytest tests/ -v      # E2E (needs infra)
cd frontend && npx tsc --noEmit               # TypeScript check
python scripts/e2e_csv_pipeline_test.py       # E2E pipeline (needs infra)
```

Docs: `docs/testing.md` | Results: `docs/test-results.md`
