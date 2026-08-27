# Changelog — backend/

Backend FastAPI control plane and LangGraph incident management workflow.

---

## [Unreleased] — 2026-06-22

### Added
- `backend/routers/observability.py` — 4 new endpoints for live observability data:
  - `GET /api/v1/observability/services` — health check for all 14 services
  - `GET /api/v1/observability/incidents/states` — PostgreSQL state distribution
  - `GET /api/v1/observability/kafka/events` — recent audit events
  - `GET /api/v1/observability/metrics/summary` — Prometheus metrics summary
- `backend/__init__.py` — Package marker (was missing, caused import issues)
- `backend/guardrails/__init__.py` — Package marker
- `backend/orchestrator/__init__.py` — Package marker

### Fixed
- Missing `__init__.py` files in `backend/`, `backend/guardrails/`, `backend/orchestrator/`
  prevented pytest from discovering tests and broke cross-package imports

### Architecture Gaps (Planned)
- Replace `MemorySaver` with `PostgresSaver` in `langgraph_workflow.py` (Q3 2026)
  - Impact: Currently, pod restart loses all in-flight workflow state
  - Fix: `from langgraph.checkpoint.postgres import PostgresSaver`
- Add Dead Letter Queue consumer for `dlq.*` topics (Q3 2026)
- Add LiteLLM router for LLM fallback (Q3 2026)
- Upgrade JWT from HMAC-SHA256 to RS256 + OIDC (Q3 2026)

---

## [1.0.0] — 2026-06-21

### Initial
- FastAPI control plane on port 8000
- 12-node LangGraph incident workflow (`langgraph_workflow.py`)
- FAST 9-agent Governor architecture
- 4-agent Swarm RAG with RRF fusion (`rag/swarm_retriever.py`)
- MCP servers: Airflow, GCS, RAG, LLM (`mcp/servers/`)
- Kafka producer + EventOrchestrator consumer
- EU AI Act compliance module (`governance/eu_ai_act_compliance.py`)
- Audit logger with SHA256 event integrity (`governance/audit_logger.py`)
- Prometheus metrics (50+ `aiagent_*` metrics in `orchestrator/metrics.py`)
- `/health`, `/metrics` endpoints
