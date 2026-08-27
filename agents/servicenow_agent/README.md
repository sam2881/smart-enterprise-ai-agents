# Incident Management Agent (ServiceNow Agent)

Automated IT incident resolution. Consumes from Kafka, runs a 12-node LangGraph workflow, returns to Kafka. Never called directly via REST.

## Entry Points

| What | Where |
|------|-------|
| EventOrchestrator (Kafka consumer) | `src/streaming/consumers/event_orchestrator.py` |
| Incident consumer | `src/streaming/consumers/incident_consumer.py` |
| DataPipelineIncidentBridge | `src/streaming/consumers/data_pipeline_incident_bridge.py` |
| LangGraph workflow | `src/orchestrator/` (mirrors `backend/orchestrator/`) |

## FAST 9-Agent Architecture

The Governor routes `incident.created` events to 9 specialized agents:

| Agent | Does |
|-------|------|
| IncidentIntelligenceAgent | RCA classification, deduplication, 15-pattern matching |
| RiskAgent | Blast radius assessment (LOW/MEDIUM/HIGH/CRITICAL) |
| ChangeManagementAgent | Creates ServiceNow CHG records |
| LLM Judge | Quality gate — scores remediation plans 0-10, blocks if safety < 7 |
| ApprovalAgent | 4-tier approval: AUTO / STANDARD / SENIOR / EXECUTIVE |
| ExecutionAgent | Runs GitHub Actions, Airflow triggers, GCP ops |
| VerificationAgent | Health checks, stabilization window |
| LearningAgent | Updates Weaviate + Neo4j knowledge base |
| PostMortemAgent | Blameless post-mortem, Jira story, GitHub commit |

## Always-On Agents

| Agent | File | Does |
|-------|------|------|
| ProactiveMonitoringAgent | `src/agents/proactive_monitoring_agent.py` | Polls 8 Prometheus metrics every 60s, creates incidents autonomously |
| PostMortemAgent | `src/agents/post_mortem_agent.py` | Triggered after feedback_loop node |

## Key Directories

```
src/
  agents/          ← 4 specialized agents + governor
  orchestrator/    ← LangGraph 12-node workflow (mirrors backend/orchestrator/)
  rag/             ← CANONICAL Swarm RAG (4 agents + RRF fusion)
  streaming/
    consumers/     ← EventOrchestrator, IncidentConsumer, DataPipelineIncidentBridge
  governance/      ← Audit logger, EU AI Act compliance, data retention
  guardrails/      ← Content safety, PII masking
  config/          ← Settings (settings.py)
```

## RAG System

`src/rag/` is the **canonical** Swarm RAG implementation. `backend/rag/` imports some utilities from here.

4 retrieval agents fused with Reciprocal Rank Fusion (k=60):
- `rag/agents/vector_agent.py` — Weaviate semantic search
- `rag/agents/graph_agent.py` — Neo4j graph traversal
- `rag/agents/keyword_agent.py` — BM25 keyword search
- `rag/agents/metadata_agent.py` — Structured metadata filters
- `rag/hybrid_search_engine.py` — RRF fusion coordinator
- `rag/intelligent_retriever.py` — Main entry point

## Kafka Topics Consumed

`incident.created`, `incident.approved`, `incident.rejected`, `incident.halt_requested`,
`pipeline.failed`, `pipeline.sla_missed`, `pipeline.config_update`, `pipeline.healthy`

## Kafka Topics Published

`incident.enriched`, `incident.plan_generated`, `incident.requires_approval`,
`incident.executed`, `incident.verified`, `incident.closed`, `incident.postmortem_ready`,
`incident.auto_closed`, `pipeline.reconfigure`

## Approval Gate (ApprovalAgent)

| Tier | Risk Score | Approver | SLA |
|------|-----------|----------|-----|
| AUTO | ≤ 0.3, confidence ≥ 0.7, not PROD | System | Immediate |
| STANDARD | 0.3–0.6 or PROD | On-call engineer | 30 min |
| SENIOR | 0.6–0.8 or blast_radius HIGH | Tech lead | 2 hours |
| EXECUTIVE | > 0.8 or CRITICAL | Director+ | 4 hours |
