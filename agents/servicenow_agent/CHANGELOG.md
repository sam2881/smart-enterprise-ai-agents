# Changelog — agents/servicenow_agent/

Incident Management Agent — FAST 9-agent workflow, Swarm RAG, Kafka consumers.

---

## [Unreleased] — 2026-06-22

### Added
- `src/agents/proactive_monitoring_agent.py` — Always-on Prometheus anomaly detector
  - Polls 8 Prometheus metrics every 60s (api_error_rate, api_latency_p95, kafka_consumer_lag,
    llm_error_rate, llm_latency_p99, approval_queue_depth, pipeline_failure_rate, process_memory_mb)
  - Detects 3 anomaly types: THRESHOLD_BREACH, TREND_ANOMALY (linear regression slope), SUDDEN_DROP
  - Correlates multiple signals from same service → MULTI_SIGNAL (boosted confidence)
  - Redis deduplication: same service can't produce duplicate alert within 30 minutes
  - Respects maintenance windows (checked via Redis `maintenance_windows` key)
  - Creates incidents via `incident.created` (source: proactive_monitoring, auto_classify: True)
  - Runs as a standalone background task alongside EventOrchestrator

- `src/agents/post_mortem_agent.py` — Automated SRE post-mortem generator
  - Reconstructs full incident timeline from `audit.audit_events` PostgreSQL table
  - Analyzes contributing factors: high blast radius, low RCA confidence, rollback needed, novel failure
  - Identifies what went well and what went wrong (rule-based analysis)
  - Generates 2-3 action items: rule-based + optional LLM assist (Claude `claude-sonnet-4-6`)
  - Renders full SRE post-mortem Markdown document
  - Commits to GitHub via `github_mcp` (creates `post-mortems/{incident_id}.md`)
  - Creates Jira story with action items (via `jira_mcp`)
  - Publishes `incident.postmortem_ready` Kafka event

- `src/streaming/consumers/data_pipeline_incident_bridge.py` — Cross-system Kafka bridge
  - Consumer group: `data-pipeline-bridge` (separate from `event-orchestrator`)
  - Consumes: `pipeline.failed`, `pipeline.sla_missed`, `pipeline.config_update`, `pipeline.healthy`
  - `pipeline.failed` → Creates `incident.created` with failure pattern, severity, RCA hint
  - `pipeline.sla_missed` → Creates P3 SLA tracking incident (no ServiceNow ticket)
  - `pipeline.config_update` → Republishes as `pipeline.reconfigure` for DeployerAgent
  - `pipeline.healthy` (≥3 runs) → Publishes `incident.auto_closed`, clears dedup key
  - Redis dedup: same DAG can't generate duplicate incident within 60 minutes
  - Deterministic incident ID: `INC-PIPE-{sha256(dag_id:run_id:window)[:12]}`
  - Severity mapping: schema_mismatch/permission_denied/data_quality_fail → P2; others → P3
  - RCA hints pre-filled for IncidentIntelligenceAgent (15 pattern names)
  - New Kafka topics produced: `incident.created`, `incident.auto_closed`, `pipeline.reconfigure`

### Architecture Notes
- `ProactiveMonitoringAgent` closes the gap where infrastructure issues are only detected
  reactively (after a ServiceNow ticket arrives). Now the platform self-detects issues
  via Prometheus signals, ~10 minutes ahead of user reports.
- `PostMortemAgent` is triggered at the end of the FAST workflow (after `feedback_loop` node).
  It runs asynchronously — post-mortem creation doesn't block incident closure.
- `DataPipelineIncidentBridge` connects System 2 (Data Engineering) to System 1 (Incident
  Management). Without this bridge, a failed pipeline would not create an incident and would
  require manual monitoring. With it, pipeline failures flow through the full FAST workflow
  including human approval for complex remediations.

---

## [1.0.0] — 2026-06-21

### Initial
- 12-node LangGraph incident workflow mirroring backend/orchestrator
- FAST 9-agent Governor architecture
- 4-agent Swarm RAG: vector_agent, graph_agent, keyword_agent, metadata_agent
- Reciprocal Rank Fusion (RRF) with k=60 (`src/rag/hybrid_search_engine.py`)
- EventOrchestrator Kafka consumer (`src/streaming/consumers/event_orchestrator.py`)
- IncidentConsumer (`src/streaming/consumers/incident_consumer.py`)
- Governance: audit logger, EU AI Act compliance, data retention
- Guardrails: content safety, PII masking
- MCP client integrations: ServiceNow, Jira, GitHub
