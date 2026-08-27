# Changelog — Enterprise Agentic Platform

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased] — 2026-06-22

### Added
- `docs/architecture-review-2026.md` — 44-gap analysis, target state architecture, Q3 2026 → Q1 2027 roadmap
- `docs/agent-capabilities-2026.md` — Full real-team vs agent capability comparison
- `docs/project-context.md` — Unified AI context document for all sessions
- `docs/agent-governance.md` — AI governance policy (AGP-001), EU AI Act compliance
- `docs/repository-audit-report.md` — Full repository inventory and cleanup recommendations
- `docs/vibe-coding-guide.md` — AI-assisted development guide with prompting strategies
- `logs/project-changes.log` — Comprehensive change history
- 5 new agents across both systems:
  - `agents/data_agent/src/agents/connection_test_agent.py` — Pre-deployment source validation
  - `agents/data_agent/src/agents/pipeline_monitoring_agent.py` — Post-deployment Airflow watcher
  - `agents/servicenow_agent/src/agents/proactive_monitoring_agent.py` — Prometheus anomaly detector
  - `agents/servicenow_agent/src/agents/post_mortem_agent.py` — Automated SRE post-mortem generator
  - `agents/servicenow_agent/src/streaming/consumers/data_pipeline_incident_bridge.py` — Cross-system bridge
- 8 new Kafka topics: `pipeline.healthy`, `pipeline.failed`, `pipeline.sla_missed`, `pipeline.config_update`, `pipeline.reconfigure`, `pipeline.health_report`, `incident.postmortem_ready`, `incident.auto_closed`
- 13 missing `__init__.py` files (critical Python package markers)

### Fixed
- Missing `__init__.py` in 13 package directories — `pytest` discovery and cross-package imports now work
- Grafana dashboard UID mismatch — frontend observability iframe now loads correctly

### Security
- Identified credential exposure in `agents/data_agent/.env` and `.claude/settings.local.json`
- **Action required:** Rotate GitHub token, ServiceNow password before cloud deployment

### Architecture Gaps Identified (to be resolved in Q3 2026)
- `G-SCALE-04`: LangGraph MemorySaver → needs PostgresSaver
- `G-SEC-03`: Shared-secret JWT → needs RS256 + OIDC
- `G-AI-01`: No LLM fallback → needs LiteLLM router
- `G-REL-01`: No Dead Letter Queue → needs `dlq.*` topics
- `G-ENT-01`: No SSO/OIDC → needs Okta/AAD integration

---

## [1.0.0] — 2026-06-21

### Initial Release
- Incident Management System: 12-node LangGraph workflow (ingest → feedback_loop)
- Data Engineering Agent: APEX 8-phase workflow (normalize → deploy)
- FAST 9-agent Governor architecture
- Swarm RAG with 4-agent retrieval + RRF fusion
- 70+ source types across 9 categories
- EU AI Act HIGH-RISK compliance documentation
- ISO 27001, ISO 42001, GDPR compliance documentation
- 14-service Docker Compose infrastructure
- Next.js 14 frontend with React Query
- Kafka event-driven architecture with 47+ topics
- Medallion architecture (Landing → Bronze → Silver → Gold, with Gold as the final analytics-ready layer)
- MCP servers: ServiceNow, Jira, GitHub, Airflow
