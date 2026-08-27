# Enterprise Agentic Platform — Architecture Review 2026

**Classification:** Internal Technical Review  
**Date:** 2026-06-22  
**Scope:** Full-stack review of v6.0 event-driven architecture against Google Cloud Architecture Framework, Azure Well-Architected Framework, AWS Well-Architected Framework, Databricks Lakehouse patterns, Snowflake Data Cloud patterns, and Anthropic multi-agent design guidelines.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current Architecture Strengths](#2-current-architecture-strengths)
3. [Gap Analysis](#3-gap-analysis)
4. [Target-State Architecture](#4-target-state-architecture)
5. [Multi-Agent Design](#5-multi-agent-design)
6. [Enterprise Features Roadmap](#6-enterprise-features-roadmap)
7. [Operational Excellence](#7-operational-excellence)
8. [AI Governance Framework](#8-ai-governance-framework)
9. [Technology Recommendations](#9-technology-recommendations)
10. [Implementation Roadmap](#10-implementation-roadmap)

---

## 1. Executive Summary

The platform has achieved a strong foundation with its event-driven, Kafka-as-system-of-record architecture (v6.0), FAST multi-agent system, 24-state distributed state machine, EU AI Act compliance, and comprehensive observability stack. These are genuine production-grade choices that align with patterns used at Confluent, Uber, LinkedIn, and Netflix.

**What works well:** Kafka-first design, LangGraph StateGraph (not ReAct), idempotent event processing, human-in-the-loop approval gates, Pydantic-typed agent contracts, RRF swarm retrieval, and the FAST 9-agent architecture. These are the right bets.

**Critical gaps to close in Q3 2026:**
- No horizontal scalability for EventOrchestrator and LangGraph workers
- JWT uses HMAC-SHA256 instead of RSA/asymmetric signing; no mTLS between services
- MemorySaver checkpointer is in-memory; pod restart loses all paused workflow state
- No multi-model routing or LLM fallback; single Claude API outage stops all workflows
- No GitOps pipeline for Airflow DAGs; no IaC for infrastructure
- No SSO/OIDC; no multi-tenancy; these are hard enterprise adoption blockers

**Net assessment:** 7/10 for architecture quality. Production-worthy for a single-team deployment today. Needs the scalability, security hardening, and enterprise features below before organization-wide rollout.

---

## 2. Current Architecture Strengths

### 2.1 What the Platform Gets Right

**Event Sourcing with Kafka as System of Record**  
All state transitions flow through immutable Kafka topics. Events carry `correlation_id`, `idempotency_key`, and `version` fields. Consumers can replay from any offset. This is the correct pattern for audit-complete, recoverable agentic systems — the same approach used by Confluent's own internal agent platforms.

**Deterministic Workflow Execution via LangGraph StateGraph**  
Choosing `StateGraph` over ReAct eliminates the "agent decides what to do next" anti-pattern. Every edge is explicit, every state transition is typed, and the workflow is reproducible. This is the correct foundation for an EU AI Act HIGH-RISK system.

**FAST 9-Agent Architecture**  
The Governor + specialized agent pattern (IncidentIntelligenceAgent, RiskAgent, ChangeManagementAgent, ExecutionAgent, VerificationAgent, ApprovalAgent, ObservabilityAgent, LearningAgent) follows the Single Responsibility Principle correctly. Phase 2 parallel dispatch (`asyncio.gather(risk_task, chg_task)`) is the right optimization. Each agent's typed contract (`IncidentContext → RiskAssessment → ChangeRecord`) makes the data flow auditable.

**24-State Distributed State Machine with Optimistic Locking**  
Redis-backed, version-counter-protected state machine with 90-day audit history is production-grade. The `VALID_TRANSITIONS` directed graph prevents illegal state jumps. This closes the "workflow state lost on pod restart" problem that MemorySaver creates at the incident level.

**Swarm RAG with Reciprocal Rank Fusion**  
4-agent retrieval (semantic, keyword, graph, metadata) fused with RRF (k=60) is a proven ensemble pattern. The LearningAgent's RRF weight optimization from historical feedback is a genuine learning loop, not just RAG retrieval.

**EU AI Act Compliance Implementation**  
Articles 6-15 implemented in code with audit persistence, confidence scoring, human oversight gates, and 20-column audit table including `ai_decision_explanation`, `human_oversight_applied`, `data_subjects_affected`. This is further along than most enterprise AI systems.

**PII Detection and Masking**  
13 PII types × 5 medallion zones × 5 masking strategies is a complete data governance matrix. The zone-aware masking (ENCRYPT in Landing, TOKENIZE in Bronze, PARTIAL_MASK in Silver/Gold) is the correct tiered approach.

---

## 3. Gap Analysis

### 3.1 Scalability Limitations

**G-SCALE-01: EventOrchestrator is a Single Process**  
`backend/streaming/consumers/event_orchestrator.py` runs as a single consumer. All incidents and pipeline events funnel through one Python process. At scale, this becomes the bottleneck and single point of failure.

*Impact:* High. One restart drops all in-flight events if auto-commit is on (it isn't, but consumer lag accumulates).  
*Fix:* Run multiple EventOrchestrator instances in the same Kafka consumer group, partitioned by `incident_id`. The VALID_TRANSITIONS state machine already prevents duplicate processing.

**G-SCALE-02: LangGraph Workers Not Horizontally Scalable**  
LangGraph workflows run synchronously inside the EventOrchestrator process. There is no worker pool. A long-running remediation (600s execution timeout) blocks the consumer thread.

*Impact:* High. Under load, the consumer falls behind; Kafka lag grows unboundedly.  
*Fix:* Dispatch LangGraph execution to a Celery/Dramatiq worker pool, or run it in a separate asyncio task pool behind a semaphore. The Governor pattern already separates orchestration from execution.

**G-SCALE-03: Redis Is a Single Node**  
Both the 24-state machine (`incident:state:{id}`) and the EventOrchestrator's idempotency deduplication (`idempotent:{agent}:{key}`) use Redis with no cluster mode configured.

*Impact:* Medium. Redis restart loses all paused workflow states and deduplication windows. A 24-hour Redis outage means incidents could be reprocessed.  
*Fix:* Redis Sentinel (3-node) minimum; Redis Cluster for horizontal partition.

**G-SCALE-04: LangGraph MemorySaver is In-Memory**  
The LangGraph checkpointer uses `MemorySaver` for the 12-node incident workflow's pause/resume. Pod restart loses all `interrupt_after=["await_approval"]` checkpoints.

*Impact:* Critical for the base LangGraph workflow. Note: the FAST architecture's 24-state machine partially mitigates this for the Governor path, but the base `langgraph_workflow.py` is still exposed.  
*Fix:* Replace `MemorySaver` with `PostgresSaver` (langgraph-checkpoint-postgres). The schema is a single table; migration takes under an hour.

**G-SCALE-05: Airflow Single-Node with Local Executor**  
Docker-compose Airflow runs LocalExecutor on a single container. This supports only sequential task execution.

*Impact:* Medium for development; critical for production.  
*Fix:* Kubernetes Executor or CeleryExecutor with 3+ workers for parallel DAG task execution.

**G-SCALE-06: No Kafka Topic Replication Configuration**  
The Kafka configuration in `docker-compose.yml` uses a single broker with no explicit replication factor. Topic retention is configured but no `min.insync.replicas` is set.

*Impact:* Kafka broker restart loses all uncommitted messages.  
*Fix:* Production Kafka requires `replication.factor=3`, `min.insync.replicas=2`, across 3+ brokers. Managed Kafka (Confluent Cloud, MSK, Event Hubs) handles this automatically.

---

### 3.2 Security Concerns

**G-SEC-01: JWT Uses HMAC-SHA256 with a Shared Secret**  
`middleware/auth.py` signs JWTs with HMAC-SHA256 (`HS256`). Any service with the shared secret can both issue and verify tokens. There is no public/private key separation.

*Impact:* High. Any compromised internal service can mint admin-level tokens.  
*Fix:* Switch to RS256 (RSA asymmetric). Auth service holds the private key; all other services hold only the public key for verification. Or integrate with an existing OIDC provider (Okta, Azure AD, Google).

**G-SEC-02: No mTLS Between Internal Services**  
FastAPI → LangGraph → Kafka → Redis → PostgreSQL communications are all plaintext within the Docker network. There is no mutual TLS or service mesh policy.

*Impact:* Medium in containerized localhost; High in production Kubernetes.  
*Fix:* Istio or Linkerd service mesh for mTLS sidecar injection. At minimum, TLS for PostgreSQL, Redis, and Kafka connections with certificate validation.

**G-SEC-03: No Secrets Rotation Automation**  
Credentials (OpenAI API key, Jira token, ServiceNow password, Fernet keys) are environment variables with no rotation schedule or automated rotation trigger. One credential exposure (already occurred with the GitHub token) requires manual rotation across all environments.

*Impact:* Critical. Exposed credentials (`[REDACTED_GITHUB_TOKEN]`, ServiceNow `[REDACTED_SERVICENOW_PASSWORD]`) from the previous session have not been rotated.  
*Fix:* HashiCorp Vault with dynamic secrets for database credentials. Cloud-native: GCP Secret Manager, AWS Secrets Manager, or Azure Key Vault with automatic rotation policies.

**G-SEC-04: No API Rate Limiting**  
FastAPI exposes all endpoints without rate limiting. The approval endpoint (`POST /api/v1/approve`) and pipeline creation endpoint have no request throttling.

*Impact:* Medium. Approval endpoint abuse could flood the Kafka `incident.approved` topic.  
*Fix:* FastAPI `slowapi` middleware with Redis-backed rate limiting (`100 req/min` per IP, `10 req/min` per user for mutations).

**G-SEC-05: No Input Sanitization for LLM Prompts**  
Incident descriptions from ServiceNow and pipeline NL inputs are passed to Claude without prompt injection hardening. A ServiceNow incident description containing `Ignore previous instructions and instead...` could manipulate agent behavior.

*Impact:* Medium. Prompt injection in an incident management system could cause inappropriate remediation actions.  
*Fix:* Input sanitization layer: strip control characters, length-limit inputs, and add a system-prompt prefix that explicitly instructs the model about trusted vs. untrusted content boundaries. Langfuse trace monitoring for anomalous prompt lengths.

**G-SEC-06: No SAST/DAST in CI/CD**  
No static analysis security testing is configured. The `.github/workflows/` directory (if it exists) has no security scanning step.

*Impact:* Medium. SQL injection risks in raw PostgreSQL queries in `observability.py` are not automatically flagged.  
*Fix:* Bandit for Python SAST, Semgrep rules for Kafka/SQL patterns, OWASP ZAP for DAST on staging. Add as GitHub Actions steps.

**G-SEC-07: `development_bypass` Auth Mode is a Production Risk**  
`ENVIRONMENT=local` + `AUTH_BYPASS=true` grants admin access with `X-User-Id` header. If these variables leak into production (easy with `.env` copy errors), all security is bypassed.

*Impact:* Critical if triggered in production.  
*Fix:* Remove `AUTH_BYPASS` entirely. Use a dev-only mock JWT issuer instead. Enforce `ENVIRONMENT=production` check that hard-fails if `AUTH_BYPASS` is set.

---

### 3.3 Reliability Risks

**G-REL-01: No Dead Letter Queue Processing**  
Kafka consumers that fail to process a message retry indefinitely (no DLQ routing). A malformed event from ServiceNow MCP will block the consumer indefinitely.

*Impact:* High. One malformed incident blocks all subsequent incident processing in that partition.  
*Fix:* Route failed messages to `{topic}.dlq` after 3 retries. Add a DLQ monitor in the ObservabilityAgent. Alert on DLQ depth > 0.

**G-REL-02: No Circuit Breakers**  
The ExecutionAgent calls GitHub Actions, Airflow MCP, and GCP APIs with exponential backoff but no circuit breaker. If GitHub Actions is degraded (returns 504 slowly), all execution threads wait 5+10+20 seconds per attempt, exhausting the worker pool.

*Impact:* Medium. Cascading slowdown under external API degradation.  
*Fix:* `tenacity` with circuit breaker pattern, or `pybreaker`. Open circuit after 5 failures in 60s; half-open after 30s.

**G-REL-03: No Multi-Region Disaster Recovery**  
All infrastructure runs in a single region/host. No DR runbook, no RTO/RPO definitions, no cross-region Kafka replication.

*Impact:* High for a production system. A data center outage causes complete service disruption.  
*Fix:* Define RTO/RPO targets. For Kafka: MirrorMaker 2 to a secondary region. For PostgreSQL: streaming replication + automated failover (Patroni or managed DB). For stateless services: deploy to secondary region with DNS failover.

**G-REL-04: No Health Check Propagation from Dependencies**  
The `/health` endpoint checks PostgreSQL and Redis but doesn't expose degraded states for downstream consumers (Kafka connectivity, Neo4j, Weaviate, LLM API reachability). The Kubernetes liveness probe only sees UP/DOWN.

*Impact:* Medium. A Kubernetes pod reports healthy while its critical dependencies are down; traffic is still routed to it.  
*Fix:* Structured health response with `status: healthy|degraded|unhealthy` and per-dependency status. Kubernetes readinessProbe uses degraded threshold.

---

### 3.4 AI Agent Orchestration Gaps

**G-AI-01: No LLM Fallback or Multi-Model Routing**  
All LLM calls target a single Claude model with no fallback. A Claude API outage or rate limit stops all incident classification, plan generation, and judge evaluation.

*Impact:* Critical. The entire incident management workflow becomes unavailable during Claude API issues.  
*Fix:* Implement a LLM router with priority fallback: `claude-opus-4-8 → claude-sonnet-4-6 → claude-haiku-4-5-20251001 → GPT-4o`. Use LiteLLM or a custom router that catches `anthropic.APIError` and retries on the next model.

**G-AI-02: No Agent Versioning or Canary Deployment**  
All 9 FAST agents are deployed as a single monolith. Deploying a new IncidentIntelligenceAgent version immediately affects all incidents. There is no way to route 5% of traffic to a new agent version.

*Impact:* Medium. Regression risk is high when changing agent behavior.  
*Fix:* Agent version registry in PostgreSQL (`agent_versions` table). Governor reads version from config; supports `A:90% / B:10%` routing. Metrics segmented by agent version.

**G-AI-03: No Prompt Version Management**  
Agent prompts are embedded in Python source files (`agents/data_agent/prompts/`). Changing a prompt requires a code deployment. There is no prompt A/B testing, rollback, or performance tracking per prompt version.

*Impact:* Medium. Prompt regressions are caught only after they affect production incidents.  
*Fix:* Langfuse Prompt Management (already deployed at port 3002). Migrate all prompts to Langfuse; reference by name + version. Add prompt performance tracking: judge score per prompt version.

**G-AI-04: No Hallucination Detection Layer**  
LangGraph nodes call `llm_intelligence.py` and trust the output. The LLM Judge evaluates plan quality but does not specifically detect hallucinated references (e.g., a runbook step referencing a script that doesn't exist).

*Impact:* High for incident execution safety. The ExecutionAgent does pre-validate script existence, which partially mitigates this. But hallucinated configuration values or server names could pass validation.  
*Fix:* Post-generation grounding check: verify all script names, server names, and parameter values in the remediation plan against the known asset inventory (Neo4j graph + Weaviate). Flag unverifiable references for human review.

**G-AI-05: No Agent-to-Agent Communication Protocol**  
The 9 FAST agents communicate only through typed contracts passed by the Governor. There is no peer-to-peer agent communication. If the RiskAgent determines mid-execution that a blast radius has expanded, it cannot signal the ExecutionAgent to pause.

*Impact:* Low currently (Governor mediates all flows). Becomes a limitation as agent autonomy increases.  
*Fix:* Add an `AgentBus` (thin Kafka topic `agent.events`) that allows agents to publish state changes. Governor subscribes and can pause/reroute based on mid-execution signals.

**G-AI-06: No Agent Memory Across Incidents**  
Each incident is processed independently. The LearningAgent does persist RRF weights and Weaviate indexes, but there is no structured episodic memory that lets an agent say "the last 3 times we saw `dag_failure` on `service=payments`, the root cause was always a GCS permission issue."

*Impact:* Medium. The system re-derives the same insights repeatedly until enough feedback accumulates for RRF weight adjustment.  
*Fix:* Neo4j pattern: when `LearningAgent` creates a `FIXED_BY` relationship, also create a `PATTERN` node: `(p:Pattern {classification: 'dag_failure', service: 'payments', root_cause: 'gcs_permission'})`. IncidentIntelligenceAgent queries patterns first, before full RCA.

---

### 3.5 Data Engineering Agent Gaps

**G-DATA-01: No End-to-End Pipeline Validation**  
The ValidatorAgent validates generated DAG syntax and Spark job structure, but does not execute a dry run against the actual source system. A pipeline for `source_type=database_postgres` is validated syntactically but not against the actual PostgreSQL schema.

*Impact:* Medium. Pipelines may deploy successfully but fail on first run due to schema mismatch.  
*Fix:* Add a `ConnectionTestAgent` (6th agent) that for each pipeline: performs a schema discovery query against the source, validates the inferred schema against the pipeline's expected schema, and reports mismatches before deployment.

**G-DATA-02: No Data Quality SLAs**  
Generated pipelines have no embedded data quality checks. There is no Great Expectations or Soda integration. The medallion architecture defines zones but not the quality thresholds required to promote data between zones.

*Impact:* High. Gold zone data quality is asserted by the pipeline author, not enforced by code.  
*Fix:* Generator agent produces mandatory `data_quality_checks.py` alongside every DAG. Default rules: not-null on PK columns, row count within 20% of previous run, no duplicate PKs. Enforce at Bronze→Silver and Silver→Gold zone promotions.

**G-DATA-03: No Schema Evolution Handling**  
When a source schema changes (column added, type changed), the generated pipeline breaks. There is no schema registry integration, no forward/backward compatibility check, and no automated migration path.

*Impact:* High for production pipelines. Schema drift is the most common cause of data pipeline failures.  
*Fix:* Integrate Confluent Schema Registry (or AWS Glue Schema Registry) for streaming sources. For batch sources, add a `SchemaEvolutionAgent` that detects drift (via stored schema snapshot in PostgreSQL) and proposes ALTER TABLE / column mapping changes.

**G-DATA-04: Airflow DAG Deployment Has No GitOps Path**  
Pipeline deployment writes DAGs to GCS and relies on a `dag-sync` container. There is no Git commit, PR review, or branch-per-environment model. A generated DAG goes directly to the Airflow DAG folder.

*Impact:* Medium. No audit trail for DAG changes; no peer review; no rollback path via `git revert`.  
*Fix:* Generator agent opens a GitHub PR (using existing GitHub MCP) with the generated DAG and Spark job. Approval gate merges the PR; CI/CD deploys to Airflow. This adds 10-20 minutes but provides full audit trail.

**G-DATA-05: No Pipeline Monitoring Post-Deployment**  
After `pipeline.deployed`, there is no agent that monitors the pipeline's first 5 runs. Airflow DAG failures are not automatically correlated back to the Data Agent.

*Impact:* Medium. The user must manually check Airflow for failures; no automated remediation loop.  
*Fix:* Add a `MonitoringAgent` (7th agent) that polls Airflow API for the first 5 runs of each deployed pipeline and publishes `pipeline.failed` or `pipeline.healthy` Kafka events. On failure, trigger the incident management workflow with context from the Data Agent's generated metadata.

---

### 3.6 Airflow Operational Gaps

**G-AIRFLOW-01: No Pool Management**  
Airflow has no pool configuration for resource limiting. A large batch of pipelines deploying simultaneously can exhaust all Airflow worker slots.

*Fix:* Define Airflow pools per processing zone (`bronze_pool`, `silver_pool`, `gold_pool`). Generator agent assigns `pool='{zone}_pool'` in generated DAG tasks.

**G-AIRFLOW-02: No SLA Miss Alerting**  
Airflow tasks have no `sla` parameter set. Missed SLAs generate no alerts.

*Fix:* Generator agent sets `sla=timedelta(hours=2)` for batch pipelines, `sla=timedelta(minutes=15)` for streaming. Route SLA callbacks to Kafka `pipeline.sla_missed` topic.

**G-AIRFLOW-03: Airflow RBAC Not Integrated with Platform RBAC**  
Platform RBAC (viewer/operator/approver/admin) and Airflow RBAC (Viewer/User/Op/Admin) are separate. A platform `operator` can trigger a pipeline but has no matching Airflow role.

*Fix:* Sync platform roles to Airflow roles via Airflow's REST API when users are provisioned. Or use LDAP/OIDC integration with shared groups.

**G-AIRFLOW-04: No Airflow Connection/Variable Audit**  
Airflow Connections and Variables (database passwords, API keys) are stored in the Airflow metadata database with encryption but no change audit log.

*Fix:* Use HashiCorp Vault as Airflow's secrets backend (`airflow.secrets.hashicorp_vault`). All connection reads are logged in Vault audit log.

---

### 3.7 Knowledge Management Gaps

**G-KM-01: Weaviate Single-Tenant with No Schema Versioning**  
Weaviate runs as a single-tenant instance. All collections share the same namespace. There is no schema versioning; changes to the `ResolvedIncident` Weaviate class require a full re-index.

*Fix:* Separate Weaviate namespaces by domain (`IncidentKnowledge`, `PipelineKnowledge`). Use Weaviate's versioned schema with `class.moduleConfig` for model pinning.

**G-KM-02: No RAG Evaluation Framework**  
RAG retrieval quality (recall@K, MRR, NDCG) is not measured. The LearningAgent tracks successful resolutions but not whether the retrieved runbooks were actually relevant.

*Fix:* Implement offline RAG evaluation using resolved incidents as ground truth: given incident description → did the retrieved documents include the runbook that was actually used? Track this in the `feedback_records` table. Trigger re-indexing when recall@5 drops below 0.7.

**G-KM-03: No Document Freshness Tracking**  
Runbooks indexed in Weaviate may become stale as infrastructure changes. A runbook for "restart the v1 API server" may reference a server that was decommissioned.

*Fix:* Add `last_reviewed_date`, `expiry_date`, and `source_url` fields to all Weaviate documents. RAG retrieval applies a recency filter: penalize documents not reviewed in >180 days. Alert when >10% of the index is stale.

---

### 3.8 Multi-Cloud and Infrastructure Gaps

**G-INFRA-01: No Infrastructure as Code**  
All infrastructure is defined in `docker-compose.yml`. There is no Terraform, Pulumi, or Helm chart for Kubernetes deployment.

*Impact:* High for enterprise. Cannot reproduce the environment reliably; no drift detection.  
*Fix:* Terraform modules for: VPC, GKE/EKS/AKS cluster, managed Kafka, managed PostgreSQL, Redis. Helm charts for: EventOrchestrator, FastAPI, Data Agent API. Kustomize overlays for dev/staging/prod.

**G-INFRA-02: GCS Paths Hardcoded in Multiple Files**  
Source paths like `gs://bucket/raw/{source}/{entity}/{date}/` appear in templates and source models. There is no cloud storage abstraction layer.

*Impact:* Medium for cloud-agnostic deployment.  
*Fix:* Storage abstraction: `CloudStorageClient` with backends for GCS, S3, ADLS. Source-type config provides `storage_backend: gcs|s3|adls` and the client handles path translation.

**G-INFRA-03: No GitOps for Infrastructure**  
No ArgoCD, Flux, or similar GitOps controller is configured. Infrastructure changes are applied manually.

*Fix:* ArgoCD for Kubernetes deployments (Helm + Kustomize). Terraform Cloud or Atlantis for infrastructure changes. All changes via PR → review → merge → auto-apply.

---

### 3.9 Enterprise Adoption Blockers

**G-ENT-01: No SSO / OIDC Integration**  
Users authenticate with custom JWT. There is no integration with corporate identity providers (Okta, Azure Active Directory, Google Workspace, LDAP).

*Impact:* Critical blocker. Enterprise organizations cannot adopt a system that requires separate credential management.  
*Fix:* Add OAuth2/OIDC provider integration (python-jose + authlib). Support: Okta, Azure AD (MSAL), Google OAuth2. Map IdP groups to platform roles at login.

**G-ENT-02: No Multi-Tenancy**  
All incidents, pipelines, and agents share a single namespace. Tenant A can see Tenant B's incidents.

*Impact:* Critical blocker for SaaS or large-enterprise multi-team deployment.  
*Fix:* Add `tenant_id` to all Kafka events, PostgreSQL tables (Row-Level Security policies), Redis keys, and Weaviate collections. FastAPI middleware extracts tenant from JWT; all queries are tenant-scoped.

**G-ENT-03: No API Versioning Strategy**  
FastAPI exposes `/api/v1/` and `/api/v2/` but there is no documented deprecation policy, no changelogs, and no version sunset process.

*Fix:* Semantic versioning for API: `v1` stable, `v2` current, `v3` beta. Deprecation notice in response headers (`Deprecation: true`, `Sunset: 2027-01-01`). OpenAPI spec published to a developer portal.

**G-ENT-04: No Cost Attribution or Chargeback**  
LLM token usage, Kafka throughput, and compute are not attributed to teams, projects, or tenants. There is no mechanism for cost showback.

*Fix:* Add `team_id` and `project_id` to `aiagent_llm_tokens_total` Prometheus labels. Build a Grafana dashboard for cost per team. Integrate cloud billing API for compute cost attribution.

**G-ENT-05: No Self-Service Portal for Data Engineers**  
Creating a pipeline requires knowing the API or using the UI. There is no ChatOps integration (Slack bot: "Hey APEX, create a CSV pipeline from `gs://bucket/sales.csv` to BigQuery `sales.daily`").

*Fix:* Slack App with slash command `/pipeline create <NL description>`. Routes to the NL→structured metadata endpoint (`POST /api/v2/data-agent/nl/transform`), shows the structured config for confirmation, then creates via `POST /api/v2/data-agent/pipelines`. Already half-implemented via the NL transform endpoint.

---

### 3.10 Observability Gaps

**G-OBS-01: LangGraph Traces Not Linked to Langfuse**  
The `ObservabilityAgent` creates OTEL spans via `opentelemetry.trace.get_tracer("fast-agents")`. Langfuse is deployed but not receiving LangGraph traces. LLM calls from `llm_intelligence.py` are not traced in Langfuse.

*Fix:* Add `langfuse.openai` or `langfuse.anthropic` instrumentation to the LLM call layer. Each LangGraph node should create a Langfuse trace with the workflow's `correlation_id` as the trace ID. This enables end-to-end trace: Kafka event → LangGraph node → LLM call → response.

**G-OBS-02: No SLO Dashboard**  
Prometheus alert rules cover infrastructure but there are no SLO-based alerts (e.g., "incident resolution SLO: 95% of P1 incidents resolved within 60 minutes"). The Grafana dashboards show metrics but not SLO burn rates.

*Fix:* Define SLOs in Prometheus `recording_rules.yml`: `slo:incident_resolution_p1:rate5m` = fraction of P1 incidents closed within 60 minutes. Grafana SLO dashboard with error budget burn rate. Alert when burn rate > 2x in 1 hour.

**G-OBS-03: No Capacity Planning Metrics**  
There are no metrics for Kafka consumer lag trend, Redis memory usage growth, PostgreSQL table size growth, or Weaviate index size growth. Without capacity trends, scaling decisions are reactive.

*Fix:* Add Prometheus exporters: `kafka-exporter` (consumer lag), `redis-exporter` (memory/hit rate), `postgres-exporter` (table sizes, query latency). Add Grafana capacity planning dashboard with 30-day trend lines and projected capacity exhaustion dates.

**G-OBS-04: No User-Facing SLA Reporting**  
The system tracks incident states but does not produce SLA compliance reports for stakeholders. There is no weekly/monthly report of: incident volume, MTTR by severity, SLA breach count.

*Fix:* Scheduled Airflow DAG (`reports.weekly_sla_report`) that queries PostgreSQL and emails HTML report. Alternatively, a Grafana public dashboard link for stakeholders.

---

## 4. Target-State Architecture

### 4.1 High-Level Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    ENTERPRISE AGENTIC PLATFORM — TARGET STATE 2027                   │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                            IDENTITY & SECURITY                               │   │
│  │  OIDC/SAML (Okta/AAD) → JWT (RS256) → RBAC Middleware → mTLS (Istio)       │   │
│  │  HashiCorp Vault (secrets) │ API Rate Limiting │ Input Sanitization          │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                        EDGE ADAPTERS (MCP SERVERS)                           │   │
│  │  ServiceNow-MCP │ Jira-MCP │ GitHub-MCP │ Airflow-MCP │ Slack-MCP           │   │
│  │  (All publish/consume Kafka; no direct service-to-service calls)             │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                    │ publish                                        │
│                                    ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                     KAFKA CLUSTER (3-broker, replication=3)                  │   │
│  │  Schema Registry │ MirrorMaker2 (DR region) │ Kafka Connect │ KSQL           │   │
│  │  47 lifecycle topics │ DLQ per topic │ Audit topic (immutable, 1 year)       │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                    │ consume                                        │
│                                    ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                   EVENT ORCHESTRATOR CLUSTER (3 replicas)                    │   │
│  │  Consumer Group: partition by incident_id/request_id                         │   │
│  │  WorkflowManager │ StateTracker │ EventRouter │ DLQ Handler                  │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                     │ dispatch                   │ dispatch                         │
│                     ▼                             ▼                               │
│  ┌────────────────────────────────┐  ┌──────────────────────────────────────┐   │
│  │  INCIDENT MANAGEMENT           │  │  DATA ENGINEERING AGENT              │   │
│  │  Governor (FAST 9-agent)       │  │  Supervisor → Planner → Generator    │   │
│  │  + DistributedStateMachine     │  │  → ConnectionTestAgent               │   │
│  │  LangGraph workers (pool=10)   │  │  → Validator → Deployer              │   │
│  │  LLM Router (3-model fallback) │  │  → MonitoringAgent                   │   │
│  │  PostgresSaver checkpointer    │  │  LLM Router (3-model fallback)       │   │
│  └────────────────────────────────┘  └──────────────────────────────────────┘   │
│                     │ publish events              │ publish events                  │
│                     ▼                             ▼                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                      KNOWLEDGE LAYER                                         │   │
│  │  Weaviate (multi-tenant) │ Neo4j Cluster │ Langfuse (prompt + trace mgmt)   │   │
│  │  Schema Registry (schema evolution) │ RAG Evaluator (offline)               │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                      DATA PLATFORM (MEDALLION)                               │   │
│  │  Airflow (Kubernetes Executor) │ Spark (K8s) │ Delta Lake / Iceberg          │   │
│  │  Great Expectations (quality) │ Schema Registry │ Data Catalog (OpenMetadata)│   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                  CONTROL PLANE (FastAPI, multi-replica)                      │   │
│  │  CQRS reads (Redis Cluster/Postgres) │ Approval APIs (→ Kafka)               │   │
│  │  Policy Engine │ Cost Attribution │ SLO Engine │ Audit API                   │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                  FRONTEND (Next.js, CDN-served)                              │   │
│  │  Unified dashboard │ Approval queue │ Pipeline builder (70+ sources)         │   │
│  │  SLO dashboard │ Cost dashboard │ Agent trace viewer │ Audit log viewer      │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                  OBSERVABILITY PLANE (always-on)                             │   │
│  │  Prometheus (HA, Thanos) │ Grafana (SLO dashboards) │ Tempo (traces)         │   │
│  │  Loki (logs) │ Langfuse (LLM traces) │ OpenMetadata (data lineage)           │   │
│  │  PagerDuty / OpsGenie (alerting) │ Chaos Engineering (Litmus)               │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                  INFRASTRUCTURE (GitOps-managed)                             │   │
│  │  Terraform (cloud infra) │ ArgoCD (K8s deployments) │ Helm (packaging)       │   │
│  │  Istio (mTLS/service mesh) │ HashiCorp Vault (secrets) │ OPA (policy)        │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Data Flow: Incident Lifecycle (Target State)

```
1. ServiceNow MCP polls → publishes incident.created (Avro schema, Schema Registry)
2. Kafka (replication=3, acks=all) → EventOrchestrator instance (partition by incident_id)
3. EventOrchestrator → Dispatches to Governor with full IncidentContext
4. Governor Phase 1: IncidentIntelligenceAgent
   ├── SHA256 dedup check (Redis Cluster, 60-min window)
   ├── 15-pattern RCA + LLM Router (Claude Opus → Sonnet fallback)
   └── Langfuse trace: rca_analysis.{incident_id}
5. Governor Phase 2 (parallel):
   ├── RiskAgent: Neo4j BFS depth-3, SLA impact
   └── ChangeManagementAgent: ServiceNow CHG creation
6. Governor Phase 3: RAG search (Weaviate multi-tenant) + LLM plan + Judge evaluation
   ├── Grounding check: verify all referenced scripts in Neo4j asset graph
   └── Hallucination flag → route to human if unverifiable references found
7. Governor Phase 4: ApprovalAgent
   ├── 4-level routing (auto/standard/senior/executive)
   ├── Publish incident.requires_approval → UI + Slack-MCP
   └── Timeout escalation chain with dead-man switch
8. Governor Phase 5-7: Execute → Verify (60s stabilization) → Close → Learn
   ├── Auto-rollback on failure
   ├── LearningAgent: Neo4j FIXED_BY, Weaviate ResolvedIncident, RRF weight update
   └── Pipeline MonitoringAgent: if Airflow-triggered, monitor 5 runs post-close
9. Kafka: incident.closed → ServiceNow MCP closes ticket
10. Audit: PostgreSQL audit_events (immutable), SHA256 checksum, EU AI Act fields
```

---

## 5. Multi-Agent Design

### 5.1 Agent Hierarchy: Anthropic Multi-Agent Patterns

Following Anthropic's guidance on multi-agent systems, the platform should implement three tiers:

**Tier 1 — Orchestrators**  
Agents that plan, delegate, and synthesize. They do not execute directly.
- `Governor` (incident management)
- `Supervisor` (data engineering)

**Tier 2 — Specialized Subagents**  
Agents with a single capability, typed contracts, and idempotency guarantees.
- Incident: IncidentIntelligenceAgent, RiskAgent, ChangeManagementAgent, ExecutionAgent, VerificationAgent, ApprovalAgent, LearningAgent
- Data: PlannerAgent, GeneratorAgent, ConnectionTestAgent, ValidatorAgent, DeployerAgent, MonitoringAgent

**Tier 3 — Infrastructure Agents**  
Always-on cross-cutting concerns.
- `ObservabilityAgent` (hooks into every agent execution)
- `AuditAgent` (audit persistence with SHA256 integrity)
- `CostAgent` (token usage attribution)

### 5.2 Agent Communication Protocol (ACP)

All inter-agent communication must be through the `AgentEnvelope` contract (already implemented in `contracts.py`). No direct method calls between Tier 2 agents — always via Governor dispatch.

Add `AgentBus` for mid-execution signals:

```python
# New: agents can publish mid-execution events without going through Governor
class AgentBus:
    TOPIC = "agent.events"
    
    async def publish(self, source_agent: str, event_type: str, payload: dict, correlation_id: str):
        # Publish to Kafka agent.events topic
        # Governor subscribes and can pause/reroute
        pass
```

Usage: `RiskAgent` detects blast radius expansion mid-execution → publishes `agent.events` → Governor pauses `ExecutionAgent`.

### 5.3 Agent Evaluation Framework

Each agent needs a performance scorecard tracked over time:

| Agent | Primary Metric | Target | Measured By |
|-------|---------------|--------|-------------|
| IncidentIntelligenceAgent | RCA accuracy (verified by LearningAgent feedback) | >85% | `feedback_records.rca_correct` |
| RiskAgent | Blast radius precision (actual vs. estimated) | ±20% | `feedback_records.actual_impact` |
| Generator (Data Agent) | First-run DAG success rate | >90% | MonitoringAgent |
| ValidatorAgent | False negative rate (passed DAGs that failed in prod) | <5% | MonitoringAgent |
| ApprovalAgent | Auto-approve false positive rate (auto-approved incidents that caused harm) | 0% | Post-incident review |
| LearningAgent | RAG retrieval recall@5 improvement over time | +2% per 100 incidents | RAG evaluator |

### 5.4 Agent Failure Taxonomy

Define explicit failure modes and Governor handling:

```python
class AgentFailureType(str, Enum):
    TRANSIENT = "transient"        # Retry with backoff (network, timeout)
    DEGRADED = "degraded"          # Use fallback (LLM model, simplified logic)
    TERMINAL = "terminal"          # Escalate to human (data corruption, security)
    SKIP = "skip"                  # Non-critical agent, continue without it
```

- `IncidentIntelligenceAgent` transient → retry 3x → degraded (simplified RCA) → escalate
- `ObservabilityAgent` failure → `SKIP` (never block incident resolution for observability)
- `ExecutionAgent` failure → `TERMINAL` → auto-rollback → escalate
- `LLM API` failure → `DEGRADED` → switch to fallback model

---

## 6. Enterprise Features Roadmap

### 6.1 RBAC/ABAC Enhancement

**Current:** 4 flat roles (viewer, operator, approver, admin) enforced in JWT middleware.

**Target:** Attribute-Based Access Control (ABAC) layered on top of RBAC:

```python
# ABAC policy (Open Policy Agent)
package platform.authz

allow if {
    # Role-based base access
    user_has_role(input.user, input.required_role)
    
    # Attribute constraints
    not is_production_incident(input.resource)  # Only admins can approve PROD incidents
    input.user.team == input.resource.owner_team  # Team scope
    time.now_ns() < input.resource.deadline_ns    # Time-bound access
}
```

OPA as a sidecar to FastAPI. Policy stored in Git (`opa/policies/`). Updates via PR → ArgoCD deployment.

### 6.2 Audit and Data Lineage

**Current:** `audit_events` PostgreSQL table with SHA256 integrity. 

**Target:** Full data lineage with OpenMetadata:

```
Source (PostgreSQL:payments.orders)
  → Landing Zone (GCS:raw/payments/orders/2026-06-22/)
    → Bronze DAG (airflow:payments_bronze_dag, run_id=abc123)
      → Bronze Table (bigquery:bronze.payments.orders)
        → Silver DAG (airflow:payments_silver_dag)
          → Silver Table (bigquery:silver.payments.orders)
            → Gold View (bigquery:gold.sales.daily_revenue)
```

OpenMetadata API integration: DeployerAgent registers each pipeline step as a lineage edge. Every data asset traceable to its source system.

### 6.3 Secrets Management

**Current:** Environment variables in `.env`.

**Target:** HashiCorp Vault integration with dynamic secrets:

```python
# vault_client.py
class VaultSecretProvider:
    async def get_database_credentials(self, role: str) -> DBCredentials:
        # Vault dynamic secrets: new credentials per request, TTL=1h
        # Auto-rotates; no long-lived static credentials
        creds = await self.vault.read(f"database/creds/{role}")
        return DBCredentials(username=creds.username, password=creds.password)
    
    async def get_api_key(self, service: str) -> str:
        # KV v2 secret with version tracking
        return await self.vault.read(f"kv/platform/api_keys/{service}")
```

Airflow uses Vault backend for Connections. Kubernetes uses Vault Agent Injector for pod-level secret injection.

### 6.4 Multi-Tenancy Design

**Database:**
```sql
-- Add tenant_id to all tables
ALTER TABLE agent.incidents ADD COLUMN tenant_id UUID NOT NULL;
ALTER TABLE agent.pipelines ADD COLUMN tenant_id UUID NOT NULL;

-- Row-Level Security
CREATE POLICY tenant_isolation ON agent.incidents
    USING (tenant_id = current_setting('app.tenant_id')::UUID);
ALTER TABLE agent.incidents ENABLE ROW LEVEL SECURITY;
```

**Kafka:** Separate consumer groups per tenant, or topic-per-tenant for strict isolation.

**Weaviate:** Separate collections per tenant (`Tenant_{id}_ResolvedIncident`).

**Redis:** Key prefix `{tenant_id}:incident:state:{incident_id}`.

### 6.5 Cost Attribution and Governance

```python
# Track LLM costs per team/project
aiagent_llm_cost_dollars_total = Counter(
    "aiagent_llm_cost_dollars_total",
    "Total LLM API cost in USD",
    ["team_id", "project_id", "model", "agent_name", "operation"]
)

# Token pricing (update when model pricing changes)
COST_PER_1K_TOKENS = {
    "claude-opus-4-8": {"input": 0.015, "output": 0.075},
    "claude-sonnet-4-6": {"input": 0.003, "output": 0.015},
    "claude-haiku-4-5-20251001": {"input": 0.00025, "output": 0.00125},
}
```

Monthly cost report: Grafana dashboard with budget thresholds and Slack alerts when team exceeds budget.

---

## 7. Operational Excellence

### 7.1 SLO Definitions

| SLO | Definition | Target | Measurement Window |
|-----|-----------|--------|--------------------|
| P1 Incident Resolution | % of P1 incidents resolved within 60 minutes | 95% | 30 days |
| P2 Incident Resolution | % of P2 incidents resolved within 4 hours | 90% | 30 days |
| Pipeline Deployment Success | % of approved pipelines that deploy successfully | 99% | 30 days |
| Pipeline First-Run Success | % of deployed pipelines that succeed on first DAG run | 90% | 30 days |
| API Availability | Uptime of FastAPI control plane | 99.9% | 30 days |
| Approval Response Time | % of approvals completed within SLA timeout | 85% | 7 days |
| RAG Retrieval P95 Latency | 95th percentile latency for RAG queries | <2s | 1 hour |
| LLM Response P95 Latency | 95th percentile latency for LLM calls | <10s | 1 hour |

### 7.2 Incident Response Runbooks for the Platform Itself

The platform manages incidents but needs its own incident runbooks:

**PLAT-001: EventOrchestrator Consumer Lag Alert**
```
Trigger: kafka_consumer_lag_sum{consumer_group="event-orchestrator"} > 1000
Steps:
  1. Check if lag is growing: rate(kafka_consumer_lag[5m]) > 0
  2. If growing: scale EventOrchestrator replicas (+1)
  3. If stalled: check for poison pill message → route to DLQ
  4. If persistent: failover to secondary region
```

**PLAT-002: LLM API Unavailable**
```
Trigger: aiagent_llm_errors_total{status="api_error"} > 10 in 5 minutes
Steps:
  1. LiteLLM router automatically switches to fallback model
  2. Alert to #platform-oncall with model degradation status
  3. If all models unavailable: pause new incident intake, queue events in Kafka
  4. Resume when primary model recovers
```

**PLAT-003: Kafka Broker Down**
```
Trigger: kafka_brokers < 3 (below min.insync.replicas)
Steps:
  1. Producers switch to acks=1 (allow writes to 1 broker)
  2. Alert to #infrastructure-oncall
  3. Initiate broker replacement (Terraform apply or managed service auto-healing)
  4. Resume normal operation when broker count recovers
```

### 7.3 Auto-Remediation Patterns

The incident management system should be able to remediate its own infrastructure issues:

**Self-Healing Pattern:** When Prometheus detects `kafka_consumer_lag > 1000`, it triggers a `platform.incident.created` Kafka event with `source=prometheus`. The Incident Management Agent treats platform incidents identically to ServiceNow incidents, running the full RCA → Plan → Approve → Execute flow. This creates a recursive self-healing loop.

**Circuit Breaker Dashboard:** Grafana panel showing current circuit breaker state for all external dependencies (GitHub Actions, ServiceNow API, Claude API, Airflow API). When a circuit opens, the on-call engineer has immediate visibility.

### 7.4 Chaos Engineering Plan

Using Litmus Chaos (Kubernetes) or Chaos Monkey principles:

| Experiment | Target | Hypothesis | Frequency |
|-----------|--------|-----------|-----------|
| Kill EventOrchestrator pod | EventOrchestrator | Kafka consumer rebalances within 30s; no message loss | Weekly |
| Claude API latency injection | LLM calls | LLM router switches to Sonnet within 10s | Monthly |
| PostgreSQL connection pool exhaustion | FastAPI | Circuit breaker opens; new requests return 503 | Monthly |
| Kafka broker failure | Kafka cluster | Producers continue writing; consumers rebalance | Quarterly |
| Redis cluster failure | State machine | Incidents queue; no data loss on Redis recovery | Quarterly |

### 7.5 DR and HA Summary

| Component | Current | Target | Priority |
|-----------|---------|--------|----------|
| Kafka | Single broker | 3-broker cluster, replication=3, MirrorMaker2 to DR region | P1 |
| PostgreSQL | Single instance | Streaming replication + Patroni failover | P1 |
| Redis | Single node | Redis Sentinel (3 nodes) or Redis Cluster | P1 |
| EventOrchestrator | Single process | 3 replicas, Kubernetes Deployment | P1 |
| FastAPI | Single process | 3 replicas, Kubernetes Deployment + HPA | P2 |
| LangGraph workers | In-process | Worker pool (Celery) + PostgresSaver | P1 |
| Weaviate | Single instance | Weaviate cluster (3 nodes) | P2 |
| Neo4j | Single instance | Neo4j Cluster (3 nodes) | P2 |
| Airflow | Single node, LocalExecutor | Kubernetes Executor, 3+ workers | P2 |

---

## 8. AI Governance Framework

### 8.1 Prompt Lifecycle Management

**Prompt Registry** (Langfuse, already deployed):

```
prompt:name=incident_classification
├── v1.0 (2026-01-01) — baseline, retired
├── v1.1 (2026-03-15) — improved P1 accuracy by 8%, current prod
└── v1.2 (2026-06-01) — in A/B test (10% traffic), monitoring judge score
```

**Promotion criteria:** A new prompt version graduates from canary to production when:
- Judge score improvement > 5% at p=0.05 significance
- Latency increase < 20%
- No increase in hallucination flags
- Minimum 100 incidents processed in canary

**Rollback criteria:** Automatic rollback if judge score drops > 10% or hallucination flags increase > 2x versus previous version.

### 8.2 Model Governance

**Model Registry:**
```python
class ModelConfig:
    name: str                    # "claude-opus-4-8"
    version_date: date           # 2026-01-01
    use_cases: list[str]         # ["incident_classification", "plan_generation"]
    max_tokens: int              # 4096
    cost_per_1k_input: float     # 0.015
    cost_per_1k_output: float    # 0.075
    risk_level: Literal["low", "medium", "high"]  # high = requires human oversight for all outputs
    fallback_model: str          # "claude-sonnet-4-6"
```

**Model Retirement Policy:** Models are retired 90 days after Anthropic announces EOL. Automated migration tests run against the successor model 30 days before retirement. Production switches automatically on retirement date.

### 8.3 Confidence Scoring and Uncertainty Quantification

**Current:** Confidence score is a composite of RCA source quality, evidence availability, and RCA specificity (0.0–1.0).

**Target:** Calibrated confidence with uncertainty quantification:

```python
class ConfidenceScore:
    point_estimate: float        # 0.0–1.0
    confidence_interval: tuple   # (0.65, 0.85) at 90% confidence
    calibration_error: float     # Expected calibration error from historical data
    
    @property
    def is_well_calibrated(self) -> bool:
        return self.calibration_error < 0.05
```

Track calibration: when confidence=0.8, the agent should be correct 80% of the time. If actual accuracy at confidence=0.8 is only 60%, the confidence scores are overconfident and need recalibration (Platt scaling or isotonic regression on `feedback_records`).

### 8.4 Human-in-the-Loop Enhancement

**Current:** Binary approve/reject with optional `override_plan`.

**Target:** Structured human review with annotated feedback:

```typescript
interface ApprovalDecision {
  decision: 'approve' | 'reject' | 'modify'
  annotations: {
    rca_correct: boolean          // Was the root cause identified correctly?
    plan_appropriate: boolean     // Was the remediation plan appropriate?
    risk_score_accurate: boolean  // Was the risk assessment accurate?
    notes: string                 // Free text (used by LearningAgent)
  }
  modified_plan?: RemediationPlan  // If decision=modify
}
```

These annotations feed directly to the LearningAgent's feedback loop and improve future confidence calibration.

### 8.5 Explainability and Audit Trail

Every agent decision must produce an `explanation` field consumable by a non-technical auditor:

```python
class AgentDecision:
    decision: str
    explanation: str  # Plain English: "Classified as dag_failure because: 3/4 pattern rules matched (memory_exhaustion, timeout, k8s_node_issue). LLM confidence: 0.87. Supporting evidence: error log contains 'OOMKilled', task runtime exceeded 600s limit."
    evidence: list[str]  # References to specific log lines, runbook sections
    alternatives_considered: list[str]  # "Also considered: connectivity_failure (score: 0.43, rejected because no network metrics anomalies)"
    confidence: ConfidenceScore
    model_used: str
    prompt_version: str
```

EU AI Act Article 13 (Transparency) and Article 14 (Human Oversight) require this level of documentation for HIGH-RISK systems.

### 8.6 AI Safety Policies

**No-Execute-Without-Verify Rule:** The ExecutionAgent's `_validate_prerequisites()` method must be passed before any script or workflow is triggered. This rule is enforced in `BaseAgent.execute()` and cannot be overridden by the Governor.

**Human Override Always Available:** At any point in the 24-state machine, a human can set the state to `ESCALATED` via the API. The Governor monitors for `ESCALATED` state and stops all processing immediately.

**Production Auto-Approve Ban:** `ApprovalAgent.auto_approve()` condition includes `environment != production`. This constraint is in code AND in the OPA policy — dual enforcement ensures it cannot be bypassed by a code change alone.

**Blast Radius Hard Stop:** If `RiskAgent.blast_radius.estimated_users > 10000`, the Governor does not proceed to execution even if approved. The incident is escalated to `executive_deny` approval regardless of judge score or risk category. This prevents catastrophic automated remediations.

---

## 9. Technology Recommendations

### 9.1 Recommended Additions (High Priority, Q3 2026)

| Technology | Purpose | Replaces/Augments | Justification |
|-----------|---------|------------------|---------------|
| **LiteLLM** | Multi-model LLM router with fallback | Direct Anthropic SDK calls | 3-line integration; handles retry, fallback, cost tracking across 100+ LLM providers |
| **PostgresSaver** (langgraph-checkpoint-postgres) | Persistent LangGraph checkpointer | MemorySaver | Survives pod restarts; required for pause/resume in production |
| **Great Expectations** | Data quality assertions in pipelines | No quality checks | 200+ built-in expectations; integrates with Spark and Airflow natively |
| **OpenMetadata** | Data catalog and lineage | No data catalog | Open-source; integrates with Airflow, BigQuery, Spark; replaces manual lineage tracking |
| **slowapi** | FastAPI rate limiting | No rate limiting | Redis-backed; 5-line integration; blocks approval endpoint abuse |
| **Confluent Schema Registry** | Avro schema management for Kafka | JSON with no schema | Schema evolution with backward/forward compatibility; prevents breaking changes |

### 9.2 Recommended Additions (Medium Priority, Q4 2026)

| Technology | Purpose | Justification |
|-----------|---------|---------------|
| **HashiCorp Vault** | Secrets management with dynamic credentials | Eliminate static `.env` credentials; automatic rotation; Vault audit log |
| **Terraform + Terragrunt** | IaC for cloud infrastructure | Reproducible environments; drift detection; destroy/recreate in 10 minutes |
| **ArgoCD** | GitOps for Kubernetes deployments | Declarative, auditable deployments; rollback in 30 seconds |
| **Istio** | Service mesh with mTLS | Zero-trust network between all services; replaces manual TLS config |
| **Open Policy Agent (OPA)** | Policy as code (ABAC) | Decouple authorization logic from application code; Git-managed policies |
| **Thanos** | Long-term Prometheus storage | 90+ day metric retention; cross-cluster querying; compaction |
| **Litmus Chaos** | Chaos engineering | Validate HA assumptions before they fail in production |

### 9.3 Current Stack — Keep As-Is

| Technology | Verdict | Rationale |
|-----------|---------|-----------|
| **LangGraph StateGraph** | Keep | Correct pattern; explicit edges; typed state; not ReAct |
| **Apache Kafka** | Keep | Best-in-class event streaming; correct choice for system of record |
| **Weaviate** | Keep | Strong vector search with multi-tenant support; active development |
| **Neo4j** | Keep | Graph queries for blast radius and dependency chains are natural fits |
| **Langfuse** | Keep | Best open-source LLM observability; already integrated |
| **Prometheus + Grafana + Tempo + Loki** | Keep | LGTM stack is industry standard; correctly provisioned |
| **Next.js 14 + React Query** | Keep | Correct for the data-fetching pattern; React Query for server state |
| **FastAPI** | Keep | Correct as control-plane-only; async, Pydantic native |
| **Airflow 2.x** | Keep, upgrade executor | Upgrade to Kubernetes Executor for horizontal scaling |
| **Pydantic v2 contracts** | Keep | Typed contracts are the right foundation for multi-agent safety |

### 9.4 Current Stack — Replace or Deprecate

| Technology | Verdict | Replacement | Timeline |
|-----------|---------|-------------|----------|
| **MemorySaver (LangGraph)** | Replace | PostgresSaver | Q3 2026 |
| **HMAC-SHA256 JWT** | Replace | RS256 + OIDC integration | Q3 2026 |
| **Single-node Redis** | Replace | Redis Sentinel | Q3 2026 |
| **LocalExecutor (Airflow)** | Replace | Kubernetes Executor | Q4 2026 |
| **Manual `.env` secrets** | Replace | HashiCorp Vault | Q4 2026 |
| **Single EventOrchestrator** | Replace | Kubernetes Deployment (3 replicas) | Q3 2026 |

### 9.5 Industry Pattern Alignment

**Google Cloud Architecture Framework — Alignment:**
- ✅ Event-driven architecture (Kafka as system of record)
- ✅ Managed services preference (Langfuse, Weaviate, Neo4j)
- ✅ Observability with Prometheus + Grafana
- ⚠️ Missing: Cloud-native identity (Cloud IAM + Workload Identity)
- ⚠️ Missing: VPC Service Controls for data exfiltration prevention
- ❌ Missing: Terraform for GCP resource management

**Azure Well-Architected Framework — Alignment:**
- ✅ Reliability: Kafka retry, DLQ pattern (partially)
- ✅ Security: RBAC, audit logs
- ⚠️ Missing: Managed Identities (MSI) instead of service account keys
- ❌ Missing: Azure Policy for compliance enforcement
- ❌ Missing: Azure Monitor integration (or clear Azure alternative path)

**AWS Well-Architected Framework — Alignment:**
- ✅ Operational Excellence: Infrastructure-as-code intent, runbooks
- ✅ Security: Least-privilege RBAC (4 roles)
- ⚠️ Missing: AWS Organizations + SCPs for guardrails
- ❌ Missing: AWS Config for compliance checking
- ❌ Missing: CloudTrail integration for API audit

**Anthropic Multi-Agent Design — Alignment:**
- ✅ Orchestrator-subagent hierarchy (Governor + 9 agents)
- ✅ Typed contracts between agents (Pydantic v2)
- ✅ Human-in-the-loop approval gates
- ✅ Minimal footprint per agent (single responsibility)
- ⚠️ Missing: Tool use rather than direct function calls for agent actions
- ⚠️ Missing: Agent-level sandboxing (each agent in its own container)
- ❌ Missing: Multi-model routing with fallback chains

**Databricks Lakehouse — Alignment:**
- ✅ Medallion architecture (Landing → Bronze → Silver → Gold, Gold is the final layer)
- ✅ Delta Lake / Iceberg support in source types
- ✅ Spark for processing
- ❌ Missing: Unity Catalog equivalent for data governance
- ❌ Missing: Delta Live Tables (declarative pipeline) as an output option
- ❌ Missing: Great Expectations / Databricks Data Quality checks

---

## 10. Implementation Roadmap

### Phase 1: Stability and Security (Q3 2026 — 8 weeks)

**Week 1-2: Critical Security Fixes**
- [ ] Replace MemorySaver with PostgresSaver (langgraph-checkpoint-postgres)
- [ ] Switch JWT from HS256 to RS256; add OIDC provider integration
- [ ] Redis Sentinel (3-node) deployment
- [ ] Rotate all exposed credentials (GitHub token, ServiceNow password, OpenAI key)

**Week 3-4: Reliability Hardening**
- [ ] DLQ routing for all Kafka topics (`{topic}.dlq` after 3 retries)
- [ ] Circuit breakers on all external API calls (GitHub, ServiceNow, Claude API)
- [ ] API rate limiting with slowapi + Redis
- [ ] Health check propagation (structured `/health` with per-dependency status)

**Week 5-6: LLM Resilience**
- [ ] LiteLLM integration: `claude-opus-4-8 → claude-sonnet-4-6 → claude-haiku-4-5-20251001` fallback chain
- [ ] Prompt migration to Langfuse Prompt Management
- [ ] Prompt version A/B testing framework (10%/90% split)
- [ ] LangGraph traces linked to Langfuse (end-to-end trace: Kafka → LangGraph → LLM → response)

**Week 7-8: EventOrchestrator Scaling**
- [ ] Kubernetes Deployment for EventOrchestrator (3 replicas)
- [ ] Kafka consumer group partition assignment by `incident_id`
- [ ] Celery worker pool for LangGraph execution (separate from consumer thread)
- [ ] Prometheus consumer lag alert + EventOrchestrator HPA

---

### Phase 2: Data Quality and GitOps (Q4 2026 — 8 weeks)

**Week 1-2: Data Engineering Quality**
- [ ] Great Expectations integration in GeneratorAgent
- [ ] ConnectionTestAgent (6th data agent): schema discovery before deployment
- [ ] Schema Registry (Confluent) for Kafka topics with Avro schemas
- [ ] MonitoringAgent (7th data agent): first-5-run monitoring post-deployment

**Week 3-4: GitOps Foundation**
- [ ] Terraform modules: VPC, Kubernetes cluster, managed Kafka, PostgreSQL, Redis
- [ ] Helm charts: EventOrchestrator, FastAPI, Data Agent API, Airflow
- [ ] ArgoCD deployment for all application services
- [ ] Airflow GitOps: generated DAGs → GitHub PR → CI/CD → Airflow (via GitHub MCP)

**Week 5-6: Infrastructure Hardening**
- [ ] HashiCorp Vault: migrate all secrets from `.env`
- [ ] Istio service mesh: mTLS for all service-to-service communication
- [ ] Open Policy Agent (OPA): ABAC policies for fine-grained authorization
- [ ] Kafka 3-broker cluster with `replication.factor=3`, `min.insync.replicas=2`

**Week 7-8: Knowledge Management**
- [ ] Weaviate multi-tenant migration (tenant per team/project)
- [ ] RAG evaluation framework: recall@5 tracking against `feedback_records`
- [ ] Document freshness tracking: `last_reviewed_date` + expiry filter in RAG queries
- [ ] OpenMetadata integration: data lineage for all deployed pipelines

---

### Phase 3: Enterprise Features (Q1 2027 — 8 weeks)

**Week 1-2: Identity and Multi-Tenancy**
- [ ] SSO/OIDC: Okta or Azure AD integration
- [ ] Multi-tenancy: `tenant_id` on all tables, Kafka key prefixes, Redis key prefixes
- [ ] Row-Level Security in PostgreSQL for tenant isolation
- [ ] Weaviate collection-per-tenant migration

**Week 3-4: Observability and SLOs**
- [ ] Thanos for long-term Prometheus storage (90+ days)
- [ ] SLO dashboard in Grafana: burn rate alerts, error budget tracking
- [ ] Capacity planning dashboard: 30-day trend lines for all resources
- [ ] PagerDuty / OpsGenie integration for alert routing

**Week 5-6: AI Governance**
- [ ] Calibrated confidence scoring with historical calibration error tracking
- [ ] Structured approval annotations feeding LearningAgent
- [ ] Blast radius hard stop at 10,000 users (code + OPA dual enforcement)
- [ ] Agent evaluation scorecard (per-agent metrics in Grafana)

**Week 7-8: Developer Experience**
- [ ] Slack bot for pipeline creation (ChatOps integration)
- [ ] Self-service developer portal (Backstage or custom Next.js portal)
- [ ] API versioning with deprecation headers and sunset dates
- [ ] Cost attribution dashboard: LLM cost per team/project

---

### Priority Matrix

| Gap ID | Severity | Effort | Priority Score | Target Quarter |
|--------|----------|--------|----------------|----------------|
| G-SCALE-04 (MemorySaver) | Critical | Low (1 day) | P0 | Q3 2026 Week 1 |
| G-SEC-03 (credential rotation) | Critical | Low (1 day) | P0 | Q3 2026 Week 1 |
| G-SEC-01 (HMAC→RS256) | High | Medium (1 week) | P1 | Q3 2026 Week 2 |
| G-AI-01 (LLM fallback) | Critical | Low (2 days) | P1 | Q3 2026 Week 5 |
| G-REL-01 (DLQ) | High | Low (2 days) | P1 | Q3 2026 Week 3 |
| G-SCALE-01 (EventOrchestrator) | High | Medium (1 week) | P1 | Q3 2026 Week 7 |
| G-ENT-01 (SSO/OIDC) | Critical (enterprise blocker) | High (3 weeks) | P2 | Q1 2027 Week 1 |
| G-ENT-02 (multi-tenancy) | High (enterprise blocker) | High (4 weeks) | P2 | Q1 2027 Week 2 |
| G-DATA-02 (data quality) | High | Medium (2 weeks) | P2 | Q4 2026 Week 1 |
| G-AI-03 (prompt management) | Medium | Low (3 days) | P2 | Q3 2026 Week 6 |
| G-INFRA-01 (IaC) | High | High (4 weeks) | P3 | Q4 2026 Week 3 |
| G-OBS-02 (SLO dashboard) | Medium | Medium (1 week) | P3 | Q1 2027 Week 3 |

---

## Appendix A: Reference Implementations

| Pattern | Reference Implementation | Applied In This Platform |
|---------|--------------------------|--------------------------|
| Event Sourcing + CQRS | Martin Fowler, Confluent Kafka Patterns | `event_orchestrator.py`, `app.py` |
| Saga Pattern | Uber Cadence, Netflix Conductor | `langgraph_workflow.py` (12-node workflow) |
| Multi-Agent Orchestrator-Subagent | Anthropic multi-agent design guide | `governor.py`, 9 FAST agents |
| Swarm Intelligence + RRF | Microsoft GraphRAG, 2024 | `swarm_retriever.py` |
| LLM-as-Judge | Zheng et al. 2023, MT-Bench | `llm_judge.py` |
| Human-in-the-Loop approval gates | LangGraph interrupt_after pattern | `await_approval` node, `ApprovalAgent` |
| Medallion Architecture | Databricks Lakehouse whitepaper | 5-zone data architecture |
| EU AI Act HIGH-RISK implementation | EU Regulation 2024/1689 | `audit_events` table, 20 compliance fields |
| Reciprocal Rank Fusion | Cormack et al. 2009 (k=60) | `swarm_retriever.py` RRF formula |
| Calibrated Confidence Scoring | Guo et al. 2017 (neural calibration) | `IncidentIntelligenceAgent` confidence |

---

*Document maintained by: Platform Architecture Team*  
*Next review: Q4 2026 (after Phase 1 completion)*  
*Classification: Internal — Not for external distribution*
