# EU AI Act Compliance Assessment

**Document Reference:** COMP-EUAIA-001
**Version:** 1.0
**Date:** 2026-06-22
**Classification:** INTERNAL
**Owner:** AI Safety Officer
**Review Cycle:** Annual (next review: 2027-06-22)

---

## 1. Executive Summary

This document provides a comprehensive assessment of the Enterprise Agentic AI Platform's compliance with Regulation (EU) 2024/1689 (EU AI Act), which entered into force on 1 August 2024 with full applicability for high-risk AI systems from 2 August 2026.

The platform comprises two AI agent systems deployed in production:

- **Incident Management Agent**: Automates detection, triage, and remediation of IT infrastructure incidents via a 12-node LangGraph StateGraph workflow integrated with ServiceNow.
- **Data Engineering Agent (APEX)**: Automates data pipeline design, code generation, validation, and deployment across 70+ source types via a 5-agent LangGraph workflow.

**System Version:** 4.0.0
**AI System Provider:** Enterprise Platform Engineering
**Deployment Regions:** EU, US
**Registration ID (Article 51 EU Database):** AI-INC-AGENT-4-EU-2026

### Classification Summary

| AI System | EU AI Act Classification | Annex III Category |
|-----------|--------------------------|-------------------|
| Incident Management Agent | HIGH-RISK | Category 2 — Critical Infrastructure |
| Data Engineering Agent (APEX) | HIGH-RISK | Category 2 — Critical Infrastructure |

### Overall Compliance Status

| Article | Requirement | Status |
|---------|-------------|--------|
| Article 6 | Classification as High-Risk | COMPLIANT |
| Article 9 | Risk Management System | COMPLIANT |
| Article 10 | Data and Data Governance | COMPLIANT |
| Article 11 | Technical Documentation | COMPLIANT |
| Article 12 | Record Keeping | COMPLIANT |
| Article 13 | Transparency and Information to Users | COMPLIANT |
| Article 14 | Human Oversight | COMPLIANT |
| Article 15 | Accuracy, Robustness and Cybersecurity | COMPLIANT |

**Overall Assessment: COMPLIANT** with Articles 6–15 (Essential Requirements for High-Risk AI Systems).

This assessment was prepared by the AI Safety Officer based on code review of the production codebase, architecture documentation, and governance module analysis. It is subject to annual review and must be updated whenever significant changes are made to either AI system.

---

## 2. System Classification (Article 6)

### 2.1 Legal Basis for High-Risk Classification

Article 6(2) of the EU AI Act establishes that AI systems listed in Annex III are classified as high-risk. Both AI systems on this platform meet this classification on multiple grounds.

### 2.2 Annex III Category: Critical Infrastructure Protection

**Annex III, Point 2** covers AI systems intended to be used as safety components in the management and operation of critical digital infrastructure, road traffic, and the supply of water, gas, heating and electricity.

The Incident Management Agent directly satisfies this criterion:

- It monitors, triages, and autonomously proposes remediation actions for IT infrastructure incidents that may affect production services, databases, and critical application layers.
- Automated remediation actions (script execution, configuration changes, service restarts) are executed against production systems following human sign-off.
- A failure in the AI's classification or remediation planning could cause service degradation, data corruption, or unplanned outages affecting business-critical systems.

The Data Engineering Agent (APEX) satisfies this criterion via its impact on data infrastructure:

- It generates, validates, and deploys data pipelines processing business-critical data across the medallion architecture (Landing → Bronze → Silver → Gold zones, with Gold as the final analytics-ready layer).
- Incorrect pipeline generation or deployment could corrupt production data assets relied upon by downstream analytics, regulatory reporting, and business intelligence.
- Deployment decisions are automated and affect GCP BigQuery production datasets.

### 2.3 Automated Decision-Making with Production Impact

Both systems make consequential decisions with real-world impact:

**Incident Management Agent:**
- Classifies incident severity (P1–P4) — determines escalation paths and SLA response times
- Selects remediation scripts from the knowledge base for execution against production systems
- Generates and proposes multi-step remediation plans that may involve database operations, configuration changes, or service restarts

**Data Engineering Agent:**
- Selects source connectors and generates Apache Spark / Dataflow pipeline code
- Determines data schema mapping, transformation logic, and target BigQuery table write modes
- Deploys pipelines to Cloud Composer (Airflow) via automated DAG generation

### 2.4 Conformity Assessment Path (Article 43)

Per Article 43(2), the conformity assessment path for both systems is **internal control** (Annex VI), applicable where the AI system does not use techniques listed in Annex I(a) (neural networks trained on data). Both systems use pre-trained foundation models via API (OpenAI GPT-4, Anthropic Claude) rather than training custom neural networks, satisfying the conditions for the internal control path.

The assessment documented herein constitutes the required internal conformity assessment. It will be repeated:
- Whenever either AI system is substantially modified (Article 43(4))
- At minimum annually
- Prior to any new deployment in an EU-regulated context

### 2.5 EU AI Act Database Registration (Article 51)

High-risk AI systems must be registered in the EU AI Act database before being placed on the market or put into service. Registration details:

| Field | Value |
|-------|-------|
| System Name | AI Incident Management Agent |
| Version | 4.0.0 |
| Provider | Enterprise Platform Engineering |
| Risk Category | HIGH |
| Intended Purpose | Automated incident detection, classification, and remediation in IT infrastructure |
| Registration Date | 2026-06-22 |
| Deployment Regions | EU, US |

---

## 3. Risk Management System (Article 9)

### 3.1 Legal Requirement

Article 9 requires providers of high-risk AI systems to establish, implement, document, and maintain a risk management system throughout the entire AI system lifecycle. The risk management system must:

- Identify and analyse known and foreseeable risks associated with the AI system
- Estimate and evaluate risks that may emerge when used as intended and under conditions of reasonably foreseeable misuse
- Evaluate other risks based on data gathered from post-market monitoring
- Adopt appropriate risk management measures

### 3.2 Implementation

**Primary implementation:** `backend/governance/eu_ai_act_compliance.py`

The `EUAIActCompliance` class implements the risk management system and maintains compliance status across all seven regulated articles. The `validate_decision()` method enforces risk management rules at the point of each AI decision:

- Decisions with `risk_level` of `high` or `critical` that lack `has_human_oversight=True` are flagged as non-compliant with Article 14.
- Decisions with `confidence < 0.6` are flagged as failing the Article 15 accuracy threshold.
- All decisions must specify a `decision_type` for Article 13 transparency compliance.

### 3.3 LLM Guardrails

**Implementation:** `backend/guardrails/llm_guardrails.py`

The `LLMGuardrails` class provides multi-layer input and output validation wrapping every LLM call:

| Guardrail Component | Function | Threshold |
|--------------------|----------|-----------|
| `InputValidator` | Prompt injection detection via 16 regex patterns | Score < 0.5 → blocked |
| `InputValidator` | Command injection detection (incident context) | Any match → score 0.3 |
| `ContentModerator` | Blocked topic detection (malware, DDoS, attacks) | Any match → score 0.0 |
| `ContentModerator` | Sensitive topic detection (DROP TABLE, PROD DB) | Any match → score 0.7 |
| `OutputValidator` | Harmful command detection (rm -rf /, fork bomb) | Any match → score 0.2 |
| `OutputValidator` | Secrets/credential exposure detection | Any match → score 0.2 |
| `RateLimiter` | Rate limiting per identifier | 60/min, 500/hour |

The combined safety score is computed as `min(input_score, moderation_score)`. Any score below 0.5 blocks the LLM call and logs the rejection to the audit trail.

### 3.4 Circuit Breaker

**Implementation:** `backend/utils/circuit_breaker.py`

A circuit breaker tracks consecutive LLM call failures and opens after 5 consecutive failures, preventing cascading failures and protecting against systemic model degradation. The circuit breaker transitions:

- **Closed (normal):** All LLM calls pass through
- **Open (tripped):** All LLM calls are rejected; human review required
- **Half-open (recovery):** Single test call allowed to determine if model service has recovered

### 3.5 Confidence Thresholds

All LLM outputs are evaluated against confidence thresholds before actions are taken:

| Component | Minimum Confidence | Action if Below Threshold |
|-----------|-------------------|---------------------------|
| RAG retrieval | 0.60 | Return low-confidence flag; require human review |
| Remediation plan generation | 0.70 | Escalate to human; do not auto-proceed |
| LLM judge (safety evaluation) | 0.70 | Block action; require re-evaluation |
| Auto-execute decision | 0.95 | Route to human approval |

### 3.6 Human Approval Gate

All PROD-impacting actions require explicit human approval before execution. The approval gate is implemented as a Kafka event-driven pause in `backend/orchestrator/langgraph_workflow.py` at node `node_await_approval` (see Section 8 for full details).

**Risk Management Status: COMPLIANT**

---

## 4. Data and Data Governance (Article 10)

### 4.1 Legal Requirement

Article 10 requires that high-risk AI systems be developed using training, validation, and testing datasets that meet quality criteria, are subject to appropriate data governance practices, and are examined for biases.

### 4.2 Training Data Approach

Both AI systems on this platform do not train custom models. They use pre-trained foundation models accessed via API:

- **OpenAI GPT-4 / GPT-4 Turbo:** Used for incident classification, remediation plan generation, LLM judge evaluation, and data pipeline NL interpretation.
- **Anthropic Claude (claude-sonnet-4-6):** Used as an alternative LLM provider for pipeline generation and NL-to-structured transformation.

Because no custom model training occurs, Article 10's training data requirements apply to the **operational data** fed to the models at inference time, not to training datasets. This operational data is governed as described in Sections 4.3–4.6.

### 4.3 Data Quality Requirements for Operational Data

All operational data passed to LLMs is validated through the guardrails pipeline (Section 3.3) prior to submission. Validation includes:

- Input length limits (10,000 characters maximum) to prevent context overflow
- PII/sensitive data detection before LLM submission (Section 4.6)
- Structured Pydantic schema validation for all incident and pipeline objects before LLM submission
- Output format validation (JSON schema, script format) after LLM response

The RAG knowledge base (runbooks, past incident resolutions) is maintained with quality controls:
- Sources are versioned and attributed
- Embeddings are refreshed when source content is updated (`backend/rag/embedding_service.py`)
- RAG retrieval scores are logged and monitored; low-quality retrievals are flagged

### 4.4 Audit Logging

**Implementation:** `backend/governance/audit_logger.py`

The `AuditLogger` class and `AuditEventType` enum implement comprehensive event logging across 15 distinct event types:

**AI Decision Events:**
- `AI_DECISION` — Every autonomous AI decision
- `AI_RECOMMENDATION` — AI-generated recommendations surfaced to users
- `AI_CLASSIFICATION` — Incident severity and category classifications
- `AI_RISK_ASSESSMENT` — Risk level evaluations

**Human Oversight Events:**
- `HUMAN_APPROVAL` — Human approval of AI recommendation
- `HUMAN_REJECTION` — Human rejection of AI recommendation
- `HUMAN_OVERRIDE` — Human override where decision differs from AI recommendation
- `HUMAN_REVIEW` — Human review without decision change

**Data Events:**
- `DATA_ACCESS` — Access to sensitive or operational data
- `DATA_MODIFICATION` — Modifications to data assets
- `DATA_DELETION` — Deletion of data
- `PII_ACCESS` — Access to records containing PII

**System Events:**
- `SYSTEM_CONFIG_CHANGE` — Configuration changes
- `MODEL_UPDATE` — Model version or provider changes
- `REMEDIATION_EXECUTION` — Execution of remediation scripts

**Security Events:**
- `AUTH_SUCCESS` / `AUTH_FAILURE` / `UNAUTHORIZED_ACCESS`

Each `AuditEvent` record includes: event ID, ISO 8601 timestamp, actor, actor type (human/ai/system), action, resource, outcome, risk level, AI decision explanation, human oversight flag, confidence score, PII involvement flag, and a SHA-256 integrity checksum.

### 4.5 Data Lineage

**Implementation:** `dags/dag_utilities/logging/lineage_tracker.py` · `agents/data_agent/src/repository/migration_repository.py`

End-to-end data lineage is tracked for all data engineering pipelines. Lineage records capture:
- Source system and extraction timestamp
- Transformation steps applied at each medallion layer (Landing → Bronze → Silver → Gold)
- Target dataset and load timestamp
- Schema versions at each stage
- Data quality metrics at each transformation boundary

**Legacy Migration Lineage:** The `migration_lineage` table (DDL: `agents/data_agent/ddl/apex/14_legacy_migration.sql`) provides object-level dependency lineage for migrated stored procedures. Each record documents: parent object, child object, reference type (`CALLS`, `SELECTS_FROM`, `INSERTS_INTO`, `UPDATES`, `DELETES_FROM`), topological execution level, and migration job context. This creates a complete provenance chain from source SSIS package → extracted stored procedure → generated PySpark artifact.

Lineage data supports Article 10 compliance by providing a complete audit trail of how operational data flows through the platform, enabling post-market monitoring and bias detection.

### 4.6 PII Detection and Data Protection

**Implementation:** `agents/data_agent/src/security/pii_detection.py`

The `PIIDetector` class automatically scans DataFrames for PII before data is loaded into target zones. It detects the following PII and PHI types:

| PII Type | Detection Method | Recommended Masking |
|----------|-----------------|---------------------|
| SSN | Regex pattern `\d{3}-\d{2}-\d{4}` | HASH (SHA-256) |
| Credit Card | Regex pattern (16 digits, separators) | PARTIAL_MASK (last 4) |
| Email | Regex + column name indicators | TOKENIZE |
| Phone | Regex pattern (US formats) | PARTIAL_MASK |
| IP Address | Regex pattern (IPv4) | HASH |
| Date of Birth | Column name indicators (dob, birthdate) | FAKE (synthetic date) |
| Driver's License | Column name indicators | HASH |
| Passport | Column name indicators | HASH |
| Medical Record Number | Column name indicators (mrn, patient_id) | HASH |
| Health Plan Number | Column name indicators | HASH |
| Bank Account | Column name indicators | PARTIAL_MASK |
| Routing Number | Column name indicators | HASH |
| Name | Column name indicators (name, first_name) | TOKENIZE |

**Masking strategies available:**

- `REDACT` — Replace value with `***REDACTED***`
- `HASH` — SHA-256 hash of original value (irreversible)
- `TOKENIZE` — Format-preserving token via MD5 prefix `TOK_...`
- `PARTIAL_MASK` — Show last 4 characters, mask remainder
- `ENCRYPT` — Reversible encryption for values requiring recovery
- `NULL` — Replace with NULL
- `FAKE` — Replace with synthetic but realistic data

PII detection results are persisted to the `data_classification` PostgreSQL table via `persist_classifications()`, linking detected PII to the data governance enforcement pipeline.

**Data Retention for PII:** PII data is retained for a maximum of 90 days per `backend/governance/data_retention.py` (`DataCategory.PII_DATA`, retention_days=90, legal_basis="GDPR Article 5(1)(e)"). No archive is created before deletion of PII records.

**Data and Data Governance Status: COMPLIANT**

---

## 5. Technical Documentation (Article 11)

### 5.1 Legal Requirement

Article 11 requires providers to draw up technical documentation before a high-risk AI system is placed on the market or put into service. The documentation must demonstrate compliance with the requirements in Chapter III, Section 2, and must be kept up-to-date.

### 5.2 Documentation Inventory

The following documentation is maintained and kept current:

| Document | Path | Content |
|----------|------|---------|
| Architecture Reference | `docs/architecture.md` | Event-driven architecture, Kafka topics (6 incident topics), component responsibilities, Kafka-as-system-of-record pattern |
| Platform Specification | `docs/spec.md` | Full platform specification — authoritative reference for both AI systems |
| EU AI Act Compliance Assessment | `docs/compliance/eu-ai-act.md` | This document — Article-by-article compliance evidence |
| ISO 42001 Assessment | `docs/compliance/iso-42001.md` | AI Management System conformance assessment |
| Compliance Matrix | `docs/compliance/compliance-matrix.md` | 45-control matrix across SOC2, ISO42001, NIST AI RMF, EU AI Act, MITRE ATLAS, Observability |
| Data Agent Guide | `docs/data-agent-guide.md` | APEX data agent E2E testing and validation procedures |
| Testing Strategy | `docs/testing.md` | E2E test plan for both systems, chaos engineering scenarios |
| APEX Agent README | `agents/data_agent/APEX_README.md` | Data Engineering Agent capabilities, 70+ source types, medallion zones |
| MCP Server Integrations | `mcp-servers/README.md` | Model Context Protocol server integrations and tool definitions |
| Legacy Migration DDL | `agents/data_agent/ddl/apex/14_legacy_migration.sql` | 4 tables: migration_job, migration_object, migration_lineage, migration_artifact — records all SP extraction, dependency graph, and LLM artifact generation |

### 5.3 Codebase Documentation

Technical documentation is embedded in code through structured docstrings and type annotations:

- `backend/governance/eu_ai_act_compliance.py` — Article-by-article implementation with compliance status tracking
- `backend/guardrails/llm_guardrails.py` — Guardrail classes with usage examples in module docstring
- `backend/governance/audit_logger.py` — Full audit event type documentation via `AuditEventType` enum
- `agents/data_agent/src/models/source.py` — 70+ source type definitions with Pydantic models
- `agents/data_agent/src/models/` — Canonical Pydantic models for all platform data structures

### 5.4 System Performance Metrics (Documented)

| Metric | Target | Monitoring |
|--------|--------|------------|
| Incident classification accuracy | ≥95% correct | Audit log analysis |
| RAG retrieval confidence | ≥0.60 average | Langfuse tracing |
| LLM judge safety score | ≥0.70 average | Prometheus metrics |
| Incident resolution time (P95) | ≤5 minutes | Grafana dashboards |
| Human oversight coverage (PROD) | 100% of PROD actions | Approval audit log |
| Pipeline generation success rate | ≥90% | APEX metrics |

**Technical Documentation Status: COMPLIANT**

---

## 6. Record Keeping (Article 12)

### 6.1 Legal Requirement

Article 12 requires high-risk AI systems to be designed and built with capabilities enabling automatic recording of events (logs) throughout the AI system's lifetime. The logs must be able to ensure a level of traceability of the AI system's functioning throughout its lifetime appropriate to the intended purpose.

### 6.2 Audit Log Implementation

**Implementation:** `backend/governance/audit_logger.py`

The `AuditLogger` class provides enterprise-grade audit logging with the following characteristics:

**Event Coverage:** 18 event types across 5 categories (AI decisions, human oversight, data events, system events, security events) — covering every significant action taken by or toward the AI systems.

**Log Record Fields:**

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | String (AUD-{timestamp}-{counter}) | Unique event identifier |
| `timestamp` | ISO 8601 UTC | When the event occurred |
| `event_type` | AuditEventType enum | Category of event |
| `actor` | String | Who/what performed the action |
| `actor_type` | human / ai / system | Actor category |
| `action` | String | What was done |
| `resource` | String | What was acted upon |
| `resource_type` | String | Type of resource |
| `outcome` | success / failure / pending | Result |
| `risk_level` | low / medium / high / critical | Risk classification |
| `ai_decision_explanation` | String | LLM-generated explanation of the decision |
| `human_oversight_applied` | Boolean | Whether human reviewed this action |
| `confidence_score` | Float (0.0–1.0) | AI model confidence |
| `pii_involved` | Boolean | Whether PII was processed |
| `cross_border_transfer` | Boolean | GDPR cross-border flag |
| `checksum` | SHA-256 (16 chars) | Integrity verification |

**Integrity Protection:** Each audit record includes a SHA-256 checksum computed from `event_id + timestamp + event_type + action + resource`. This detects tampering with log records.

### 6.3 Retention Policy

**Implementation:** `backend/governance/data_retention.py`

The `DataRetentionManager` class implements differentiated retention policies by data category:

| Data Category | Retention Period | Legal Basis | Deletion Method |
|---------------|-----------------|-------------|-----------------|
| Audit Logs | 2,555 days (7 years) | EU AI Act Article 12, SOC2 | Secure delete after archive |
| AI Decisions | 365 days (1 year) | EU AI Act Article 12 | Secure delete after archive |
| Incident Data | 365 days (1 year) | Business requirement, EU AI Act | Anonymize then delete |
| LLM Traces | 90 days | EU AI Act Article 12 | Secure delete after archive |
| PII Data | 90 days | GDPR Article 5(1)(e) | Secure delete, NO archive |
| Metrics | 180 days | Operational requirement | Aggregate then delete |
| Embeddings | 365 days | RAG knowledge base maintenance | Recompute without source |

**Audit log retention at 7 years (2,555 days)** satisfies the EU AI Act's requirement that logs for high-risk AI systems be retained for the period defined by applicable law, with a minimum of 6 months.

### 6.4 Storage Architecture

Audit events are persisted to the `audit.events` PostgreSQL table:
- Entries are append-only (no UPDATE or DELETE operations permitted on audit records)
- Records are protected by row-level security preventing modification by application roles
- The table is partitioned by month for query performance
- Offloaded to cold storage (GCS) after 1 year for cost efficiency while maintaining 7-year retention

### 6.5 GDPR Right to Erasure Interaction

When a GDPR Article 17 erasure request is received, the `DataRetentionManager.handle_deletion_request()` method executes the following:

- **Will delete:** PII data, incident data (if containing user personal data)
- **Legally retained:** Audit logs, AI decisions — retained on grounds of "EU AI Act Article 12 requires record keeping"

This balancing is documented and legally defensible under GDPR Article 17(3)(b) (retention required by law).

**Record Keeping Status: COMPLIANT**

---

## 7. Transparency and Information to Users (Article 13)

### 7.1 Legal Requirement

Article 13 requires that high-risk AI systems be designed and developed to ensure sufficient transparency to enable deployers to interpret the system's output and use it appropriately. The system must provide:
- A description of the intended purpose
- An indication of the level of accuracy, robustness, and cybersecurity
- Any known circumstances or limitations that may affect performance
- The degree of human oversight appropriate for the system

### 7.2 Confidence Score Transparency

Every output from the RAG retrieval system and the LLM judge is accompanied by a confidence score returned to the caller:

- RAG recommendations include `retrieval_score` (0.0–1.0) indicating similarity to the knowledge base query
- The LLM judge evaluation includes `safety_score` and `confidence` fields in its structured JSON output
- Remediation plans include a `plan_confidence` field

These scores are surfaced in the frontend approval UI, enabling human reviewers to make informed approval decisions.

### 7.3 LLM Judge Reasoning

The LLM judge node evaluates every remediation plan and returns structured reasoning in the `judge_reasoning` field. This field is:
- Stored in the incident record
- Displayed in the frontend approval UI alongside the remediation plan
- Logged in the audit trail via `ai_decision_explanation`

This satisfies Article 13's requirement that users can interpret AI outputs.

### 7.4 Frontend Transparency Features

The approval UI at `/approvals` displays the following information for each pending incident:

| Information | Source | Display Purpose |
|-------------|--------|-----------------|
| Risk level | `risk_level` field | Signals urgency and required oversight level |
| Confidence score | `confidence_score` | Enables human to judge AI certainty |
| Remediation rationale | `judge_reasoning` | Explains why this action was selected |
| AI-generated badge | Static UI indicator | Discloses AI authorship of recommendation |
| Affected systems | Incident metadata | Contextualizes impact of approval |

### 7.5 AI-Generated Content Disclosure

All LLM outputs served through the platform are marked as AI-generated. The system disclosure appears:
- In the incident detail view at `/incidents/[id]`
- In the approval queue at `/approvals`
- In the pipeline review interface at `/pipelines`
- In API responses (via `generated_by: "ai"` field in all recommendation objects)

### 7.6 System Capabilities and Limitations

Documentation of system capabilities and known limitations is maintained in:
- `docs/architecture.md` — Component responsibilities and known boundaries
- `docs/spec.md` — Full specification including out-of-scope scenarios
- `agents/data_agent/APEX_README.md` — Data agent capability matrix for 70+ source types

**Transparency and Information to Users Status: COMPLIANT**

---

## 8. Human Oversight (Article 14)

### 8.1 Legal Requirement

Article 14 requires that high-risk AI systems be designed and developed in such a way, including with appropriate human-machine interface tools, that they can be effectively overseen by natural persons during the period in which the AI system is in use. Human oversight must enable persons to:

- Understand the AI system's capabilities and limitations
- Monitor operation and detect anomalies and malfunctions
- Remain aware of possible automation bias
- Intervene and override AI decisions
- Interrupt the system through a "stop" button

### 8.2 Human-in-the-Loop Implementation

**Primary implementation:** `backend/orchestrator/langgraph_workflow.py` → node `node_await_approval`

The Incident Management Agent implements a Kafka event-driven Human-in-the-Loop (HITL) pause mechanism:

**Workflow pause sequence:**
1. The workflow reaches `node_await_approval` after the LLM judge has evaluated the remediation plan
2. The node publishes an `incident.requires_approval` Kafka event containing the full incident context, remediation plan, confidence score, and judge reasoning
3. The LangGraph workflow enters a suspended state — no further nodes execute
4. The frontend `/approvals` page displays the pending approval to authorized personnel
5. The human reviewer examines the remediation plan, confidence score, and judge reasoning
6. The human POSTs to `/api/v1/incidents/{id}/approve` (resumes workflow → `node_execute`) or `/api/v1/incidents/{id}/reject` (workflow terminates, incident escalated)
7. The resume/reject action publishes `incident.approved` or a rejection event to Kafka
8. The EventOrchestrator routes the event, resuming or terminating the LangGraph workflow

### 8.3 Override and Control Capabilities

| Capability | Implementation | Article 14 Requirement |
|-----------|----------------|------------------------|
| Approve AI recommendation | `POST /api/v1/incidents/{id}/approve` | Human can allow AI action |
| Reject AI recommendation | `POST /api/v1/incidents/{id}/reject` | Human can block AI action |
| Emergency workflow stop | Control plane termination endpoint | "Stop" button requirement |
| Modify before approval | Approval UI edit fields | Human can override AI output |
| View full AI reasoning | `judge_reasoning` in approval UI | Understand AI decision |
| Audit trail of decisions | `audit_logger.log_human_oversight()` | Traceability of oversight |

### 8.4 Risk-Based Approval Tiers

Not all actions require the same level of oversight. The platform implements risk-based approval tiering:

| Risk Level | Approval Requirement | Rationale |
|------------|---------------------|-----------|
| LOW | Auto-approve with logging | Low impact; human oversight via audit trail |
| MEDIUM | Single approver required | Moderate impact; one authorized reviewer |
| HIGH | Single approver required; dual recommended | High impact; human judgment essential |
| CRITICAL | Manual execution only; AI provides plan, human executes | Maximum impact; AI advisory role only |

**All PROD deployments (Data Engineering Agent)** require explicit human sign-off regardless of risk level. The `deployer` node does not proceed without a confirmed approval event in the Kafka stream.

### 8.5 Response Time SLA

To prevent approval queue bottlenecks from delaying critical incident resolution:
- Approval must be granted or rejected within **4 hours** of the `incident.requires_approval` event
- After 4 hours without a decision, the incident is automatically escalated to the on-call manager
- Escalation is published as a Kafka event and triggers a PagerDuty notification

### 8.6 Automation Bias Safeguards

To mitigate automation bias — the tendency to over-trust AI recommendations — the approval UI is designed to:
- Show the AI confidence score prominently (high confidence ≠ mandatory approval)
- Display the judge's reasoning in plain language, not just a score
- Require explicit affirmative action (click Approve) rather than passive non-rejection
- Log all approvals with a timestamp for retrospective analysis of approval patterns

**Human Oversight Status: COMPLIANT**

---

## 9. Accuracy, Robustness and Cybersecurity (Article 15)

### 9.1 Legal Requirement

Article 15 requires high-risk AI systems to achieve appropriate levels of accuracy, robustness, and cybersecurity, and to be resilient against attempts by unauthorized third parties to alter use or performance.

### 9.2 Accuracy Controls

**LLM Judge Evaluation:**
Every remediation plan generated by the Incident Management Agent is evaluated by a separate LLM judge instance (GPT-4) that scores the plan on:
- Technical accuracy of the proposed commands
- Appropriateness for the classified incident type
- Risk level of the proposed actions
- Potential for unintended side effects

The judge returns a `safety_score` between 0.0 and 1.0. Plans scoring below 0.70 are automatically routed to human review without execution.

**Confidence Threshold Enforcement:**

| Decision Point | Threshold | Below Threshold Action |
|---------------|-----------|------------------------|
| RAG knowledge retrieval | ≥ 0.60 | Flag low-confidence; add human review note |
| Remediation plan confidence | ≥ 0.70 | Route to human; do not auto-proceed |
| LLM judge safety score | ≥ 0.70 | Block action; require re-evaluation |
| Auto-execute (low risk only) | ≥ 0.95 | Human approval required for scores below |

**Safety score < 0.50** triggers automatic rejection — the plan is discarded and the incident is escalated for fully manual handling.

### 9.3 Robustness Controls

**Circuit Breaker:** `backend/utils/circuit_breaker.py`

The circuit breaker pattern protects against LLM service degradation:
- Tracks consecutive LLM call failures
- Opens after 5 consecutive failures, blocking all subsequent LLM calls
- Logs circuit open event to audit trail (SYSTEM_CONFIG_CHANGE event type)
- Requires manual reset or automatic recovery via half-open probe after configurable timeout

**Retry Mechanisms:**
- LLM calls implement exponential backoff with jitter (max 3 retries)
- Kafka consumer retry logic with dead-letter queue for failed event processing
- Pipeline generation retries with alternative model provider on primary failure

**Fallback Behavior:**
- If LLM is unavailable, workflows pause at the current node rather than failing silently
- Fallback to Anthropic Claude when OpenAI GPT-4 is unavailable (Data Engineering Agent)
- If RAG retrieval fails, the system signals human review rather than proceeding without context

**Chaos Engineering:**
- Adversarial scenarios tested via `tests/chaos/chaos_engineering.py`
- Tests include: LLM timeout, partial response, hallucinated JSON, injection attempts
- Circuit breaker behavior validated under simulated failure conditions

### 9.4 Cybersecurity Controls

**Input Validation — Prompt Injection:**
`backend/guardrails/llm_guardrails.py` `InputValidator` class detects 16 categories of prompt injection attack patterns, including:
- Direct instruction override (`ignore previous instructions`)
- Role manipulation (`you are now a`, `pretend to be`)
- Jailbreak attempts (`DAN mode`, `developer mode enabled`)
- System prompt extraction (`reveal your system prompt`)
- Encoding-based bypasses (base64, hex)

**Input Validation — Command Injection:**
For incident descriptions, additional regex patterns detect shell command injection attempts (pipe to `rm`, `wget`, `curl`, backtick substitution, `$(...)` substitution).

**Output Validation — Harmful Content:**
`OutputValidator` scans all LLM outputs for dangerous commands before they are presented to users:
- Destructive filesystem commands (`rm -rf /`, `mkfs.`)
- Fork bombs (`:(){ :|:& };:`)
- Disk overwrite commands (`dd if=... of=/dev/`)
- Credential exposure (`password: "..."`, `api_key: "..."`)
- Privilege escalation (`chmod 777`, `sudo su -`)

**Pydantic Validation:**
All incident objects and pipeline inputs are validated against strict Pydantic schemas before reaching LLM nodes. Type errors, missing required fields, and out-of-range values are rejected at the API boundary before any LLM processing occurs.

**Authentication and Authorization:**
- API key authentication on all endpoints
- Role-based access control (RBAC) for approval authority
- TLS encryption for all data in transit
- PostgreSQL row-level security on audit tables

**Adversarial Robustness:**
Any input with a guardrail safety score < 0.50 is automatically rejected and logged with `UNAUTHORIZED_ACCESS` audit event type.

**Accuracy, Robustness and Cybersecurity Status: COMPLIANT**

---

## 10. Conformity Assessment Checklist (Article 43)

The following table summarises conformity assessment findings across all applicable articles:

| Article | Requirement | Implementation Evidence | Status |
|---------|-------------|------------------------|--------|
| Art. 6 | High-risk classification | Annex III Cat. 2 — critical infrastructure; automated PROD decisions | PASS |
| Art. 9 | Risk management system | `eu_ai_act_compliance.py`; LLM guardrails; circuit breaker; confidence thresholds | PASS |
| Art. 9(4) | Testing for risks | `tests/chaos/chaos_engineering.py`; adversarial input testing | PASS |
| Art. 10(1) | Training data governance | Pre-trained models via API; no custom training; RAG quality controls | PASS |
| Art. 10(2) | Data quality examination | Pydantic validation; guardrails PII scan; input length limits | PASS |
| Art. 10(3) | Bias assessment | Diverse incident training scenarios; RAG source diversity | PASS |
| Art. 10(5) | PII handling | `pii_detection.py`; 13 PII types; 5 masking strategies | PASS |
| Art. 11(1) | Technical documentation | `docs/architecture.md`, `docs/spec.md`, this document | PASS |
| Art. 11(3) | Documentation currency | Annual review cycle; change-triggered updates | PASS |
| Art. 12(1) | Automatic logging capability | `audit_logger.py`; 18 event types; immutable PostgreSQL entries | PASS |
| Art. 12(2) | Log retention | 2,555 days (7 years) per `data_retention.py` | PASS |
| Art. 13(1) | Transparency design | Confidence scores, AI badges, judge reasoning in approval UI | PASS |
| Art. 13(3) | Instructions for use | `docs/architecture.md`, `APEX_README.md`, API documentation | PASS |
| Art. 14(1) | Human oversight design | HITL node `node_await_approval`; Kafka-driven pause/resume | PASS |
| Art. 14(4)(a) | Understand AI capabilities | Judge reasoning displayed; confidence scores surfaced | PASS |
| Art. 14(4)(b) | Aware of automation bias | Explicit approval UI design; audit pattern monitoring | PASS |
| Art. 14(4)(c) | Interpret outputs | `judge_reasoning` field; structured remediation plans | PASS |
| Art. 14(4)(d) | Override and intervene | `/approve` and `/reject` API endpoints | PASS |
| Art. 14(4)(e) | Interrupt the system | Emergency stop via control plane; workflow termination | PASS |
| Art. 14(5) | Override for PROD | All PROD actions require explicit human sign-off | PASS |
| Art. 15(1) | Appropriate accuracy level | LLM judge ≥0.70; RAG ≥0.60; auto-execute ≥0.95 | PASS |
| Art. 15(3) | Resilience/adversarial robustness | Prompt injection detection; safety score < 0.5 → reject | PASS |
| Art. 15(4) | Technical redundancy | Circuit breaker; retry with backoff; provider fallback | PASS |
| Art. 51 | EU database registration | `AISystemRegistration` dataclass; registration details in Section 2.5 | PASS |

---

## 11. Residual Risks and Mitigations

Despite the controls in place, the following residual risks remain and are monitored on an ongoing basis:

| Risk | Likelihood | Impact | Current Mitigation | Residual Risk | Owner |
|------|-----------|--------|-------------------|---------------|-------|
| LLM hallucination in remediation | MEDIUM | HIGH | LLM judge with 0.70 threshold; human approval required | LOW | AI Safety Officer |
| False positive incident classification | MEDIUM | MEDIUM | Dual classification (ML + LLM); human review for P1/P2 | LOW-MEDIUM | Platform Engineering |
| API supply chain risk (OpenAI/Anthropic outage) | LOW | HIGH | Circuit breaker; provider fallback (Claude ↔ GPT-4); human escalation on failure | LOW | Platform Admin |
| Data poisoning in RAG knowledge base | LOW | HIGH | Source attribution and versioning; audit log of embedding updates; anomaly detection | LOW | Data Steward |
| Approval fatigue (reviewers rubber-stamping) | MEDIUM | HIGH | Audit analysis of approval patterns; mandatory review time minimum; rotation of approvers | MEDIUM | AI Safety Officer |
| Clock/timing attacks on Kafka events | LOW | MEDIUM | Event timestamps validated against system clock; Kafka consumer idempotency checks | LOW | Platform Admin |
| Prompt injection via incident descriptions | LOW | HIGH | `InputValidator` with 16 injection patterns; score < 0.5 → block | LOW | Platform Engineering |
| Model drift — GPT-4 behavior change with API updates | MEDIUM | MEDIUM | Version-pinned model calls; post-update regression testing; performance monitoring in Langfuse | LOW-MEDIUM | Platform Engineering |

**Residual Risk Summary:** Two risks are assessed at LOW-MEDIUM (false positive classification, approval fatigue). These are monitored monthly and will trigger a risk management review if the likelihood increases. No residual risks are assessed as HIGH.

---

## 12. Post-Market Monitoring (Article 61)

### 12.1 Legal Requirement

Article 61 requires providers of high-risk AI systems to establish and document a post-market monitoring system and to actively collect, document, and analyse data gathered from deployers to identify and address any issues.

### 12.2 Monitoring Infrastructure

**Prometheus Metrics:**
50+ metrics are defined in `backend/orchestrator/metrics.py`, covering:
- Incident processing volume and latency
- LLM call success/failure/latency rates
- Confidence score distributions
- Guardrail trigger rates (injection attempts, harmful content blocks)
- Human approval/rejection rates and time-to-decision
- Circuit breaker state changes
- Kafka consumer lag

**Grafana Dashboards:**
Real-time dashboards aggregate Prometheus metrics for:
- AI system health overview (`/observability`)
- Incident workflow throughput and resolution times
- Human oversight coverage rate (target: 100% of PROD actions)
- LLM model performance trends
- Security event frequency

**Langfuse LLM Tracing:**
All LLM calls are traced via Langfuse, capturing:
- Input prompts and output responses (for audit)
- Token usage and cost
- Latency per LLM call and per workflow node
- Model version used per call
- Confidence scores at each step

This enables retrospective analysis of model behavior and detection of performance degradation.

### 12.3 Audit Log Anomaly Detection

The audit log is monitored for anomalous patterns:
- Sudden increase in `HUMAN_REJECTION` events (may indicate model quality degradation)
- Increase in `AUTH_FAILURE` or `UNAUTHORIZED_ACCESS` events (security incident signal)
- Drop in `confidence_score` distribution below baseline (model drift indicator)
- Increase in guardrail trigger rate (adversarial activity signal)

Anomaly alerts are routed to the AI Safety Officer and Platform Engineering team via PagerDuty.

### 12.4 Compliance Review Schedule

| Review Type | Frequency | Owner | Outputs |
|-------------|-----------|-------|---------|
| Compliance dashboard review | Monthly | AI Safety Officer | Status report; anomaly findings |
| Risk register review | Quarterly | AI Safety Officer | Updated risk register |
| Technical documentation review | Annual | Platform Engineering | Updated architecture docs |
| Full conformity assessment | Annual or on substantial modification | AI Safety Officer | Updated this document |
| Third-party compliance audit | Biennial | External auditor | Independent assessment report |

### 12.5 Serious Incident Reporting

In accordance with Article 62, serious incidents or malfunctions that constitute a risk must be reported to the relevant market surveillance authority within 15 days of becoming aware. A serious incident is defined as:
- Any incident resulting in death or serious harm
- Any unintended disruption of critical infrastructure caused by the AI system
- Serious violations of fundamental rights

The Platform Engineering team maintains an incident response playbook for EU AI Act serious incident scenarios, including notification procedures for the relevant supervisory authority.

---

## 13. Document Control

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-22 | AI Safety Officer | Initial release |

**Next Mandatory Review:** 2027-06-22

This document must be reviewed and updated:
- At minimum annually
- Upon any substantial modification to either AI system
- Upon any change in applicable EU AI Act guidance or implementing acts
- Following any serious incident or near-miss event

*Document classification: INTERNAL. Not for external distribution without AI Safety Officer approval.*
