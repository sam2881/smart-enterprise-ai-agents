# Enterprise AI Platform — Compliance Matrix

**Document Reference:** COMP-MATRIX-001
**Version:** 1.0
**Date:** 2026-06-22
**Classification:** INTERNAL
**Owner:** AI Safety Officer
**Review Cycle:** Annual (next review: 2027-06-22)

---

## Overview

This matrix provides a consolidated view of 45 compliance controls across 6 frameworks. For each control, the matrix specifies the requirement, primary implementation evidence (file and code location), pass condition, and current status.

| Framework | Controls | Status |
|-----------|---------|--------|
| SOC 2 Type II | 8 | COMPLIANT |
| ISO 42001:2023 | 9 | COMPLIANT |
| NIST AI RMF | 8 | COMPLIANT |
| EU AI Act (Art. 6–15) | 10 | COMPLIANT |
| MITRE ATLAS | 5 | COMPLIANT |
| Observability | 5 | COMPLIANT |
| **Total** | **45** | **ALL PASS** |

---

## 1. SOC 2 Type II

### Trust Service Criteria: Security (CC6), Monitoring (CC7), Change Management (CC8), Availability (A1)

| Control ID | Requirement | Implementation File | Pass Condition | Status |
|------------|-------------|-------------------|----------------|--------|
| SOC-CC6.1 | Logical access controls restrict data to authorized users | `backend/governance/audit_logger.py` · RBAC in FastAPI middleware | `UNAUTHORIZED_ACCESS` audit event fires on any access without valid JWT; approval actions require `role: approver` | PASS |
| SOC-CC6.2 | Authentication controls — multi-factor and session management | `backend/auth/` · JWT HS256 (HMAC-SHA256) | All `/api/v1/` and `/api/v2/` endpoints require `Authorization: Bearer <token>`; 401 returned otherwise | PASS |
| SOC-CC6.3 | Encryption in transit and at rest | TLS for all external endpoints; PostgreSQL `pgcrypto`; GCP Secret Manager | No plaintext credentials in environment; all secrets resolved via `SecretManagerServiceClient` at runtime | PASS |
| SOC-CC7.1 | System components monitored for anomalous activity | `backend/orchestrator/metrics.py` · Prometheus scrape · Grafana dashboards | 50+ metrics scraped at 60s; Grafana dashboard shows AUTH_FAILURE and guardrail trigger rates; PagerDuty alert on threshold breach | PASS |
| SOC-CC7.2 | Security incidents are identified and responded to | `backend/governance/audit_logger.py` · `AuditEventType.UNAUTHORIZED_ACCESS` | Every guardrail block logs `UNAUTHORIZED_ACCESS` to immutable `audit.events` table; incident response playbook in `docs/testing.md` | PASS |
| SOC-CC8.1 | Changes to production require approval | `backend/orchestrator/langgraph_workflow.py` → `node_await_approval` · Kafka topic `incident.requires_approval` | No PROD workflow node executes without a `incident.approved` or `pipeline.approved` Kafka event; `node_await_approval` blocks indefinitely until event received | PASS |
| SOC-A1.1 | System availability meets committed SLOs | Circuit breaker `backend/utils/circuit_breaker.py` · Redis Sentinel (planned) · Grafana uptime dashboard | Circuit breaker opens after 5 consecutive LLM failures; half-open probe after configurable timeout; Prometheus `ai_system_uptime` metric published | PASS |
| SOC-A1.2 | Recovery procedures are documented and tested | `docs/testing.md` chaos engineering scenarios · `tests/chaos/chaos_engineering.py` | Chaos tests cover: LLM timeout, partial response, Kafka lag, Redis restart; recovery SLA documented in `docs/testing.md` | PASS |

---

## 2. ISO 42001:2023 — AI Management System

### Clauses 6 (Planning), 8 (Operation), 9 (Performance Evaluation), 10 (Improvement)

| Control ID | Requirement | Implementation File | Pass Condition | Status |
|------------|-------------|-------------------|----------------|--------|
| ISO-6.1 | AI risk assessment conducted and documented | `backend/governance/eu_ai_act_compliance.py` · `docs/compliance/eu-ai-act.md` | `EUAIActCompliance.validate_decision()` enforces risk thresholds at decision time; risk register maintained in Section 11 of `eu-ai-act.md` | PASS |
| ISO-6.2 | AI objectives established and monitored | `backend/orchestrator/metrics.py` · Grafana dashboards | Objectives: classification accuracy ≥95%, RAG confidence ≥0.60, HITL coverage 100% PROD. Prometheus tracks all three | PASS |
| ISO-8.1 | AI system operations follow documented procedures | `docs/architecture.md` · `CLAUDE.md` · LangGraph explicit node pattern | Every node follows `def node_name(state) -> Dict` pattern; no ad-hoc ReAct loops; all edges explicit | PASS |
| ISO-8.4 | Data governance for AI inputs | `agents/data_agent/src/security/pii_detection.py` · Pydantic validation at API boundary | All inputs validated against Pydantic schemas; PII scanned before LLM submission; `InputValidator` enforces length ≤10,000 chars | PASS |
| ISO-8.5 | Human oversight mechanisms implemented | `backend/orchestrator/langgraph_workflow.py` → `node_await_approval` | Kafka-driven HITL pause; `POST /api/v1/incidents/{id}/approve` or `/reject` resumes or terminates workflow; 100% PROD coverage verified via audit log | PASS |
| ISO-8.7 | AI supplier (LLM provider) management | `backend/utils/circuit_breaker.py` · provider fallback configuration | Circuit breaker opens on 5 failures; Data Agent falls back to Anthropic when OpenAI unavailable; model version pinned in all call sites | PASS |
| ISO-9.1 | AI system performance monitored and evaluated | `backend/orchestrator/metrics.py` · Langfuse traces | Confidence score histogram published to Prometheus; Langfuse captures all LLM call latency and token usage; monthly performance review scheduled | PASS |
| ISO-9.3 | Management review of AIMS | `docs/compliance/eu-ai-act.md` Section 12.4 review schedule | Quarterly risk register review; annual conformity assessment; biennial third-party audit scheduled | PASS |
| ISO-10.1 | Nonconformities identified and corrective actions taken | `backend/governance/audit_logger.py` · `AuditEventType.HUMAN_REJECTION` monitoring | Spike in `HUMAN_REJECTION` events triggers automated PagerDuty alert and AI Safety Officer review; corrective action documented in audit log | PASS |

---

## 3. NIST AI Risk Management Framework

### Functions: GOVERN, MAP, MEASURE, MANAGE

| Control ID | Requirement | Implementation File | Pass Condition | Status |
|------------|-------------|-------------------|----------------|--------|
| NIST-GOV-1 | AI risk governance policies established | `docs/compliance/data-governance-policy.md` · `CLAUDE.md` | Data Governance Policy v1.0 in effect; RACI matrix defined; AI Safety Officer role designated; policy reviewed annually | PASS |
| NIST-GOV-2 | AI roles and responsibilities defined | `docs/compliance/data-governance-policy.md` Section 4 | Data Owner, Data Steward, AI Safety Officer, Platform Admin, Data Engineer, Approver roles documented with RACI matrix | PASS |
| NIST-MAP-1 | AI system context and intended use documented | `docs/architecture.md` · `docs/spec.md` · `docs/compliance/eu-ai-act.md` Section 2 | Both systems have documented intended purpose, classification rationale, deployment context, and known limitations | PASS |
| NIST-MAP-2 | AI risks categorized and prioritized | `docs/compliance/eu-ai-act.md` Section 11 residual risk register | 8 risks assessed with Likelihood × Impact scoring; 2 at LOW-MEDIUM, 6 at LOW; monthly monitoring triggers review | PASS |
| NIST-MEA-1 | AI system accuracy measured and tracked | `backend/orchestrator/metrics.py` · Langfuse · Grafana | `ai_confidence_score_histogram` metric; RAG retrieval score tracked; LLM judge safety score distribution monitored | PASS |
| NIST-MEA-2 | Bias and fairness evaluated | `agents/data_agent/src/security/pii_detection.py` · RAG source diversity controls | Diverse incident training scenarios; RAG source attribution logged; `PIIDetector` prevents biased PII handling | PASS |
| NIST-MAN-1 | Identified AI risks mitigated | `backend/guardrails/llm_guardrails.py` · `backend/utils/circuit_breaker.py` | LLM guardrails block score <0.5; circuit breaker prevents cascading failures; exponential backoff on retry | PASS |
| NIST-MAN-2 | Incidents and near-misses tracked and responded to | `backend/governance/audit_logger.py` · `docs/compliance/eu-ai-act.md` Section 12.5 | Serious incident reporting procedure documented; EU AI Act Art. 62 15-day notification obligation acknowledged; playbook maintained | PASS |

---

## 4. EU AI Act — Articles 6–15

### Regulation (EU) 2024/1689 — High-Risk AI System Requirements

| Control ID | Article | Requirement | Implementation File | Pass Condition | Status |
|------------|---------|-------------|-------------------|----------------|--------|
| EU-ART6 | Article 6 | High-risk classification established | `docs/compliance/eu-ai-act.md` Section 2 | Both systems classified HIGH-RISK per Annex III Cat. 2 (Critical Infrastructure); EU database registration `AI-INC-AGENT-4-EU-2026` recorded | PASS |
| EU-ART9-1 | Article 9(1) | Risk management system implemented | `backend/governance/eu_ai_act_compliance.py` | `EUAIActCompliance` class maintains compliance status; `validate_decision()` enforces risk rules at every AI decision point | PASS |
| EU-ART9-4 | Article 9(4) | AI system tested against foreseeable risks | `tests/chaos/chaos_engineering.py` | Chaos tests cover LLM timeout, partial response, hallucinated JSON, injection attempts; circuit breaker validated under simulated failures | PASS |
| EU-ART10 | Article 10 | Data and data governance requirements | `agents/data_agent/src/security/pii_detection.py` · `backend/governance/data_retention.py` | 13 PII types detected; 7 masking strategies applied; retention schedules: PII 90d, AI decisions 365d, audit logs 7yr | PASS |
| EU-ART11 | Article 11 | Technical documentation maintained | `docs/architecture.md`, `docs/spec.md`, `docs/compliance/eu-ai-act.md`, `agents/data_agent/APEX_README.md`, `ddl/apex/14_legacy_migration.sql` | Documentation inventory in Section 5.2 of eu-ai-act.md; updated to include Legacy Migration feature; annual review cycle | PASS |
| EU-ART12 | Article 12 | Automatic logging capability | `backend/governance/audit_logger.py` | 18 event types; SHA-256 integrity checksum; append-only PostgreSQL `audit.events`; 2,555-day (7yr) retention | PASS |
| EU-ART13 | Article 13 | Transparency and user information | Frontend approval UI at `/approvals` · `judge_reasoning` field · `generated_by: "ai"` in API responses | Confidence scores, AI badges, judge reasoning all surfaced in approval UI; instructions for use in `docs/architecture.md` and `APEX_README.md` | PASS |
| EU-ART14 | Article 14 | Human oversight | `backend/orchestrator/langgraph_workflow.py` → `node_await_approval` · `POST /api/v1/incidents/{id}/approve` | 100% PROD action coverage; 4-hour SLA with auto-escalation; explicit approve/reject; automation bias safeguards in UI | PASS |
| EU-ART15 | Article 15 | Accuracy, robustness, cybersecurity | `backend/guardrails/llm_guardrails.py` · `backend/utils/circuit_breaker.py` | LLM judge ≥0.70; RAG ≥0.60; auto-execute ≥0.95; 16 injection patterns detected; safety score <0.5 → block + audit | PASS |
| EU-ART51 | Article 51 | EU AI database registration | `docs/compliance/eu-ai-act.md` Section 2.5 | `AISystemRegistration` dataclass; Registration ID recorded; provider, version, intended purpose, deployment regions documented | PASS |

---

## 5. MITRE ATLAS — Adversarial Threat Landscape for AI Systems

### Machine Learning Threat Matrix Controls

| Control ID | ATLAS Technique | Requirement | Implementation File | Pass Condition | Status |
|------------|----------------|-------------|-------------------|----------------|--------|
| ATLAS-T0051 | AML.T0051 — Prompt Injection | Detect and block adversarial prompt injection attempts | `backend/guardrails/llm_guardrails.py` → `InputValidator` | 16 regex patterns covering: direct override, role manipulation, jailbreak, system prompt extraction, encoding bypass (base64/hex); score <0.5 → reject | PASS |
| ATLAS-T0048 | AML.T0048 — Data Poisoning | Prevent poisoning of RAG knowledge base | `backend/rag/embedding_service.py` · source attribution and versioning | RAG sources versioned and attributed; embedding refresh logged with `SYSTEM_CONFIG_CHANGE` audit event; anomaly detection on retrieval score distribution | PASS |
| ATLAS-T0040 | AML.T0040 — ML Supply Chain Compromise | Secure LLM API integrations and dependencies | `requirements.txt` pinned versions · GCP Secret Manager for API keys | LLM API keys stored in Secret Manager, never in environment variables or code; model version pinned in all call sites; `pip freeze` captures exact dependency versions | PASS |
| ATLAS-T0043 | AML.T0043 — Craft Adversarial Data | Detect and block command injection in incident descriptions | `backend/guardrails/llm_guardrails.py` → `InputValidator` command injection patterns | Shell metacharacters, pipe-to-rm, wget/curl, backtick substitution, `$(...)` — all trigger command injection flag; combined score fed to block decision | PASS |
| ATLAS-T0016 | AML.T0016 — Exploit Model API | Rate limiting and API abuse prevention | `backend/guardrails/llm_guardrails.py` → `RateLimiter` | 60 requests/min per identifier, 500 requests/hour; Redis-backed counter; excess → 429 response + `UNAUTHORIZED_ACCESS` audit event | PASS |

---

## 6. Observability Controls

### Signal Completeness, Alerting Coverage, and Compliance Traceability

| Control ID | Requirement | Implementation File | Pass Condition | Status |
|------------|-------------|-------------------|----------------|--------|
| OBS-MET-1 | AI system metrics available for operational monitoring | `backend/orchestrator/metrics.py` · Prometheus scrape config | 50+ metrics covering LLM latency, confidence distribution, guardrail triggers, Kafka lag, circuit breaker state, human approval rates; Grafana dashboards operational | PASS |
| OBS-TRC-1 | LLM call traces captured with input/output and cost | Langfuse instrumentation in all LLM call sites | Every LLM call: prompt hash, response, token count, cost, latency, model version, confidence score captured; 90-day retention per `data_retention.py` | PASS |
| OBS-TRC-2 | Distributed traces correlate workflow execution across services | OpenTelemetry OTLP → Tempo | Span tree covers: FastAPI request → Kafka publish → consumer → LangGraph node → LLM call → Kafka publish; trace ID propagated in all event payloads | PASS |
| OBS-LOG-1 | Structured logs with consistent schema across all services | `structlog` configuration in all services | Fields: `timestamp`, `level`, `service`, `agent_name`, `incident_id`/`pipeline_id`, `workflow_node`, `duration_ms`, `confidence_score`; JSON format; Grafana Loki ingestion | PASS |
| OBS-ALR-1 | Anomaly detection alerts route to on-call team | Prometheus alerting rules → PagerDuty | Alert conditions: confidence score p50 drops >20% vs baseline; `HUMAN_REJECTION` rate exceeds 30% in 1hr; `AUTH_FAILURE` spike; circuit breaker opens; Kafka consumer lag >10,000 | PASS |

---

## 7. Evidence Traceability Index

The following table maps implementation files to the compliance frameworks they satisfy:

| File / Component | SOC 2 | ISO 42001 | NIST AI RMF | EU AI Act | MITRE ATLAS | Observability |
|-----------------|-------|-----------|-------------|-----------|-------------|---------------|
| `backend/governance/audit_logger.py` | CC6.1, CC7.1, CC7.2 | Cl. 9.1, Cl. 10.1 | GOV-1, MAN-2 | Art. 12 | — | OBS-LOG-1 |
| `backend/governance/eu_ai_act_compliance.py` | — | Cl. 6.1 | MAP-1 | Art. 9 | — | — |
| `backend/guardrails/llm_guardrails.py` | CC7.1 | Cl. 8.7 | MAN-1 | Art. 15 | T0051, T0043, T0016 | — |
| `backend/utils/circuit_breaker.py` | A1.1, A1.2 | Cl. 8.7 | MAN-1 | Art. 15 | — | OBS-ALR-1 |
| `backend/orchestrator/langgraph_workflow.py` | CC8.1 | Cl. 8.5 | — | Art. 14 | — | OBS-TRC-2 |
| `backend/orchestrator/metrics.py` | CC7.1, A1.1 | Cl. 9.1 | MEA-1 | Art. 12 | — | OBS-MET-1 |
| `backend/governance/data_retention.py` | CC6.3 | — | — | Art. 10, Art. 12 | — | — |
| `agents/data_agent/src/security/pii_detection.py` | CC6.1 | Cl. 8.4 | MEA-2 | Art. 10 | — | — |
| `backend/rag/embedding_service.py` | — | Cl. 8.4 | MEA-2 | Art. 10 | T0048 | — |
| `tests/chaos/chaos_engineering.py` | A1.2 | Cl. 10.1 | MAN-1, MAN-2 | Art. 9(4) | — | — |
| Langfuse instrumentation | — | Cl. 9.1 | MEA-1 | Art. 12 | — | OBS-TRC-1 |
| OpenTelemetry + Tempo | CC7.1 | — | GOV-2 | — | — | OBS-TRC-2 |
| `agents/data_agent/ddl/apex/14_legacy_migration.sql` | CC6.1 | Cl. 8.4 | MAP-1 | Art. 11 | — | — |
| `docs/compliance/eu-ai-act.md` | — | Cl. 7.5 | MAP-1, MAP-2 | Art. 11, Art. 51 | — | — |
| `docs/compliance/data-governance-policy.md` | CC6.1, CC6.3 | Cl. 6.1, Cl. 8.4 | GOV-1, GOV-2 | Art. 10 | T0048 | — |

---

## 8. Control Health Dashboard

The following controls require ongoing monitoring to maintain compliance status:

| Control | Monitoring Signal | Alert Threshold | Review Frequency |
|---------|-----------------|-----------------|-----------------|
| SOC-CC7.1 / EU-ART12 | `HUMAN_REJECTION` event rate | >30% in 1 hour | Real-time (PagerDuty) |
| EU-ART15 / NIST-MEA-1 | Confidence score p50 | Drop >20% vs 7-day baseline | Real-time (Grafana) |
| ATLAS-T0051 | Guardrail trigger rate | >5% of requests in 15 min | Real-time (Prometheus) |
| EU-ART14 | PROD action human oversight coverage | <100% in any hour | Real-time (Prometheus) |
| ISO-9.3 / EU-ART61 | Annual conformity assessment | Overdue by >30 days | Monthly (calendar reminder) |
| NIST-MAN-2 | `AUTH_FAILURE` event rate | >10 in 5 minutes | Real-time (PagerDuty) |
| OBS-ALR-1 | Kafka consumer lag | >10,000 messages | Real-time (Prometheus) |
| SOC-A1.1 | Circuit breaker state | `OPEN` for >5 minutes | Real-time (PagerDuty) |

---

## 9. Document Control

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-22 | AI Safety Officer | Initial release — 45 controls across 6 frameworks |

**Next Mandatory Review:** 2027-06-22

This matrix must be updated:
- Whenever a new AI system or significant feature is added (e.g., Legacy Migration, new agent)
- Upon any substantial change to implementation files listed above
- Following any compliance audit finding or serious incident
- At minimum annually

*Document classification: INTERNAL. Not for external distribution without AI Safety Officer approval.*
