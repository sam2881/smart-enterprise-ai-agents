# GDPR Compliance Assessment

**Document Reference:** COMP-GDPR-001
**Version:** 1.0
**Date:** 2026-06-22
**Regulation:** Regulation (EU) 2016/679 — General Data Protection Regulation
**Review Cycle:** Annual or on material change
**Owner:** AI Safety Officer / Data Protection Officer (TBC)

---

## 1. Data Controller Information

**Controller Name:** Enterprise Agentic AI Platform (operating organization)
**Role:** Data Controller for all personal data processed by the platform
**Contact:** samrat.tidke@gmail.com
**Supervisory Authority:** Relevant EU Member State Data Protection Authority (determined by establishment)

**Data Processors (key):**
- Langfuse — LLM trace storage and analysis
- Google Cloud — Infrastructure, GCS, BigQuery
- OpenAI — LLM API processing
- Anthropic — LLM API processing
- Confluent — Managed Kafka (if used in production)

All processors are engaged under Data Processing Agreements (DPAs). See Section 9 for international transfer details.

---

## 2. Article 30 — Records of Processing Activities (RoPA)

The following table constitutes the Article 30 register required for controllers processing personal data at scale.

| # | Processing Activity | Purpose | Legal Basis | Personal Data Categories | Retention | Recipients | Third-Country Transfers |
|---|--------------------|---------|-----------|----|------|------|------|
| 1 | IT Incident Processing | Automated IT incident triage and resolution | Art 6(1)(f) Legitimate interests — efficient IT operations | Employee names, email addresses, system usernames, device identifiers, IP addresses in incident logs | 2 years from closure | IT Operations team, Platform service accounts | ServiceNow (US — SCCs) → Platform (EU) |
| 2 | Pipeline Metadata Storage | Data pipeline lifecycle management and audit | Art 6(1)(b) Contract — service delivery to data engineering teams | Jira reporter email, `created_by` field (user email), approver identity | 90 days active; 2 years audit trail | Data Engineering team, Platform Admin | Jira/Atlassian (US — SCCs) → Platform → GCS |
| 3 | Audit Log Maintenance | EU AI Act Art 12 transparency and accountability obligation | Art 6(1)(c) Legal obligation — EU AI Act compliance | User IDs, action types, AI decision records, approval/rejection events, authentication events | 7 years (immutable) | AI Safety Officer, Legal, Regulatory auditors | PostgreSQL (GCP region — adequacy/SCCs) |
| 4 | LLM Prompt/Completion Logging | Platform performance optimization, debugging, safety monitoring | Art 6(1)(f) Legitimate interests — platform improvement | Prompts may contain names or personal details from incident descriptions | 30 days | Platform Admin only | Langfuse processor (EU region if configured; else SCCs) |
| 5 | Weaviate Vector Embeddings | RAG knowledge base for incident resolution | Art 6(1)(f) Legitimate interests — AI system function | Pseudonymized incident data (embeddings, no raw PII policy) | Until re-indexed or system migration | Platform service accounts | Local Weaviate instance (no third-country transfer) |
| 6 | Human Approval Records | Record of human oversight decisions for accountability | Art 6(1)(c) Legal obligation — EU AI Act Art 14 human oversight | Approver name, approval/rejection timestamp, decision rationale | 7 years | AI Safety Officer | PostgreSQL only |
| 7 | Authentication Events | Security monitoring and unauthorized access detection | Art 6(1)(f) Legitimate interests — platform security | User ID, IP address, timestamp, success/failure status | 2 years | Platform Admin, AI Safety Officer | PostgreSQL only |
| 8 | PII Detection Events | Data minimization enforcement and compliance evidence | Art 6(1)(c) Legal obligation — GDPR Art 25 by design | PII type detected, zone, pipeline ID (no raw PII value stored in log) | 7 years (in audit log) | AI Safety Officer | PostgreSQL only |

---

## 3. Lawful Basis Analysis (Article 6)

### 3.1 Legitimate Interests (Art 6(1)(f))

**Processing activities:** IT incident processing, LLM trace logging, Weaviate embeddings, authentication logging.

**Legitimate Interest Assessment (LIA) Summary:**

- **Purpose test:** The interest is legitimate — efficient IT operations, platform security, and AI system improvement are genuine business needs not overridden by data subject interests.
- **Necessity test:** Personal data processing is necessary for the stated purposes. Incident data must include reporter identity for accountability. LLM traces require prompt content to enable debugging.
- **Balancing test:** Data subject impact is mitigated by: 30-day retention on LLM traces, pseudonymization in embeddings, PII masking in Gold zone, and the right to object (Art 21).
- **Conclusion:** Legitimate interests lawful basis is supportable subject to the documented safeguards.

### 3.2 Legal Obligation (Art 6(1)(c))

**Processing activities:** Audit logs (EU AI Act Art 12), human approval records (EU AI Act Art 14), PII detection events (GDPR Art 25).

The EU AI Act creates specific legal obligations for high-risk AI systems:
- Art 12: Logging requirements — audit trail of AI system operation
- Art 14: Human oversight — records of human intervention and decisions
- Art 17: Quality management — documented procedures and records

These obligations make the relevant processing activities mandatory. Data minimization still applies — only data necessary to fulfill the legal obligation is processed.

### 3.3 Contract (Art 6(1)(b))

**Processing activities:** Pipeline metadata, Jira ticket linkage.

Processing of `created_by` email and Jira reporter is necessary to perform the data engineering service contract. Users who create pipelines necessarily have their identity recorded for accountability and service delivery.

---

## 4. Special Categories of Personal Data (Article 9)

The platform does **not** intentionally process special category data (Art 9(1): racial or ethnic origin, political opinions, religious beliefs, trade union membership, genetic data, biometric data, health data, sex life or sexual orientation).

### 4.1 Inadvertent Collection Prevention

`agents/data_agent/src/security/pii_detection.py` detects and blocks special category data at every zone boundary:

| Special Category | Detection Method | Response |
|-----------------|-----------------|----------|
| Health/medical data | Pattern matching on field names and values (NHS numbers, diagnoses, ICD codes) | CRITICAL severity → pipeline BLOCKED |
| Biometric data | Field name detection (fingerprint, face_id, iris, retina) | CRITICAL severity → blocked at Bronze zone |
| Financial account data (special context) | IBAN, bank routing, credit card detection | REDACT or PARTIAL_MASK applied |
| Passport/national ID | Pattern-based detection with country variants | REDACT or TOKENIZE applied |

### 4.2 Critical Severity Escalation

When `pii_detection.py` returns a CRITICAL severity classification:
- The pipeline is immediately halted and set to `BLOCKED` status
- A `PII_DETECTED` audit event is written with severity=CRITICAL
- The Data Steward and AI Safety Officer are notified
- Manual review is required before the pipeline can proceed
- No data advances beyond the Bronze zone until review is complete

---

## 5. Data Subject Rights Implementation (Articles 15-22)

All data subject rights requests must be submitted to the contact email above. The platform has a 30-calendar-day response obligation under GDPR Art 12(3).

### 5.1 Right of Access (Article 15)

**What data subjects can request:** Confirmation of processing; copy of personal data; purposes, categories, recipients, retention period, rights information.

**Technical mechanism:** Audit log query by user identifier (`GET /governance/audit-logs?user_id={id}`); incident query by reporter email; pipeline query by `created_by` field.

**Response SLA:** 30 days from verified request receipt. Extension to 60 days permitted for complex requests with notification at day 30.

**Current limitation:** No self-service Subject Access Request (SAR) portal. Manual retrieval by Platform Admin on receipt of request. SAR portal is a planned enhancement.

### 5.2 Right to Rectification (Article 16)

**Scope:** Correction of inaccurate personal data.

**Technical mechanism:** Pipeline metadata `created_by` and incident reporter details can be corrected via Admin API. Audit log entries cannot be modified (immutability requirement) — a correction note can be appended as a separate audit event.

**Limitation:** LLM prompt/completion logs in Langfuse are deleted after 30 days; within-retention rectification requires Langfuse Admin API access.

### 5.3 Right to Erasure (Article 17)

**Scope:** Erasure where data is no longer necessary, consent withdrawn, or unlawful processing — subject to exceptions.

**Exceptions applicable to this platform:**
- Audit logs: exempt under Art 17(3)(b) — legal obligation (EU AI Act 7-year retention)
- Human approval records: exempt under Art 17(3)(b) — legal obligation
- Authentication security logs: exempt under Art 17(3)(e) — legal claims

**Erasable data:** Pipeline metadata (after 90-day retention), LLM traces (after 30 days, or on request), incident data (after 2-year retention or on request where no legal hold applies).

**Technical mechanism:** `backend/governance/data_retention.py` executes scheduled deletion. Manual erasure via Admin API for within-retention requests.

### 5.4 Right to Restriction of Processing (Article 18)

**Applicable where:** Accuracy contested, processing unlawful but erasure refused, data needed for legal claims, or objection pending.

**Technical mechanism:** Platform Admin can flag a data subject record in PostgreSQL as `processing_restricted = true`, causing API responses to exclude the record from standard processing. Restricted records are still retained but not used in AI processing.

### 5.5 Right to Data Portability (Article 20)

**Scope:** Applies where processing is based on consent (Art 6(1)(a)) or contract (Art 6(1)(b)), and processing is automated.

**Applicable processing:** Pipeline metadata (Art 6(1)(b) — contract). Provided in machine-readable JSON format via `GET /api/v2/pipelines?created_by={email}&format=json`.

**Not applicable:** Audit logs (legal obligation), incident data (legitimate interests).

### 5.6 Right to Object (Article 21)

**Scope:** Applies to processing based on legitimate interests (Art 6(1)(f)).

**Processing subject to objection:** Incident processing, LLM trace logging, Weaviate embeddings, authentication logging.

**Response:** Platform Admin assesses whether compelling legitimate grounds override the objection. If the objection is upheld, processing ceases and data is deleted (subject to legal hold exceptions).

**Note:** Objection to AI-based incident processing triggers manual review of the specific incident. The `await_approval` node ensures human decision-making remains available.

### 5.7 Rights Related to Automated Decision-Making (Article 22)

**Assessment:** The Incident Management LangGraph system makes automated recommendations for IT incident remediation. This constitutes automated processing with significant effects.

**Exception claimed:** Art 22(2)(a) — decision is necessary for the performance of a contract between data subject and controller (IT service delivery).

**Safeguard (mandatory under Art 22(3)):**
- Human approval gate (`await_approval` node) is **mandatory** for all P1/P2 incidents
- The `HUMAN_APPROVAL` or `HUMAN_REJECTION` event is logged for every decision
- Data subjects can request human review of any automated recommendation via the contact email
- Explanations of AI recommendations are captured in the `plan` field of incident state

**Implementation:** `backend/orchestrator/langgraph_workflow.py` — `await_approval` node; Kafka topic `incident.requires_approval`.

---

## 6. Privacy by Design and Default (Article 25)

### 6.1 Data Minimization

- Pipelines collect only fields declared in the source schema definition — no speculative column collection
- `created_by` field is mandatory but is the minimum identity data needed for accountability
- LLM prompts are constructed to avoid unnecessary PII inclusion (prompt templates in `agents/data_agent/prompts/`)
- Weaviate embeddings store vector representations only — raw incident text is not stored in the vector database

### 6.2 PII Detection at Zone Boundaries

Privacy by design is implemented at the technical level: `agents/data_agent/src/security/pii_detection.py` runs automatically at every Medallion zone transition:

| Zone Transition | PII Check | Action on Detection |
|----------------|-----------|-------------------|
| Ingest → Landing | Detection only (logged) | Alert logged; data accepted |
| Landing → Bronze | Detection + classification | Severity assigned; CRITICAL blocks |
| Bronze → Silver | Masking enforcement | REDACT/HASH/TOKENIZE applied per type |
| Silver → Gold | Final verification | No raw PII permitted; pipeline fails if found. Gold is analytics-ready (zero PII). |

### 6.3 Pseudonymization Strategies

`agents/data_agent/src/security/governance_enforcer.py` implements five masking strategies:

| Strategy | Description | Use Case |
|----------|-------------|---------|
| REDACT | Replace with `[REDACTED]` literal | Fields not needed downstream |
| HASH | SHA-256 hash of value | Deduplication without identity |
| TOKENIZE | Reversible token via secure lookup table | Fields needed for joining but not display |
| PARTIAL_MASK | Preserve first/last characters (e.g., `J***n`) | User-facing display with context |
| ENCRYPT | AES-256 encryption with platform key | Fields that must be recovered by authorized users |

### 6.4 Default Privacy Settings

- No PII permitted in Gold zone without explicit Data Owner approval and AI Safety Officer sign-off
- New pipelines default to `processing_mode: batch` with PII detection enabled
- LLM trace logging defaults to 30-day retention (cannot be extended without policy exception)
- Weaviate: no PII storage policy enforced at index creation

---

## 7. Security of Processing (Article 32)

### 7.1 Technical Measures

| Measure | Implementation |
|---------|---------------|
| Encryption at rest | AES-256 on GCS; application-level encryption on sensitive PostgreSQL columns |
| Encryption in transit | TLS 1.2+ for all external connections; HTTPS for all API calls |
| Access control | RBAC role definitions; GCP IAM for production; API key auth locally |
| PII detection | Automated at every zone boundary |
| Audit logging | 23-event types, immutable PostgreSQL storage |
| Secrets management | GCP Secret Manager; no secrets in code or logs |
| Input validation | Pydantic models on all API endpoints |
| LLM guardrails | Prompt injection detection; output filtering |

### 7.2 Organizational Measures

| Measure | Status |
|---------|--------|
| Information security policy (this ISMS) | Implemented — `docs/compliance/iso-27001.md` |
| Data governance policy | Implemented — `docs/compliance/data-governance-policy.md` |
| Security awareness training | Planned — Q4 2026 |
| Annual security review | Planned — Q1 2027 |
| Incident response procedure | Implemented — Section 8 below |
| Human approval gates | Implemented — `await_approval` node |
| Supplier DPAs | In place for all processors |

For full technical control detail, see `docs/compliance/iso-27001.md` Section 6 (Cryptography) and Section 7 (Operations Security).

---

## 8. Data Breach Notification (Articles 33-34)

### 8.1 Internal Detection and Assessment

Breaches are detected via:
- Prometheus/Grafana alerts (unauthorized access patterns)
- `AUTH_FAILURE` spike detection in audit logs
- `SECURITY_INCIDENT` manual reports
- Langfuse anomaly detection on LLM outputs

On detection, the AI Safety Officer conducts an initial impact assessment within 2 hours:
- Was personal data accessed, disclosed, altered, or lost?
- How many data subjects are affected?
- What categories of data were involved (special category data elevates severity)?
- Is there ongoing risk to data subjects?

### 8.2 Notification to Supervisory Authority (Article 33)

**Timeline:** 72 hours from becoming aware of the breach (unless breach is unlikely to result in risk to individuals).

**Required content (Art 33(3)):**
- Nature of the breach (categories and approximate number of data subjects and records)
- Contact details of the DPO or other contact point
- Likely consequences of the breach
- Measures taken or proposed to address the breach

**Process:**
1. AI Safety Officer notified immediately on breach detection
2. Incident logged as P1 in audit trail with `SECURITY_INCIDENT` event
3. Legal team engaged within 1 hour
4. Supervisory authority notification filed by hour 70 (2-hour buffer)
5. Notification reference number recorded in incident record

### 8.3 Notification to Data Subjects (Article 34)

Required when breach is likely to result in **high risk** to individuals (e.g., identity theft risk, financial loss, discrimination).

**High-risk indicators for this platform:**
- Unmasked PII (SSN, passport, credit card) exposed externally
- Health or biometric data exposed
- Authentication credentials compromised
- Incident data with personal details published externally

**Notification content:** Nature of breach, contact details, likely consequences, mitigation steps taken, data subject rights.

**Channel:** Email to affected data subjects; platform notification if email unavailable.

### 8.4 Documentation Requirements

All breaches, regardless of notification requirement, must be documented in the Breach Register (separate from audit log) containing:
- Date and time of discovery
- Date and time the breach occurred (if known)
- Description of the breach
- Categories and estimated number of data subjects affected
- Categories and estimated number of records affected
- Consequences and impact assessment
- Remediation measures implemented
- Notification decision and rationale (notify / not notify + reasoning)
- Notifications sent (to authority and/or data subjects) with timestamps

---

## 9. International Data Transfers (Articles 44-49)

All personal data transfers outside the European Economic Area (EEA) require a valid transfer mechanism.

| Recipient | Country | Personal Data Transferred | Transfer Mechanism | DPA Reference |
|-----------|---------|--------------------------|-------------------|---------------|
| OpenAI | United States | Incident descriptions (may contain names), pipeline metadata | Standard Contractual Clauses (SCCs) — 2021 EU Commission SCCs | OpenAI DPA v2023 |
| Anthropic | United States | Incident descriptions (may contain names), pipeline metadata | Standard Contractual Clauses (SCCs) — 2021 EU Commission SCCs | Anthropic DPA |
| Google Cloud (GCS, BigQuery) | EU region (europe-west) preferred; US possible | Pipeline artifacts, audit logs | Adequacy (within EU region); SCCs if US region used | Google Cloud DPA |
| Atlassian (Jira) | United States | Jira reporter email, ticket metadata | Standard Contractual Clauses (SCCs) | Atlassian DPA |
| Langfuse | EU region (self-hosted or cloud) | LLM prompts/completions (may contain names) | Same jurisdiction if EU-hosted; SCCs if US-hosted | Langfuse DPA |
| ServiceNow | United States | Incident data (employee names, emails) — read only | Standard Contractual Clauses (SCCs) | ServiceNow DPA |

### 9.1 SCC Implementation Notes

- All SCCs reference the 2021 EU Commission standard clauses (Module 2: Controller to Processor)
- Transfer Impact Assessments (TIAs) completed for US transfers, assessing US FISA 702 / Executive Order 14086 protections
- Data subjects may request copies of applicable SCCs via the contact email

### 9.2 Mitigation Measures for US Transfers

- LLM prompts are constructed to minimize PII inclusion (prompt templates reviewed by AI Safety Officer)
- LLM provider zero-retention policies requested where available (OpenAI: API data not used for training by default)
- GCP EU region mandated for production audit log storage and pipeline artifact storage

---

## 10. Formal Retention Schedule

| Data Category | Retention Period | Legal Basis for Retention | Deletion Method | Implementation |
|--------------|------------------|--------------------------|-----------------|----------------|
| Audit logs | 7 years | EU AI Act Art 12 legal obligation | Logical delete (soft delete flag); data remains queryable by authorized auditors | `backend/governance/data_retention.py` |
| Human approval records | 7 years | EU AI Act Art 14 legal obligation | Same as audit logs (records within audit log table) | PostgreSQL platform_audit_log table |
| IT Incident data | 2 years from closure | Business need — legitimate interests | Hard delete from PostgreSQL after retention period | `data_retention.py` scheduled task |
| Pipeline metadata (active) | Duration of pipeline + 90 days | Contract — Art 6(1)(b) | Soft delete on pipeline archive; hard delete at 90 days | PostgreSQL TTL + `data_retention.py` |
| GCS pipeline artifacts | 90 days | Data minimization — Art 5(1)(e) | GCS lifecycle policy: `DELETE` action after 90 days | GCS bucket lifecycle rule |
| LLM prompts / completions (Langfuse) | 30 days | Data minimization — Art 5(1)(e) | Langfuse built-in data retention setting | Langfuse configuration |
| Weaviate embeddings | Until pipeline re-index or system migration | Legitimate interests — RAG function | Manual re-index clears vectors; migration procedure documented | Manual process + data governance policy |
| PII detected (Bronze zone) | 90 days maximum; masked or deleted at Silver promotion | GDPR Art 5(1)(e) — storage limitation | Zone promotion applies masking; unpromoted data deleted at 90 days | Zone lifecycle policy |
| Authentication / security logs | 2 years | Legitimate interests — security | Hard delete via `data_retention.py` | PostgreSQL TTL |
| Breach register entries | 5 years from breach date | Art 33(5) accountability | Secure archival; not deleted | Separate breach register |

### 10.1 Deletion Verification

`backend/governance/data_retention.py` runs scheduled deletion tasks and logs each deletion event to the audit trail with:
- Data category
- Count of records deleted
- Retention period applied
- Execution timestamp

This provides documentary evidence of compliance with the retention schedule.

---

## 11. Data Protection Officer

### 11.1 DPO Requirement Assessment

Under GDPR Art 37, a DPO is mandatory where:
- (a) Processing is carried out by public authority or body
- (b) Core activities involve large-scale systematic monitoring of individuals
- (c) Core activities involve large-scale processing of special category data (Art 9) or criminal data (Art 10)

**Assessment:**
- Criterion (a): Does not apply — private platform
- Criterion (b): The Incident Management system monitors IT incidents which may involve employee data at scale. If deployed across an enterprise with thousands of employees, criterion (b) may be triggered
- Criterion (c): Does not apply — no intentional special category processing

**Conclusion:** DPO appointment may be required depending on deployment scale. It is strongly recommended to appoint a DPO or designate a privacy lead with equivalent functions. AI Safety Officer currently performs DPO-equivalent functions.

**Action:** Legal assessment to confirm DPO requirement by Q3 2026.

### 11.2 DPO Functions (Current / Planned)

| Function | Current Owner | Status |
|----------|--------------|--------|
| GDPR compliance oversight | AI Safety Officer | Active |
| Data subject rights handling | Platform Admin (manual) | Active — SAR portal planned |
| Breach notification coordination | AI Safety Officer | Active |
| Privacy impact assessments | AI Safety Officer | Partial |
| Supervisory authority liaison | Legal / AI Safety Officer | Planned |
| Staff training | AI Safety Officer | Q4 2026 |

---

## 12. Privacy Notice Template

The following template should be presented to data subjects whose personal data is processed by the platform.

---

**Privacy Notice — Enterprise Agentic AI Platform**

**Who we are:** [Organization name and contact details]

**What personal data we process:** We process your name, email address, and system identifiers in connection with IT incident management and data pipeline services. If you have raised or are named in an IT incident, we process the content of that incident record.

**Why we process it:**
- To provide IT incident resolution services (legitimate interests — Art 6(1)(f))
- To manage data pipeline services on your behalf (contract — Art 6(1)(b))
- To maintain mandatory audit records of AI system decisions (legal obligation — Art 6(1)(c), EU AI Act)

**How long we keep it:** Incident data for 2 years; pipeline metadata for 90 days; audit records for 7 years.

**Who we share it with:** IT Operations team; Data Engineering team; AI Safety Officer. Limited sharing with service providers (Google Cloud, OpenAI, Anthropic) under Data Processing Agreements and Standard Contractual Clauses for non-EEA transfers.

**Automated decision-making:** This platform uses AI to generate IT incident remediation recommendations. Automated recommendations always require human approval before any action is taken. You may request human review of any automated decision affecting you.

**Your rights:** You have the right to access, correct, erase, or restrict processing of your data; to data portability; and to object to processing based on legitimate interests. Submit requests to: [contact email]. Response within 30 days.

**Complaints:** You may lodge a complaint with your national Data Protection Authority.

---

## Document Control

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-22 | AI Safety Officer | Initial GDPR compliance assessment |

**Next Review:** 2027-06-22 or on material change to processing activities or applicable regulation.

*This document is classified CONFIDENTIAL. Distribution limited to authorized personnel and regulatory authorities.*
