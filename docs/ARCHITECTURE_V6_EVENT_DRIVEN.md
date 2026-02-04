# Architecture v6.0 - Event-Driven Architecture

## Overview

This document describes the v6.0 event-driven architecture refactoring. The key principle is:

> **Kafka is the system of record. All state transitions flow through Kafka.**

## Mental Model

```
MCPs sense → Kafka remembers → Orchestrator routes → LangGraph reasons & acts → FastAPI governs
```

## Core Principles

### 1. Kafka is the System of Record
- All state transitions are Kafka events
- Events are immutable and ordered
- Consumers can replay from any point
- Idempotency keys prevent duplicate processing

### 2. FastAPI is Control Plane Only
- Serves UI (reads from Redis/Postgres - CQRS)
- Handles human approvals (publishes to Kafka)
- Provides policy management
- Does NOT directly execute workflows

### 3. MCP Servers are Edge Adapters
- Poll external systems (ServiceNow, Jira)
- Publish events to Kafka
- Consume command events from Kafka
- Execute actions on external systems

### 4. LangGraph Owns Execution and Reasoning
- Consumes events from Kafka (via EventOrchestrator)
- Processes through workflow nodes
- Publishes state transition events
- Pauses at approval nodes, resumes from Kafka events

### 5. Humans Approve, Agents Execute
- Every significant action requires approval
- Approvals flow through Kafka
- Agents only execute after explicit approval

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         EVENT-DRIVEN ARCHITECTURE v6.0                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌──────────────────┐                           ┌──────────────────┐           │
│  │  ServiceNow MCP  │────poll────▶ ServiceNow   │    Jira MCP      │──poll───▶│
│  │  (Edge Adapter)  │◀───────────              │  (Edge Adapter)  │◀──────── │
│  └────────┬─────────┘                           └────────┬─────────┘           │
│           │ publish                                      │ publish              │
│           ▼                                              ▼                      │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                           KAFKA CLUSTER                                   │  │
│  │  ┌───────────────────────────────────────────────────────────────────┐   │  │
│  │  │                    INCIDENT LIFECYCLE TOPICS                       │   │  │
│  │  │  incident.created → received → enriched → plan_generated →        │   │  │
│  │  │  requires_approval → approved/rejected → executed → verified →    │   │  │
│  │  │  close_execute → closed                                           │   │  │
│  │  └───────────────────────────────────────────────────────────────────┘   │  │
│  │  ┌───────────────────────────────────────────────────────────────────┐   │  │
│  │  │                    PIPELINE LIFECYCLE TOPICS                       │   │  │
│  │  │  pipeline.requested → planned → generated → validated →           │   │  │
│  │  │  requires_approval → approved/rejected → deploy_execute → deployed│   │  │
│  │  └───────────────────────────────────────────────────────────────────┘   │  │
│  │  ┌───────────────────────────────────────────────────────────────────┐   │  │
│  │  │                    REMEDIATION TOPICS                              │   │  │
│  │  │  remediation.started → executed → failed → rollback               │   │  │
│  │  └───────────────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                    │ consume                        │ consume                   │
│                    ▼                                ▼                           │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                      EVENT ORCHESTRATOR                                   │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │  │
│  │  │ WorkflowManager │  │  StateTracker   │  │  EventRouter    │          │  │
│  │  │ (Redis state)   │  │ (Redis/Postgres)│  │ (topic mapping) │          │  │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘          │  │
│  │           │                    │                    │                     │  │
│  │           └────────────────────┼────────────────────┘                     │  │
│  │                                │                                          │  │
│  │                                ▼                                          │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐    │  │
│  │  │                    LANGGRAPH WORKFLOW                             │    │  │
│  │  │  ingest → parse → classify → swarm_rag → generate_plan →         │    │  │
│  │  │  judge_evaluation → control_plane → await_approval →              │    │  │
│  │  │  [PAUSE] ←──── incident.approved from Kafka                      │    │  │
│  │  │  [RESUME] → execute → verify → close_ticket → feedback_loop      │    │  │
│  │  └──────────────────────────────────────────────────────────────────┘    │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                    │ publish events                 ▲                          │
│                    ▼                                │ approval events          │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                        FASTAPI CONTROL PLANE                              │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │  │
│  │  │   UI Serving    │  │ Approval APIs   │  │ Policy Engine   │          │  │
│  │  │ (reads Redis/PG)│  │(publishes Kafka)│  │ (governance)    │          │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘          │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Event Flow: Incident Lifecycle

### 1. Incident Detection
```
ServiceNow MCP polls → detects new incident → publishes incident.created
```

### 2. Workflow Initiation
```
EventOrchestrator consumes incident.created → triggers LangGraph workflow
```

### 3. Processing
```
LangGraph processes:
  node_ingest      → publishes incident.received
  node_parse       → (internal)
  node_classify    → publishes incident.enriched
  node_swarm_rag   → (internal)
  node_generate_plan → publishes incident.plan_generated
  node_judge_evaluation → (internal)
  node_control_plane → determines approval route
  node_await_approval → publishes incident.requires_approval
                        [WORKFLOW PAUSES]
```

### 4. Human Approval
```
UI displays pending approval → Human clicks Approve/Reject
FastAPI publishes incident.approved or incident.rejected
EventOrchestrator consumes → resumes LangGraph workflow
```

### 5. Execution
```
LangGraph resumes:
  node_process_approval → updates state
  node_execute → publishes remediation.started → triggers GitHub Actions
               → waits for completion → publishes remediation.executed
  node_verify  → publishes incident.verified
  node_close_ticket → publishes incident.close_execute
```

### 6. Ticket Closure
```
ServiceNow MCP consumes incident.close_execute → calls ServiceNow API
ServiceNow MCP publishes incident.closed
```

---

## Key Components

### 1. MCP Servers (`mcp-servers/`)

| Server | Purpose | Publishes | Consumes |
|--------|---------|-----------|----------|
| ServiceNow MCP | Poll incidents, close tickets | incident.created | incident.close_execute |
| Jira MCP | Poll pipeline requests | pipeline.requested | pipeline.completed |

### 2. Kafka Topics (`backend/streaming/schemas.py`)

#### Incident Lifecycle
- `incident.created` - New incident detected by MCP
- `incident.received` - LangGraph started processing
- `incident.enriched` - Classification complete
- `incident.plan_generated` - Remediation plan ready
- `incident.requires_approval` - Pending human approval
- `incident.approved` - Human approved
- `incident.rejected` - Human rejected
- `incident.close_execute` - Command to MCP to close
- `incident.closed` - Workflow complete

#### Pipeline Lifecycle
- `pipeline.requested` - New request from Jira
- `pipeline.planned` - Planning complete
- `pipeline.generated` - Code generated
- `pipeline.validated` - Validation passed
- `pipeline.requires_approval` - Pending human
- `pipeline.approved` - Human approved
- `pipeline.deploy_execute` - Command to deploy
- `pipeline.deployed` - Deployment complete

#### Remediation
- `remediation.started` - Execution began
- `remediation.executed` - Execution complete
- `remediation.failed` - Execution failed
- `remediation.rollback` - Rollback triggered

### 3. Event Orchestrator (`backend/streaming/consumers/event_orchestrator.py`)

The central router that:
- Consumes all lifecycle events
- Routes to appropriate LangGraph workflows
- Manages workflow state in Redis
- Handles pause/resume for approvals

### 4. LangGraph Workflow (`backend/orchestrator/langgraph_workflow.py`)

v6.0 changes:
- Publishes Kafka events at each state transition
- Uses MemorySaver checkpointer for pause/resume
- `interrupt_after=["await_approval"]` enables pause
- `node_close_ticket` publishes command event (doesn't call ServiceNow directly)

### 5. FastAPI Control Plane (`backend/app.py`)

v6.0 responsibilities:
- Serve UI (CQRS - reads from Redis/Postgres)
- Approval endpoints publish to Kafka
- Policy engine (governance)
- Does NOT directly execute workflows

### 6. Frontend UI (`frontend/`)

v2.0 Canonical Models Architecture:
- **Framework**: Next.js 14 + React Query + TailwindCSS
- **Type System**: TypeScript types mirror Pydantic models exactly
- **Key Files**: `src/types/pipeline-canonical.ts`, `src/components/pipeline/`

#### Frontend Pages
| Page | Purpose | System |
|------|---------|--------|
| `/incidents` | View IT incidents from ServiceNow | Incident Management |
| `/approvals` | Approve/reject remediation plans | Incident Management |
| `/workflows` | Monitor LangGraph execution | Incident Management |
| `/pipelines` | Create/view data pipelines (70+ sources) | Data Engineering |
| `/jira/[id]` | Jira-integrated pipeline creation | Data Engineering |

#### Pipeline Components (v2.0)
| Component | Purpose |
|-----------|---------|
| `UnifiedPipelineForm` | Main form with 3 input modes |
| `SourceTypeSelector` | 9-category picker for 70+ source types |
| `SourceConfigForms` | 6 type-specific configuration forms |
| `NLTransformInput` | NL → structured metadata converter |
| `DTSXMigrationForm` | SSIS package migration tool |

#### 3 Input Modes
1. **UI Structured** - Type-safe form with 70+ source types across 9 categories
2. **Natural Language** - LLM converts NL to structured config (NEVER executes NL directly)
3. **DTSX Migration** - Parses SSIS packages and maps to Airflow

#### Frontend-Backend Communication
```
Frontend (Next.js)
    │
    ├─── GET /api/v1/incidents ──────────► FastAPI (reads Redis/Postgres)
    │
    ├─── POST /api/v1/approve ───────────► FastAPI → Kafka (incident.approved)
    │
    └─── POST /api/v2/data-agent/pipelines ► Data Agent API → LangGraph
```

---

## CQRS Pattern

```
┌─────────────────────────────────────────────────────────────────┐
│                       CQRS ARCHITECTURE                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  COMMAND SIDE                      QUERY SIDE                   │
│  ════════════                      ══════════                   │
│                                                                 │
│  ┌─────────────┐                   ┌─────────────┐             │
│  │   FastAPI   │                   │   FastAPI   │             │
│  │  Approval   │                   │     UI      │             │
│  │  Endpoints  │                   │  Endpoints  │             │
│  └──────┬──────┘                   └──────┬──────┘             │
│         │                                 │                     │
│         ▼                                 ▼                     │
│  ┌─────────────┐                   ┌─────────────┐             │
│  │    Kafka    │                   │   Redis /   │             │
│  │   Topics    │──────────────────▶│  Postgres   │             │
│  └─────────────┘  (consumers       └─────────────┘             │
│                    update)                                      │
│                                                                 │
│  Commands go to Kafka              Queries read from Redis/PG  │
│  Events are processed              State is eventually         │
│  by consumers                      consistent                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Pause/Resume Architecture

### How Approvals Work

1. **LangGraph publishes** `incident.requires_approval` with:
   - `approval_token` - Unique ID for this approval request
   - `plan` - The remediation plan to approve
   - `judge_score` - LLM-as-Judge evaluation

2. **Workflow pauses** at `node_await_approval` (via `interrupt_after`)

3. **FastAPI** receives approval from UI, publishes `incident.approved`:
   - `incident_id` - Which incident
   - `approved` - Boolean
   - `approver` - Who approved
   - `approval_token` - Links to original request

4. **EventOrchestrator** consumes approval, calls:
   ```python
   workflow_orchestrator.resume(
       incident_id=incident_id,
       approval_decision={
           "approved": True,
           "approver": "user@company.com"
       }
   )
   ```

5. **LangGraph resumes** from checkpoint, continues execution

---

## Directory Structure

```
backend/
├── streaming/
│   ├── consumers/
│   │   ├── __init__.py
│   │   └── event_orchestrator.py    # Main Kafka consumer, routes to workflows
│   ├── kafka_producer.py            # Unified producer for all events
│   └── schemas.py                   # Topics, event schemas, serialization
├── orchestrator/
│   ├── langgraph_workflow.py        # v6.0 event-driven workflow
│   ├── llm_judge.py                 # LLM-as-Judge evaluator
│   └── llm_intelligence.py          # LLM operations
├── mcp/
│   └── servers/                     # Tool servers (RAG, GCS, LLM)
└── app.py                           # FastAPI control plane

mcp-servers/
├── servicenow-mcp/
│   └── event_driven_server.py       # Polls ServiceNow, publishes events
└── jira-mcp/
    └── event_driven_server.py       # Polls Jira, publishes events
```

---

## Migration from v5.0

### What Changed

| Component | v5.0 | v6.0 |
|-----------|------|------|
| Approval waiting | Poll Redis | Kafka event + checkpoint |
| ServiceNow close | Direct API call | Publish `incident.close_execute` |
| Workflow start | FastAPI calls LangGraph | EventOrchestrator triggers |
| State updates | Direct in workflow | Kafka events → consumers update |

### Breaking Changes

1. **Approval flow** - UI must publish to Kafka, not call workflow directly
2. **ServiceNow closure** - MCP server must consume close events
3. **State queries** - Read from Redis/Postgres, not Kafka

---

## Best Practices

### 1. Event Design
- Include `idempotency_key` for deduplication
- Include `correlation_id` for tracing
- Timestamp in ISO 8601 format

### 2. Consumer Design
- Commit offsets after successful processing
- Handle duplicates gracefully
- Log with correlation_id for tracing

### 3. Error Handling
- Publish to dead-letter queue on failure
- Include error details in failure events
- Enable retry with backoff

---

## Running the System

```bash
# 1. Start Kafka
docker-compose up -d kafka zookeeper

# 2. Start MCP servers
python -m mcp-servers.servicenow-mcp.event_driven_server &
python -m mcp-servers.jira-mcp.event_driven_server &

# 3. Start Event Orchestrator
python -c "from backend.streaming.consumers import EventOrchestrator; EventOrchestrator().start()"

# 4. Start FastAPI
uvicorn backend.app:app --host 0.0.0.0 --port 8000
```

---

## Observability

All events include:
- `correlation_id` - Trace across systems
- `timestamp` - Event time
- `event_type` - Classification

Use Grafana + Prometheus + Tempo for:
- Event flow visualization
- Latency tracking
- Error alerting

---

## Workflow Flow Diagrams

### Incident Management — LangGraph 12-Node Workflow

```
START
  │
  ▼
┌─────────────┐
│   ingest    │──▶ Kafka: incident.received
└─────────────┘
       │
       ▼
┌─────────────┐
│    parse    │  Extract structured data from raw incident
└─────────────┘
       │
       ▼
┌─────────────┐
│  classify   │──▶ Kafka: incident.enriched
└─────────────┘  LLM classifies incident type
       │
       ▼
┌─────────────┐
│ swarm_rag   │  4 RAG agents vote on best runbook (RRF fusion)
└─────────────┘
       │
       ▼
┌─────────────┐
│generate_plan│──▶ Kafka: incident.plan_generated
└─────────────┘  LLM generates remediation plan
       │
       ▼
┌─────────────┐       ┌─────────────┐
│   judge     │──────▶│generate_plan│  Retry loop (max 2)
│ evaluation  │ FAIL  └─────────────┘
└─────────────┘
       │ PASS
       ▼
┌─────────────┐
│control_plane│  Policy engine determines approval route
└─────────────┘
       │
  ┌────┴────┐
  ▼         ▼
┌──────────┐ ┌──────────────┐
│auto_     │ │await_approval│──▶ Kafka: incident.requires_approval
│approve   │ └──────────────┘    [WORKFLOW PAUSES]
└──────────┘       │
       │           │◀── Kafka: incident.approved (from UI/Slack)
       └─────┬─────┘
             ▼
      ┌─────────────┐
      │   execute   │──▶ Kafka: remediation.started / remediation.executed
      └─────────────┘  Trigger GitHub Actions workflow
             │
             ▼
      ┌─────────────┐
      │   verify    │──▶ Kafka: incident.verified
      └─────────────┘
             │
             ▼
      ┌─────────────┐
      │close_ticket │──▶ Kafka: incident.close_execute
      └─────────────┘
             │
             ▼
      ┌─────────────┐
      │feedback_loop│──▶ Kafka: incident.closed
      └─────────────┘
             │
             ▼
           END
```

### Data Agent — 5-Agent Workflow

```
START
  │
  ▼
┌──────────────────────────────────────────────┐
│              SUPERVISOR AGENT                 │
│  Coordinates workflow, handles errors         │
└──────────────────────────────────────────────┘
       │
       ▼
┌─────────────┐
│  PLANNER    │──▶ Kafka: pipeline.planned
│   AGENT     │  Determines: create/modify/upgrade/no_change
└─────────────┘
       │
       ▼
┌─────────────┐
│ GENERATOR   │──▶ Kafka: pipeline.generated
│   AGENT     │  Renders Jinja2 templates: DAG + Spark jobs
└─────────────┘
       │
       ▼
┌─────────────┐       ┌─────────────┐
│ VALIDATOR   │──────▶│ GENERATOR   │  Retry loop
│   AGENT     │ FAIL  └─────────────┘
└─────────────┘──▶ Kafka: pipeline.validated
       │ PASS
       ▼
┌──────────────────────────────────────────────┐
│  APPROVAL CHECK (if PROD or schema_change)    │
│  publish: pipeline.requires_approval          │
│  [WORKFLOW PAUSES]                            │
└──────────────────────────────────────────────┘
       │◀── Kafka: pipeline.approved (from UI)
       ▼
┌─────────────┐
│ DEPLOYER    │──▶ Kafka: pipeline.deploy_execute
│   AGENT     │  Creates PR, triggers CI/CD
└─────────────┘
       │
       ▼
     END ──▶ Kafka: pipeline.deployed
```

---

## Kafka Topics Reference

### Topic Naming Convention

```
{domain}.{event_type}
```

### Incident Lifecycle Topics

| Topic | Publisher | Consumer | Description |
|-------|-----------|----------|-------------|
| `incident.created` | ServiceNow MCP | EventOrchestrator | New incident detected |
| `incident.received` | LangGraph | State consumers | LangGraph started processing |
| `incident.enriched` | LangGraph | State consumers | Classification complete |
| `incident.plan_generated` | LangGraph | State consumers | Remediation plan ready |
| `incident.requires_approval` | LangGraph | UI/Slack | Human approval required |
| `incident.approved` | FastAPI | EventOrchestrator | Human approved |
| `incident.rejected` | FastAPI | EventOrchestrator | Human rejected |
| `incident.executed` | LangGraph | State consumers | Execution complete |
| `incident.verified` | LangGraph | State consumers | Fix verified |
| `incident.close_execute` | LangGraph | ServiceNow MCP | Command to close ticket |
| `incident.closed` | ServiceNow MCP | State consumers | Ticket closed |
| `incident.failed` | LangGraph | Alerting | Workflow failed |

### Pipeline Lifecycle Topics

| Topic | Publisher | Consumer | Description |
|-------|-----------|----------|-------------|
| `pipeline.requested` | Jira MCP / FastAPI | EventOrchestrator | New pipeline request |
| `pipeline.planned` | Data Agent | State consumers | Planning complete |
| `pipeline.generated` | Data Agent | State consumers | Code generated |
| `pipeline.validated` | Data Agent | State consumers | Validation passed |
| `pipeline.requires_approval` | Data Agent | UI | PROD approval required |
| `pipeline.approved` | FastAPI | EventOrchestrator | Human approved |
| `pipeline.deploy_execute` | Data Agent | Airflow MCP | Command to deploy |
| `pipeline.deployed` | Airflow MCP | State consumers | Deployment complete |

### Remediation Topics

| Topic | Publisher | Consumer | Description |
|-------|-----------|----------|-------------|
| `remediation.started` | LangGraph | Monitoring | Execution began |
| `remediation.executed` | LangGraph | State consumers | Execution complete |
| `remediation.failed` | LangGraph | Alerting | Execution failed |
| `remediation.rollback` | LangGraph | Alerting | Rollback triggered |

### MCP Command Topics

| Topic | Publisher | Consumer | Description |
|-------|-----------|----------|-------------|
| `mcp.servicenow.commands` | FastAPI/Orchestrator | ServiceNow MCP | Close/update tickets |
| `mcp.github.commands` | FastAPI/Orchestrator | GitHub MCP | Trigger workflows |
| `mcp.airflow.commands` | FastAPI/Orchestrator | Airflow MCP | Trigger DAGs |

### Topic Configuration

- **Partitioning**: Incident topics by `incident_id`, Pipeline topics by `request_id`
- **Retention**: Lifecycle 7 days, Commands 1 day, Audit 30 days
- **Consumer Groups**: `event-orchestrator` (all lifecycle), `state-projector` (all lifecycle), `audit-logger` (all), `servicenow-mcp` (close), `airflow-mcp` (deploy)

---

## Event Model

### Event Base Structure

```python
class EventBase(BaseModel):
    event_id: str          # UUID
    event_type: str        # e.g., "incident.created"
    timestamp: str         # ISO 8601
    correlation_id: str    # Links related events
    source: str            # System that published
    version: str           # Schema version "6.0"
```

### Idempotency Key Pattern

```python
idempotency_key: str = f"{incident_id}:{event_type}:{timestamp}"
```

Consumers deduplicate using Redis with 24h TTL.

### Event Categories

1. **Lifecycle Events** — Published by LangGraph on state transitions
2. **Control Events** — Published by FastAPI on human decisions
3. **Command Events** — Published to trigger MCP actions

### Event Serialization

```python
# Publishing
producer = get_producer()
event = IncidentCreatedEvent(...)
await producer.publish_event(topic=Topics.INCIDENT_CREATED, event=event.model_dump(), key=incident_id)

# Consuming
event = event_from_json(json.dumps(message.value), event_type)
```

---

## Component Responsibility Matrix

| Component | Kafka Pub | Kafka Sub | LLM | RAG | HTTP | External API |
|-----------|-----------|-----------|-----|-----|------|--------------|
| MCP Servers | Yes | Yes | No | No | No | Yes |
| EventOrchestrator | No | Yes | No | No | No | No |
| LangGraph Workflow | Yes | No | Yes | Yes | No | No |
| FastAPI | Yes | No | No | No | Yes | No |
| LLM Intelligence | No | No | Yes | No | No | No |
| LLM Judge | No | No | Yes | No | No | No |
| Swarm RAG | No | No | No | Yes | No | No |
| Policy Engine | No | No | No | No | No | No |
| Audit Logger | No | No | No | No | No | No |

### Component Details

- **MCP Servers**: Stateless edge adapters — poll external systems, normalize events, publish to Kafka, consume commands
- **EventOrchestrator**: Central router — consumes lifecycle events, routes to LangGraph, manages pause/resume
- **LangGraph Workflow**: Execution engine — 12-node incident workflow, publishes state transitions, pauses for approval
- **FastAPI Control Plane**: Governance — serves UI (CQRS reads), handles approvals (publishes to Kafka)
- **LLM Intelligence/Judge**: AI reasoning — classification, plan generation, evaluation (called by LangGraph nodes)
- **Swarm RAG**: Document retrieval — 4 agents + RRF fusion (called by LangGraph node)
- **Policy Engine**: Governance — risk evaluation, approval route determination
- **Audit Logger**: Compliance — immutable logs for EU AI Act

---

## Architecture & AI Patterns

### Architecture Patterns

| Pattern | Purpose | Implementation |
|---------|---------|----------------|
| Event Sourcing | All state changes as immutable Kafka events | `langgraph_workflow.py` |
| CQRS | Separate write (Kafka) from read (Redis/Postgres) | `app.py` |
| Saga | Long-running workflows with rollback | `langgraph_workflow.py` |
| Hub-and-Spoke | EventOrchestrator as central router | `event_orchestrator.py` |
| Adapter | MCP servers translate external ↔ Kafka | `mcp-servers/` |

### AI Patterns

| Pattern | Purpose | Implementation |
|---------|---------|----------------|
| LangGraph StateGraph | Deterministic workflow (NOT ReAct) | `langgraph_workflow.py` |
| Chain-of-Thought | Step-by-step LLM reasoning for plans | `llm_intelligence.py` |
| Self-Reflection (Judge) | Separate LLM evaluates plan quality/safety | `llm_judge.py` |
| Swarm Intelligence | 4 RAG agents + RRF fusion voting | `swarm_retriever.py` |
| Plan-Execute | Planning → Validation → Approval → Deploy | `data_agent/` |
| Human-in-the-Loop | Pause/resume via Kafka + checkpoint | `langgraph_workflow.py` |

### Anti-Patterns

- **ReAct for workflow control** — LLM should NOT control flow; use StateGraph
- **Direct API calls from workflows** — Publish command events; let MCPs execute
- **Implicit LLM memory** — All state must be explicit TypedDict/Pydantic
