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

---

## FAST Multi-Agent Architecture

### Overview

**FAST = Federated Agents with Strict Transitions.** FAST is a production-hardened multi-agent architecture for autonomous IT incident resolution. It replaces the monolithic 12-node LangGraph workflow with 9 specialized agents coordinated by a Governor orchestrator. Each agent has a single responsibility, typed contracts, idempotency guarantees, and audit-on-every-action.

```
MCPs sense → Kafka remembers → Governor routes → FAST agents reason & act → FastAPI governs
```

**Source files:** `agents/servicenow_agent/src/fast_agents/` and `agents/servicenow_agent/src/middleware/`

```
┌────────────────────────────────────────────────────────────┐
│                        Governor                            │
│  (7-phase orchestration, state machine, parallel dispatch) │
└────┬──────────┬──────────┬──────────┬──────────┬───────────┘
     │ Phase 1  │ Phase 2  │ Phase 3  │ Phase 4  │ Phase 5-7
     │ (seq)    │ (parallel│ (seq)    │ (pause)  │ (seq)
┌────▼───┐ ┌───▼───┐ ┌───▼───┐ ┌────▼────┐ ┌───▼─────┐
│ Intell │ │ Risk  │ │  CHG  │ │Approval │ │  Exec   │
│ Agent  │ │ Agent │ │  Mgmt │ │  Agent  │ │  Agent  │
└────────┘ └───────┘ └───────┘ └─────────┘ └─────────┘
                                                │
                                           ┌────▼────┐
                                           │ Verify  │
                                           │  Agent  │
                                           └─────────┘
     │             │             │              │
┌────▼─────────────▼─────────────▼──────────────▼────────┐
│  Observability Agent  (always-on hooks, every phase)   │
│  Learning Agent       (post-closure feedback loop)     │
└────────────────────────────────────────────────────────┘
```

---

### The 9 FAST Agents

#### 1. IncidentIntelligenceAgent

**File:** `incident_intelligence.py` | **Closes:** C11, H8, H9

Root cause analysis, deduplication, correlation, and confidence scoring.

| Capability | Detail |
|------------|--------|
| RCA Patterns | 15 rule-based patterns (memory_exhaustion, crash_loop, connectivity_failure, disk_full, certificate_issue, permission_denied, dag_failure, timeout, cpu_saturation, k8s_node_issue, db_replication, queue_backlog, dns_failure, rate_limited, database_deadlock) with LLM fallback |
| Deduplication | SHA256 fingerprint of `service:classification:normalized_error`, stored in Redis with 60-minute sliding window, `NX` flag for atomicity |
| Correlation | Neo4j Cypher query for incidents on the same service within 24-hour window |
| Confidence Scoring | Composite score (0.0-1.0) from RCA source quality, evidence availability, RCA specificity; blended with LLM confidence when available |
| SLA Computation | Severity-based deadlines (P1=60m, P2=4h, P3=24h, P4=7d) |

**Input:** Raw incident dict from ServiceNow | **Output:** `IncidentContext`

#### 2. RiskAgent

**File:** `risk_agent.py` | **Closes:** H6, H7, M1, M2

Blast radius calculation, SLA impact assessment, and risk-based approval routing.

| Capability | Detail |
|------------|--------|
| Blast Radius | Neo4j BFS traversal `DEPENDS_ON*1..3` (max depth 3), counts impacted services and estimated users. Falls back to conservative estimate (3 unknown dependents) when graph unavailable |
| SLA Urgency | Remaining time calculation with breach risk flag at <30% threshold |
| Risk Scoring | Composite formula: `base_action_weight * env_multiplier + blast_factor + sla_urgency - mitigations`. 18 configurable action risk weights (e.g., `delete=0.9`, `restart=0.3`, `terraform_destroy=0.95`) |
| Environment Multipliers | production=1.5x, staging=1.0x, qa=0.7x, development=0.3x, sandbox=0.1x |
| Approval Recommendation | 4 levels: auto (LOW risk), standard (MEDIUM), senior (HIGH), executive (CRITICAL) |
| Dependency Chain | Returns `List[DependencyNode]` with service name, tier (tier-1/2/3), and user count |

**Input:** `IncidentContext` | **Output:** `RiskAssessment`

#### 3. ChangeManagementAgent

**File:** `change_management.py` | **Closes:** H4, H5

ServiceNow Change Record (CHG) creation with ITIL-compliant classification.

| Capability | Detail |
|------------|--------|
| CHG Creation | ServiceNow Table API integration, falls back to local CHG number (`CHG-LOCAL-*`) when API unavailable |
| Change Classification | Emergency (P1 + SLA breach), Normal (P1/P2 + HIGH/CRITICAL risk), Standard (everything else) |
| Business Hours | Configurable change window (default 06:00-22:00 UTC), weekend enforcement, emergency bypass |
| Linked Records | Auto-links CHG to incident and up to 5 correlated incidents |
| Plans | Auto-generates implementation plan and rollback plan text |

**Input:** `IncidentContext` + `RiskAssessment` | **Output:** `ChangeRecord`

#### 4. ExecutionAgent

**File:** `execution_agent.py` | **Closes:** C8, H18, H19, M3

Pre-validated execution with retry, timeout, and auto-rollback.

| Capability | Detail |
|------------|--------|
| Pre-Validation | Script/workflow existence check, rollback availability verification, parameter validation |
| 3 Dispatchers | GitHub Actions (workflow trigger + wait), Airflow MCP (DAG trigger via Kafka), GCP API (Compute Engine start/stop via `asyncio.to_thread`) |
| Retry | Exponential backoff (5s, 10s, 20s), max 3 attempts, configurable via `EXECUTION_MAX_RETRIES` |
| Timeout | `asyncio.wait_for()` with configurable deadline (default 600s) |
| Auto-Rollback | Triggered on execution failure when `rollback_verified=True`. Builds rollback plan from original plan's rollback fields. Rollback is never dry-run |

**Input:** `RemediationPlan` + `IncidentContext` | **Output:** `ExecutionResult`

#### 5. VerificationAgent

**File:** `verification_agent.py` | **Closes:** H1, H2, H3

Multi-check verification with stabilization window and proof of recovery.

| Capability | Detail |
|------------|--------|
| Stabilization Window | Default 60 seconds (`STABILIZATION_WINDOW_SECONDS`), configurable, waits before first health check |
| Health Check: GCP VM | Queries Compute Engine API, verifies `status == RUNNING` |
| Health Check: Airflow | Queries Airflow REST API `/api/v1/dags/{id}/dagRuns`, verifies latest run state |
| Health Check: HTTP | Probes common health endpoints (`:8080/health`, `:8000/health`, `/healthz`) |
| Verification Logic | Requires execution_output pass AND at least one type-specific check pass. Retries checks up to `VERIFICATION_RETRIES` (default 2) |
| Proof of Recovery | Structured text with `[PASS]`/`[FAIL]` per check, attached to resolution |

**Rule:** No incident may close without proof of recovery.

**Input:** `ExecutionResult` + `IncidentContext` | **Output:** `VerificationResult`

#### 6. ApprovalAgent

**File:** `approval_agent.py` | **Closes:** C1, C12, H10, H11

Evidence-first human-in-the-loop with multi-level routing.

| Capability | Detail |
|------------|--------|
| Evidence Payload | Bundles `IncidentContext`, `RiskAssessment`, `ChangeRecord`, `RemediationPlan`, and judge evaluation into a single `ApprovalPayload` |
| 4-Level Routing | `auto_approve` (low risk, high judge, high confidence, non-prod), `standard_approve` (medium risk), `senior_approve` (high risk or safety gate failure), `executive_deny` (critical risk, always requires human) |
| Auto-Approve Conditions | Risk score <= 0.3 AND judge quality >= 7.0 AND confidence >= 0.7 AND environment != production |
| Timeout Escalation | Standard -> Senior -> Executive -> Auto-escalate to manual on-call. Configurable via `APPROVAL_TIMEOUT_SECONDS` (default 3600s) |
| Override Support | Approver can modify the remediation plan via `override_plan` field |
| Notification | Slack integration with risk-level emoji indicators |

**Input:** All context + judge scores | **Output:** `ApprovalPayload`

#### 7. ObservabilityAgent

**File:** `observability_agent.py` | **Closes:** C3, C14, C15, H12, H13, M12

Always-on observability layer that hooks into every agent execution.

| Capability | Detail |
|------------|--------|
| Audit Persistence | PostgreSQL `audit_events` table with 20 columns, SHA256 checksum (includes details field), `ON CONFLICT DO NOTHING` for idempotency, indexed on `resource` and `timestamp` |
| Prometheus Metrics | Records `WORKFLOW_NODE_EXECUTIONS`, `WORKFLOW_NODE_DURATION`, `ERRORS` per agent per phase |
| OTEL Tracing | Creates spans via `opentelemetry.trace.get_tracer("fast-agents")`, propagates W3C traceparent |
| Kafka Trace Headers | Injects trace context into Kafka event headers via `opentelemetry.propagators.inject()` |
| Self-Health | Reports PostgreSQL connectivity, OTEL availability, and in-memory audit count |

**Input:** Agent execution hooks (called by Governor) | **Output:** Metrics, traces, audit records

#### 8. LearningAgent

**File:** `learning_agent.py` | **Closes:** C9, C10, M7, M8, M9

Post-resolution feedback loop and RAG weight optimization.

| Capability | Detail |
|------------|--------|
| Feedback Storage | PostgreSQL `feedback_records` table with outcome, confidence, risk, resolution time, and per-retriever weights |
| Neo4j Graph Update | Creates `Incident` and `Script` nodes, `FIXED_BY` relationship (with outcome, confidence, resolution time, judge score), `AFFECTS` relationship to `Service` nodes. Buffers in Redis on Neo4j failure |
| Weaviate Indexing | Indexes resolved incidents as `ResolvedIncident` class with description, resolution notes, category, service, and root cause for future RAG queries |
| RRF Weight Optimization | Computes optimal weights from feedback history: averages successful resolution weights per service/classification pair. Stores in `rrf_weights` table. Requires minimum 20 feedback records (`MIN_FEEDBACK_RECORDS`) before adjusting |
| Default Weights | `semantic=0.35, safety=0.25, context=0.20, graph=0.20` |

**Input:** `FeedbackRecord` + description + resolution notes | **Output:** Updated weights and learning metrics

#### 9. Governor

**File:** `governor.py` | **Closes:** Orchestration of all agents

The FAST orchestrator that runs the full incident lifecycle through 7 phases.

| Phase | Name | Agents | Mode |
|-------|------|--------|------|
| 1 | INTAKE | IncidentIntelligenceAgent | Sequential |
| 2 | PARALLEL ANALYSIS | RiskAgent + ChangeManagementAgent | Parallel (`asyncio.gather`) |
| 3 | PLAN + JUDGE | RAG search + LLM plan generation + LLM Judge | Sequential |
| 4 | APPROVAL GATE | ApprovalAgent | Pause/resume via Kafka |
| 5 | EXECUTION | ExecutionAgent | Sequential, guarded |
| 6 | VERIFICATION | VerificationAgent | Sequential |
| 7 | CLOSURE + LEARNING | ServiceNow close (Kafka) + LearningAgent | Sequential |

**Key behaviors:**
- Parallel dispatch in Phase 2 via `asyncio.gather(risk_task, chg_task, return_exceptions=True)` -- Risk and Change Management have no dependency on each other
- Stuck incident monitor runs as a background loop (`GOVERNOR_POLL_INTERVAL=30s`), escalates incidents stuck in `PENDING_APPROVAL` for longer than `STUCK_APPROVAL_MINUTES` (default 60)
- Approval resume via `handle_approval_event()` method, called by EventOrchestrator on Kafka `incident.approved` events
- All failures escalate to `ESCALATED` terminal state for human takeover
- ObservabilityAgent hooks wrap every `_call_agent()` invocation

---

### 24-State Distributed State Machine

**File:** `state_machine.py`

The `DistributedStateMachine` replaces the in-memory LangGraph checkpointer with a persistent, Redis-backed state machine that survives pod restarts.

#### State Lifecycle

```
NEW → DEDUP_CHECK → DUPLICATE (terminal)
                   → ANALYZING → RCA_COMPLETE → RISK_ASSESSED → CHG_CREATED
                     → PLAN_GENERATED → JUDGE_PASSED → PENDING_APPROVAL
                                      → JUDGE_FAILED → PLAN_GENERATED (retry)
                                                      → ESCALATED (terminal)
PENDING_APPROVAL → APPROVED → EXECUTING → EXECUTED → VERIFYING → VERIFIED → CLOSING → CLOSED (terminal)
                 → REJECTED → PLAN_GENERATED (retry)
                             → ESCALATED (terminal)
EXECUTING → EXECUTION_FAILED → ROLLING_BACK → ROLLED_BACK → PLAN_GENERATED (retry)
                                             → ESCALATED (terminal)
VERIFYING → VERIFICATION_FAILED → ROLLING_BACK → ...
                                 → ESCALATED (terminal)
```

#### 24 States

| State | Description | Terminal |
|-------|-------------|----------|
| `NEW` | Incident just received | No |
| `DEDUP_CHECK` | Checking for duplicates | No |
| `DUPLICATE` | Linked to parent incident | Yes |
| `ANALYZING` | RCA in progress | No |
| `RCA_COMPLETE` | Root cause identified | No |
| `RISK_ASSESSED` | Risk and blast radius computed | No |
| `CHG_CREATED` | Change record created | No |
| `PLAN_GENERATED` | Remediation plan ready | No |
| `JUDGE_PASSED` | Judge evaluation passed | No |
| `JUDGE_FAILED` | Judge evaluation failed (retry or escalate) | No |
| `PENDING_APPROVAL` | Waiting for human decision | No |
| `APPROVED` | Human approved | No |
| `REJECTED` | Human rejected (retry or escalate) | No |
| `EXECUTING` | Remediation running | No |
| `EXECUTED` | Remediation completed | No |
| `EXECUTION_FAILED` | Remediation failed | No |
| `VERIFYING` | Health checks in progress | No |
| `VERIFIED` | Recovery confirmed | No |
| `VERIFICATION_FAILED` | Health checks failed | No |
| `ROLLING_BACK` | Rollback in progress | No |
| `ROLLED_BACK` | Rollback completed (retry or escalate) | No |
| `CLOSING` | Ticket closure in progress | No |
| `CLOSED` | Incident fully resolved | Yes |
| `ESCALATED` | Human takeover required | Yes |

#### Storage Design

| Key Pattern | TTL | Purpose |
|-------------|-----|---------|
| `incident:state:{incident_id}` | 30 days | Current state, version counter, data payload |
| `incident:history:{incident_id}` | 90 days | Full state transition history for audit replay |

#### Concurrency Control

- **Optimistic locking** via version counter: every `transition()` reads the current version, increments it, and verifies the stored version has not changed before writing
- `StateTransitionError` raised on illegal transitions (enforced via `VALID_TRANSITIONS` directed graph) or version conflicts

---

### Security Layer

**File:** `agents/servicenow_agent/src/middleware/auth.py` | **Closes:** C1, C2

JWT + RBAC middleware that secures all `/api/` endpoints.

#### 4 Roles (Hierarchical)

| Role | Permissions | Inherits |
|------|-------------|----------|
| `viewer` | Read-only access to incidents and dashboards | -- |
| `operator` | Trigger workflows, view details | viewer |
| `approver` | Approve/reject remediation plans | viewer, operator |
| `admin` | Full access including config changes | viewer, operator, approver |

#### Implementation Details

| Feature | Detail |
|---------|--------|
| Token Format | JWT with HMAC-SHA256 (lightweight, no PyJWT dependency) |
| Token Fields | `sub` (user ID), `role`, `iat`, `exp` (configurable via `JWT_EXPIRY_SECONDS`, default 3600s) |
| Middleware | `RBACMiddleware` (Starlette `BaseHTTPMiddleware`) auto-authenticates all `/api/` requests |
| Exempt Paths | `/health`, `/metrics`, `/docs`, `/openapi.json`, `/redoc` |
| Endpoint Mapping | `GET /api/incidents` -> viewer, `POST /api/langgraph/approve/` -> approver, `DELETE /api/` -> admin |
| Development Bypass | `ENVIRONMENT=local` + `AUTH_BYPASS=true` grants admin access with `X-User-Id` header |
| Identity Extraction | Approver identity is always extracted from JWT claims, never from request body |

---

### Foundation Layer

The FAST architecture is built on 4 foundation modules.

#### contracts.py — Typed Agent Contracts

**11 Pydantic v2 contracts** that enforce typed input/output for every agent:

| Contract | Used By | Purpose |
|----------|---------|---------|
| `AgentEnvelope` | All agents | Tracing wrapper with `envelope_id`, `correlation_id`, `trace_id`, `span_id`, `idempotency_key`, version counter |
| `IncidentContext` | IncidentIntelligenceAgent | Enriched incident with RCA, dedup, correlation, SLA |
| `RiskAssessment` | RiskAgent | Risk score, blast radius, dependency chain, approval recommendation |
| `ChangeRecord` | ChangeManagementAgent | ServiceNow CHG record fields, schedule, linked incidents |
| `RemediationPlan` | Governor / ExecutionAgent | Script path, workflow name, parameters, rollback fields, confidence |
| `ExecutionResult` | ExecutionAgent | Success/failure, exit code, method, duration, rollback tracking |
| `VerificationResult` | VerificationAgent | Multi-check results, stabilization status, proof of recovery |
| `ApprovalPayload` | ApprovalAgent | Evidence-first bundle with routing, token, timeout, override support |
| `FeedbackRecord` | LearningAgent | Outcome, scores, resolution time, per-retriever weights |
| `AgentEvent` | Kafka integration | Standard Kafka event wrapper for inter-agent communication |
| `AgentHealth` | Governor / Registry | Agent status, heartbeat, error count, average latency |

**6 Enumerations:** `Severity` (P1-P4), `AgentPhase` (9 phases), `IncidentState` (24 states), `ChangeType` (standard/normal/emergency), `RiskCategory` (low/medium/high/critical), `ApprovalDecision` (approve/reject/override/escalate)

#### base_agent.py — Agent Foundation

Every FAST agent extends `BaseAgent`, which provides:

| Capability | Mechanism |
|------------|-----------|
| Idempotency | Redis key `idempotent:{agent_name}:{idempotency_key}` with 7-day TTL. Governor interprets `None` return as "already done" |
| Audit | Pre/post-audit logging on every `execute()` call with outcome tracking |
| Metrics | Latency, success/failure counters recorded per agent |
| Health | `health()` method returns `AgentHealth` with status (healthy if <5 errors, else degraded), error count, average latency |
| Error Handling | All exceptions wrapped in `AgentError` with agent name, incident ID, and retriable flag |
| Entry Point | `execute()` is the ONLY method called by Governor: idempotency check -> pre-audit -> `process()` -> mark processed -> post-audit -> metrics |

#### state_machine.py — Distributed State Machine

See the [24-State Distributed State Machine](#24-state-distributed-state-machine) section above.

#### registry.py — Agent Registry

Singleton `AgentRegistry` for agent lifecycle management:

- `register(agent)` — Registers an agent by its `NAME`
- `get(name)` — Lookup agent by name for Governor dispatch
- `health_report()` — Aggregates `AgentHealth` from all registered agents
- `reset()` — Clears registry (for testing)

---

### Infrastructure Hardening

#### Kafka Consumer Offset Fixes

- Consumer group `auto.offset.reset` set to `earliest` to prevent message loss on new consumer registration
- Manual offset commit after successful processing (no auto-commit) to prevent at-most-once delivery

#### Prometheus Alert Rules

10 alert rules defined for production monitoring:

| Rule | Condition | Severity |
|------|-----------|----------|
| High Error Rate | Error rate > 5% over 5 minutes | critical |
| Workflow Duration | P95 latency > 300s | warning |
| Kafka Consumer Lag | Consumer lag > 1000 messages | critical |
| Stuck Incidents | Incidents in PENDING_APPROVAL > 60 minutes | warning |
| Agent Health Degraded | Any agent reports `degraded` status | warning |
| Execution Failure Rate | Execution failures > 10% | critical |
| Rollback Triggered | Any rollback event | warning |
| SLA Breach Risk | SLA remaining < 30% of total | critical |
| Approval Timeout | Approval pending > timeout threshold | warning |
| Audit Persistence Failure | PostgreSQL audit write failures | critical |

#### Audit Persistence to PostgreSQL

- Audit events written to `audit_events` table with 20 columns (EU AI Act compliance fields: `ai_system_id`, `ai_decision_explanation`, `human_oversight_applied`, `confidence_score`, `data_subjects_affected`, `pii_involved`, `cross_border_transfer`)
- SHA256 checksum computed over full event data (including `details` field) -- closes gap M12
- `ON CONFLICT (event_id) DO NOTHING` for idempotent writes
- Indexed on `resource` and `timestamp` for audit query performance

---

### Gap Closure Summary

The FAST architecture closes **44 total gaps** identified in the production audit across three severity levels.

#### CRITICAL (17 gaps)

| ID | Gap | Resolution |
|----|-----|------------|
| C1 | No RBAC on approval endpoints | JWT + RBAC middleware (`middleware/auth.py`) |
| C2 | Approver identity from request body | Identity extracted from JWT claims |
| C3 | Audit logs in-memory only | PostgreSQL audit persistence (`observability_agent.py`) |
| C8 | Rollback plans never executed | Auto-rollback in `ExecutionAgent` on failure |
| C9 | `feedback_optimizer` module missing | `LearningAgent` with RRF weight optimization |
| C10 | `graph_scorer.record_successful_remediation()` not implemented | `LearningAgent` Neo4j FIXED_BY relationships |
| C11 | No incident deduplication | SHA256 fingerprint + Redis window in `IncidentIntelligenceAgent` |
| C12 | Approval decision unvalidated | `ApprovalAgent.validate_decision()` with role checks |
| C14 | No OTEL tracing on workflow nodes | `ObservabilityAgent` span creation per agent |
| C15 | No Kafka trace context propagation | W3C traceparent injection via `inject_kafka_headers()` |

#### HIGH (19 gaps)

| ID | Gap | Resolution |
|----|-----|------------|
| H1 | Non-GCP incidents trust execution completion | `VerificationAgent` multi-check for ALL incident types |
| H2 | No stabilization window | 60-second configurable stabilization in `VerificationAgent` |
| H3 | No root cause reassessment post-execution | Symptom reassessment in verification flow |
| H4 | No CHG record creation | `ChangeManagementAgent` ServiceNow API integration |
| H5 | No standard vs emergency classification | 3-way classification (standard/normal/emergency) |
| H6 | No SLA impact assessment | `RiskAgent` SLA remaining + breach risk calculation |
| H7 | Blast radius = `len(affected_resources)` only | Neo4j BFS depth 3 with service/user impact |
| H8 | No root cause analysis | 15-pattern RCA with LLM fallback |
| H9 | Confidence scores not used in routing | Confidence drives approval routing in `ApprovalAgent` |
| H10 | No multi-level approval for CRITICAL risk | 4-level routing (auto/standard/senior/executive) |
| H11 | No timeout enforcement on paused workflows | Timeout escalation chain in `ApprovalAgent` + Governor stuck monitor |
| H12 | Prometheus metrics defined but not recorded | `ObservabilityAgent` hooks record per-agent metrics |
| H13 | No alert rules in Prometheus | 10 production alert rules |
| H18 | GitHub Actions path has no retry logic | Exponential backoff (3 attempts) in `ExecutionAgent` |
| H19 | Script existence not validated before execution | Pre-validation in `ExecutionAgent._validate_prerequisites()` |

#### MEDIUM (8 gaps)

| ID | Gap | Resolution |
|----|-----|------------|
| M1 | Action risk weights hardcoded | 18 configurable weights in `RiskAgent` (env-overridable) |
| M2 | No escalation classification | Risk-based escalation with `ESCALATED` terminal state |
| M3 | GCP waits synchronously for 120s | `asyncio.to_thread()` for non-blocking GCP operations |
| M7 | Neo4j graph missing SIMILAR_TO, DEPENDS_ON | `LearningAgent` creates FIXED_BY, AFFECTS relationships |
| M8 | Indexed resolutions not queried by search | `LearningAgent` Weaviate `ResolvedIncident` indexing |
| M9 | No cold-start handling for new script types | Default RRF weights with minimum-sample threshold |
| M12 | Audit checksum excludes details | Full SHA256 checksum over all fields including details |
