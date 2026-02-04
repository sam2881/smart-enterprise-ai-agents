# Enterprise Agentic Platform - Claude Code Instructions

## 🎯 Architecture v6.0 - Event-Driven

> **Kafka is the system of record. All state transitions flow through Kafka.**

```
MCPs sense → Kafka remembers → Orchestrator routes → LangGraph reasons & acts → FastAPI governs
```

---

## 📚 Required Context Files

**READ THESE FILES BEFORE ANY IMPLEMENTATION:**

1. **`docs/ARCHITECTURE_V6_EVENT_DRIVEN.md`** - Event-driven architecture patterns, Kafka topics, event flow
2. **`docs/CLAUDE_CODE_MASTER_CONTEXT.md`** - Complete platform architecture, both systems, E2E testing
3. **`docs/CLAUDE_CODE_CONTEXT.md`** - Data agent implementation patterns, code templates
4. **`docs/ENTERPRISE_AGENTIC_DATA_PLATFORM_README.md`** - Full platform specification

---

## 🏗️ Two Systems Overview

### 1. Incident Management System (`backend/`)
- **Purpose**: Automated IT incident resolution from ServiceNow
- **Workflow**: 12-node LangGraph (ingest → parse → classify → swarm_rag → generate_plan → judge → control_plane → await_approval → execute → verify → close_ticket → feedback_loop)
- **Key Files**: `backend/orchestrator/langgraph_workflow.py`
- **Architecture**: Event-driven via Kafka, pause/resume for approvals

### 2. Data Engineering Agent (`agents/data_agent/`)
- **Purpose**: Automated data pipeline generation
- **Workflow**: 5-agent LangGraph (supervisor → planner → generator → validator → deployer)
- **Key Files**: `agents/data_agent/src/graphs/main_graph.py`
- **Source Types**: 70+ across 9 categories (File, Database, Streaming, API, Legacy, NoSQL, Logs, Cloud, Advanced)
- **Input Modes**: UI Structured, Natural Language, DTSX Migration

### 3. Frontend UI (`frontend/`)
- **Purpose**: Unified UI for both agent systems
- **Framework**: Next.js 14 with React Query and TailwindCSS
- **Architecture**: TypeScript types mirror backend Pydantic models exactly
- **Key Components**:
  - **UnifiedPipelineForm**: Main pipeline creation form with 3 input modes
  - **SourceTypeSelector**: Categorized picker for 70+ source types
  - **NLTransformInput**: Natural language → structured metadata converter
  - **DTSXMigrationForm**: SSIS package migration tool

---

## 🎨 Frontend Architecture (v2.0 - Canonical Models)

### Canonical Type System
**Pydantic (Backend) ↔ TypeScript (Frontend) - EXACT MIRROR**

```typescript
// frontend/src/types/pipeline-canonical.ts mirrors agents/data_agent/src/models/

export interface UnifiedPipelineInput {
  input_type: 'ui_structured' | 'natural_language' | 'dtsx_migration'
  created_by: string
  jira_ticket?: string

  // Mode 1: Structured UI (70+ source types)
  pipeline?: Partial<PipelineConfig>
  source?: Partial<SourceConfig>
  schema?: Partial<SchemaConfig>
  target?: Partial<TargetConfig>
  execution_policy?: Partial<ExecutionPolicy>

  // Mode 2: Natural Language
  natural_language?: string

  // Mode 3: DTSX Migration
  dtsx_path?: string
}
```

### 70+ Source Types (9 Categories)

| Category | Examples | Form Component |
|----------|----------|----------------|
| **A. File-Based** (14) | CSV, Parquet, Excel, EBCDIC, Fixed-Width | FileSourceConfigForm |
| **B. Database** (9) | Postgres, MySQL, Snowflake, Oracle, DB2 | DatabaseSourceConfigForm |
| **C. Streaming** (8) | Kafka, Pub/Sub, Kinesis, EventHub | StreamingSourceConfigForm |
| **D. API & SaaS** (12) | REST, GraphQL, Salesforce, SAP | APISourceConfigForm |
| **E. Legacy** (7) | DTSX, AS400, Mainframe, COBOL | DTSXSourceConfigForm |
| **F. NoSQL** (9) | MongoDB, Cassandra, DynamoDB | DatabaseSourceConfigForm |
| **G. Logs** (5) | Splunk, Datadog, CloudWatch | StreamingSourceConfigForm |
| **H. Cloud** (4) | S3, GCS, Azure Blob | FileSourceConfigForm |
| **I. Advanced** (6) | CDC, Delta Lake, Iceberg | EBCDICSourceConfigForm |

### 3 Input Modes

#### Mode 1: UI Structured (Preferred)
```
User fills form → Validates → Sends UnifiedPipelineInput → Backend generates artifacts
```
- Type-safe configuration
- Category-based source selection
- Schema builder with data types
- Target zone configuration (Landing → Bronze → Silver → Gold → Trusted)

#### Mode 2: Natural Language
```
User writes NL → LLM converts to structured → Preview → User approves → Executes structured config
```
**CRITICAL**: NL is NEVER executed directly - always converted to structured metadata first

Example: "Load daily sales CSV, clean nulls, calculate running total by customer"
→ Generates: `TransformConfig` with `transform_type: 'window'`, `partition_by: ['customer_id']`, etc.

#### Mode 3: DTSX Migration
```
User uploads DTSX → Parser extracts components → Maps to Airflow → Generates pipeline
```
- Parses SSIS package XML
- Extracts sources, transforms, destinations
- Maps OLEDB/ADO.NET → BigQuery
- Maps T-SQL → PySpark

### Medallion Architecture (Data Zones)

```
Landing  → Raw STRING columns (no schema enforcement)
   ↓
Bronze   → Schema enforcement, basic types
   ↓
Silver   → Data cleaning, validation, deduplication
   ↓
Gold     → Business logic, aggregations, joins
   ↓
Trusted  → Curated datasets for analytics
```

### Component Pattern

```typescript
// 1. Type-specific source config form
<FileSourceConfigForm
  config={source.file_config}
  onChange={(config) => setSource({...source, file_config: config})}
/>

// 2. Main form orchestrator
<UnifiedPipelineForm
  jiraTicket="DATA-1234"
  createdBy="user@company.com"
  onSubmit={async (input: UnifiedPipelineInput) => {
    await api.createPipelineUnified(input)
  }}
/>

// 3. Natural language transform
<NLTransformInput
  zone="gold"
  schema={columns}
  onTransformAdd={(transform: TransformConfig) => {
    // Transform is structured metadata, NOT raw NL
  }}
/>
```

---

## 🚨 Critical Rules (NON-NEGOTIABLE)

### Backend Rules
| DO ✅ | NEVER ❌ |
|-------|----------|
| LangGraph StateGraph | ReAct pattern |
| Explicit Pydantic/TypedDict state | Implicit LLM memory |
| Fail fast with error state | Auto-fix errors |
| Validated JSON from UI | Parse free-text from Jira |
| Human approval for PROD | Auto-deploy to PROD |
| Jinja2 templates | Hard-coded business logic |

### Frontend Rules
| DO ✅ | NEVER ❌ |
|-------|----------|
| Mirror Pydantic types in TypeScript EXACTLY | Create divergent frontend types |
| Use canonical types (`pipeline-canonical.ts`) | Use legacy types (`pipeline-enhanced.ts`) |
| Convert NL to structured metadata | Execute natural language directly |
| Type-specific source config forms | Generic "one size fits all" forms |
| UnifiedPipelineInput for all pipeline creation | Multiple inconsistent input types |
| React Query for API state | Manual fetch with useState |
| Category-based source selection | Flat 70+ item dropdown |

### Natural Language Processing Rules
| DO ✅ | NEVER ❌ |
|-------|----------|
| NL → Structured metadata → Execute | NL → Execute directly |
| Show confidence score (>80% required) | Blindly execute LLM output |
| Preview generated code before execution | Auto-execute without review |
| Store NL for reference only | Use NL as executable config |

---

## 📂 Project Structure

```
AI_AGENT_APP/
├── backend/                    # Incident Management System
│   ├── orchestrator/          # LangGraph workflow (12 nodes)
│   ├── rag/                   # Swarm RAG (4 agents)
│   ├── mcp/                   # MCP servers
│   └── streaming/             # Kafka consumers
├── agents/
│   └── data_agent/            # Data Pipeline Agent
│       ├── src/agents/        # 5 LangGraph agents
│       ├── src/graphs/        # StateGraph workflow
│       ├── src/models/        # Pydantic canonical models
│       │   ├── canonical.py   # UnifiedPipelineInput (3 modes)
│       │   ├── source.py      # 70+ SourceType across 9 categories
│       │   ├── transformation.py  # Transform types & configs
│       │   ├── target.py      # Target zones & write modes
│       │   ├── execution.py   # Execution policies
│       │   └── quality.py     # Data quality rules
│       ├── src/templates/     # Jinja2 (DAG, Spark, SQL)
│       └── tests/
├── frontend/                   # Next.js 14 UI (React Query + TailwindCSS)
│   ├── src/app/
│   │   ├── incidents/         # Incident Management UI
│   │   ├── approvals/         # Approval workflow UI
│   │   ├── workflows/         # LangGraph visualization
│   │   ├── pipelines/         # Data Pipeline UI (70+ sources)
│   │   └── jira/[id]/         # Jira integration
│   ├── src/components/
│   │   └── pipeline/
│   │       ├── UnifiedPipelineForm.tsx    # Main form (3 input modes)
│   │       ├── SourceTypeSelector.tsx     # 9-category source picker
│   │       ├── SourceConfigForms.tsx      # 6 type-specific forms
│   │       ├── NLTransformInput.tsx       # NL → structured conversion
│   │       └── DTSXMigrationForm.tsx      # SSIS migration
│   └── src/types/
│       ├── pipeline-canonical.ts  # TypeScript mirror of Pydantic models
│       └── pipeline.ts            # Legacy types (being phased out)
└── docs/                       # Context files
```

---

## 🧪 Testing & Validation

### Backend Testing
```bash
# Full E2E validation
python scripts/e2e_validator.py --all

# Health checks only
python scripts/e2e_validator.py --health

# Unit tests
pytest tests/unit -v

# Data agent tests
cd agents/data_agent && pytest tests/ -v
```

### Frontend E2E Testing
```bash
# Run comprehensive UI test script
./test_ui_e2e.sh

# Includes:
# - Service health checks (Frontend, Backend API, Data Agent API)
# - API endpoint validation
# - TypeScript compilation check
# - Interactive testing instructions
# - Quick access links
```

### Manual UI Testing

#### System 1: Incident Management
- **http://localhost:3000/incidents** - View IT incidents from ServiceNow
- **http://localhost:3000/approvals** - Approve/reject remediation plans
- **http://localhost:3000/workflows** - Monitor LangGraph execution
- **http://localhost:3000/graph/[id]** - Visualize workflow DAG

#### System 2: Data Engineering Agent
- **http://localhost:3000/pipelines** - Create/view pipelines (70+ sources)
- **http://localhost:3000/jira/[id]** - Jira-integrated pipeline creation

**Test Cases:**
1. **Structured UI Mode**: Select source type → Configure → Define schema → Set target → Submit
2. **Natural Language Mode**: Enter NL description → Generate structured config → Review → Submit
3. **DTSX Migration Mode**: Upload DTSX → Parse → Map connections → Submit

### API Documentation
- **Backend API**: http://localhost:8000/docs (FastAPI Swagger)
- **Data Agent API**: http://localhost:8001/docs (FastAPI Swagger)

### Test Resources
- **E2E_TEST_PLAN.md** - Comprehensive testing guide for both systems
- **test_ui_e2e.sh** - Automated health checks and test script
- **MIGRATION_COMPLETE.md** - Frontend migration summary and validation

---

## 🔧 Quick Reference

### Backend: LangGraph Node Pattern
```python
def node_name(state: AgentState) -> Dict[str, Any]:
    try:
        # Logic here
        return {"key": "partial_state_update"}
    except Exception as e:
        return {"error_message": str(e), "error_agent": "node_name"}
```

### Backend: Conditional Edge Pattern
```python
def should_continue(state: AgentState) -> Literal["next", "error"]:
    return "error" if state.get("error_message") else "next"
```

### Frontend: UnifiedPipelineInput Pattern
```typescript
// Creating a pipeline from UI
const input: UnifiedPipelineInput = {
  input_type: 'ui_structured',
  created_by: 'user@company.com',
  jira_ticket: 'DATA-1234',
  pipeline: {
    dag_id: 'sales_daily_pipeline',
    domain: 'sales',
    environment: 'dev'
  },
  source: {
    source_type: 'file_csv',
    file_config: {
      gcs_path: 'gs://bucket/data.csv',
      delimiter: ',',
      header: true
    }
  },
  schema: {
    columns: [
      { name: 'customer_id', type: 'string', nullable: false },
      { name: 'amount', type: 'decimal', nullable: false }
    ]
  },
  target: {
    target_zone: 'gold',
    bq_dataset: 'sales_data',
    bq_table: 'daily_sales',
    write_mode: 'append'
  },
  execution_policy: {
    schedule_interval: '@daily',
    processing_mode: 'batch',
    retry_count: 2
  }
}

await api.createPipelineUnified(input)
```

### Frontend: Source Type Selection Pattern
```typescript
// Dynamic form rendering based on source type
const renderSourceConfigForm = () => {
  const sourceTypeStr = selectedSourceType?.toString()

  if (sourceTypeStr?.startsWith('file_')) {
    return <FileSourceConfigForm config={source.file_config} onChange={...} />
  }
  if (sourceTypeStr?.startsWith('database_')) {
    return <DatabaseSourceConfigForm config={source.database_config} onChange={...} />
  }
  if (sourceTypeStr?.startsWith('streaming_')) {
    return <StreamingSourceConfigForm config={source.streaming_config} onChange={...} />
  }
  // ... etc for all 9 categories
}
```

### Frontend: NL → Structured Conversion Pattern
```typescript
// Convert natural language to structured metadata
const generateFromNL = async () => {
  // Send NL to backend
  const response = await fetch('/api/v2/data-agent/nl/transform', {
    method: 'POST',
    body: JSON.stringify({
      description: nlDescription,
      schema: columns,
      zone: 'gold'
    })
  })

  const result = await response.json()

  // Create STRUCTURED transform (NOT raw NL)
  const transform: TransformConfig = {
    transform_type: result.transform_type,  // e.g., 'window', 'aggregate'
    config: result.config,                  // Structured config object
    nl_description: nlDescription,          // Stored for reference ONLY
    generated_pyspark: result.pyspark_code,
    is_active: true
  }

  onTransformAdd(transform)  // Adds structured config to pipeline
}
```

---

## 📡 Event-Driven Architecture

### Kafka Topics (System of Record)
| Topic | Purpose |
|-------|---------|
| `incident.created` | MCP detected new incident |
| `incident.received` | LangGraph started processing |
| `incident.enriched` | Classification complete |
| `incident.plan_generated` | Remediation plan ready |
| `incident.requires_approval` | Pending human approval |
| `incident.approved` | Human approved (from FastAPI) |
| `incident.close_execute` | Command to MCP to close |
| `incident.closed` | Workflow complete |

### Component Responsibilities
| Component | Role |
|-----------|------|
| **MCP Servers** | Poll external systems, publish events to Kafka |
| **EventOrchestrator** | Consume events, route to LangGraph workflows |
| **LangGraph** | Process events, publish state transitions, pause for approvals |
| **FastAPI** | Control plane only - serve UI, publish approval events |

### Protocol Usage
| Protocol | When to Use |
|----------|-------------|
| **Kafka** | All state transitions (SYSTEM OF RECORD) |
| **MCP** | Agent-to-tool invocation (RAG, LLM, GCS) |
| **REST** | GitHub Actions, external APIs |

---

## ✅ Pre-Commit Checklist

### Backend
- [ ] Uses LangGraph StateGraph (not ReAct)
- [ ] State is explicit Pydantic/TypedDict
- [ ] Error handling returns proper state
- [ ] Audit logging present
- [ ] Tests written
- [ ] PROD requires human approval

### Frontend
- [ ] TypeScript types mirror Pydantic models exactly
- [ ] Uses canonical types (`pipeline-canonical.ts`)
- [ ] NL converts to structured metadata (never executed directly)
- [ ] Type-specific forms for different source categories
- [ ] React Query for API state management
- [ ] No console errors in browser DevTools
- [ ] Works with all 70+ source types
- [ ] Jira integration auto-populates fields

---

## 📝 Key Principles

### Platform-Wide
- Platform is a **COMPILER**, not ETL tool
- Business logic lives in **metadata**, not code
- Generated artifacts are **deterministic**
- Memory is **explicit and versioned** (PostgreSQL + Git)
- Human approval is **mandatory** for production

### Backend Principles
- **Kafka is the system of record** - All state transitions flow through Kafka
- **LangGraph for workflows** - Explicit state machines, no implicit memory
- **Pydantic for contracts** - Type-safe data models, validated at runtime
- **Fail fast** - Return error state immediately, don't auto-fix

### Frontend Principles
- **TypeScript mirrors Pydantic** - Frontend types are exact copies of backend models
- **Canonical over legacy** - Use `pipeline-canonical.ts`, not `pipeline-enhanced.ts`
- **NL is metadata, not code** - Natural language ALWAYS converts to structured config first
- **Category-based UI** - Group 70+ sources into 9 logical categories
- **Type-specific forms** - Different configuration UIs for different source types
- **Three input modes** - Support UI Structured, Natural Language, and DTSX Migration

---

## 📖 Documentation Files

| File | Purpose |
|------|---------|
| **CLAUDE.md** | This file - Quick reference for Claude Code |
| **MIGRATION_COMPLETE.md** | Frontend v2.0 migration summary |
| **E2E_TEST_PLAN.md** | Comprehensive testing guide |
| **docs/ARCHITECTURE_V6_EVENT_DRIVEN.md** | Event-driven architecture details |
| **docs/CLAUDE_CODE_MASTER_CONTEXT.md** | Complete platform architecture |
| **docs/CLAUDE_CODE_CONTEXT.md** | Data agent patterns |
| **docs/ENTERPRISE_AGENTIC_DATA_PLATFORM_README.md** | Full platform spec |

---

**For complete details, see `docs/CLAUDE_CODE_MASTER_CONTEXT.md` and `MIGRATION_COMPLETE.md`**