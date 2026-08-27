# Vibe Coding Guide — AI-Assisted Development
**Version:** 1.0 | **Last Updated:** 2026-06-22 | **Platform:** Enterprise Agentic Platform

---

## What is Vibe Coding?

Vibe coding is AI-assisted development where you describe intent and the AI generates implementation. For a platform this complex (430+ files, 14 services, 70+ source types), effective AI collaboration requires structure: the AI needs to know what you're building, what constraints exist, and what patterns to follow.

This guide documents how to work with Claude Code (and future AI agents) on this codebase to maximize output quality while maintaining architectural integrity.

---

## Section 1: Before You Start Any Task

### 1.1 What the AI Always Reads First

When you start a new session, Claude Code reads `CLAUDE.md` automatically. This file contains:
- The system map (where things live)
- Critical rules (what's allowed vs forbidden)
- Key patterns (LangGraph, Kafka, FastAPI contracts)

**You do not need to re-explain these basics in every session.** Start from what you want done.

### 1.2 The Context Hierarchy

The AI builds context from these files in this order (most authoritative first):
1. `CLAUDE.md` — always loaded, absolute rules
2. `docs/project-context.md` — deep architecture context (read when asked)
3. `docs/spec.md` — full platform spec (authoritative for new features)
4. `docs/architecture.md` — event patterns and component responsibilities
5. `docs/architecture-review-2026.md` — gap analysis and future roadmap

**For complex new features**, explicitly tell the AI: "Read docs/spec.md section X before implementing."

### 1.3 Giving Good Task Descriptions

| Vague (avoid) | Precise (do this) |
|--------------|-------------------|
| "Add a new agent" | "Add a `BackfillAgent` node in `agents/data_agent/src/graphs/apex_workflow.py` between `validator` and `deployer`. It should detect if the source has historical data (>30 days) and generate a backfill DAG alongside the regular DAG. Use the same `AgentState` type." |
| "Fix the pipeline API" | "The `/api/v2/data-agent/pipelines` POST endpoint in `agents/data_agent/src/api/main.py` returns 422 when `source.file_config` is null. The issue is that null configs should be accepted when the source_type doesn't require it. Fix the Pydantic model to make file_config Optional." |
| "Add monitoring" | "The ProactiveMonitoringAgent in `agents/servicenow_agent/src/agents/proactive_monitoring_agent.py` currently polls every 60s. Add a METRIC_RULES entry for `weaviate_query_latency_p99` with threshold 500ms, checking the Prometheus metric `weaviate_query_latency_seconds{quantile='0.99'}`." |

---

## Section 2: Architecture Rules for AI-Assisted Changes

### 2.1 The Non-Negotiable Patterns

When the AI generates code for this platform, these patterns must ALWAYS be present:

**LangGraph nodes** — every node must:
```python
def my_node(state: AgentState) -> Dict[str, Any]:
    try:
        # logic
        return {"result": value}
    except Exception as e:
        return {"error_message": str(e), "error_agent": "my_node"}
```

**Kafka publishing** — all state transitions:
```python
await producer.publish_event(topic="thing.happened", event=payload, key=entity_id)
# NEVER: requests.post("http://internal-service/update")
```

**Pydantic models** — all new data contracts:
```python
class NewModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    field: str  # Required
    optional_field: Optional[str] = None
```

**Idempotency** — all Kafka consumers:
```python
key = f"{agent_name}:{entity_id}:{event_type}"
if redis.set(f"idempotent:{key}", "1", nx=True, ex=86400):
    # Process
else:
    return  # Already handled
```

### 2.2 What to Watch for in AI-Generated Code

After the AI writes a new node/agent/consumer, verify:

- [ ] Uses `StateGraph`, not `AgentExecutor` or raw LLM loops
- [ ] All exceptions propagate via `error_message` key in state
- [ ] All state transitions go through Kafka (not REST or direct function calls)
- [ ] New Pydantic models have `extra="forbid"` on `model_config`
- [ ] New Python packages have `__init__.py` in their directory
- [ ] New FastAPI endpoints return structured responses (not raw dicts)
- [ ] Kafka consumers have idempotency keys
- [ ] LLM calls use `claude-sonnet-4-6` (not hardcoded older model names)

---

## Section 3: Task Categories and Prompting Strategies

### 3.1 Adding a New LangGraph Node

**Template prompt:**
```
Add a new node `[name]_node` to [workflow file]. 

The node should [description of business logic].

Input state keys it reads: [list from AgentState]
Output state keys it sets: [list]
Position in graph: after [previous_node], before [next_node]

It should call the existing [service/utility] at [file path].
If it fails, return error_message=str(e), error_agent="[name]_node".

Also update the graph wiring: add the node, add the edge from [prev] to [next],
and add a conditional edge that routes to "error" if error_message is set.
```

### 3.2 Adding a New Source Type

```
Add source type `[prefix]_[name]` to the platform.

1. Add to `agents/data_agent/src/models/source.py` — new SourceType enum value
   and a new `[NameConfig](BaseModel)` with these fields: [list]
   
2. Add to the `UnifiedSourceConfig` union type in the same file

3. Add a Jinja2 template `agents/data_agent/src/templates/[prefix]_ingest_dag.j2`
   based on the pattern in `file_ingest_dag.j2` but with these differences: [list]

4. Add a form component `frontend/src/components/pipeline/[Name]SourceConfigForm.tsx`
   following the same pattern as `FileSourceConfigForm.tsx`

5. Wire the form in `frontend/src/components/pipeline/SourceConfigSection.tsx`
   inside the `if (sourceType.startsWith('[prefix]_'))` block

Don't add frontend routes — just the component and the model.
```

### 3.3 Adding a New Kafka Consumer

```
Add a new Kafka consumer `[Name]Consumer` in `agents/[system]/src/streaming/consumers/[name]_consumer.py`.

Topics to subscribe: [[list]]
Consumer group: [group-name]

For each topic, implement:
- `_handle_[topic_underscore](payload)` async method
- Log using structlog: `logger.bind(entity_id=...).info("event_name")`
- Add idempotency key: `[system]:[entity_id]:[event_type]`
- On error: log the exception, don't re-raise (let the consumer continue)

The consumer should follow the same pattern as DataPipelineIncidentBridge in
`agents/servicenow_agent/src/streaming/consumers/data_pipeline_incident_bridge.py`.
```

### 3.4 Adding a FastAPI Endpoint

```
Add endpoint `[METHOD] /api/v[n]/[path]` to `[api file]`.

Request model: [describe fields or say "use existing ModelName"]
Response model: [describe or say "returns existing ModelName"]

This endpoint should:
1. [step 1]
2. [step 2]
3. Publish to Kafka topic "[topic.name]" with key=[key]
4. Return {"status": "queued", "id": entity_id}

Do NOT run LangGraph from this endpoint — only publish to Kafka.
Follow the pattern of existing endpoints in the same file.
```

### 3.5 Adding a Frontend Component

```
Add a React component `[Name]` at `frontend/src/components/[dir]/[Name].tsx`.

It should:
- Accept props: [describe props]
- Fetch data using React Query: useQuery({queryKey: [...], queryFn: () => api.[method]()})
- Show a loading skeleton while fetching (use the existing Skeleton from ui/)
- Display [describe the UI]

The data comes from API endpoint `[endpoint]` which returns `[type from types/]`.
Import the type from `@/types/[file]`.

Follow the same pattern as `[existing similar component]`.
```

---

## Section 4: Common Pitfalls and Anti-Patterns

### 4.1 Things the AI Might Do Wrong

**Pitfall 1: Direct LangGraph invocation from FastAPI**
```python
# AI might generate this — WRONG
@app.post("/run")
async def run_workflow(req: Request):
    result = await app.ainvoke({"incident": req})  # NO
```
Correct it: "Publish to Kafka, return immediately. LangGraph is triggered by EventOrchestrator."

**Pitfall 2: Using `pipeline.ts` instead of `pipeline-canonical.ts`**
If the AI imports from `@/types/pipeline`, correct to `@/types/pipeline-canonical`.

**Pitfall 3: Creating a new settings.py**
The platform already has 3 (backend, data_agent, servicenow_agent). Don't add more. Use the existing `Settings` class from the relevant subsystem.

**Pitfall 4: Adding `requests` sync calls inside async functions**
All HTTP calls inside async context must use `httpx.AsyncClient`, not `requests`.

**Pitfall 5: Hardcoding LLM model names as strings**
```python
# WRONG
client.messages.create(model="claude-3-opus-20240229", ...)
# RIGHT — use the constant from settings
client.messages.create(model=settings.llm_model, ...)  # Default: claude-sonnet-4-6
```

**Pitfall 6: Adding a new `__init__.py` to scripts/ directories**
`scripts/`, `agents/data_agent/scripts/`, `dags/` are NOT Python packages. Don't add `__init__.py` there.

### 4.2 Review Checklist After AI-Generated Code

```
[ ] Does the code follow the established patterns in that file?
[ ] Are all external calls (HTTP, DB, Kafka) awaited with proper error handling?
[ ] Are Pydantic models used for all inputs/outputs (not raw dicts)?
[ ] Does new code have a __init__.py if it's a new package directory?
[ ] Does the TypeScript code use types from pipeline-canonical.ts?
[ ] Is there any inline business logic that should be in a Jinja2 template?
[ ] Does any FastAPI endpoint call LangGraph directly?
[ ] Are there any imports of deprecated files (pipeline.ts, old settings)?
```

---

## Section 5: Working with the LangGraph Workflows

### 5.1 Incident Management Workflow (12 nodes)

File: `backend/orchestrator/langgraph_workflow.py`

Node order: `ingest → parse → classify → swarm_rag → generate_plan → judge → control_plane → await_approval → execute → verify → close_ticket → feedback_loop`

**When adding a new node:**
- Add the node function
- Add `graph.add_node("new_node", new_node)`
- Add `graph.add_edge("previous", "new_node")` (or conditional edge)
- Add `graph.add_edge("new_node", "next")` (or conditional)
- Update `AgentState` TypedDict with any new state keys

**Never re-wire existing edges without understanding what they do.** Each edge is there for a reason (approval gate, error routing, SLA timing).

### 5.2 Data Engineering Workflow (APEX, 8 phases)

File: `agents/data_agent/src/graphs/apex_workflow.py`

Phase order: `normalize → resolve_pattern → load_metadata → generate_artifacts → validate → persist_metadata → await_approval → deploy`

**New phases go between existing ones.** Don't change the start/end unless you understand the full flow.

---

## Section 6: The Review Process

### 6.1 Self-Review Before Committing

After any AI-generated change:

1. **Read the diff.** The AI summarized what it did — read what it actually did.
2. **Run the type checker:** `cd frontend && npx tsc --noEmit`
3. **Run relevant tests:** `pytest tests/unit -v` or `cd agents/data_agent && pytest tests/ -v`
4. **Check for TODOs.** AI sometimes leaves `# TODO: implement` placeholders.
5. **Verify new imports resolve.** Especially for cross-package imports.

### 6.2 Architectural Review Triggers

Trigger a deeper review when:
- A new LangGraph graph is added (not just a node)
- A new Kafka topic is introduced (update `docs/architecture.md`)
- A new database table is added (DDL must go in `sql/ddl/apex/`)
- A new external service dependency is added (update docker-compose.yml)
- Any change to the approval gate logic

---

## Section 7: Efficient AI Session Patterns

### 7.1 Starting a New Session

Best opening messages:
```
# For a focused bug fix
"The connection test in `agents/data_agent/src/agents/connection_test_agent.py` 
times out after 5s for Oracle sources. The Oracle JDBC connection often needs 
10-15s on first connect. Increase the timeout to 15s and make it configurable 
via settings."

# For a new feature
"Read `docs/spec.md` section on BackfillAgent, then implement it following 
the patterns in `agents/data_agent/src/agents/planner_agent.py`."

# For architecture questions
"Without changing any code, analyze the RAG retrieval flow in 
`agents/servicenow_agent/src/rag/` and tell me where the bottleneck is 
likely to be under high load (1000 queries/hour)."
```

### 7.2 Avoiding Context Window Waste

- Don't ask the AI to read every file before every change — it tracks what it's read
- Give specific file paths: `backend/orchestrator/langgraph_workflow.py:line_456` not "the workflow file"
- For documentation questions, ask the AI to reference `CLAUDE.md` or `docs/spec.md` instead of explaining architecture again

### 7.3 Parallel Work

The AI can write to multiple files in a single response. Use this for:
- Model + migration + test (all together)
- Frontend type + API client method + component (all together)
- LangGraph node + state key + conditional edge (all together)

Combine related changes into one request rather than drip-feeding one file at a time.

---

## Section 8: Key Files Reference

| Task | Primary File |
|------|-------------|
| Add an incident workflow node | `backend/orchestrator/langgraph_workflow.py` |
| Add a data pipeline workflow phase | `agents/data_agent/src/graphs/apex_workflow.py` |
| Add a new source type model | `agents/data_agent/src/models/source.py` |
| Add a Jinja2 code-gen template | `agents/data_agent/src/templates/` |
| Add a Kafka consumer | `agents/servicenow_agent/src/streaming/consumers/` |
| Add a new metric to monitor | `agents/servicenow_agent/src/agents/proactive_monitoring_agent.py` |
| Add a frontend route | `frontend/src/app/[route]/page.tsx` |
| Add a TypeScript type | `frontend/src/types/pipeline-canonical.ts` |
| Add a frontend API method | `frontend/src/lib/api.ts` |
| Add a DAG utility | `agents/data_agent/src/dag_utilities/` |
| Add a Spark job phase | `agents/data_agent/src/spark_jobs/v2/` |
| Change PII masking rules | `agents/data_agent/src/security/pii_detection.py` |
| Change schema evolution policy | `agents/data_agent/src/quality/schema_evolution.py` |
| Change approval thresholds | `backend/orchestrator/langgraph_workflow.py` (ApprovalAgent node) |
| Add an observability metric | `backend/orchestrator/metrics.py` |

---

*Last updated: 2026-06-22 by Claude Code automated review. Update this guide when architecture patterns change.*
