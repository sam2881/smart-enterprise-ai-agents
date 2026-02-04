# End-to-End Testing Plan - Both Agent Systems

> **Last Updated**: 2026-01-24
> **Frontend Version**: v2.0 (Canonical Models)
> **Migration Status**: ✅ Complete

---

## Frontend Migration Summary

The frontend has been migrated to use **canonical pipeline models** with:
- ✅ **70+ source types** across 9 categories
- ✅ **UnifiedPipelineForm** with 3 input modes
- ✅ **TypeScript types** mirroring Pydantic models exactly
- ✅ **Type-specific source config forms** for each category
- ✅ **NL → Structured conversion** (never executes NL directly)

**Key Components Updated:**
- `UnifiedPipelineForm.tsx` - Main form orchestrator
- `SourceTypeSelector.tsx` - 9-category source picker
- `SourceConfigForms.tsx` - 6 type-specific forms
- `pipeline-canonical.ts` - 680 lines of canonical types

**Pages Migrated:**
- `/pipelines` - Now uses `UnifiedPipelineForm`
- `/jira/[id]` - Now uses `UnifiedPipelineForm`

---

## System 1: Incident Management Agent

### UI Pages:
1. **Incidents Page** (`/incidents`)
   - View all IT incidents from ServiceNow
   - Track incident status and progress
   - Filter and search incidents

2. **Approvals Page** (`/approvals`)
   - Review pending remediation plans
   - Approve/reject automated fixes
   - View approval history

3. **Workflows Page** (`/workflows`)
   - Monitor active LangGraph workflows
   - View workflow state transitions
   - Track 12-node processing pipeline

4. **Graph Visualization** (`/graph/[id]`)
   - Visual representation of workflow DAG
   - Node-by-node execution tracking
   - Real-time state updates

### E2E Test Flow:
```
1. Navigate to /incidents
2. Check for new incidents from ServiceNow MCP
3. Click on incident to view details
4. Monitor workflow progress through phases:
   - ingest → parse → classify → swarm_rag → generate_plan
   - judge → control_plane → await_approval
5. Navigate to /approvals
6. Review generated remediation plan
7. Approve/reject the plan
8. Monitor execution and verification
9. Check incident closure
```

## System 2: Data Engineering Agent

### UI Pages:
1. **Pipelines Page** (`/pipelines`)
   - View all data pipelines
   - Create new pipelines (3 input modes)
   - Monitor pipeline generation progress
   - Filter by status, environment

2. **Jira Integration Page** (`/jira/[id]`)
   - Create pipelines from Jira tickets
   - Link pipelines to Jira work items
   - Auto-populate pipeline metadata

### E2E Test Flow:

#### Mode 1: Structured UI (70+ Source Types)
```
1. Navigate to /pipelines
2. Click "Create Pipeline"
3. Fill in Pipeline Identity:
   - DAG ID: test_sales_pipeline
   - Domain: sales
   - Environment: dev
4. Select Source Type from categorized list:
   - Category: File-Based Sources
   - Type: file_csv
5. Configure file source:
   - GCS Path: gs://bucket/data.csv
   - Delimiter, encoding options
6. Define Schema:
   - Add columns with types
7. Configure Target:
   - Zone: gold
   - Dataset: sales_data
   - Table: daily_sales
8. Set Execution Policy:
   - Schedule: @daily
   - Processing Mode: batch
9. Submit and monitor:
   - planning → generating → validating → awaiting_approval → deploying → complete
10. Check generated artifacts in GitHub
```

#### Mode 2: Natural Language
```
1. Navigate to /pipelines  
2. Create Pipeline
3. Use NL Transform:
   - "Load daily sales CSV from GCS, clean nulls, calculate running total by customer"
4. Review structured metadata conversion
5. Verify PySpark code generation
6. Check confidence score (>80%)
7. Submit pipeline
```

#### Mode 3: DTSX Migration
```
1. Navigate to /pipelines
2. Create Pipeline
3. Select DTSX Migration source
4. Upload DTSX file to GCS
5. Parse SSIS package
6. Review extracted:
   - Data sources
   - Transformations
   - Destinations
7. Map connections
8. Submit migration
9. Verify SSIS → Airflow conversion
```

#### Jira Integration Test
```
1. Navigate to /jira/DATA-1234 (example ticket)
2. Auto-populated fields from Jira:
   - Assignee → created_by
   - Ticket ID → jira_ticket
3. Fill remaining pipeline config
4. Submit
5. Check pipeline linked to Jira ticket
```

## Pre-Test Checklist

### Backend Services Required:
- [ ] FastAPI backend running (port 8000)
- [ ] Data Agent service running
- [ ] PostgreSQL database running
- [ ] Kafka broker running (for event-driven arch)
- [ ] MCP servers running (ServiceNow, GitHub, GCS)

### Frontend:
- [ ] Next.js dev server running (port 3000)
- [ ] Environment variables configured
- [ ] API endpoints accessible

## Test Commands

### Start Backend Services:
```bash
# Incident Management Backend
cd backend
uvicorn app.main:app --reload --port 8000

# Data Agent
cd agents/data_agent
python -m src.api.main

# Kafka (if not running)
docker-compose up kafka zookeeper

# PostgreSQL (if not running)
docker-compose up postgres
```

### Start Frontend:
```bash
cd frontend
npm run dev
# Opens on http://localhost:3000
```

### Health Checks:
```bash
# Backend API
curl http://localhost:8000/health

# Data Agent API
curl http://localhost:8001/health

# Kafka
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Expected Results

### Incident Management:
- ✅ Incidents list loads from ServiceNow
- ✅ Workflow progresses through all 12 nodes
- ✅ Remediation plan generated with RAG scripts
- ✅ Human approval flow works
- ✅ Execution completes and ticket closes

### Data Engineering Agent:
- ✅ All 70+ source types available in categorized UI
- ✅ Source config forms render correctly
- ✅ Schema definition works
- ✅ NL transforms convert to structured metadata
- ✅ DTSX parsing extracts components
- ✅ Pipeline generation completes
- ✅ Artifacts generated and deployed
- ✅ Jira integration populates fields

## Known Issues to Watch For

1. **Module Resolution Errors** - TypeScript may show IDE errors but build should work
2. **Kafka Connection** - Ensure Kafka is running before starting backends
3. **Database Migrations** - Run Alembic migrations if schema changed
4. **MCP Permissions** - Ensure MCP servers have correct GCP credentials
5. **CORS Issues** - Check frontend/backend can communicate

## Success Criteria

✅ Both systems' UIs load without errors
✅ All navigation works
✅ Forms submit successfully
✅ Real-time updates via WebSocket/polling work
✅ Generated artifacts appear in correct locations
✅ No console errors in browser DevTools
✅ API responses are fast (<2s for queries)
