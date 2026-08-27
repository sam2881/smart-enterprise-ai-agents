# Frontend

Next.js 14 (App Router) + React Query + Tailwind. Unified UI for both Incident Management and Data Engineering Agent.

## Routes

| Route | Purpose |
|-------|---------|
| `/` | Dashboard — active incidents + pipeline health |
| `/incidents` | Incident list with live WebSocket updates |
| `/incidents/[id]` | Single incident + 12-node workflow state visualization |
| `/approvals` | Human approval queue (approve / reject / comment) |
| `/workflows` | LangGraph DAG visualization |
| `/pipelines` | Pipeline creation — 70+ sources, 3 input modes |
| `/jira/[id]` | Jira-integrated pipeline creation |
| `/catalog` | Data catalog |
| `/observability` | Live service health, Kafka stream, Grafana embed |

## Key Files

```
src/
  app/               ← Next.js App Router pages (one folder per route)
  components/
    pipeline/        ← Source config forms, target config, pipeline wizard
    incidents/       ← Incident cards, timeline, state machine view
    approvals/       ← Approval form, risk display
    layout/          ← Shell, sidebar, header
    ui/              ← shadcn/ui base components
    workflow/        ← LangGraph DAG visualization
  types/
    pipeline-canonical.ts  ← CANONICAL types (always use this)
    pipeline.ts            ← DEPRECATED — do not import
    observability.ts       ← Observability API types
  lib/
    api.ts           ← Axios API client (all API methods here)
    websocket.ts     ← Socket.IO client for real-time incident updates
    constants.ts     ← WS_URL, API base URLs
```

## API Proxy

`next.config.js` rewrites:
- `/api/v1/*` → `http://localhost:8000/api/v1/*` (backend)
- `/api/v2/*` → `http://localhost:8001/api/v2/*` (data agent)

## Type Rule

```typescript
// CORRECT
import { UnifiedPipelineInput, SourceType } from '@/types/pipeline-canonical'

// WRONG — pipeline.ts is deprecated
import { Pipeline } from '@/types/pipeline'
```

## Source Form Dispatch

```typescript
if (sourceType.startsWith('file_'))      return <FileSourceConfigForm />
if (sourceType.startsWith('database_'))  return <DatabaseSourceConfigForm />
if (sourceType.startsWith('streaming_')) return <StreamingSourceConfigForm />
if (sourceType.startsWith('api_'))       return <APISourceConfigForm />
if (sourceType.startsWith('legacy_'))    return <LegacySourceConfigForm />
if (sourceType.startsWith('nosql_'))     return <DatabaseSourceConfigForm />
if (sourceType.startsWith('logs_'))      return <LogsSourceConfigForm />
if (sourceType.startsWith('cloud_'))     return <FileSourceConfigForm />
if (sourceType.startsWith('cdc_'))       return <StreamingSourceConfigForm />
```

## Running

```bash
cd frontend
npm install
npm run dev          # http://localhost:3000
npx tsc --noEmit     # type check
npm run build        # production build
```
