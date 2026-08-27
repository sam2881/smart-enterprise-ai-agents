# Changelog — frontend/

Next.js 14 + React Query + Tailwind UI for both Incident Management and Data Engineering Agent.

---

## [Unreleased] — 2026-06-22

### Changed
- `src/app/observability/page.tsx` — Replaced hardcoded mock data with live API feeds:
  - Service health grid: live health checks via `GET /api/v1/observability/services` (refetch every 15s)
  - Incident state distribution: live PostgreSQL counts via `GET /api/v1/observability/incidents/states`
  - Kafka event stream: seeded from `GET /api/v1/observability/kafka/events` + Socket.IO real-time updates
  - Metrics (traces, latency, tokens): live Prometheus data via `GET /api/v1/observability/metrics/summary`
  - Agent status panel: remains mocked (FAST agents not mapped to live data source; visible in Grafana)

### Added
- `src/types/observability.ts` — TypeScript types for observability API responses:
  - `ServiceHealth`, `ServiceStatus`, `IncidentStateCount`, `KafkaEventItem`, `MetricsSummary`
  - `ObservabilityServicesResponse`

### Fixed
- Grafana iframe URL — dashboard UID corrected to `ai-agent-platform` (was `platform-overview`)
  The iframe now loads the actual Grafana dashboard instead of showing a 404.

### Known Issues (Pre-existing)
- `Cannot find module 'socket.io-client'` TypeScript error in `src/lib/websocket.ts`
  Runtime works (npm run dev). Type error only on `tsc --noEmit`. Low priority.
  Fix: `npm install --save-dev @types/socket.io-client` or upgrade socket.io-client.
- `frontend/src/types/pipeline.ts` is deprecated — all new components must use `pipeline-canonical.ts`

---

## [1.0.0] — 2026-06-21

### Initial
- Next.js 14 App Router with TypeScript
- React Query for all API state management
- Tailwind CSS + shadcn/ui component library
- Routes: `/`, `/incidents`, `/incidents/[id]`, `/approvals`, `/workflows`, `/pipelines`, `/jira/[id]`, `/catalog`, `/observability`
- Pipeline creation form supporting 70+ source types via type-prefix dispatch:
  `file_` → FileSourceConfigForm, `database_` → DatabaseSourceConfigForm, etc.
- WebSocket client (`src/lib/websocket.ts`) for real-time incident updates
- API client (`src/lib/api.ts`) with full typed methods for both backend APIs
- Canonical TypeScript types (`src/types/pipeline-canonical.ts`) mirroring Pydantic models
- Workflow DAG visualization (`/workflows` route)
- Human approval UI (`/approvals` route) with approve/reject/comment
- LangGraph incident state machine visualization
