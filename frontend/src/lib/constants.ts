// Dynamic API URL - works from any hostname
// Uses same-origin requests through Next.js rewrites proxy
// This avoids CORS issues and firewall problems with external IPs
export const getApiBaseUrl = () => {
  // In browser: use empty string for same-origin requests (goes through Next.js proxy)
  // Next.js rewrites /api/* to http://orchestrator:8000/api/*
  if (typeof window !== 'undefined') {
    return ''  // Same-origin - /api/incidents goes through Next.js rewrite
  }
  // Server-side: use direct URL
  return process.env.NEXT_PUBLIC_API_URL || 'http://orchestrator:8000'
}

const getWsUrl = () => {
  if (process.env.NEXT_PUBLIC_WS_URL) return process.env.NEXT_PUBLIC_WS_URL
  if (typeof window === 'undefined') return 'ws://localhost:8000'
  if (window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
    return `ws://${window.location.hostname}:8000`
  }
  return 'ws://localhost:8000'
}

export const API_BASE_URL = getApiBaseUrl()
export const WS_URL = getWsUrl()

export const INCIDENT_PRIORITIES = ['P1', 'P2', 'P3', 'P4'] as const
export const INCIDENT_STATUSES = ['New', 'In Progress', 'Resolved', 'Closed'] as const

export const APPROVAL_TYPES = {
  ROUTING_OVERRIDE: 'routing_override',
  PLAN_APPROVAL: 'plan_approval',
} as const

export const RISK_LEVELS = ['Low', 'Medium', 'High', 'Critical'] as const

export const ROUTES = {
  HOME: '/',
  INCIDENTS: '/incidents',
  INCIDENT_DETAIL: (id: string) => `/incidents/${id}`,
  APPROVALS: '/approvals',
  APPROVAL_DETAIL: (id: string) => `/approvals/${id}`,
  AGENTS: '/agents',
  AGENT_DETAIL: (name: string) => `/agents/${name}`,
  EVENTS: '/events',
  SETTINGS: '/settings',
} as const

export const QUERY_KEYS = {
  INCIDENTS: 'incidents',
  INCIDENT: 'incident',
  APPROVALS: 'approvals',
  APPROVAL: 'approval',
  AGENTS: 'agents',
  AGENT: 'agent',
  AGENT_METRICS: 'agent-metrics',
  AGENT_LOGS: 'agent-logs',
  EVENTS: 'events',
  STATS: 'stats',
} as const

export const WEBSOCKET_EVENTS = {
  INCIDENT_CREATED: 'incident_created',
  INCIDENT_UPDATED: 'incident_updated',
  APPROVAL_REQUIRED: 'approval_required',
  APPROVAL_RESOLVED: 'approval_resolved',
  AGENT_STATUS_CHANGED: 'agent_status_changed',
  TICKET_CREATED: 'ticket_created',
  TASK_COMPLETED: 'task_completed',
} as const
