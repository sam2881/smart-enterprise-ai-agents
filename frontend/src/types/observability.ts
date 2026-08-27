export type ServiceStatus = 'healthy' | 'degraded' | 'down';

export interface ServiceHealth {
  name: string;
  status: ServiceStatus;
  port: number;
  latency_ms: number | null;
  uptime_pct: number | null;
}

export interface IncidentStateCount {
  state: string;
  count: number;
  percentage: number;
}

export interface KafkaEventItem {
  topic: string;
  event_type: string;
  timestamp: string;
  summary: string;
}

export interface MetricsSummary {
  llm_calls_total: number | null;
  llm_latency_p95_ms: number | null;
  llm_tokens_total: number | null;
  active_incidents: number | null;
  pending_approvals: number | null;
  prometheus_available: boolean;
}

export interface ObservabilityServicesResponse {
  services: ServiceHealth[];
  timestamp: string;
}
