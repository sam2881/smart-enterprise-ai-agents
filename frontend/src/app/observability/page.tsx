'use client'

import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Card, CardContent } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { api } from '@/lib/api'
import { wsClient } from '@/lib/websocket'
import type { KafkaEventItem, IncidentStateCount } from '@/types/observability'
import {
  Activity,
  Server,
  Database,
  Radio,
  Globe,
  Shield,
  Cpu,
  Bot,
  Clock,
  AlertTriangle,
  CheckCircle,
  XCircle,
  ExternalLink,
  BarChart3,
  Zap,
  Eye,
  RefreshCw,
  ArrowUpRight,
  Layers,
  GitBranch,
  Brain,
  Gauge,
  MessageSquare,
  Workflow,
} from 'lucide-react'

// =============================================================================
// Types
// =============================================================================

interface ServiceHealth {
  name: string
  port: number
  status: 'healthy' | 'degraded' | 'down'
  uptime?: string
}

interface AgentStatus {
  name: string
  status: 'active' | 'idle' | 'error'
  phase: string
  lastExecuted?: string
  errors: number
  avgLatency: string
}

interface StateMachineState {
  state: string
  count: number
  color: string
}

interface KafkaEvent {
  id: string
  topic: string
  event_type: string
  incident_id: string
  timestamp: string
}

interface LangfuseStats {
  totalTraces: number
  avgLatency: string
  tokenUsage: string
}

// =============================================================================
// Mock Data
// =============================================================================

const mockServices: ServiceHealth[] = [
  { name: 'Orchestrator API', port: 8000, status: 'healthy', uptime: '99.98%' },
  { name: 'Data Agent API', port: 8001, status: 'healthy', uptime: '99.95%' },
  { name: 'ServiceNow MCP', port: 8003, status: 'healthy', uptime: '99.90%' },
  { name: 'Jira MCP', port: 8004, status: 'healthy', uptime: '99.92%' },
  { name: 'RAG MCP', port: 8005, status: 'healthy', uptime: '99.88%' },
  { name: 'Airflow MCP', port: 8006, status: 'healthy', uptime: '99.85%' },
  { name: 'Kafka', port: 9092, status: 'healthy', uptime: '99.99%' },
  { name: 'PostgreSQL', port: 5432, status: 'healthy', uptime: '99.99%' },
  { name: 'Redis', port: 6379, status: 'healthy', uptime: '99.97%' },
  { name: 'Neo4j', port: 7474, status: 'healthy', uptime: '99.80%' },
  { name: 'Weaviate', port: 8080, status: 'healthy', uptime: '99.75%' },
  { name: 'Prometheus', port: 9090, status: 'healthy', uptime: '99.99%' },
]

const mockAgents: AgentStatus[] = [
  { name: 'Governor', status: 'active', phase: 'orchestration', errors: 0, avgLatency: '1.2s', lastExecuted: new Date(Date.now() - 30000).toISOString() },
  { name: 'IncidentIntelligence', status: 'active', phase: 'intake', errors: 0, avgLatency: '0.8s', lastExecuted: new Date(Date.now() - 45000).toISOString() },
  { name: 'RiskAgent', status: 'active', phase: 'analysis', errors: 0, avgLatency: '0.5s', lastExecuted: new Date(Date.now() - 60000).toISOString() },
  { name: 'ChangeManagement', status: 'idle', phase: 'analysis', errors: 0, avgLatency: '0.6s', lastExecuted: new Date(Date.now() - 120000).toISOString() },
  { name: 'ApprovalAgent', status: 'active', phase: 'approval', errors: 0, avgLatency: '0.3s', lastExecuted: new Date(Date.now() - 15000).toISOString() },
  { name: 'ExecutionAgent', status: 'idle', phase: 'execution', errors: 1, avgLatency: '2.5s', lastExecuted: new Date(Date.now() - 300000).toISOString() },
  { name: 'VerificationAgent', status: 'idle', phase: 'verification', errors: 0, avgLatency: '1.1s', lastExecuted: new Date(Date.now() - 600000).toISOString() },
  { name: 'ObservabilityAgent', status: 'active', phase: 'always-on', errors: 0, avgLatency: '0.1s', lastExecuted: new Date(Date.now() - 5000).toISOString() },
  { name: 'LearningAgent', status: 'idle', phase: 'post-closure', errors: 0, avgLatency: '0.4s', lastExecuted: new Date(Date.now() - 900000).toISOString() },
]

const mockStateMachine: StateMachineState[] = [
  { state: 'NEW', count: 3, color: 'bg-blue-500' },
  { state: 'ANALYZING', count: 2, color: 'bg-blue-400' },
  { state: 'RCA_COMPLETE', count: 1, color: 'bg-blue-300' },
  { state: 'RISK_ASSESSED', count: 1, color: 'bg-cyan-500' },
  { state: 'CHG_CREATED', count: 2, color: 'bg-cyan-400' },
  { state: 'PLAN_GENERATED', count: 1, color: 'bg-indigo-500' },
  { state: 'JUDGE_PASSED', count: 1, color: 'bg-indigo-400' },
  { state: 'PENDING_APPROVAL', count: 4, color: 'bg-yellow-500' },
  { state: 'APPROVED', count: 2, color: 'bg-blue-500' },
  { state: 'EXECUTING', count: 1, color: 'bg-blue-600' },
  { state: 'VERIFIED', count: 1, color: 'bg-blue-400' },
  { state: 'CLOSING', count: 1, color: 'bg-emerald-400' },
  { state: 'CLOSED', count: 12, color: 'bg-emerald-500' },
  { state: 'ESCALATED', count: 1, color: 'bg-red-500' },
]

const mockKafkaEvents: KafkaEvent[] = [
  { id: '1', topic: 'incident.created', event_type: 'IncidentCreated', incident_id: 'INC0010042', timestamp: new Date(Date.now() - 10000).toISOString() },
  { id: '2', topic: 'incident.enriched', event_type: 'ClassificationComplete', incident_id: 'INC0010041', timestamp: new Date(Date.now() - 25000).toISOString() },
  { id: '3', topic: 'incident.plan_generated', event_type: 'RemediationPlanReady', incident_id: 'INC0010040', timestamp: new Date(Date.now() - 40000).toISOString() },
  { id: '4', topic: 'incident.requires_approval', event_type: 'ApprovalRequired', incident_id: 'INC0010039', timestamp: new Date(Date.now() - 55000).toISOString() },
  { id: '5', topic: 'incident.approved', event_type: 'HumanApproved', incident_id: 'INC0010038', timestamp: new Date(Date.now() - 70000).toISOString() },
  { id: '6', topic: 'incident.closed', event_type: 'WorkflowComplete', incident_id: 'INC0010037', timestamp: new Date(Date.now() - 120000).toISOString() },
  { id: '7', topic: 'incident.created', event_type: 'IncidentCreated', incident_id: 'INC0010036', timestamp: new Date(Date.now() - 180000).toISOString() },
  { id: '8', topic: 'incident.close_execute', event_type: 'CloseCommand', incident_id: 'INC0010035', timestamp: new Date(Date.now() - 240000).toISOString() },
  { id: '9', topic: 'incident.received', event_type: 'LangGraphStarted', incident_id: 'INC0010034', timestamp: new Date(Date.now() - 300000).toISOString() },
  { id: '10', topic: 'incident.closed', event_type: 'WorkflowComplete', incident_id: 'INC0010033', timestamp: new Date(Date.now() - 360000).toISOString() },
]

const mockLangfuseStats: LangfuseStats = {
  totalTraces: 1847,
  avgLatency: '1.3s',
  tokenUsage: '2.4M',
}

// =============================================================================
// Helper Components
// =============================================================================

function StatusDot({ status }: { status: 'healthy' | 'degraded' | 'down' }) {
  const colorMap = {
    healthy: 'bg-emerald-500',
    degraded: 'bg-yellow-500',
    down: 'bg-red-500',
  }

  return (
    <div className="relative">
      <div className={`w-2.5 h-2.5 rounded-full ${colorMap[status]}`} />
      {status === 'healthy' && (
        <div className={`absolute inset-0 w-2.5 h-2.5 rounded-full ${colorMap[status]} animate-ping opacity-40`} />
      )}
    </div>
  )
}

function formatRelativeTime(isoString: string): string {
  const now = Date.now()
  const then = new Date(isoString).getTime()
  const diffMs = now - then
  const diffSec = Math.floor(diffMs / 1000)
  const diffMin = Math.floor(diffSec / 60)
  const diffHour = Math.floor(diffMin / 60)

  if (diffSec < 60) return `${diffSec}s ago`
  if (diffMin < 60) return `${diffMin}m ago`
  if (diffHour < 24) return `${diffHour}h ago`
  return new Date(isoString).toLocaleDateString()
}

function getTopicColor(topic: string): string {
  if (topic.includes('closed')) return 'text-emerald-400 bg-emerald-500/10 border-emerald-500/30'
  if (topic.includes('created') || topic.includes('received')) return 'text-blue-400 bg-blue-500/10 border-blue-500/30'
  if (topic.includes('error') || topic.includes('escalated')) return 'text-red-400 bg-red-500/10 border-red-500/30'
  if (topic.includes('approval') || topic.includes('approved')) return 'text-amber-400 bg-amber-500/10 border-amber-500/30'
  if (topic.includes('enriched') || topic.includes('plan')) return 'text-purple-400 bg-purple-500/10 border-purple-500/30'
  return 'text-gray-400 bg-gray-500/10 border-gray-500/30'
}

function getAgentIcon(name: string) {
  const iconMap: Record<string, React.ReactNode> = {
    Governor: <Shield className="w-4 h-4 text-white" />,
    IncidentIntelligence: <Brain className="w-4 h-4 text-white" />,
    RiskAgent: <AlertTriangle className="w-4 h-4 text-white" />,
    ChangeManagement: <GitBranch className="w-4 h-4 text-white" />,
    ApprovalAgent: <CheckCircle className="w-4 h-4 text-white" />,
    ExecutionAgent: <Zap className="w-4 h-4 text-white" />,
    VerificationAgent: <Eye className="w-4 h-4 text-white" />,
    ObservabilityAgent: <Gauge className="w-4 h-4 text-white" />,
    LearningAgent: <Bot className="w-4 h-4 text-white" />,
  }
  return iconMap[name] || <Cpu className="w-4 h-4 text-white" />
}

function getServiceIcon(name: string) {
  if (name.includes('API') || name.includes('Orchestrator')) return <Server className="w-4 h-4" />
  if (name.includes('MCP')) return <Globe className="w-4 h-4" />
  if (name.includes('Kafka')) return <Radio className="w-4 h-4" />
  if (name.includes('PostgreSQL') || name.includes('Neo4j') || name.includes('Redis') || name.includes('Weaviate')) return <Database className="w-4 h-4" />
  if (name.includes('Prometheus') || name.includes('Grafana')) return <BarChart3 className="w-4 h-4" />
  return <Cpu className="w-4 h-4" />
}

// =============================================================================
// Section: Service Health Grid
// =============================================================================

function ServiceHealthGrid({ services }: { services: ServiceHealth[] }) {
  const healthyCount = services.filter(s => s.status === 'healthy').length
  const totalCount = services.length

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-white flex items-center gap-3">
          <div className="p-2 rounded-lg bg-gradient-to-br from-emerald-500 to-green-600">
            <Shield className="w-5 h-5 text-white" />
          </div>
          Service Health
        </h2>
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-emerald-500/10 border border-emerald-500/30">
          <CheckCircle className="w-3.5 h-3.5 text-emerald-400" />
          <span className="text-xs font-semibold text-emerald-400">{healthyCount}/{totalCount} Operational</span>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
        {services.map((service) => (
          <div
            key={service.name}
            className="relative overflow-hidden rounded-xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-sm p-4 hover:border-white/20 transition-all duration-200 group"
          >
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2.5">
                <div className="p-1.5 rounded-lg bg-white/5 text-gray-400 group-hover:text-white transition-colors">
                  {getServiceIcon(service.name)}
                </div>
                <span className="text-sm font-medium text-gray-300">{service.name}</span>
              </div>
              <StatusDot status={service.status} />
            </div>
            <div className="flex items-center justify-between">
              <span className="text-xs text-gray-600">Port {service.port}</span>
              {service.uptime && (
                <span className="text-xs text-gray-500 font-mono">{service.uptime} uptime</span>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// =============================================================================
// Section: FAST Agent Status
// =============================================================================

function AgentStatusGrid({ agents }: { agents: AgentStatus[] }) {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-white flex items-center gap-3">
          <div className="p-2 rounded-lg bg-gradient-to-br from-purple-500 to-violet-600">
            <Bot className="w-5 h-5 text-white" />
          </div>
          FAST Agent Status
        </h2>
        <div className="flex items-center gap-3">
          <Badge variant="success" size="sm">
            {agents.filter(a => a.status === 'active').length} Active
          </Badge>
          <Badge variant="warning" size="sm">
            {agents.filter(a => a.status === 'idle').length} Idle
          </Badge>
          {agents.filter(a => a.status === 'error').length > 0 && (
            <Badge variant="error" size="sm">
              {agents.filter(a => a.status === 'error').length} Error
            </Badge>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {agents.map((agent) => {
          const statusColor = {
            active: { dot: 'bg-emerald-500', text: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/30' },
            idle: { dot: 'bg-amber-500', text: 'text-amber-400', bg: 'bg-amber-500/10', border: 'border-amber-500/30' },
            error: { dot: 'bg-red-500', text: 'text-red-400', bg: 'bg-red-500/10', border: 'border-red-500/30' },
          }[agent.status]

          const phaseGradient: Record<string, string> = {
            orchestration: 'from-violet-500 to-purple-600',
            intake: 'from-blue-500 to-cyan-600',
            analysis: 'from-cyan-500 to-teal-600',
            approval: 'from-amber-500 to-orange-600',
            execution: 'from-red-500 to-rose-600',
            verification: 'from-emerald-500 to-green-600',
            'always-on': 'from-indigo-500 to-blue-600',
            'post-closure': 'from-gray-500 to-slate-600',
          }

          return (
            <div
              key={agent.name}
              className="relative overflow-hidden rounded-xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-sm p-4 hover:border-white/20 transition-all duration-200"
            >
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center gap-3">
                  <div className={`p-2 rounded-lg bg-gradient-to-br ${phaseGradient[agent.phase] || 'from-gray-500 to-slate-600'} shadow-lg`}>
                    {getAgentIcon(agent.name)}
                  </div>
                  <div>
                    <h4 className="text-sm font-semibold text-white">{agent.name}</h4>
                    <span className="text-xs text-gray-500 capitalize">{agent.phase}</span>
                  </div>
                </div>
                <div className={`flex items-center gap-1.5 px-2 py-1 rounded-full ${statusColor.bg} border ${statusColor.border}`}>
                  <div className={`w-1.5 h-1.5 rounded-full ${statusColor.dot}`} />
                  <span className={`text-[10px] font-semibold ${statusColor.text} capitalize`}>{agent.status}</span>
                </div>
              </div>

              <div className="grid grid-cols-3 gap-2">
                <div className="rounded-lg bg-white/[0.03] border border-white/5 p-2 text-center">
                  <div className="text-xs font-bold text-white">{agent.avgLatency}</div>
                  <div className="text-[10px] text-gray-600">Avg Latency</div>
                </div>
                <div className="rounded-lg bg-white/[0.03] border border-white/5 p-2 text-center">
                  <div className={`text-xs font-bold ${agent.errors > 0 ? 'text-red-400' : 'text-white'}`}>{agent.errors}</div>
                  <div className="text-[10px] text-gray-600">Errors</div>
                </div>
                <div className="rounded-lg bg-white/[0.03] border border-white/5 p-2 text-center">
                  <div className="text-xs font-bold text-white">{agent.lastExecuted ? formatRelativeTime(agent.lastExecuted) : '-'}</div>
                  <div className="text-[10px] text-gray-600">Last Run</div>
                </div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// =============================================================================
// Section: State Machine Distribution
// =============================================================================

function StateMachineDistribution({ states }: { states: StateMachineState[] }) {
  const totalCount = states.reduce((sum, s) => sum + s.count, 0)

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-white flex items-center gap-3">
          <div className="p-2 rounded-lg bg-gradient-to-br from-blue-500 to-cyan-600">
            <Workflow className="w-5 h-5 text-white" />
          </div>
          State Machine Distribution
        </h2>
        <span className="text-sm text-gray-500">{totalCount} total incidents</span>
      </div>

      <div className="rounded-xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-sm p-6">
        {/* Visual Bar */}
        <div className="flex rounded-lg overflow-hidden h-8 mb-6">
          {states.filter(s => s.count > 0).map((state) => (
            <div
              key={state.state}
              className={`${state.color} relative group transition-all duration-200 hover:opacity-90`}
              style={{ width: `${(state.count / totalCount) * 100}%` }}
              title={`${state.state}: ${state.count}`}
            >
              <div className="absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity">
                <span className="text-[10px] font-bold text-white drop-shadow-lg">{state.count}</span>
              </div>
            </div>
          ))}
        </div>

        {/* Legend Grid */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-7 gap-2">
          {states.map((state) => (
            <div key={state.state} className="flex items-center gap-2 px-2 py-1.5 rounded-lg bg-white/[0.02] border border-white/5">
              <div className={`w-2.5 h-2.5 rounded-sm ${state.color} flex-shrink-0`} />
              <div className="min-w-0">
                <span className="text-[10px] text-gray-400 truncate block">{state.state}</span>
                <span className="text-xs font-bold text-white">{state.count}</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// =============================================================================
// Section: Kafka Event Stream
// =============================================================================

function KafkaEventStream({ events }: { events: KafkaEvent[] }) {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-white flex items-center gap-3">
          <div className="p-2 rounded-lg bg-gradient-to-br from-orange-500 to-amber-600">
            <Radio className="w-5 h-5 text-white" />
          </div>
          Kafka Event Stream
        </h2>
        <Badge variant="info" size="sm">Last 10 events</Badge>
      </div>

      <div className="rounded-xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-sm overflow-hidden">
        {/* Table Header */}
        <div className="grid grid-cols-12 gap-4 px-4 py-3 border-b border-white/5 bg-white/[0.02]">
          <div className="col-span-4 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Topic</div>
          <div className="col-span-3 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Event Type</div>
          <div className="col-span-3 text-[10px] font-semibold text-gray-500 uppercase tracking-wider">Incident ID</div>
          <div className="col-span-2 text-[10px] font-semibold text-gray-500 uppercase tracking-wider text-right">Time</div>
        </div>

        {/* Event Rows */}
        <div className="divide-y divide-white/5">
          {events.map((event) => {
            const topicStyle = getTopicColor(event.topic)
            return (
              <div
                key={event.id}
                className="grid grid-cols-12 gap-4 px-4 py-3 hover:bg-white/[0.02] transition-colors"
              >
                <div className="col-span-4">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-md text-[10px] font-semibold border ${topicStyle}`}>
                    {event.topic}
                  </span>
                </div>
                <div className="col-span-3">
                  <span className="text-xs text-gray-300 font-medium">{event.event_type}</span>
                </div>
                <div className="col-span-3">
                  <span className="text-xs text-blue-400 font-mono">{event.incident_id}</span>
                </div>
                <div className="col-span-2 text-right">
                  <span className="text-xs text-gray-500">{formatRelativeTime(event.timestamp)}</span>
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

// =============================================================================
// Section: Embedded Grafana
// =============================================================================

function GrafanaEmbed() {
  const [grafanaUrl, setGrafanaUrl] = useState('')

  useEffect(() => {
    if (typeof window !== 'undefined') {
      setGrafanaUrl(`http://${window.location.hostname}:3001/d/ai-agent-platform/ai-agent-platform?orgId=1&kiosk`)
    }
  }, [])

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-white flex items-center gap-3">
          <div className="p-2 rounded-lg bg-gradient-to-br from-orange-500 to-red-600">
            <BarChart3 className="w-5 h-5 text-white" />
          </div>
          Grafana Dashboards
        </h2>
        {grafanaUrl && (
          <a
            href={grafanaUrl.replace('&kiosk', '')}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-3 py-1.5 text-xs font-medium text-orange-400 hover:text-orange-300 bg-orange-500/10 border border-orange-500/30 rounded-lg hover:bg-orange-500/20 transition-all"
          >
            Open Grafana for full experience
            <ExternalLink className="w-3.5 h-3.5" />
          </a>
        )}
      </div>

      <div className="rounded-xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-sm overflow-hidden">
        {grafanaUrl ? (
          <iframe
            src={grafanaUrl}
            width="100%"
            height="600"
            className="border-0"
            title="Grafana Dashboard"
            loading="lazy"
          />
        ) : (
          <div className="flex items-center justify-center h-[600px] text-gray-500">
            <div className="text-center">
              <BarChart3 className="w-12 h-12 mx-auto mb-3 text-gray-600" />
              <p className="text-sm">Loading Grafana dashboard...</p>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// =============================================================================
// Section: Langfuse LLM Traces
// =============================================================================

function LangfusePanel({ stats }: { stats: LangfuseStats }) {
  const [langfuseUrl, setLangfuseUrl] = useState('')

  useEffect(() => {
    if (typeof window !== 'undefined') {
      setLangfuseUrl(`http://${window.location.hostname}:3002`)
    }
  }, [])

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-white flex items-center gap-3">
          <div className="p-2 rounded-lg bg-gradient-to-br from-pink-500 to-rose-600">
            <MessageSquare className="w-5 h-5 text-white" />
          </div>
          Langfuse LLM Traces
        </h2>
        {langfuseUrl && (
          <a
            href={langfuseUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-3 py-1.5 text-xs font-medium text-pink-400 hover:text-pink-300 bg-pink-500/10 border border-pink-500/30 rounded-lg hover:bg-pink-500/20 transition-all"
          >
            Open Langfuse Dashboard
            <ExternalLink className="w-3.5 h-3.5" />
          </a>
        )}
      </div>

      <div className="rounded-xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-sm p-6">
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
          <div className="rounded-xl bg-gradient-to-br from-pink-500/10 to-transparent border border-pink-500/20 p-4">
            <div className="flex items-center gap-2 mb-2">
              <Layers className="w-4 h-4 text-pink-400" />
              <span className="text-xs text-gray-500 font-medium">Total Traces</span>
            </div>
            <div className="text-2xl font-bold text-white">{stats.totalTraces.toLocaleString()}</div>
          </div>
          <div className="rounded-xl bg-gradient-to-br from-purple-500/10 to-transparent border border-purple-500/20 p-4">
            <div className="flex items-center gap-2 mb-2">
              <Clock className="w-4 h-4 text-purple-400" />
              <span className="text-xs text-gray-500 font-medium">Avg Latency</span>
            </div>
            <div className="text-2xl font-bold text-white">{stats.avgLatency}</div>
          </div>
          <div className="rounded-xl bg-gradient-to-br from-violet-500/10 to-transparent border border-violet-500/20 p-4">
            <div className="flex items-center gap-2 mb-2">
              <Zap className="w-4 h-4 text-violet-400" />
              <span className="text-xs text-gray-500 font-medium">Token Usage</span>
            </div>
            <div className="text-2xl font-bold text-white">{stats.tokenUsage}</div>
          </div>
        </div>

        <div className="rounded-lg bg-white/[0.02] border border-white/5 p-4">
          <p className="text-sm text-gray-400">
            Langfuse provides comprehensive LLM observability including trace analysis, prompt versioning,
            cost tracking, and evaluation metrics. All LangGraph agent calls, RAG queries, and LLM judge
            evaluations are traced automatically.
          </p>
        </div>
      </div>
    </div>
  )
}

// =============================================================================
// Main Observability Dashboard
// =============================================================================

// Color palette for mapping backend states to display colors
const STATE_COLOR_MAP: Record<string, string> = {
  NEW: 'bg-blue-500',
  ANALYZING: 'bg-blue-400',
  RCA_COMPLETE: 'bg-blue-300',
  RISK_ASSESSED: 'bg-cyan-500',
  CHG_CREATED: 'bg-cyan-400',
  PLAN_GENERATED: 'bg-indigo-500',
  JUDGE_PASSED: 'bg-indigo-400',
  PENDING_APPROVAL: 'bg-yellow-500',
  APPROVED: 'bg-blue-500',
  EXECUTING: 'bg-blue-600',
  VERIFIED: 'bg-blue-400',
  CLOSING: 'bg-emerald-400',
  CLOSED: 'bg-emerald-500',
  ESCALATED: 'bg-red-500',
}

function mapStateToDisplay(s: IncidentStateCount): StateMachineState {
  return {
    state: s.state,
    count: s.count,
    color: STATE_COLOR_MAP[s.state] ?? 'bg-gray-500',
  }
}

export default function ObservabilityPage() {
  const [refreshCount, setRefreshCount] = useState(0)

  // Services health — live check of all 12 services
  const { data: servicesData } = useQuery({
    queryKey: ['observability-services', refreshCount],
    queryFn: () => api.getObservabilityServices(),
    refetchInterval: 15000,
    staleTime: 10000,
  })

  // Incident state distribution — from PostgreSQL
  const { data: statesData } = useQuery({
    queryKey: ['incident-states', refreshCount],
    queryFn: () => api.getIncidentStates(),
    refetchInterval: 15000,
    staleTime: 10000,
  })

  // Platform metrics summary — from Prometheus
  const { data: metricsData } = useQuery({
    queryKey: ['metrics-summary', refreshCount],
    queryFn: () => api.getMetricsSummary(),
    refetchInterval: 30000,
    staleTime: 20000,
  })

  // Live Kafka events via Socket.IO (seeded from REST, updated in real-time)
  const [liveKafkaEvents, setLiveKafkaEvents] = useState<KafkaEvent[]>([])

  useEffect(() => {
    // Seed with recent events from the audit log
    api.getKafkaEvents(20).then(data => {
      if (data?.events) {
        const mapped: KafkaEvent[] = data.events.map((e: KafkaEventItem, i: number) => ({
          id: String(i + 1),
          topic: e.topic,
          event_type: e.event_type,
          incident_id: e.summary,
          timestamp: e.timestamp,
        }))
        setLiveKafkaEvents(mapped)
      }
    }).catch(() => {})

    // Subscribe to real-time events via Socket.IO
    const handleIncidentCreated = (data: any) => {
      const event: KafkaEvent = {
        id: Date.now().toString(),
        topic: 'incident.created',
        event_type: 'INCIDENT_CREATED',
        incident_id: data?.incident_id || '',
        timestamp: new Date().toISOString(),
      }
      setLiveKafkaEvents(prev => [event, ...prev].slice(0, 20))
    }

    const handleIncidentUpdated = (data: any) => {
      const event: KafkaEvent = {
        id: Date.now().toString(),
        topic: `incident.${data?.status?.toLowerCase() || 'updated'}`,
        event_type: 'INCIDENT_UPDATED',
        incident_id: data?.incident_id || '',
        timestamp: new Date().toISOString(),
      }
      setLiveKafkaEvents(prev => [event, ...prev].slice(0, 20))
    }

    const handleApprovalRequired = (data: any) => {
      const event: KafkaEvent = {
        id: Date.now().toString(),
        topic: 'incident.requires_approval',
        event_type: 'APPROVAL_REQUIRED',
        incident_id: data?.incident_id || 'unknown',
        timestamp: new Date().toISOString(),
      }
      setLiveKafkaEvents(prev => [event, ...prev].slice(0, 20))
    }

    try {
      wsClient.on('incident_created', handleIncidentCreated)
      wsClient.on('incident_updated', handleIncidentUpdated)
      wsClient.on('approval_required', handleApprovalRequired)
    } catch (e) {
      // Socket.IO might not be connected — silently ignore
    }

    return () => {
      try {
        wsClient.off('incident_created', handleIncidentCreated)
        wsClient.off('incident_updated', handleIncidentUpdated)
        wsClient.off('approval_required', handleApprovalRequired)
      } catch (e) {}
    }
  }, [])

  // Map live service data to local ServiceHealth shape
  const services: ServiceHealth[] = servicesData?.services
    ? servicesData.services.map(s => ({
        name: s.name,
        port: s.port,
        status: s.status,
        uptime: s.uptime_pct != null ? `${s.uptime_pct.toFixed(2)}%` : undefined,
      }))
    : mockServices

  const agents = mockAgents

  // Map backend state counts to display format (with colors)
  const stateMachineStates: StateMachineState[] = statesData?.states
    ? statesData.states.map(mapStateToDisplay)
    : mockStateMachine

  const kafkaEvents: KafkaEvent[] = liveKafkaEvents.length > 0 ? liveKafkaEvents : mockKafkaEvents

  // Build LangfuseStats from live metrics with mock fallback
  const langfuseStats: LangfuseStats = {
    totalTraces: metricsData?.llm_calls_total ?? mockLangfuseStats.totalTraces,
    avgLatency: metricsData?.llm_latency_p95_ms != null
      ? `${(metricsData.llm_latency_p95_ms / 1000).toFixed(1)}s`
      : mockLangfuseStats.avgLatency,
    tokenUsage: metricsData?.llm_tokens_total != null
      ? `${(metricsData.llm_tokens_total / 1000000).toFixed(1)}M`
      : mockLangfuseStats.tokenUsage,
  }

  // Quick links for external tools
  const quickLinks = [
    { name: 'Grafana', port: 3001, color: 'text-orange-400 bg-orange-500/10 border-orange-500/30 hover:bg-orange-500/20' },
    { name: 'Prometheus', port: 9090, color: 'text-red-400 bg-red-500/10 border-red-500/30 hover:bg-red-500/20' },
    { name: 'Langfuse', port: 3002, color: 'text-pink-400 bg-pink-500/10 border-pink-500/30 hover:bg-pink-500/20' },
    { name: 'Tempo', port: 3200, color: 'text-cyan-400 bg-cyan-500/10 border-cyan-500/30 hover:bg-cyan-500/20' },
  ]

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="sticky top-0 z-50 border-b border-white/5 bg-[#0a0f1a]/80 backdrop-blur-xl">
        <div className="px-8 py-5">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-5">
              <div className="relative">
                <div className="p-3 rounded-2xl bg-gradient-to-br from-emerald-500 via-teal-500 to-cyan-500 shadow-xl shadow-emerald-500/25">
                  <Activity className="w-7 h-7 text-white" />
                </div>
                <div className="absolute -bottom-1 -right-1 w-4 h-4 rounded-full bg-emerald-500 border-2 border-[#0a0f1a]" />
              </div>
              <div>
                <h1 className="text-2xl font-bold text-white tracking-tight">Platform Observability</h1>
                <p className="text-sm text-gray-500 mt-0.5">FAST Multi-Agent Architecture - Real-time Monitoring</p>
              </div>
            </div>

            <div className="flex items-center gap-3">
              {/* Quick Links */}
              <div className="hidden md:flex items-center gap-2">
                {quickLinks.map((link) => (
                  <a
                    key={link.name}
                    href={typeof window !== 'undefined' ? `http://${window.location.hostname}:${link.port}` : `http://localhost:${link.port}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border rounded-lg transition-all ${link.color}`}
                  >
                    {link.name}
                    <ArrowUpRight className="w-3 h-3" />
                  </a>
                ))}
              </div>

              {/* Live Indicator */}
              <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-emerald-500/10 border border-emerald-500/30">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
                </span>
                <span className="text-xs font-semibold text-emerald-400">Live</span>
              </div>

              <button
                onClick={() => setRefreshCount(c => c + 1)}
                className="flex items-center gap-2 px-4 py-2.5 text-sm text-gray-400 hover:text-white bg-white/5 hover:bg-white/10 rounded-xl border border-white/10 hover:border-white/20 transition-all duration-200"
              >
                <RefreshCw className="w-4 h-4" />
                Refresh
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="px-8 py-8 space-y-8">
        {/* Section 1: Service Health Grid */}
        <ServiceHealthGrid services={services} />

        {/* Section 2: FAST Agent Status */}
        <AgentStatusGrid agents={agents} />

        {/* Section 3: State Machine Distribution */}
        <StateMachineDistribution states={stateMachineStates} />

        {/* Section 4: Kafka Event Stream */}
        <KafkaEventStream events={kafkaEvents} />

        {/* Section 5: Embedded Grafana */}
        <GrafanaEmbed />

        {/* Section 6: Langfuse LLM Traces */}
        <LangfusePanel stats={langfuseStats} />

        {/* Footer */}
        <div className="mt-16 text-center">
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-white/5 border border-white/10">
            <Cpu className="w-4 h-4 text-emerald-400" />
            <span className="text-sm text-gray-500">
              AI Agent Platform v6.0 | Observability Hub | Auto-refresh: 15s
            </span>
          </div>
        </div>
      </main>
    </div>
  )
}
