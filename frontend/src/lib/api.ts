import axios, { AxiosInstance } from 'axios'
import { API_BASE_URL } from './constants'
import type {
  TaskRequest,
  TaskResponse,
  Incident,
  IncidentPriority,
  IncidentStatus,
  CreateIncidentRequest,
  ApprovalRequest,
  ApproveRoutingRequest,
  ApprovePlanRequest,
  RejectPlanRequest,
  Agent,
  AgentMetrics,
  AgentLog,
} from '@/types'
import type {
  PipelineIntent,
  PipelineStatus,
  CreatePipelineResponse,
  ApprovalResponse,
  TemplatesResponse,
  PipelineListItem,
} from '@/types/pipeline'
import type {
  UnifiedPipelineInput,
  PipelineMetadata,
  ExecutionRecord,
} from '@/types/pipeline-canonical'
import type {
  ObservabilityServicesResponse,
  IncidentStateCount,
  KafkaEventItem,
  MetricsSummary,
} from '@/types/observability'

class ApiClient {
  private client: AxiosInstance

  constructor() {
    this.client = axios.create({
      baseURL: API_BASE_URL,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    })

    // Request interceptor
    this.client.interceptors.request.use(
      (config) => {
        // Add auth token if available
        const token = localStorage.getItem('auth_token')
        if (token) {
          config.headers.Authorization = `Bearer ${token}`
        }
        return config
      },
      (error) => Promise.reject(error)
    )

    // Response interceptor
    this.client.interceptors.response.use(
      (response) => response,
      (error) => {
        console.error('API Error:', error.response?.data || error.message)
        return Promise.reject(error)
      }
    )
  }

  // Helper to transform ServiceNow data to frontend format
  private transformIncident(inc: any): Incident {
    // Map ServiceNow status numbers to frontend enums
    const statusMap: Record<string, IncidentStatus> = {
      '1': 'New',
      '2': 'In Progress',
      '3': 'In Progress',
      '4': 'In Progress',
      '5': 'Resolved',
      '6': 'Resolved',
      '7': 'Closed',
    }

    // Map ServiceNow priority numbers to frontend format
    const priorityMap: Record<string, IncidentPriority> = {
      '1': 'P1',
      '2': 'P2',
      '3': 'P3',
      '4': 'P4',
      '5': 'P4',
    }

    // Handle assigned_to - ServiceNow returns it as object {link, value} or string
    let assignedTo: string | undefined
    if (inc.assigned_to) {
      if (typeof inc.assigned_to === 'object' && inc.assigned_to.value) {
        assignedTo = inc.assigned_to.value
      } else if (typeof inc.assigned_to === 'string') {
        assignedTo = inc.assigned_to
      }
    }

    return {
      incident_id: inc.incident_id || inc.number,
      short_description: inc.short_description || '',
      description: inc.description || '',
      priority: priorityMap[String(inc.priority)] || 'P3',
      status: statusMap[String(inc.status)] || 'New',
      category: inc.category || undefined,
      assigned_to: assignedTo,
      created_at: inc.created_at || inc.sys_created_on || new Date().toISOString(),
      updated_at: inc.updated_at || inc.sys_updated_on || undefined,
      affected_service: inc.affected_service || undefined,
    }
  }

  // Health check
  async healthCheck() {
    return this.client.get('/health')
  }

  // Tasks
  async createTask(data: TaskRequest): Promise<TaskResponse> {
    const response = await this.client.post('/task', data)
    return response.data
  }

  // Incidents
  async getIncidents(filters?: Record<string, any>): Promise<Incident[]> {
    const response = await this.client.get('/api/incidents', { params: filters })
    // Backend returns { total: number, incidents: Incident[] }
    const incidents = response.data.incidents || []
    return incidents.map((inc: any) => this.transformIncident(inc))
  }

  async getIncidentById(id: string): Promise<Incident> {
    // Fetch from ServiceNow via backend
    const response = await this.client.get('/api/incidents')
    const incidents = response.data.incidents || []
    // Search by incident_id (e.g., INC0010009), number, or id (sys_id)
    const incident = incidents.find((inc: any) =>
      inc.incident_id === id ||
      inc.number === id ||
      inc.id === id
    )

    if (!incident) {
      console.error(`Incident ${id} not found in`, incidents.map((i: any) => i.incident_id))
      throw new Error(`Incident ${id} not found`)
    }

    return this.transformIncident(incident)
  }

  async createIncident(data: CreateIncidentRequest): Promise<Incident> {
    const response = await this.client.post('/task', {
      description: `${data.short_description}\n\n${data.description}`,
      priority: data.priority,
      metadata: {
        category: data.category,
        affected_service: data.affected_service,
      },
    })
    return response.data
  }

  // Approvals - Using LangGraph workflow approvals
  async getPendingApprovals(): Promise<ApprovalRequest[]> {
    try {
      const response = await this.client.get('/api/langgraph/approvals')
      // Transform LangGraph approvals to ApprovalRequest format
      const approvals = response.data.approvals || []
      return approvals.map((a: any) => ({
        id: a.workflow_id || a.incident_id,
        incident_id: a.incident_id,
        workflow_id: a.workflow_id,
        status: 'pending',
        plan: a.plan,
        judge_score: a.judge_score,
        approval_decision: a.approval_decision,
        created_at: a.created_at,
        approval_token: a.approval_token,
      }))
    } catch (error) {
      console.error('Error fetching approvals:', error)
      return []
    }
  }

  async getApprovalById(id: string): Promise<ApprovalRequest> {
    const response = await this.client.get(`/api/langgraph/workflow/${id}`)
    return response.data
  }

  async approveIncident(incidentId: string, data: { approver: string; reason: string }) {
    const response = await this.client.post(`/api/langgraph/approve/${incidentId}`, data)
    return response.data
  }

  async rejectIncident(incidentId: string, data: { approver: string; reason: string }) {
    const response = await this.client.post(`/api/langgraph/reject/${incidentId}`, data)
    return response.data
  }

  async retryGitHubTrigger(incidentId: string) {
    const response = await this.client.post(`/api/langgraph/retry/${incidentId}`)
    return response.data
  }

  async overrideIncident(incidentId: string, data: {
    approver: string
    reason: string
    override_script: {
      script_id: string
      workflow_name: string
      action_type: string
      parameters?: Record<string, string>
    }
  }) {
    const response = await this.client.post(`/api/langgraph/override/${incidentId}`, data)
    return response.data
  }

  async getAvailableScripts() {
    const response = await this.client.get('/api/langgraph/scripts')
    return response.data
  }

  // Legacy HITL endpoints (kept for compatibility)
  async approveRouting(id: string, data: ApproveRoutingRequest) {
    const response = await this.client.post(`/api/hitl/approvals/${id}/approve-routing`, data)
    return response.data
  }

  async approvePlan(id: string, data: ApprovePlanRequest) {
    const response = await this.client.post(`/api/hitl/approvals/${id}/approve-plan`, data)
    return response.data
  }

  async rejectPlan(id: string, data: RejectPlanRequest) {
    const response = await this.client.post(`/api/hitl/approvals/${id}/reject-plan`, data)
    return response.data
  }

  // Agents
  async getAgents(): Promise<Agent[]> {
    try {
      // Get regular agents from backend
      const agentsResponse = await this.client.get('/api/agents')
      const agents = agentsResponse.data || []

      // Get MCP servers from MCP Gateway
      try {
        const mcpResponse = await fetch('http://localhost:8001/api/mcp/servers')
        if (mcpResponse.ok) {
          const mcpData = await mcpResponse.json()
          const mcpAgents = (mcpData.servers || []).map((server: any) => ({
            name: server.name,
            display_name: `${server.icon || '🔌'} ${server.display_name}`,
            status: server.status === 'active' ? 'active' : 'inactive',
            type: 'mcp',
            description: `MCP Server - ${server.tools?.length || 0} tools available`,
            tools: server.tools || [],
            last_active: server.last_call || null,
            metrics: {
              tasks_completed: 0,
              success_rate: server.status === 'active' ? 100 : 0,
              avg_response_time: 0
            }
          }))
          return [...agents, ...mcpAgents]
        }
      } catch (mcpError) {
        console.log('MCP Gateway not available')
      }

      return agents
    } catch (error) {
      console.error('Failed to fetch agents:', error)
      return []
    }
  }

  // MCP Servers
  async getMCPServers() {
    try {
      const response = await fetch('http://localhost:8001/api/mcp/servers')
      if (response.ok) {
        return await response.json()
      }
    } catch (error) {
      console.error('Failed to fetch MCP servers:', error)
    }
    return { servers: [] }
  }

  async callMCPTool(server: string, toolName: string, args: any) {
    const response = await fetch('http://localhost:8001/api/mcp/call', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ server, tool_name: toolName, arguments: args })
    })
    return response.json()
  }

  async getAgentMetrics(name: string): Promise<AgentMetrics> {
    const response = await this.client.get(`/api/agents/${name}/metrics`)
    return response.data
  }

  async getAgentLogs(name: string, limit: number = 100): Promise<AgentLog[]> {
    const response = await this.client.get(`/api/agents/${name}/logs`, {
      params: { limit },
    })
    return response.data
  }

  // Stats
  async getDashboardStats() {
    const response = await this.client.get('/api/stats')
    return response.data
  }

  async getSystemHealth() {
    const response = await this.client.get('/api/stats')
    return response.data
  }

  async getActivityData(hours: number = 24) {
    // Return mock data for now
    return []
  }

  // Jira Integration
  async getJiraTickets(filters?: Record<string, any>) {
    const response = await this.client.get('/api/jira/tickets', { params: filters })
    return response.data
  }

  async getJiraTicket(ticketId: string) {
    const response = await this.client.get(`/api/jira/tickets/${ticketId}`)
    return response.data
  }

  async getJiraStories() {
    const response = await this.client.get('/api/jira/stories')
    return response.data
  }

  async processJiraTicket(ticketId: string) {
    const response = await this.client.post(`/api/jira/tickets/${ticketId}/process`, {}, {
      timeout: 120000  // 2 minute timeout for code generation
    })
    return response.data
  }

  async createJiraTicket(data: {
    summary: string
    description: string
    issue_type?: string
    priority?: string
    labels?: string[]
    project_key?: string
  }) {
    const response = await this.client.post('/api/jira/tickets', data)
    return response.data
  }

  async pushIncidentToJira(data: {
    incident_id: string
    short_description: string
    description?: string
    priority?: string
    category?: string
    labels?: string[]
  }) {
    const response = await this.client.post('/api/jira/push-incident', data)
    return response.data
  }

  // GitHub Integration
  async getGitHubRepo() {
    const response = await this.client.get('/api/github/repos')
    return response.data
  }

  async getGitHubPRs() {
    const response = await this.client.get('/api/github/prs')
    return response.data
  }

  // Remediation API
  async getRemediationRunbooks() {
    const response = await this.client.get('/api/remediation/runbooks')
    return response.data
  }

  async analyzeForRemediation(incidentId: string, logs?: string) {
    const response = await this.client.post('/api/remediation/analyze', null, {
      params: { incident_id: incidentId, logs }
    })
    return response.data
  }

  async matchRunbooks(incidentId: string, context: any) {
    const response = await this.client.post('/api/remediation/match', null, {
      params: { incident_id: incidentId, context: JSON.stringify(context) }
    })
    return response.data
  }

  async runFullRemediation(incidentId: string, mode: string = 'dry_run', logs?: string) {
    const response = await this.client.post('/api/remediation/full', {
      incident_id: incidentId,
      mode,
      logs
    }, {
      timeout: 120000  // 2 minute timeout for full analysis
    })
    return response.data
  }

  // Runbook RAG Search API
  async searchRunbooks(query: string, service?: string, limit: number = 5) {
    const response = await this.client.post('/api/runbooks/search', null, {
      params: { query, service, limit }
    })
    return response.data
  }

  async indexRunbooks() {
    const response = await this.client.post('/api/runbooks/index')
    return response.data
  }

  // Sync runbooks from GitHub repository
  async syncRunbooksFromGitHub(params: {
    repo_owner?: string,
    repo_name?: string,
    branch?: string,
    runbooks_path?: string
  } = {}) {
    const response = await this.client.post('/api/runbooks/sync-from-github', params, {
      timeout: 60000  // 1 minute timeout for sync
    })
    return response.data
  }

  async getRunbookDetails(scriptId: string) {
    const response = await this.client.get(`/api/runbooks/${scriptId}`)
    return response.data
  }

  async executeRunbookDryRun(scriptId: string, incidentId?: string) {
    const response = await this.client.post(`/api/runbooks/${scriptId}/execute`, null, {
      params: { incident_id: incidentId }
    })
    return response.data
  }

  // Execute runbook with REAL effects (not dry-run)
  async executeRunbookReal(scriptId: string, params: {
    incident_id?: string,
    instance_name?: string,
    zone?: string,
    project?: string,
    namespace?: string,
    deployment?: string,
    target_host?: string
  } = {}) {
    const response = await this.client.post(`/api/runbooks/${scriptId}/execute-real`, params, {
      timeout: 300000  // 5 minute timeout for real execution
    })
    return response.data
  }

  // Save remediation result to RAG for learning
  async saveRemediationToRAG(incidentId: string, remediationData: any) {
    const response = await this.client.post('/api/remediation/save-to-rag', {
      incident_id: incidentId,
      ...remediationData
    })
    return response.data
  }

  // Get similar past remediations from RAG
  async getSimilarRemediations(incidentDescription: string, limit: number = 3) {
    const response = await this.client.post('/api/remediation/similar', {
      description: incidentDescription,
      limit
    })
    return response.data
  }

  // LLM as Judge - evaluate remediation success
  async llmJudgeEvaluate(data: {
    incident_id: string
    incident_description?: string
    script_executed?: string
    execution_output?: string
    validation_passed?: boolean | null
  }) {
    const response = await this.client.post('/api/remediation/llm-judge', data)
    return response.data
  }

  // Close ServiceNow incident
  async closeServiceNowIncident(incidentId: string, data: {
    resolution_code: string
    close_notes: string
    work_notes?: string
  }) {
    const response = await this.client.post(`/api/servicenow/incidents/${incidentId}/close`, data)
    return response.data
  }

  // ==========================================================================
  // Data Agent Pipeline API
  // ==========================================================================

  /**
   * Create a new data pipeline from intent configuration.
   * Triggers the LangGraph workflow for pipeline generation.
   */
  async createPipeline(intent: PipelineIntent): Promise<CreatePipelineResponse> {
    const response = await this.client.post('/api/v2/data-agent/pipelines', intent, {
      timeout: 60000, // 1 minute timeout for initial request
    })
    return response.data
  }

  /**
   * Get status of a specific pipeline request.
   */
  async getPipelineStatus(requestId: string): Promise<PipelineStatus> {
    const response = await this.client.get(`/api/v2/data-agent/pipelines/${requestId}`)
    return response.data
  }

  /**
   * List all active pipelines.
   */
  async getActivePipelines(): Promise<PipelineListItem[]> {
    const response = await this.client.get('/api/v2/data-agent/pipelines')
    return response.data
  }

  /**
   * Get pipelines associated with a specific Jira ticket.
   */
  async getPipelinesByTicket(ticketId: string): Promise<PipelineStatus[]> {
    const response = await this.client.get('/api/v2/data-agent/pipelines', {
      params: { jira_ticket: ticketId },
    })
    return response.data
  }

  /**
   * Approve a pipeline that's awaiting human approval.
   * Used for PROD deployments and schema upgrades.
   */
  async approvePipeline(requestId: string): Promise<ApprovalResponse> {
    const response = await this.client.post(
      `/api/v2/data-agent/pipelines/${requestId}/approve`,
      {},
      { timeout: 30000 }
    )
    return response.data
  }

  /**
   * Reject a pipeline that's awaiting human approval.
   */
  async rejectPipeline(requestId: string): Promise<ApprovalResponse> {
    const response = await this.client.post(
      `/api/v2/data-agent/pipelines/${requestId}/reject`,
      {},
      { timeout: 30000 }
    )
    return response.data
  }

  /**
   * Get available pipeline templates.
   */
  async getPipelineTemplates(): Promise<TemplatesResponse> {
    const response = await this.client.get('/api/v2/data-agent/templates')
    return response.data
  }

  /**
   * Get pipeline artifacts (generated code) for a completed pipeline.
   */
  async getPipelineArtifacts(requestId: string): Promise<{
    dag_code: string
    spark_jobs: Record<string, string>
    sql_scripts?: Record<string, string>
  }> {
    const response = await this.client.get(
      `/api/v2/data-agent/pipelines/${requestId}/artifacts`
    )
    return response.data
  }

  /**
   * Retry a failed pipeline with the same configuration.
   */
  async retryPipeline(requestId: string): Promise<CreatePipelineResponse> {
    const response = await this.client.post(
      `/api/v2/data-agent/pipelines/${requestId}/retry`,
      {},
      { timeout: 60000 }
    )
    return response.data
  }

  /**
   * Cancel a running pipeline.
   */
  async cancelPipeline(requestId: string): Promise<{ success: boolean; message: string }> {
    const response = await this.client.post(
      `/api/v2/data-agent/pipelines/${requestId}/cancel`
    )
    return response.data
  }

  // ==========================================================================
  // New Canonical Pipeline API (Unified Input Model)
  // ==========================================================================

  /**
   * Create a new pipeline using the unified input model.
   * Supports structured UI input, natural language, or DTSX migration.
   */
  async createPipelineUnified(input: UnifiedPipelineInput): Promise<CreatePipelineResponse> {
    const response = await this.client.post('/api/v2/data-agent/pipelines/unified', input, {
      timeout: 60000, // 1 minute timeout for initial request
    })
    return response.data
  }

  /**
   * Get pipeline metadata by DAG ID and environment.
   * Returns the canonical PipelineMetadata from PostgreSQL.
   */
  async getPipelineMetadata(dagId: string, environment: string = 'dev'): Promise<PipelineMetadata> {
    const response = await this.client.get(`/api/v2/data-agent/pipelines/metadata/${dagId}`, {
      params: { environment },
    })
    return response.data
  }

  /**
   * Get pipeline execution history.
   */
  async getPipelineExecutions(dagId: string, limit: number = 20): Promise<ExecutionRecord[]> {
    const response = await this.client.get(`/api/v2/data-agent/pipelines/${dagId}/executions`, {
      params: { limit },
    })
    return response.data
  }

  /**
   * Validate pipeline configuration without creating it.
   * Useful for pre-flight checks in the UI.
   */
  async validatePipeline(input: UnifiedPipelineInput): Promise<{
    valid: boolean
    errors: string[]
    warnings: string[]
  }> {
    const response = await this.client.post('/api/v2/data-agent/pipelines/validate', input)
    return response.data
  }

  /**
   * Get source type category information for UI display.
   */
  async getSourceCategories(): Promise<any> {
    const response = await this.client.get('/api/v2/data-agent/sources/categories')
    return response.data
  }

  /**
   * Parse a DTSX file and return extracted metadata.
   * Used for DTSX migration preview.
   */
  async parseDTSX(dtsxPath: string): Promise<{
    sources: any[]
    transforms: any[]
    destinations: any[]
  }> {
    const response = await this.client.post('/api/v2/data-agent/dtsx/parse', {
      dtsx_path: dtsxPath,
    })
    return response.data
  }

  /**
   * Parse an EBCDIC copybook and return field definitions.
   */
  async parseCopybook(copybookPath: string): Promise<{
    fields: any[]
    record_length: number
  }> {
    const response = await this.client.post('/api/v2/data-agent/copybook/parse', {
      copybook_path: copybookPath,
    })
    return response.data
  }

  /**
   * Convert natural language pipeline description to structured metadata.
   * Returns the normalized PipelineMetadata without creating the pipeline.
   */
  async convertNLToPipeline(naturalLanguage: string, createdBy: string): Promise<PipelineMetadata> {
    const response = await this.client.post('/api/v2/data-agent/nl/convert', {
      natural_language: naturalLanguage,
      created_by: createdBy,
    })
    return response.data
  }

  // ==========================================================================
  // Observability API
  // ==========================================================================

  /**
   * Get live health status of all platform services.
   */
  async getObservabilityServices(): Promise<ObservabilityServicesResponse> {
    const response = await this.client.get('/api/v1/observability/services')
    return response.data
  }

  /**
   * Get incident state distribution from PostgreSQL.
   */
  async getIncidentStates(): Promise<{ states: IncidentStateCount[] }> {
    const response = await this.client.get('/api/v1/observability/incidents/states')
    return response.data
  }

  /**
   * Get recent Kafka events from the audit log.
   */
  async getKafkaEvents(limit = 20): Promise<{ events: KafkaEventItem[] }> {
    const response = await this.client.get(`/api/v1/observability/kafka/events?limit=${limit}`)
    return response.data
  }

  /**
   * Get platform metrics summary from Prometheus.
   */
  async getMetricsSummary(): Promise<MetricsSummary> {
    const response = await this.client.get('/api/v1/observability/metrics/summary')
    return response.data
  }
}

export const api = new ApiClient()

// Query keys for React Query
export const QUERY_KEYS = {
  INCIDENTS: 'incidents',
  INCIDENT: 'incident',
  APPROVALS: 'approvals',
  APPROVAL: 'approval',
  AGENTS: 'agents',
  AGENT_METRICS: 'agent-metrics',
  AGENT_LOGS: 'agent-logs',
  DASHBOARD_STATS: 'dashboard-stats',
  JIRA_TICKETS: 'jira-tickets',
  JIRA_TICKET: 'jira-ticket',
  JIRA_STORIES: 'jira-stories',
  GITHUB_REPO: 'github-repo',
  GITHUB_PRS: 'github-prs',
  REMEDIATION_RUNBOOKS: 'remediation-runbooks',
  REMEDIATION: 'remediation',
  RUNBOOK_SEARCH: 'runbook-search',
  RUNBOOK_DETAILS: 'runbook-details',
  SIMILAR_REMEDIATIONS: 'similar-remediations',
  // Data Agent Pipeline keys
  PIPELINES: 'pipelines',
  PIPELINE_STATUS: 'pipeline-status',
  PIPELINE_ARTIFACTS: 'pipeline-artifacts',
  PIPELINE_TEMPLATES: 'pipeline-templates',
}
