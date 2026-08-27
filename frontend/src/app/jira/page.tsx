'use client'

/**
 * Jira Tickets List Page
 *
 * Shows all Jira tickets with pipeline creation capabilities.
 * Pipelines can ONLY be created from within a Jira ticket context.
 */

import { useState, useMemo, useCallback } from 'react'
import Link from 'next/link'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { PageLayout } from '@/components/layout/PageLayout'
import { Card, CardContent } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { Button } from '@/components/ui/Button'
import { cn } from '@/lib/utils'
import { api, QUERY_KEYS } from '@/lib/api'
import { getApiBaseUrl } from '@/lib/constants'
import {
  Ticket,
  Search,
  Filter,
  Clock,
  User,
  Tag,
  ChevronRight,
  Database,
  FileCode,
  Bug,
  BookOpen,
  Zap,
  RefreshCw,
  Plus,
  CheckCircle,
  AlertCircle,
  XCircle,
  Send,
  ExternalLink,
  Loader2,
  X,
} from 'lucide-react'

// =============================================================================
// Sample Jira Tickets Data (Demo)
// =============================================================================

interface JiraTicket {
  ticket_id: string
  summary: string
  description: string
  status: 'To Do' | 'In Progress' | 'Done' | 'In Review'
  priority: 'Critical' | 'High' | 'Medium' | 'Low'
  issue_type: 'Story' | 'Bug' | 'Task' | 'Epic'
  assignee: { name: string } | null
  reporter: { name: string }
  labels: string[]
  created: string
  updated: string
  has_pipeline: boolean
  pipeline_status?: 'pending' | 'in_progress' | 'complete' | 'failed'
}

// Sample tickets synced with real Jira (SCRUM project)
const SAMPLE_TICKETS: JiraTicket[] = [
  {
    ticket_id: 'SCRUM-4',
    summary: 'Build Cobrix EBCDIC File Parser Pipeline for Legacy Mainframe Data',
    description: `## Objective
Ingest legacy mainframe COBOL/EBCDIC files into the data lake using Cobrix parser.

## Source Details
- **File Format**: EBCDIC encoded fixed-width files
- **Copybook**: CUSTOMER-MASTER.cpy
- **Location**: gs://legacy-mainframe-exports/customer/*.dat
- **Frequency**: Daily batch at 2 AM EST
- **Volume**: ~5 million records/day

## Schema (from COBOL Copybook)
\`\`\`
01 CUSTOMER-RECORD.
   05 CUST-ID          PIC 9(10).
   05 CUST-NAME        PIC X(50).
   05 CUST-ADDRESS     PIC X(100).
   05 CUST-CITY        PIC X(30).
   05 CUST-STATE       PIC X(2).
   05 CUST-ZIP         PIC 9(5).
   05 CUST-PHONE       PIC 9(10).
   05 CUST-BALANCE     PIC S9(9)V99 COMP-3.
   05 CUST-STATUS      PIC X(1).
   05 CUST-OPEN-DATE   PIC 9(8).
\`\`\`

## Requirements
- Parse EBCDIC to UTF-8
- Handle COMP-3 (packed decimal) fields
- Validate PII fields (phone, address)
- Load to BigQuery bronze zone
- SCD Type 2 for customer dimension

## Acceptance Criteria
- [ ] Pipeline processes all EBCDIC files
- [ ] COMP-3 fields correctly converted
- [ ] PII masking applied
- [ ] Data quality checks pass (>99.5%)
- [ ] Audit logging enabled`,
    status: 'In Progress',
    priority: 'High',
    issue_type: 'Story',
    assignee: { name: 'John Smith' },
    reporter: { name: 'Sarah Johnson' },
    labels: ['data-pipeline', 'ebcdic', 'mainframe', 'cobrix', 'legacy-migration'],
    created: '2026-01-20T10:00:00Z',
    updated: '2026-01-25T14:30:00Z',
    has_pipeline: true,
    pipeline_status: 'in_progress',
  },
  {
    ticket_id: 'SCRUM-5',
    summary: 'Create Sales Transaction Pipeline from Oracle CDC',
    description: `Implement CDC-based pipeline to capture sales transactions from Oracle ERP.

## Source
- Oracle 19c
- Schema: SALES.TRANSACTIONS
- CDC via LogMiner

## Target
- BigQuery: sales_domain.transactions_silver
- Real-time streaming with 5-minute micro-batches`,
    status: 'To Do',
    priority: 'Critical',
    issue_type: 'Story',
    assignee: { name: 'Emily Chen' },
    reporter: { name: 'Michael Brown' },
    labels: ['data-pipeline', 'oracle', 'cdc', 'streaming'],
    created: '2026-01-22T09:00:00Z',
    updated: '2026-01-24T11:00:00Z',
    has_pipeline: false,
  },
  {
    ticket_id: 'SCRUM-6',
    summary: 'Migrate SSIS Package: Daily Inventory ETL',
    description: `Migrate existing SSIS package to Airflow/Spark.

## Current State
- DTSX Package: InventoryDaily.dtsx
- Runs on SQL Server Integration Services
- Processes 2M records daily

## Target State
- Airflow DAG with PySpark jobs
- Same business logic preserved
- Improved monitoring and alerting`,
    status: 'In Review',
    priority: 'Medium',
    issue_type: 'Task',
    assignee: { name: 'John Smith' },
    reporter: { name: 'Lisa Wang' },
    labels: ['data-pipeline', 'dtsx-migration', 'ssis', 'inventory'],
    created: '2026-01-18T14:00:00Z',
    updated: '2026-01-25T09:00:00Z',
    has_pipeline: true,
    pipeline_status: 'complete',
  },
  {
    ticket_id: 'SCRUM-7',
    summary: 'Build Customer 360 Gold Layer Aggregation',
    description: `Create Gold zone aggregation for Customer 360 view.

## Source Tables (Silver)
- customers_silver
- orders_silver
- support_tickets_silver
- marketing_interactions_silver

## Target
- customer_360_gold (Star Schema)
- Daily refresh with incremental updates`,
    status: 'To Do',
    priority: 'High',
    issue_type: 'Story',
    assignee: null,
    reporter: { name: 'David Kim' },
    labels: ['data-pipeline', 'gold-zone', 'customer-360', 'aggregation'],
    created: '2026-01-24T10:00:00Z',
    updated: '2026-01-24T10:00:00Z',
    has_pipeline: false,
  },
  {
    ticket_id: 'SCRUM-8',
    summary: 'Fix Data Quality Issues in Product Catalog Pipeline',
    description: `Product catalog pipeline failing DQ checks.

## Issue
- NULL values in required fields (product_name, category_id)
- Duplicate product_ids detected
- Invalid price values (negative amounts)

## Root Cause
Source system sending malformed records after recent upgrade.`,
    status: 'In Progress',
    priority: 'Critical',
    issue_type: 'Bug',
    assignee: { name: 'Emily Chen' },
    reporter: { name: 'Support Team' },
    labels: ['bug', 'data-quality', 'product-catalog', 'urgent'],
    created: '2026-01-25T08:00:00Z',
    updated: '2026-01-25T16:00:00Z',
    has_pipeline: true,
    pipeline_status: 'failed',
  },
  {
    ticket_id: 'SCRUM-9',
    summary: 'Ingest Kafka Clickstream Events to Bronze Zone',
    description: `Set up streaming pipeline for website clickstream events.

## Source
- Kafka Topic: web.clickstream.events
- Format: Avro with Schema Registry
- Volume: ~100K events/minute

## Target
- BigQuery: clickstream_domain.events_bronze
- Partitioned by event_date
- Clustered by user_id, session_id`,
    status: 'To Do',
    priority: 'Medium',
    issue_type: 'Story',
    assignee: { name: 'Alex Rivera' },
    reporter: { name: 'Product Team' },
    labels: ['data-pipeline', 'kafka', 'streaming', 'clickstream'],
    created: '2026-01-23T11:00:00Z',
    updated: '2026-01-23T11:00:00Z',
    has_pipeline: false,
  },
]

// =============================================================================
// Helper Components
// =============================================================================

const statusColors: Record<string, { bg: string; text: string; border: string }> = {
  'To Do': { bg: 'bg-gray-500/20', text: 'text-gray-400', border: 'border-gray-500/30' },
  'In Progress': { bg: 'bg-blue-500/20', text: 'text-blue-400', border: 'border-blue-500/30' },
  'In Review': { bg: 'bg-purple-500/20', text: 'text-purple-400', border: 'border-purple-500/30' },
  'Done': { bg: 'bg-green-500/20', text: 'text-green-400', border: 'border-green-500/30' },
}

const priorityColors: Record<string, { bg: string; text: string; border: string }> = {
  'Critical': { bg: 'bg-red-500/20', text: 'text-red-400', border: 'border-red-500/30' },
  'High': { bg: 'bg-orange-500/20', text: 'text-orange-400', border: 'border-orange-500/30' },
  'Medium': { bg: 'bg-yellow-500/20', text: 'text-yellow-400', border: 'border-yellow-500/30' },
  'Low': { bg: 'bg-green-500/20', text: 'text-green-400', border: 'border-green-500/30' },
}

const issueTypeIcons: Record<string, React.ReactNode> = {
  'Story': <BookOpen className="w-4 h-4 text-green-400" />,
  'Bug': <Bug className="w-4 h-4 text-red-400" />,
  'Task': <FileCode className="w-4 h-4 text-blue-400" />,
  'Epic': <Zap className="w-4 h-4 text-purple-400" />,
}

const pipelineStatusIcons: Record<string, React.ReactNode> = {
  'pending': <Clock className="w-3.5 h-3.5 text-gray-400" />,
  'in_progress': <RefreshCw className="w-3.5 h-3.5 text-blue-400 animate-spin" />,
  'complete': <CheckCircle className="w-3.5 h-3.5 text-green-400" />,
  'failed': <XCircle className="w-3.5 h-3.5 text-red-400" />,
}

function TicketCard({ ticket }: { ticket: JiraTicket }) {
  const statusStyle = statusColors[ticket.status] || statusColors['To Do']
  const priorityStyle = priorityColors[ticket.priority] || priorityColors['Medium']

  return (
    <Link href={`/jira/${ticket.ticket_id}`}>
      <Card className="bg-gray-800/30 border-gray-700/30 hover:bg-gray-800/50 hover:border-gray-600/50 transition-all cursor-pointer group">
        <CardContent className="p-5">
          <div className="flex items-start gap-4">
            {/* Issue Type Icon */}
            <div className="p-2.5 rounded-xl bg-gray-700/50 group-hover:bg-gray-700">
              {issueTypeIcons[ticket.issue_type] || <FileCode className="w-4 h-4 text-gray-400" />}
            </div>

            {/* Main Content */}
            <div className="flex-1 min-w-0">
              {/* Header */}
              <div className="flex items-center gap-2 mb-2">
                <span className="text-blue-400 font-mono text-sm">{ticket.ticket_id}</span>
                <span className={cn(
                  'px-2 py-0.5 rounded text-xs font-medium border',
                  statusStyle.bg, statusStyle.text, statusStyle.border
                )}>
                  {ticket.status}
                </span>
                <span className={cn(
                  'px-2 py-0.5 rounded text-xs font-medium border',
                  priorityStyle.bg, priorityStyle.text, priorityStyle.border
                )}>
                  {ticket.priority}
                </span>
              </div>

              {/* Summary */}
              <h3 className="text-white font-medium mb-2 group-hover:text-blue-300 transition-colors line-clamp-2">
                {ticket.summary}
              </h3>

              {/* Meta */}
              <div className="flex items-center gap-4 text-xs text-gray-500">
                {ticket.assignee && (
                  <span className="flex items-center gap-1">
                    <User className="w-3 h-3" />
                    {ticket.assignee.name}
                  </span>
                )}
                <span className="flex items-center gap-1">
                  <Clock className="w-3 h-3" />
                  {new Date(ticket.updated).toLocaleDateString()}
                </span>
              </div>

              {/* Labels */}
              {ticket.labels.length > 0 && (
                <div className="flex flex-wrap gap-1.5 mt-3">
                  {ticket.labels.slice(0, 4).map((label) => (
                    <span
                      key={label}
                      className="px-2 py-0.5 rounded text-[10px] bg-gray-700/50 text-gray-400 border border-gray-600/30"
                    >
                      {label}
                    </span>
                  ))}
                  {ticket.labels.length > 4 && (
                    <span className="px-2 py-0.5 rounded text-[10px] bg-gray-700/50 text-gray-500">
                      +{ticket.labels.length - 4} more
                    </span>
                  )}
                </div>
              )}
            </div>

            {/* Pipeline Status */}
            <div className="flex flex-col items-end gap-2">
              {ticket.has_pipeline ? (
                <div className={cn(
                  'flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-medium',
                  ticket.pipeline_status === 'complete' && 'bg-green-500/20 text-green-400',
                  ticket.pipeline_status === 'in_progress' && 'bg-blue-500/20 text-blue-400',
                  ticket.pipeline_status === 'failed' && 'bg-red-500/20 text-red-400',
                  ticket.pipeline_status === 'pending' && 'bg-gray-500/20 text-gray-400'
                )}>
                  {pipelineStatusIcons[ticket.pipeline_status || 'pending']}
                  <Database className="w-3.5 h-3.5" />
                  <span>Pipeline</span>
                </div>
              ) : (
                <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs bg-gray-700/30 text-gray-500">
                  <Plus className="w-3.5 h-3.5" />
                  <span>No Pipeline</span>
                </div>
              )}
              <ChevronRight className="w-5 h-5 text-gray-600 group-hover:text-gray-400 transition-colors" />
            </div>
          </div>
        </CardContent>
      </Card>
    </Link>
  )
}

// =============================================================================
// Main Page Component
// =============================================================================

export default function JiraTicketsPage() {
  const [searchQuery, setSearchQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [priorityFilter, setPriorityFilter] = useState('')
  const [pipelineFilter, setPipelineFilter] = useState('')
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [createLoading, setCreateLoading] = useState(false)
  const [createResult, setCreateResult] = useState<{ success: boolean; ticket_id?: string; ticket_url?: string; error?: string } | null>(null)
  const queryClient = useQueryClient()

  // Fetch from real Jira API, fallback to sample data
  const { data: jiraData, isLoading: jiraLoading, refetch } = useQuery({
    queryKey: [QUERY_KEYS.JIRA_TICKETS],
    queryFn: async () => {
      const apiUrl = getApiBaseUrl()
      const res = await fetch(`${apiUrl}/api/jira/tickets`)
      if (!res.ok) throw new Error('Failed to fetch')
      return res.json()
    },
    staleTime: 60_000,
    retry: 1,
  })

  // Use API data if available, otherwise sample data
  const tickets: JiraTicket[] = (jiraData?.tickets?.length > 0) ? jiraData.tickets : SAMPLE_TICKETS
  const dataSource = (jiraData?.tickets?.length > 0) ? 'jira' : 'sample'

  // Create ticket handler
  const handleCreateTicket = useCallback(async (formData: {
    summary: string; description: string; issue_type: string; priority: string; labels: string
  }) => {
    setCreateLoading(true)
    setCreateResult(null)
    try {
      const result = await api.createJiraTicket({
        summary: formData.summary,
        description: formData.description,
        issue_type: formData.issue_type,
        priority: formData.priority,
        labels: formData.labels.split(',').map(l => l.trim()).filter(Boolean),
      })
      setCreateResult({ success: true, ticket_id: result.ticket_id, ticket_url: result.ticket_url })
      refetch()
    } catch (err: any) {
      setCreateResult({ success: false, error: err?.response?.data?.detail || err.message || 'Failed to create ticket' })
    } finally {
      setCreateLoading(false)
    }
  }, [refetch])

  // Filter tickets
  const filteredTickets = useMemo(() => {
    return tickets.filter((ticket) => {
      if (searchQuery) {
        const query = searchQuery.toLowerCase()
        const matchesSearch =
          ticket.ticket_id.toLowerCase().includes(query) ||
          ticket.summary.toLowerCase().includes(query) ||
          ticket.labels.some(l => l.toLowerCase().includes(query))
        if (!matchesSearch) return false
      }

      if (statusFilter && ticket.status !== statusFilter) {
        return false
      }

      if (priorityFilter && ticket.priority !== priorityFilter) {
        return false
      }

      if (pipelineFilter === 'has_pipeline' && !ticket.has_pipeline) {
        return false
      }
      if (pipelineFilter === 'no_pipeline' && ticket.has_pipeline) {
        return false
      }

      return true
    })
  }, [tickets, searchQuery, statusFilter, priorityFilter, pipelineFilter])

  const clearFilters = () => {
    setSearchQuery('')
    setStatusFilter('')
    setPriorityFilter('')
    setPipelineFilter('')
  }

  const hasActiveFilters = searchQuery || statusFilter || priorityFilter || pipelineFilter

  // Stats
  const stats = {
    total: tickets.length,
    withPipeline: tickets.filter(t => t.has_pipeline).length,
    inProgress: tickets.filter(t => t.status === 'In Progress').length,
    critical: tickets.filter(t => t.priority === 'Critical').length,
  }

  return (
    <PageLayout>
      <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-900 to-gray-800 text-white p-6">
        <div className="max-w-6xl mx-auto">
          {/* Header */}
          <div className="flex items-center justify-between mb-8">
            <div>
              <h1 className="text-2xl font-bold text-white flex items-center gap-3">
                <div className="p-2 rounded-lg bg-blue-500/10">
                  <Ticket className="w-6 h-6 text-blue-400" />
                </div>
                Jira Tickets
              </h1>
              <p className="text-gray-400 mt-1">
                Select a ticket to create or manage data pipelines
              </p>
            </div>
            <div className="flex items-center gap-3">
              {dataSource === 'sample' && (
                <span className="text-xs text-yellow-400 bg-yellow-500/10 px-2 py-1 rounded">Demo Data</span>
              )}
              <Button variant="outline" className="border-gray-700 text-gray-300" onClick={() => refetch()}>
                <RefreshCw className={cn("w-4 h-4 mr-2", jiraLoading && "animate-spin")} />
                Sync from Jira
              </Button>
              <Button className="bg-blue-600 hover:bg-blue-700 text-white" onClick={() => { setShowCreateModal(true); setCreateResult(null) }}>
                <Plus className="w-4 h-4 mr-2" />
                Create Ticket
              </Button>
            </div>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-4 gap-4 mb-6">
            <div className="p-4 rounded-xl bg-gray-800/30 border border-gray-700/30">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-lg bg-gray-700/50">
                  <Ticket className="w-5 h-5 text-gray-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">{stats.total}</p>
                  <p className="text-xs text-gray-500">Total Tickets</p>
                </div>
              </div>
            </div>
            <div className="p-4 rounded-xl bg-gray-800/30 border border-gray-700/30">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-lg bg-blue-500/20">
                  <Database className="w-5 h-5 text-blue-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">{stats.withPipeline}</p>
                  <p className="text-xs text-gray-500">With Pipelines</p>
                </div>
              </div>
            </div>
            <div className="p-4 rounded-xl bg-gray-800/30 border border-gray-700/30">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-lg bg-yellow-500/20">
                  <RefreshCw className="w-5 h-5 text-yellow-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">{stats.inProgress}</p>
                  <p className="text-xs text-gray-500">In Progress</p>
                </div>
              </div>
            </div>
            <div className="p-4 rounded-xl bg-gray-800/30 border border-gray-700/30">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-lg bg-red-500/20">
                  <AlertCircle className="w-5 h-5 text-red-400" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-white">{stats.critical}</p>
                  <p className="text-xs text-gray-500">Critical</p>
                </div>
              </div>
            </div>
          </div>

          {/* Filters */}
          <div className="bg-gray-800/30 rounded-xl border border-gray-700/30 p-4 mb-6">
            <div className="flex flex-wrap items-center gap-4">
              {/* Search */}
              <div className="relative flex-1 min-w-64">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
                <input
                  type="text"
                  placeholder="Search tickets by ID, summary, or labels..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full bg-gray-800 border border-gray-700 text-gray-300 text-sm rounded-lg pl-10 pr-4 py-2.5 focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500 transition-colors placeholder-gray-500"
                />
              </div>

              {/* Status Filter */}
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="bg-gray-800 border border-gray-700 text-gray-300 text-sm rounded-lg px-3 py-2.5 focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="">All Statuses</option>
                <option value="To Do">To Do</option>
                <option value="In Progress">In Progress</option>
                <option value="In Review">In Review</option>
                <option value="Done">Done</option>
              </select>

              {/* Priority Filter */}
              <select
                value={priorityFilter}
                onChange={(e) => setPriorityFilter(e.target.value)}
                className="bg-gray-800 border border-gray-700 text-gray-300 text-sm rounded-lg px-3 py-2.5 focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="">All Priorities</option>
                <option value="Critical">Critical</option>
                <option value="High">High</option>
                <option value="Medium">Medium</option>
                <option value="Low">Low</option>
              </select>

              {/* Pipeline Filter */}
              <select
                value={pipelineFilter}
                onChange={(e) => setPipelineFilter(e.target.value)}
                className="bg-gray-800 border border-gray-700 text-gray-300 text-sm rounded-lg px-3 py-2.5 focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="">All Tickets</option>
                <option value="has_pipeline">With Pipeline</option>
                <option value="no_pipeline">Without Pipeline</option>
              </select>

              {hasActiveFilters && (
                <Button variant="ghost" onClick={clearFilters} className="text-gray-400 hover:text-white">
                  <XCircle className="w-4 h-4 mr-1" />
                  Clear
                </Button>
              )}
            </div>
          </div>

          {/* Results */}
          <div className="flex items-center justify-between mb-4">
            <p className="text-sm text-gray-400">
              Showing <span className="text-white font-medium">{filteredTickets.length}</span> of{' '}
              <span className="text-white font-medium">{tickets.length}</span> tickets
            </p>
          </div>

          {/* Tickets List */}
          {filteredTickets.length === 0 ? (
            <div className="text-center py-20 bg-gray-800/30 rounded-xl border border-gray-700/30">
              <Ticket className="w-16 h-16 text-gray-600 mx-auto mb-4" />
              <h3 className="text-xl font-semibold text-white mb-2">No tickets found</h3>
              <p className="text-gray-400 mb-6">
                {hasActiveFilters
                  ? 'Try adjusting your filters'
                  : 'No Jira tickets available'}
              </p>
              {hasActiveFilters && (
                <Button variant="outline" onClick={clearFilters}>
                  Clear Filters
                </Button>
              )}
            </div>
          ) : (
            <div className="space-y-3">
              {filteredTickets.map((ticket) => (
                <TicketCard key={ticket.ticket_id} ticket={ticket} />
              ))}
            </div>
          )}

          {/* Info Banner */}
          <div className="mt-8 p-4 rounded-xl bg-blue-500/10 border border-blue-500/30">
            <div className="flex items-start gap-3">
              <div className="p-2 rounded-lg bg-blue-500/20">
                <Database className="w-5 h-5 text-blue-400" />
              </div>
              <div>
                <h4 className="text-sm font-medium text-blue-400 mb-1">Pipeline Creation</h4>
                <p className="text-sm text-gray-400">
                  To create a data pipeline, click on a ticket and use the &quot;Configure Pipeline&quot; tab.
                  All pipelines are linked to Jira tickets for traceability and approval workflows.
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Create Ticket Modal */}
      {showCreateModal && (
        <CreateTicketModal
          onClose={() => setShowCreateModal(false)}
          onCreate={handleCreateTicket}
          loading={createLoading}
          result={createResult}
        />
      )}
    </PageLayout>
  )
}

// =============================================================================
// Create Ticket Modal
// =============================================================================

function CreateTicketModal({
  onClose,
  onCreate,
  loading,
  result,
}: {
  onClose: () => void
  onCreate: (data: { summary: string; description: string; issue_type: string; priority: string; labels: string }) => void
  loading: boolean
  result: { success: boolean; ticket_id?: string; ticket_url?: string; error?: string } | null
}) {
  const [summary, setSummary] = useState('')
  const [description, setDescription] = useState('')
  const [issueType, setIssueType] = useState('Story')
  const [priority, setPriority] = useState('Medium')
  const [labels, setLabels] = useState('data-pipeline')

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-gray-900 border border-gray-700 rounded-2xl w-full max-w-lg shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-gray-800">
          <h2 className="text-lg font-semibold text-white flex items-center gap-2">
            <Send className="w-5 h-5 text-blue-400" />
            Push Ticket to Jira
          </h2>
          <button onClick={onClose} className="text-gray-400 hover:text-white transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Body */}
        <div className="p-5 space-y-4">
          {result?.success ? (
            <div className="text-center py-6">
              <CheckCircle className="w-12 h-12 text-green-400 mx-auto mb-3" />
              <p className="text-white font-medium mb-1">Ticket Created: {result.ticket_id}</p>
              {result.ticket_url && (
                <a
                  href={result.ticket_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-400 hover:text-blue-300 text-sm flex items-center justify-center gap-1"
                >
                  <ExternalLink className="w-3.5 h-3.5" />
                  Open in Jira
                </a>
              )}
              <Button className="mt-4" variant="outline" onClick={onClose}>Close</Button>
            </div>
          ) : (
            <>
              <div>
                <label className="block text-sm text-gray-400 mb-1">Summary *</label>
                <input
                  type="text"
                  value={summary}
                  onChange={(e) => setSummary(e.target.value)}
                  placeholder="Brief ticket summary..."
                  className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded-lg px-3 py-2.5 focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                />
              </div>
              <div>
                <label className="block text-sm text-gray-400 mb-1">Description</label>
                <textarea
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="Detailed description..."
                  rows={4}
                  className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded-lg px-3 py-2.5 focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                />
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm text-gray-400 mb-1">Issue Type</label>
                  <select
                    value={issueType}
                    onChange={(e) => setIssueType(e.target.value)}
                    className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded-lg px-3 py-2.5"
                    style={{ colorScheme: 'dark' }}
                  >
                    <option value="Story">Story</option>
                    <option value="Bug">Bug</option>
                    <option value="Task">Task</option>
                    <option value="Epic">Epic</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm text-gray-400 mb-1">Priority</label>
                  <select
                    value={priority}
                    onChange={(e) => setPriority(e.target.value)}
                    className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded-lg px-3 py-2.5"
                    style={{ colorScheme: 'dark' }}
                  >
                    <option value="Critical">Critical</option>
                    <option value="High">High</option>
                    <option value="Medium">Medium</option>
                    <option value="Low">Low</option>
                  </select>
                </div>
              </div>
              <div>
                <label className="block text-sm text-gray-400 mb-1">Labels (comma-separated)</label>
                <input
                  type="text"
                  value={labels}
                  onChange={(e) => setLabels(e.target.value)}
                  placeholder="data-pipeline, feature"
                  className="w-full bg-gray-800 border border-gray-700 text-white text-sm rounded-lg px-3 py-2.5 focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                />
              </div>

              {result?.error && (
                <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">
                  {result.error}
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        {!result?.success && (
          <div className="flex items-center justify-end gap-3 p-5 border-t border-gray-800">
            <Button variant="outline" onClick={onClose} className="border-gray-700 text-gray-300">Cancel</Button>
            <Button
              className="bg-blue-600 hover:bg-blue-700 text-white"
              onClick={() => onCreate({ summary, description, issue_type: issueType, priority, labels })}
              disabled={!summary.trim() || loading}
            >
              {loading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Send className="w-4 h-4 mr-2" />}
              Push to Jira
            </Button>
          </div>
        )}
      </div>
    </div>
  )
}
