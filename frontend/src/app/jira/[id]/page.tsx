'use client'

/**
 * Jira Ticket Detail Page with Data Pipeline Integration
 *
 * Features:
 * - Three tabs: Details, Configure Pipeline, Agent Progress
 * - Pipeline configuration form for creating data pipelines
 * - Real-time pipeline progress tracking via WebSocket
 * - Existing code generation functionality preserved
 * - Sample data support for demo tickets (DATA-1001 Cobrix, etc.)
 */

import { useState, useCallback, useMemo } from 'react'
import { useParams, useRouter } from 'next/navigation'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { PageLayout } from '@/components/layout/PageLayout'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { Button } from '@/components/ui/Button'
import { api, QUERY_KEYS } from '@/lib/api'
import { UnifiedPipelineForm } from '@/components/pipeline/UnifiedPipelineForm'
import { PipelineProgress } from '@/components/pipeline/PipelineProgress'
import { PipelineStatus } from '@/types/pipeline'
import { UnifiedPipelineInput } from '@/types/pipeline-canonical'
import Link from 'next/link'
import { cn } from '@/lib/utils'
import {
  Code, GitPullRequest, Clock, User, Tag, ExternalLink, Play,
  CheckCircle, AlertCircle, FileCode, ArrowLeft, BookOpen, Bug, FileText, GitBranch,
  Settings, Activity, Database
} from 'lucide-react'

// =============================================================================
// Types
// =============================================================================

interface JiraTicket {
  ticket_id: string
  summary: string
  description: string
  status: string
  priority: string
  issue_type: string
  assignee: { name: string } | null
  reporter: { name: string } | null
  labels: string[]
  created: string
  updated: string
  url: string
}

// =============================================================================
// Sample Data for Demo
// =============================================================================

const SAMPLE_TICKETS: Record<string, JiraTicket> = {
  'SCRUM-4': {
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
\`\`\`cobol
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

## Data Type Mappings
| COBOL Field | PIC Clause | Spark Type | Notes |
|-------------|------------|------------|-------|
| CUST-ID | PIC 9(10) | LongType | Numeric ID |
| CUST-NAME | PIC X(50) | StringType | Fixed-width text |
| CUST-BALANCE | PIC S9(9)V99 COMP-3 | DecimalType(11,2) | Packed decimal (BCD) |
| CUST-OPEN-DATE | PIC 9(8) | DateType | YYYYMMDD format |

## Requirements
- Parse EBCDIC to UTF-8 using Cobrix library
- Handle COMP-3 (packed decimal) fields for balance
- Validate PII fields (phone, address) and apply masking
- Load to BigQuery bronze zone with schema enforcement
- Implement SCD Type 2 for customer dimension in silver zone

## Technical Notes
- Use \`spark-cobol\` library (com.github.AbsaOSS:cobrix)
- Copybook path: gs://copybooks/CUSTOMER-MASTER.cpy
- Character set: EBCDIC (cp037)
- Record format: Fixed-block (FB)

## Acceptance Criteria
- [ ] Pipeline processes all EBCDIC files from source
- [ ] COMP-3 fields correctly converted to decimal
- [ ] PII masking applied to phone/address fields
- [ ] Data quality checks pass (>99.5% completeness)
- [ ] Audit logging enabled for compliance
- [ ] SCD Type 2 history tracking in silver zone`,
    status: 'In Progress',
    priority: 'High',
    issue_type: 'Story',
    assignee: { name: 'John Smith' },
    reporter: { name: 'Sarah Johnson' },
    labels: ['data-pipeline', 'ebcdic', 'mainframe', 'cobrix', 'legacy-migration', 'pii'],
    created: '2026-01-20T10:00:00Z',
    updated: '2026-01-25T14:30:00Z',
    url: 'https://samrattidke.atlassian.net/browse/SCRUM-4',
  },
  'SCRUM-5': {
    ticket_id: 'SCRUM-5',
    summary: 'Create Sales Transaction Pipeline from Oracle CDC',
    description: `## Objective
Implement CDC-based pipeline to capture sales transactions from Oracle ERP.

## Source
- Oracle 19c
- Schema: SALES.TRANSACTIONS
- CDC via LogMiner

## Target
- BigQuery: sales_domain.transactions_silver
- Real-time streaming with 5-minute micro-batches

## Requirements
- Capture INSERT, UPDATE, DELETE operations
- Maintain transaction ordering
- Handle schema evolution`,
    status: 'To Do',
    priority: 'Critical',
    issue_type: 'Story',
    assignee: { name: 'Emily Chen' },
    reporter: { name: 'Michael Brown' },
    labels: ['data-pipeline', 'oracle', 'cdc', 'streaming'],
    created: '2026-01-22T09:00:00Z',
    updated: '2026-01-24T11:00:00Z',
    url: 'https://samrattidke.atlassian.net/browse/SCRUM-5',
  },
  'SCRUM-6': {
    ticket_id: 'SCRUM-6',
    summary: 'Migrate SSIS Package: Daily Inventory ETL',
    description: `## Current State
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
    url: 'https://samrattidke.atlassian.net/browse/SCRUM-6',
  },
  'SCRUM-7': {
    ticket_id: 'SCRUM-7',
    summary: 'Build Customer 360 Gold Layer Aggregation',
    description: `## Source Tables (Silver)
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
    url: 'https://samrattidke.atlassian.net/browse/SCRUM-7',
  },
  'SCRUM-8': {
    ticket_id: 'SCRUM-8',
    summary: 'Fix Data Quality Issues in Product Catalog Pipeline',
    description: `## Issue
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
    url: 'https://samrattidke.atlassian.net/browse/SCRUM-8',
  },
  'SCRUM-9': {
    ticket_id: 'SCRUM-9',
    summary: 'Ingest Kafka Clickstream Events to Bronze Zone',
    description: `## Source
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
    url: 'https://samrattidke.atlassian.net/browse/SCRUM-9',
  },
}

interface ProcessResult {
  ticket_id: string
  status: string
  pr_url?: string
  pr_number?: number
  steps: Array<{ step: string; status: string; branch?: string; file?: string; chars?: number }>
  error?: string
}

type TabId = 'details' | 'configure' | 'progress'

interface Tab {
  id: TabId
  label: string
  icon: React.ReactNode
}

// =============================================================================
// Constants
// =============================================================================

const statusColors: Record<string, string> = {
  'To Do': 'bg-gray-500',
  'In Progress': 'bg-blue-500',
  'Done': 'bg-green-500',
}

const priorityColors: Record<string, string> = {
  'Critical': 'bg-red-500',
  'High': 'bg-orange-500',
  'Medium': 'bg-yellow-500',
  'Low': 'bg-green-500',
}

const TABS: Tab[] = [
  { id: 'details', label: 'Details', icon: <FileText className="h-4 w-4" /> },
  { id: 'configure', label: 'Configure Pipeline', icon: <Settings className="h-4 w-4" /> },
  { id: 'progress', label: 'Agent Progress', icon: <Activity className="h-4 w-4" /> },
]

// =============================================================================
// Description Renderer (Simple pre-formatted text with basic styling)
// =============================================================================

function DescriptionRenderer({ content }: { content: string }) {
  // Use useMemo to parse content only once
  const parsedContent = useMemo(() => {
    const lines = content.split('\n')
    const result: Array<{ type: string; content: string; lang?: string; items?: string[]; rows?: string[][] }> = []
    let i = 0

    while (i < lines.length) {
      const line = lines[i]

      // Code block
      if (line.startsWith('```')) {
        const lang = line.slice(3).trim()
        const codeLines: string[] = []
        i++
        while (i < lines.length && !lines[i].startsWith('```')) {
          codeLines.push(lines[i])
          i++
        }
        result.push({ type: 'code', content: codeLines.join('\n'), lang })
        i++
        continue
      }

      // Table
      if (line.trim().startsWith('|') && line.trim().endsWith('|')) {
        const tableRows: string[][] = []
        while (i < lines.length && lines[i].trim().startsWith('|') && lines[i].trim().endsWith('|')) {
          const cells = lines[i].split('|').slice(1, -1).map(c => c.trim())
          tableRows.push(cells)
          i++
        }
        result.push({ type: 'table', content: '', rows: tableRows })
        continue
      }

      // Header
      if (line.startsWith('## ')) {
        result.push({ type: 'h2', content: line.slice(3) })
        i++
        continue
      }
      if (line.startsWith('# ')) {
        result.push({ type: 'h1', content: line.slice(2) })
        i++
        continue
      }

      // List items (collect consecutive)
      if (line.match(/^[-*] /) && !line.match(/^- \[[ x]\]/)) {
        const items: string[] = []
        while (i < lines.length && lines[i].match(/^[-*] /) && !lines[i].match(/^- \[[ x]\]/)) {
          items.push(lines[i].slice(2))
          i++
        }
        result.push({ type: 'list', content: '', items })
        continue
      }

      // Checkbox
      if (line.match(/^- \[[ x]\] /)) {
        const checked = line.includes('[x]')
        result.push({ type: 'checkbox', content: line.slice(6), lang: checked ? 'checked' : '' })
        i++
        continue
      }

      // Empty line
      if (line.trim() === '') {
        i++
        continue
      }

      // Regular paragraph
      result.push({ type: 'paragraph', content: line })
      i++
    }

    return result
  }, [content])

  return (
    <div className="space-y-3">
      {parsedContent.map((item, idx) => {
        switch (item.type) {
          case 'h1':
            return (
              <h1 key={idx} className="text-xl font-bold text-white mt-6 mb-3">
                {item.content}
              </h1>
            )
          case 'h2':
            return (
              <h2 key={idx} className="text-base font-semibold text-blue-400 mt-5 mb-2 flex items-center gap-2 border-b border-gray-700/50 pb-2">
                <span className="w-1.5 h-5 bg-blue-500 rounded-full" />
                {item.content}
              </h2>
            )
          case 'code':
            return (
              <pre key={idx} className="bg-gray-900/80 border border-gray-600 rounded-lg p-4 my-3 overflow-x-auto">
                <code className="text-sm text-green-400 font-mono whitespace-pre">
                  {item.content}
                </code>
              </pre>
            )
          case 'table':
            if (!item.rows || item.rows.length === 0) return null
            const [header, , ...body] = item.rows // Skip divider row
            return (
              <div key={idx} className="overflow-x-auto my-4 rounded-lg border border-gray-600">
                <table className="min-w-full text-sm">
                  {header && (
                    <thead className="bg-gray-700">
                      <tr>
                        {header.map((cell, i) => (
                          <th key={i} className="px-3 py-2.5 text-left text-white font-semibold border-b border-gray-600">
                            {cell}
                          </th>
                        ))}
                      </tr>
                    </thead>
                  )}
                  <tbody className="bg-gray-800/50">
                    {body.map((row, i) => (
                      <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                        {row.map((cell, j) => (
                          <td key={j} className="px-3 py-2 text-gray-200">
                            <InlineText text={cell} />
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          case 'list':
            return (
              <ul key={idx} className="space-y-1.5 my-3 ml-1">
                {item.items?.map((li, i) => (
                  <li key={i} className="text-sm text-gray-200 flex items-start gap-2">
                    <span className="text-blue-400 mt-1.5">•</span>
                    <span><InlineText text={li} /></span>
                  </li>
                ))}
              </ul>
            )
          case 'checkbox':
            const checked = item.lang === 'checked'
            return (
              <div key={idx} className="flex items-center gap-2.5 my-1.5 ml-1">
                <div className={cn(
                  'w-4 h-4 rounded border-2 flex items-center justify-center flex-shrink-0',
                  checked ? 'bg-green-500/30 border-green-500' : 'bg-gray-700 border-gray-500'
                )}>
                  {checked && <CheckCircle className="w-3 h-3 text-green-400" />}
                </div>
                <span className={cn('text-sm', checked ? 'text-gray-400 line-through' : 'text-gray-200')}>
                  {item.content}
                </span>
              </div>
            )
          case 'paragraph':
            return (
              <p key={idx} className="text-gray-200 text-sm my-2 leading-relaxed">
                <InlineText text={item.content} />
              </p>
            )
          default:
            return null
        }
      })}
    </div>
  )
}

// Simple inline text with bold and code formatting
function InlineText({ text }: { text: string }) {
  // Parse inline formatting with useMemo
  const parts = useMemo(() => {
    const result: Array<{ type: 'text' | 'bold' | 'code'; content: string }> = []
    let remaining = text

    while (remaining.length > 0) {
      // Find next special marker
      const codeIdx = remaining.indexOf('`')
      const boldIdx = remaining.indexOf('**')

      // No more markers
      if (codeIdx === -1 && boldIdx === -1) {
        if (remaining) result.push({ type: 'text', content: remaining })
        break
      }

      // Find which comes first
      let nextIdx = -1
      let nextType: 'code' | 'bold' = 'code'

      if (codeIdx !== -1 && (boldIdx === -1 || codeIdx < boldIdx)) {
        nextIdx = codeIdx
        nextType = 'code'
      } else if (boldIdx !== -1) {
        nextIdx = boldIdx
        nextType = 'bold'
      }

      // Add text before marker
      if (nextIdx > 0) {
        result.push({ type: 'text', content: remaining.slice(0, nextIdx) })
      }

      if (nextType === 'code') {
        // Find closing backtick
        const endIdx = remaining.indexOf('`', nextIdx + 1)
        if (endIdx === -1) {
          result.push({ type: 'text', content: remaining.slice(nextIdx) })
          break
        }
        result.push({ type: 'code', content: remaining.slice(nextIdx + 1, endIdx) })
        remaining = remaining.slice(endIdx + 1)
      } else {
        // Find closing **
        const endIdx = remaining.indexOf('**', nextIdx + 2)
        if (endIdx === -1) {
          result.push({ type: 'text', content: remaining.slice(nextIdx) })
          break
        }
        result.push({ type: 'bold', content: remaining.slice(nextIdx + 2, endIdx) })
        remaining = remaining.slice(endIdx + 2)
      }
    }

    return result
  }, [text])

  return (
    <>
      {parts.map((part, i) => {
        if (part.type === 'code') {
          return (
            <code key={i} className="px-1.5 py-0.5 rounded bg-gray-700/80 text-cyan-400 font-mono text-xs border border-gray-600">
              {part.content}
            </code>
          )
        }
        if (part.type === 'bold') {
          return (
            <strong key={i} className="font-semibold text-white">
              {part.content}
            </strong>
          )
        }
        return <span key={i}>{part.content}</span>
      })}
    </>
  )
}

// =============================================================================
// Tab Components
// =============================================================================

interface TabButtonProps {
  tab: Tab
  isActive: boolean
  onClick: () => void
  badge?: number
}

function TabButton({ tab, isActive, onClick, badge }: TabButtonProps) {
  return (
    <button
      onClick={onClick}
      className={cn(
        'flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors',
        isActive
          ? 'border-blue-500 text-blue-500'
          : 'border-transparent text-gray-400 hover:text-gray-300 hover:border-gray-600'
      )}
    >
      {tab.icon}
      {tab.label}
      {badge !== undefined && badge > 0 && (
        <Badge variant="info" className="ml-1 text-xs">
          {badge}
        </Badge>
      )}
    </button>
  )
}

// =============================================================================
// Main Page Component
// =============================================================================

export default function JiraTicketDetailPage() {
  const params = useParams()
  const router = useRouter()
  const ticketId = params.id as string
  const queryClient = useQueryClient()

  // Tab state
  const [activeTab, setActiveTab] = useState<TabId>('details')

  // Code generation state (existing functionality)
  const [isProcessing, setIsProcessing] = useState(false)
  const [processResult, setProcessResult] = useState<ProcessResult | null>(null)

  // Pipeline state
  const [pipelineRequestId, setPipelineRequestId] = useState<string | null>(null)
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus | null>(null)
  const [isCreatingPipeline, setIsCreatingPipeline] = useState(false)

  // Check if this is a sample ticket
  const sampleTicket = useMemo(() => {
    return ticketId ? SAMPLE_TICKETS[ticketId.toUpperCase()] : null
  }, [ticketId])

  // Fetch ticket data from API, fallback to sample data
  const { data: apiTicket, isLoading } = useQuery<JiraTicket>({
    queryKey: [QUERY_KEYS.JIRA_TICKETS, ticketId],
    queryFn: () => api.getJiraTicket(ticketId),
    enabled: !!ticketId && !sampleTicket, // Skip API call if we have sample data
    retry: false, // Don't retry on failure
  })

  // Use sample data if available, otherwise use API data
  const ticket = sampleTicket || apiTicket

  // Fetch active pipelines for this ticket
  const { data: activePipelines } = useQuery({
    queryKey: [QUERY_KEYS.PIPELINES, ticketId],
    queryFn: () => api.getPipelinesByTicket(ticketId),
    enabled: !!ticketId,
    refetchInterval: pipelineRequestId ? 5000 : false,
  })

  // Code generation mutation (existing functionality)
  const processTicketMutation = useMutation({
    mutationFn: () => api.processJiraTicket(ticketId),
    onSuccess: (data) => {
      setProcessResult(data)
      setIsProcessing(false)
      queryClient.invalidateQueries({ queryKey: ['github-prs'] })
    },
    onError: (error: any) => {
      setIsProcessing(false)
      setProcessResult({
        ticket_id: ticketId,
        status: 'failed',
        error: error.message,
        steps: [],
      })
    },
  })

  // Pipeline creation mutation
  const createPipelineMutation = useMutation({
    mutationFn: (input: UnifiedPipelineInput) => api.createPipelineUnified(input),
    onSuccess: (data) => {
      setPipelineRequestId(data.request_id)
      setIsCreatingPipeline(false)
      setActiveTab('progress')
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.PIPELINES] })
    },
    onError: (error: any) => {
      setIsCreatingPipeline(false)
      console.error('Pipeline creation failed:', error)
    },
  })

  // Pipeline approval mutation
  const approvePipelineMutation = useMutation({
    mutationFn: (requestId: string) => api.approvePipeline(requestId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.PIPELINES] })
    },
  })

  // Pipeline rejection mutation
  const rejectPipelineMutation = useMutation({
    mutationFn: (requestId: string) => api.rejectPipeline(requestId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.PIPELINES] })
    },
  })

  // Handlers
  const handleProcess = () => {
    setIsProcessing(true)
    setProcessResult(null)
    processTicketMutation.mutate()
  }

  const handlePipelineSubmit = useCallback(async (input: UnifiedPipelineInput) => {
    setIsCreatingPipeline(true)
    await createPipelineMutation.mutateAsync(input)
  }, [createPipelineMutation])

  const handlePipelineApprove = useCallback(async (requestId: string) => {
    await approvePipelineMutation.mutateAsync(requestId)
  }, [approvePipelineMutation])

  const handlePipelineReject = useCallback(async (requestId: string) => {
    await rejectPipelineMutation.mutateAsync(requestId)
  }, [rejectPipelineMutation])

  const handlePipelineRetry = useCallback(() => {
    setPipelineRequestId(null)
    setPipelineStatus(null)
    setActiveTab('configure')
  }, [])

  // Loading state (skip if we have sample data)
  if (isLoading && !sampleTicket) {
    return (
      <PageLayout title="Loading...">
        <div className="flex justify-center py-12">
          <div className="animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" />
        </div>
      </PageLayout>
    )
  }

  // Not found state
  if (!ticket) {
    return (
      <PageLayout title="Not Found">
        <Card className="bg-gray-800 border-gray-700">
          <CardContent className="py-12 text-center">
            <AlertCircle className="h-12 w-12 mx-auto text-red-500 mb-4" />
            <p className="text-gray-400">Ticket {ticketId} not found</p>
            <Link href="/jira">
              <Button className="mt-4 border-gray-500 bg-transparent text-gray-300">
                <ArrowLeft className="h-4 w-4 mr-2" />
                Back
              </Button>
            </Link>
          </CardContent>
        </Card>
      </PageLayout>
    )
  }

  const Icon = ticket.issue_type === 'Story' ? BookOpen : ticket.issue_type === 'Bug' ? Bug : FileCode

  return (
    <PageLayout title={ticket.ticket_id} subtitle={ticket.summary}>
      {/* Back button */}
      <Link href="/jira">
        <Button variant="ghost" size="sm" className="mb-4">
          <ArrowLeft className="h-4 w-4 mr-2" />
          Back
        </Button>
      </Link>

      {/* Sample Data Banner */}
      {sampleTicket && (
        <div className="mb-4 p-3 rounded-lg bg-amber-500/10 border border-amber-500/30 flex items-center gap-3">
          <div className="p-1.5 rounded bg-amber-500/20">
            <AlertCircle className="h-4 w-4 text-amber-400" />
          </div>
          <div className="flex-1">
            <span className="text-amber-400 text-sm font-medium">Demo Mode</span>
            <span className="text-amber-400/70 text-sm ml-2">
              This is sample data for demonstration purposes. Configure a pipeline to see the agent in action.
            </span>
          </div>
        </div>
      )}

      {/* Header Card */}
      <Card className="bg-gray-800 border-gray-700 mb-6">
        <CardContent className="pt-6">
          <div className="flex items-start justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-blue-500/20 rounded-lg">
                <Icon className="h-5 w-5" />
              </div>
              <div>
                <div className="flex items-center gap-2">
                  <span className="text-blue-400 font-mono">{ticket.ticket_id}</span>
                  <Badge className={statusColors[ticket.status] || 'bg-gray-500'}>
                    {ticket.status}
                  </Badge>
                </div>
                <span className="text-sm text-gray-500">{ticket.issue_type}</span>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Badge className={priorityColors[ticket.priority] || 'bg-gray-500'}>
                {ticket.priority}
              </Badge>
              <a href={ticket.url} target="_blank" rel="noopener noreferrer">
                <Button className="border-gray-500 bg-transparent text-gray-300" size="sm">
                  <ExternalLink className="h-4 w-4 mr-2" />
                  Jira
                </Button>
              </a>
            </div>
          </div>
          <h1 className="text-2xl font-bold text-white">{ticket.summary}</h1>
        </CardContent>
      </Card>

      {/* Tabs */}
      <div className="border-b border-gray-700 mb-6">
        <div className="flex gap-4">
          {TABS.map((tab) => (
            <TabButton
              key={tab.id}
              tab={tab}
              isActive={activeTab === tab.id}
              onClick={() => setActiveTab(tab.id)}
              badge={tab.id === 'progress' && activePipelines?.length ? activePipelines.length : undefined}
            />
          ))}
        </div>
      </div>

      {/* Tab Content */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Main Content Area */}
        <div className="lg:col-span-2 space-y-6">
          {/* Details Tab */}
          {activeTab === 'details' && (
            <>
              {/* Code Generation Action */}
              <Card className="bg-gray-800 border-gray-700">
                <CardContent className="pt-6">
                  <h3 className="text-lg font-semibold text-white mb-4">Code Generation</h3>
                  <p className="text-gray-400 text-sm mb-4">
                    Analyze this ticket and automatically generate code changes with a pull request.
                  </p>
                  <Button
                    onClick={handleProcess}
                    disabled={isProcessing}
                    className="bg-green-600 hover:bg-green-700"
                  >
                    {isProcessing ? (
                      <>
                        <div className="animate-spin h-4 w-4 border-2 border-white border-t-transparent rounded-full mr-2" />
                        Processing...
                      </>
                    ) : (
                      <>
                        <Play className="h-4 w-4 mr-2" />
                        Generate Code & Create PR
                      </>
                    )}
                  </Button>
                </CardContent>
              </Card>

              {/* Code Generation Result */}
              {processResult && (
                <Card
                  className={
                    processResult.status === 'completed'
                      ? 'bg-green-900/20 border-green-700'
                      : 'bg-red-900/20 border-red-700'
                  }
                >
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      {processResult.status === 'completed' ? (
                        <>
                          <CheckCircle className="h-5 w-5 text-green-500" />
                          <span className="text-green-400">Complete</span>
                        </>
                      ) : (
                        <>
                          <AlertCircle className="h-5 w-5 text-red-500" />
                          <span className="text-red-400">Failed</span>
                        </>
                      )}
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2 mb-4">
                      {processResult.steps.map((step, i) => (
                        <div key={i} className="flex items-center gap-2 p-2 bg-gray-800/50 rounded">
                          {step.status === 'done' ? (
                            <CheckCircle className="h-4 w-4 text-green-500" />
                          ) : (
                            <AlertCircle className="h-4 w-4 text-red-500" />
                          )}
                          <span className="text-white capitalize">{step.step}</span>
                          {step.branch && (
                            <span className="text-gray-400 text-sm">
                              <GitBranch className="h-3 w-3 inline mr-1" />
                              {step.branch}
                            </span>
                          )}
                          {step.file && (
                            <span className="text-gray-400 text-sm">
                              <FileText className="h-3 w-3 inline mr-1" />
                              {step.file}
                            </span>
                          )}
                        </div>
                      ))}
                    </div>
                    {processResult.pr_url && (
                      <div className="p-4 bg-green-900/30 rounded border border-green-700 flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          <GitPullRequest className="h-6 w-6 text-green-500" />
                          <span className="text-white">PR #{processResult.pr_number}</span>
                        </div>
                        <a href={processResult.pr_url} target="_blank" rel="noopener noreferrer">
                          <Button className="bg-green-600">
                            <ExternalLink className="h-4 w-4 mr-2" />
                            View PR
                          </Button>
                        </a>
                      </div>
                    )}
                    {processResult.error && (
                      <p className="text-red-400 p-4 bg-red-900/30 rounded">{processResult.error}</p>
                    )}
                  </CardContent>
                </Card>
              )}

              {/* Description */}
              <Card className="bg-gray-800/50 border-gray-700">
                <CardHeader className="border-b border-gray-700/50">
                  <CardTitle className="text-white">
                    <FileText className="h-5 w-5 inline mr-2 text-blue-400" />
                    Description
                  </CardTitle>
                </CardHeader>
                <CardContent className="pt-4">
                  <DescriptionRenderer content={ticket.description || 'No description'} />
                </CardContent>
              </Card>
            </>
          )}

          {/* Configure Pipeline Tab */}
          {activeTab === 'configure' && (
            <Card className="bg-gray-800 border-gray-700">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Database className="h-5 w-5" />
                  Configure Data Pipeline
                </CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-gray-400 text-sm mb-6">
                  Create a data pipeline by configuring the source (70+ types supported), schema, target, and execution policy.
                  The AI agent will generate Airflow DAGs and PySpark jobs based on your configuration.
                </p>
                <UnifiedPipelineForm
                  jiraTicket={ticketId}
                  createdBy={ticket?.assignee?.name || ticket?.reporter?.name || 'unknown'}
                  onSubmit={handlePipelineSubmit}
                  onCancel={() => setActiveTab('details')}
                  isSubmitting={isCreatingPipeline}
                />
              </CardContent>
            </Card>
          )}

          {/* Agent Progress Tab */}
          {activeTab === 'progress' && (
            <>
              {pipelineRequestId ? (
                <PipelineProgress
                  requestId={pipelineRequestId}
                  initialStatus={pipelineStatus || undefined}
                  onApprove={handlePipelineApprove}
                  onReject={handlePipelineReject}
                  onRetry={handlePipelineRetry}
                  onClose={() => setPipelineRequestId(null)}
                  showArtifacts
                />
              ) : activePipelines && activePipelines.length > 0 ? (
                <div className="space-y-4">
                  <h3 className="text-lg font-semibold text-white">Active Pipelines</h3>
                  {activePipelines.map((pipeline: PipelineStatus) => (
                    <Card
                      key={pipeline.request_id}
                      className="bg-gray-800 border-gray-700 cursor-pointer hover:border-gray-600"
                      onClick={() => setPipelineRequestId(pipeline.request_id)}
                    >
                      <CardContent className="pt-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="text-white font-medium">{pipeline.request_id}</p>
                            <p className="text-gray-400 text-sm">{pipeline.message}</p>
                          </div>
                          <div className="flex items-center gap-2">
                            <Badge
                              variant={
                                pipeline.status === 'complete'
                                  ? 'success'
                                  : pipeline.status === 'failed'
                                  ? 'error'
                                  : 'warning'
                              }
                            >
                              {pipeline.status}
                            </Badge>
                            <span className="text-gray-400 text-sm">{pipeline.progress_pct}%</span>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              ) : (
                <Card className="bg-gray-800 border-gray-700">
                  <CardContent className="py-12 text-center">
                    <Activity className="h-12 w-12 mx-auto text-gray-500 mb-4" />
                    <p className="text-gray-400">No active pipelines</p>
                    <p className="text-gray-500 text-sm mt-2">
                      Configure a pipeline to see progress here
                    </p>
                    <Button
                      className="mt-4"
                      variant="outline"
                      onClick={() => setActiveTab('configure')}
                    >
                      <Settings className="h-4 w-4 mr-2" />
                      Configure Pipeline
                    </Button>
                  </CardContent>
                </Card>
              )}
            </>
          )}
        </div>

        {/* Sidebar */}
        <div className="space-y-6">
          {/* Details Card */}
          <Card className="bg-gray-800 border-gray-700">
            <CardHeader>
              <CardTitle className="text-sm text-gray-400">Details</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <p className="text-xs text-gray-500 mb-1">Status</p>
                <Badge className={statusColors[ticket.status] || 'bg-gray-500'}>
                  {ticket.status}
                </Badge>
              </div>
              <div>
                <p className="text-xs text-gray-500 mb-1">Priority</p>
                <Badge className={priorityColors[ticket.priority] || 'bg-gray-500'}>
                  {ticket.priority}
                </Badge>
              </div>
              <div>
                <p className="text-xs text-gray-500 mb-1">Type</p>
                <span className="text-gray-300 flex items-center gap-2">
                  <Icon className="h-4 w-4" />
                  {ticket.issue_type}
                </span>
              </div>
              {ticket.assignee && (
                <div>
                  <p className="text-xs text-gray-500 mb-1">Assignee</p>
                  <span className="text-gray-300 flex items-center gap-2">
                    <User className="h-4 w-4" />
                    {ticket.assignee.name}
                  </span>
                </div>
              )}
              <div>
                <p className="text-xs text-gray-500 mb-1">Created</p>
                <span className="text-gray-300 flex items-center gap-2">
                  <Clock className="h-4 w-4" />
                  {new Date(ticket.created).toLocaleDateString()}
                </span>
              </div>
            </CardContent>
          </Card>

          {/* Labels Card */}
          {ticket.labels.length > 0 && (
            <Card className="bg-gray-800 border-gray-700">
              <CardHeader>
                <CardTitle className="text-sm text-gray-400">
                  <Tag className="h-4 w-4 inline mr-2" />
                  Labels
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {ticket.labels.map((l) => (
                    <Badge key={l} className="border-gray-500 bg-transparent text-gray-300">
                      {l}
                    </Badge>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Agent Capabilities Card */}
          <Card className="bg-gray-800 border-gray-700">
            <CardHeader>
              <CardTitle className="text-sm text-gray-400">
                <Code className="h-4 w-4 inline mr-2" />
                Agent Capabilities
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-3 text-sm">
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Analyze requirements from Jira</span>
                </li>
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Generate Airflow DAGs</span>
                </li>
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Generate PySpark jobs</span>
                </li>
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Validate syntax & security</span>
                </li>
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Create pull requests</span>
                </li>
              </ul>
            </CardContent>
          </Card>

          {/* Quick Actions */}
          <Card className="bg-gray-800 border-gray-700">
            <CardHeader>
              <CardTitle className="text-sm text-gray-400">Quick Actions</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              <Button
                variant="outline"
                className="w-full justify-start"
                onClick={() => setActiveTab('configure')}
              >
                <Database className="h-4 w-4 mr-2" />
                Create Data Pipeline
              </Button>
              <Button
                variant="outline"
                className="w-full justify-start"
                onClick={handleProcess}
                disabled={isProcessing}
              >
                <Code className="h-4 w-4 mr-2" />
                Generate Code
              </Button>
            </CardContent>
          </Card>
        </div>
      </div>
    </PageLayout>
  )
}
