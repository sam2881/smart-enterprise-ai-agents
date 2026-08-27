'use client'

import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { QUERY_KEYS } from '@/lib/constants'
import { Card, CardContent } from '../ui/Card'
import { Badge } from '../ui/Badge'
import {
  CheckCircle,
  XCircle,
  Clock,
  AlertTriangle,
  FileCode,
  Play,
  Shield,
  GitBranch,
  RefreshCw,
  Replace,
  ChevronDown,
  ChevronUp,
  Search,
  Terminal,
  Zap,
  Server,
  Database,
  HardDrive,
  Lock,
  Network,
  Workflow
} from 'lucide-react'
import toast from 'react-hot-toast'
import type { Approval } from '@/types/approval'

interface Script {
  script_id: string
  display_name: string
  description: string
  action_type: string
  workflow_name: string
  risk_level: string
  category: string
  parameters: string[]
}

interface LangGraphApprovalListProps {
  approvals: Approval[]
  onUpdate: () => void
}

const categoryIcons: Record<string, typeof Server> = {
  service: Terminal,
  infrastructure: Server,
  kubernetes: Workflow,
  storage: HardDrive,
  security: Lock,
  database: Database,
  cache: Zap,
  'data-pipeline': Workflow,
  network: Network,
}

const riskColors: Record<string, string> = {
  low: 'bg-green-100 text-green-700 border-green-200',
  medium: 'bg-yellow-100 text-yellow-700 border-yellow-200',
  high: 'bg-orange-100 text-orange-700 border-orange-200',
  critical: 'bg-red-100 text-red-700 border-red-200',
}

export function LangGraphApprovalList({ approvals, onUpdate }: LangGraphApprovalListProps) {
  const [approving, setApproving] = useState<string | null>(null)
  const [rejecting, setRejecting] = useState<string | null>(null)
  const [retrying, setRetrying] = useState<string | null>(null)
  const [overriding, setOverriding] = useState<string | null>(null)
  const [showOverridePanel, setShowOverridePanel] = useState<string | null>(null)
  const [selectedScript, setSelectedScript] = useState<Script | null>(null)
  const [scriptFilter, setScriptFilter] = useState('')
  const [categoryFilter, setCategoryFilter] = useState<string>('all')
  const queryClient = useQueryClient()

  // Fetch available scripts for override
  const { data: scriptsData } = useQuery({
    queryKey: ['available-scripts'],
    queryFn: () => api.getAvailableScripts(),
    staleTime: 5 * 60 * 1000,
  })

  const scripts: Script[] = scriptsData?.scripts || []
  const categories: string[] = scriptsData?.categories || []

  const filteredScripts = scripts.filter(s => {
    const matchesSearch = !scriptFilter ||
      s.display_name.toLowerCase().includes(scriptFilter.toLowerCase()) ||
      s.description.toLowerCase().includes(scriptFilter.toLowerCase()) ||
      s.script_id.toLowerCase().includes(scriptFilter.toLowerCase())
    const matchesCategory = categoryFilter === 'all' || s.category === categoryFilter
    return matchesSearch && matchesCategory
  })

  const approveMutation = useMutation({
    mutationFn: (incidentId: string) => api.approveIncident(incidentId, {
      approver: 'admin',
      reason: 'Approved via UI'
    }),
    onSuccess: (data) => {
      if (data.status === 'approved_but_trigger_failed') {
        toast.error('Approved but GitHub trigger failed. Click Retry to try again.')
      } else {
        toast.success('Approval granted - workflow continuing')
      }
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.APPROVALS] })
      onUpdate()
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to approve')
    },
    onSettled: () => setApproving(null)
  })

  const rejectMutation = useMutation({
    mutationFn: (incidentId: string) => api.rejectIncident(incidentId, {
      approver: 'admin',
      reason: 'Rejected via UI'
    }),
    onSuccess: () => {
      toast.success('Plan rejected')
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.APPROVALS] })
      onUpdate()
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to reject')
    },
    onSettled: () => setRejecting(null)
  })

  const retryMutation = useMutation({
    mutationFn: (incidentId: string) => api.retryGitHubTrigger(incidentId),
    onSuccess: (data) => {
      if (data.status === 'retry_success') {
        toast.success('GitHub Actions workflow triggered successfully!')
      } else {
        toast.error('Retry failed. Try again later.')
      }
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.APPROVALS] })
      onUpdate()
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Retry failed')
    },
    onSettled: () => setRetrying(null)
  })

  const overrideMutation = useMutation({
    mutationFn: ({ incidentId, script }: { incidentId: string; script: Script }) =>
      api.overrideIncident(incidentId, {
        approver: 'admin',
        reason: `Override: selected ${script.display_name} (${script.script_id})`,
        override_script: {
          script_id: script.script_id,
          workflow_name: script.workflow_name,
          action_type: script.action_type,
        }
      }),
    onSuccess: (data) => {
      toast.success(`Override approved with script: ${data.override_script?.script_id}`)
      setShowOverridePanel(null)
      setSelectedScript(null)
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.APPROVALS] })
      onUpdate()
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Override failed')
    },
    onSettled: () => setOverriding(null)
  })

  const handleApprove = (incidentId: string) => {
    setApproving(incidentId)
    approveMutation.mutate(incidentId)
  }

  const handleReject = (incidentId: string) => {
    setRejecting(incidentId)
    rejectMutation.mutate(incidentId)
  }

  const handleRetry = (incidentId: string) => {
    setRetrying(incidentId)
    retryMutation.mutate(incidentId)
  }

  const handleOverride = (incidentId: string) => {
    if (!selectedScript) {
      toast.error('Please select a script first')
      return
    }
    setOverriding(incidentId)
    overrideMutation.mutate({ incidentId, script: selectedScript })
  }

  const toggleOverridePanel = (incidentId: string) => {
    if (showOverridePanel === incidentId) {
      setShowOverridePanel(null)
      setSelectedScript(null)
      setScriptFilter('')
      setCategoryFilter('all')
    } else {
      setShowOverridePanel(incidentId)
      setSelectedScript(null)
      setScriptFilter('')
      setCategoryFilter('all')
    }
  }

  const getRiskBadgeVariant = (riskLevel: string) => {
    switch (riskLevel?.toLowerCase()) {
      case 'critical': return 'error'
      case 'high': return 'error'
      case 'medium': return 'warning'
      case 'low': return 'success'
      default: return 'info'
    }
  }

  if (approvals.length === 0) {
    return (
      <Card variant="bordered">
        <CardContent className="p-12 text-center">
          <CheckCircle className="h-12 w-12 text-green-500 mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-gray-900 mb-2">No Pending Approvals</h3>
          <p className="text-gray-500">All workflows are either auto-approved or already processed.</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-4">
      {approvals.map((approval) => (
        <Card key={approval.id} variant="bordered" className="hover:shadow-md transition-shadow">
          <CardContent className="p-6">
            <div className="flex items-start justify-between">
              {/* Left side - Info */}
              <div className="flex items-start gap-4 flex-1">
                <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-orange-100 text-orange-600">
                  <AlertTriangle className="h-6 w-6" />
                </div>

                <div className="flex-1">
                  {/* Header */}
                  <div className="flex items-center gap-3 mb-3">
                    <h4 className="font-semibold text-gray-900">
                      Incident: {approval.incident_id}
                    </h4>
                    {approval.requires_action === 'retry' ? (
                      <Badge variant="error">Needs Retry</Badge>
                    ) : (
                      <Badge variant="warning">Pending Approval</Badge>
                    )}
                    <Badge variant={getRiskBadgeVariant(approval.approval_decision?.risk_level)}>
                      {approval.approval_decision?.risk_level || 'Unknown'} Risk
                    </Badge>
                  </div>

                  {/* Error message for failed triggers */}
                  {approval.requires_action === 'retry' && approval.error && (
                    <div className="mb-3 p-2 bg-red-50 border border-red-200 rounded text-sm text-red-700">
                      <strong>Trigger Failed:</strong> {approval.error}
                    </div>
                  )}

                  {/* Plan Details */}
                  <div className="grid grid-cols-2 gap-4 mb-4">
                    <div className="space-y-2">
                      <div className="flex items-center gap-2 text-sm">
                        <FileCode className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Script:</span>
                        <span className="font-mono text-blue-600">{approval.plan?.script_id}</span>
                      </div>
                      <div className="flex items-center gap-2 text-sm">
                        <Play className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Action:</span>
                        <span>{approval.plan?.action_type}</span>
                      </div>
                      <div className="flex items-center gap-2 text-sm">
                        <GitBranch className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Workflow:</span>
                        <span>{approval.plan?.workflow_name}</span>
                      </div>
                    </div>

                    <div className="space-y-2">
                      <div className="flex items-center gap-2 text-sm">
                        <Shield className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Judge Score:</span>
                        <span className={approval.judge_score?.quality_score >= 6 ? 'text-green-600' : 'text-red-600'}>
                          {approval.judge_score?.quality_score?.toFixed(1)}/10
                        </span>
                        {approval.judge_score?.safety_passed && (
                          <Badge variant="success" size="sm">Safety ✓</Badge>
                        )}
                      </div>
                      <div className="flex items-center gap-2 text-sm">
                        <Clock className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Route:</span>
                        <span className="uppercase text-orange-600 font-medium">
                          {approval.approval_decision?.route}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 text-sm">
                        <AlertTriangle className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Risk Score:</span>
                        <span>{(approval.approval_decision?.risk_score * 100)?.toFixed(0)}%</span>
                      </div>
                    </div>
                  </div>

                  {/* Steps */}
                  {approval.plan?.steps && (
                    <div className="mb-4 p-3 bg-gray-50 rounded-lg">
                      <h5 className="text-sm font-medium text-gray-700 mb-2">Execution Steps:</h5>
                      <div className="space-y-1">
                        {approval.plan.steps.map((step: any, idx: number) => (
                          <div key={idx} className="flex items-center gap-2 text-sm text-gray-600">
                            <span className="flex h-5 w-5 items-center justify-center rounded-full bg-blue-100 text-blue-600 text-xs font-medium">
                              {step.step}
                            </span>
                            <span>{step.action}</span>
                            {step.timeout && (
                              <span className="text-gray-400">({step.timeout}s timeout)</span>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Reasoning */}
                  {approval.approval_decision?.reasoning && (
                    <div className="text-sm text-gray-500 italic mb-3">
                      &ldquo;{approval.approval_decision.reasoning}&rdquo;
                    </div>
                  )}

                  {/* === OVERRIDE PANEL === */}
                  {showOverridePanel === approval.incident_id && (
                    <div className="mt-4 border border-blue-200 rounded-lg bg-blue-50/50 p-4">
                      <div className="flex items-center gap-2 mb-3">
                        <Replace className="h-5 w-5 text-blue-600" />
                        <h5 className="font-semibold text-blue-900">Override with Different Script</h5>
                      </div>

                      {/* Search and Filter */}
                      <div className="flex gap-3 mb-3">
                        <div className="flex-1 relative">
                          <Search className="absolute left-3 top-2.5 h-4 w-4 text-gray-400" />
                          <input
                            type="text"
                            placeholder="Search scripts..."
                            value={scriptFilter}
                            onChange={(e) => setScriptFilter(e.target.value)}
                            className="w-full pl-9 pr-3 py-2 text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
                          />
                        </div>
                        <select
                          value={categoryFilter}
                          onChange={(e) => setCategoryFilter(e.target.value)}
                          className="px-3 py-2 text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 bg-white"
                        >
                          <option value="all">All Categories</option>
                          {categories.map(cat => (
                            <option key={cat} value={cat}>{cat}</option>
                          ))}
                        </select>
                      </div>

                      {/* Script List */}
                      <div className="max-h-64 overflow-y-auto space-y-2 mb-3">
                        {filteredScripts.map((script) => {
                          const IconComponent = categoryIcons[script.category] || Terminal
                          const isSelected = selectedScript?.script_id === script.script_id
                          return (
                            <button
                              key={script.script_id}
                              onClick={() => setSelectedScript(isSelected ? null : script)}
                              className={`w-full text-left p-3 rounded-lg border transition-all ${
                                isSelected
                                  ? 'border-blue-500 bg-blue-100 ring-2 ring-blue-300'
                                  : 'border-gray-200 bg-white hover:border-blue-300 hover:bg-blue-50'
                              }`}
                            >
                              <div className="flex items-start gap-3">
                                <div className={`flex h-8 w-8 items-center justify-center rounded-md ${
                                  isSelected ? 'bg-blue-500 text-white' : 'bg-gray-100 text-gray-500'
                                }`}>
                                  <IconComponent className="h-4 w-4" />
                                </div>
                                <div className="flex-1 min-w-0">
                                  <div className="flex items-center gap-2">
                                    <span className="font-medium text-sm text-gray-900">{script.display_name}</span>
                                    <span className={`px-1.5 py-0.5 text-xs rounded border ${riskColors[script.risk_level]}`}>
                                      {script.risk_level}
                                    </span>
                                    <span className="px-1.5 py-0.5 text-xs rounded bg-gray-100 text-gray-600">
                                      {script.category}
                                    </span>
                                  </div>
                                  <p className="text-xs text-gray-500 mt-0.5">{script.description}</p>
                                  <code className="text-xs text-blue-600 font-mono">{script.script_id}</code>
                                </div>
                                {isSelected && (
                                  <CheckCircle className="h-5 w-5 text-blue-600 flex-shrink-0" />
                                )}
                              </div>
                            </button>
                          )
                        })}
                        {filteredScripts.length === 0 && (
                          <div className="text-center py-4 text-sm text-gray-500">
                            No scripts match your filter
                          </div>
                        )}
                      </div>

                      {/* Override Confirm */}
                      <div className="flex items-center justify-between pt-2 border-t border-blue-200">
                        <span className="text-sm text-gray-600">
                          {selectedScript
                            ? <>Selected: <strong>{selectedScript.display_name}</strong></>
                            : 'Select a script to override'
                          }
                        </span>
                        <button
                          onClick={() => handleOverride(approval.incident_id)}
                          disabled={!selectedScript || overriding === approval.incident_id}
                          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors text-sm font-medium"
                        >
                          {overriding === approval.incident_id ? (
                            <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                          ) : (
                            <Replace className="h-4 w-4" />
                          )}
                          Approve with Override
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* Right side - Actions */}
              <div className="flex flex-col gap-2 ml-4">
                {approval.requires_action === 'retry' ? (
                  <button
                    onClick={() => handleRetry(approval.incident_id)}
                    disabled={retrying === approval.incident_id}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    {retrying === approval.incident_id ? (
                      <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                    ) : (
                      <RefreshCw className="h-4 w-4" />
                    )}
                    Retry Trigger
                  </button>
                ) : (
                  <>
                    <button
                      onClick={() => handleApprove(approval.incident_id)}
                      disabled={approving === approval.incident_id || rejecting === approval.incident_id}
                      className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                      {approving === approval.incident_id ? (
                        <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                      ) : (
                        <CheckCircle className="h-4 w-4" />
                      )}
                      Approve
                    </button>
                    <button
                      onClick={() => handleReject(approval.incident_id)}
                      disabled={approving === approval.incident_id || rejecting === approval.incident_id}
                      className="flex items-center gap-2 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                      {rejecting === approval.incident_id ? (
                        <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                      ) : (
                        <XCircle className="h-4 w-4" />
                      )}
                      Reject
                    </button>
                    <button
                      onClick={() => toggleOverridePanel(approval.incident_id)}
                      className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
                        showOverridePanel === approval.incident_id
                          ? 'bg-blue-100 text-blue-700 border border-blue-300'
                          : 'bg-gray-100 text-gray-700 hover:bg-blue-50 hover:text-blue-600 border border-gray-300'
                      }`}
                    >
                      <Replace className="h-4 w-4" />
                      Override
                      {showOverridePanel === approval.incident_id ? (
                        <ChevronUp className="h-3 w-3" />
                      ) : (
                        <ChevronDown className="h-3 w-3" />
                      )}
                    </button>
                  </>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
