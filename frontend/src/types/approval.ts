/**
 * Approval Types
 *
 * Types for HITL approval workflow.
 */

export interface ApprovalRequest {
  id: string
  incident_id: string
  workflow_id?: string
  status: 'pending' | 'approved' | 'rejected'
  plan?: any
  judge_score?: any
  approval_decision?: any
  created_at?: string
  approval_token?: string
  requires_action?: 'approval' | 'retry'
  error?: string
}

// Alias for compatibility with LangGraphApprovalList
export type Approval = ApprovalRequest

export interface ApproveRoutingRequest {
  approved_agent: string
  reason?: string
}

export interface ApprovePlanRequest {
  approver: string
  reason?: string
}

export interface RejectPlanRequest {
  approver: string
  reason: string
}
