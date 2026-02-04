/**
 * Incident Types
 *
 * Types for incident management.
 */

export type IncidentPriority = 'P1' | 'P2' | 'P3' | 'P4'

export type IncidentStatus =
  | 'New'
  | 'In Progress'
  | 'On Hold'
  | 'Resolved'
  | 'Closed'
  | 'pending_approval'

export interface Incident {
  incident_id: string
  short_description: string
  description: string
  priority: IncidentPriority
  status: IncidentStatus
  category?: string
  assigned_to?: string
  created_at: string
  updated_at?: string
  affected_service?: string
}

export interface CreateIncidentRequest {
  short_description: string
  description: string
  priority: IncidentPriority
  category?: string
  affected_service?: string
}
