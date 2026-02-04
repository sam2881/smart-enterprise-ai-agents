import { type ClassValue, clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDate(date: string | Date): string {
  return new Date(date).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export function formatRelativeTime(date: string | Date): string {
  const now = new Date()
  const past = new Date(date)
  const diffMs = now.getTime() - past.getTime()
  const diffMins = Math.floor(diffMs / 60000)
  const diffHours = Math.floor(diffMins / 60)
  const diffDays = Math.floor(diffHours / 24)

  if (diffMins < 1) return 'just now'
  if (diffMins < 60) return `${diffMins}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  if (diffDays < 7) return `${diffDays}d ago`
  return formatDate(date)
}

export function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000)
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)

  if (hours > 0) return `${hours}h ${minutes % 60}m`
  if (minutes > 0) return `${minutes}m ${seconds % 60}s`
  return `${seconds}s`
}

export function getPriorityColor(priority: string): string {
  switch (priority) {
    case 'P1':
      return 'bg-red-100 text-red-800 border-red-200'
    case 'P2':
      return 'bg-orange-100 text-orange-800 border-orange-200'
    case 'P3':
      return 'bg-yellow-100 text-yellow-800 border-yellow-200'
    case 'P4':
      return 'bg-green-100 text-green-800 border-green-200'
    default:
      return 'bg-gray-100 text-gray-800 border-gray-200'
  }
}

export function getStatusColor(status: string | undefined | null): string {
  if (!status) {
    return 'bg-gray-100 text-gray-800 border-gray-200'
  }

  switch (status.toLowerCase()) {
    case 'resolved':
    case 'completed':
    case 'approved':
    case '6': // ServiceNow resolved state
      return 'bg-green-100 text-green-800 border-green-200'
    case 'in progress':
    case 'pending':
    case '2': // ServiceNow in progress state
      return 'bg-blue-100 text-blue-800 border-blue-200'
    case 'new':
    case '1': // ServiceNow new state
      return 'bg-purple-100 text-purple-800 border-purple-200'
    case 'rejected':
    case 'failed':
    case 'error':
      return 'bg-red-100 text-red-800 border-red-200'
    default:
      return 'bg-gray-100 text-gray-800 border-gray-200'
  }
}

export function getRiskLevelColor(risk: string): string {
  switch (risk) {
    case 'Critical':
      return 'bg-red-100 text-red-800 border-red-200'
    case 'High':
      return 'bg-orange-100 text-orange-800 border-orange-200'
    case 'Medium':
      return 'bg-yellow-100 text-yellow-800 border-yellow-200'
    case 'Low':
      return 'bg-green-100 text-green-800 border-green-200'
    default:
      return 'bg-gray-100 text-gray-800 border-gray-200'
  }
}

export function truncate(str: string, length: number): string {
  if (str.length <= length) return str
  return str.slice(0, length) + '...'
}

export function getAgentStatusColor(status: string): string {
  switch (status) {
    case 'active':
      return 'text-green-600'
    case 'idle':
      return 'text-blue-600'
    case 'error':
      return 'text-red-600'
    case 'offline':
      return 'text-gray-600'
    default:
      return 'text-gray-600'
  }
}
