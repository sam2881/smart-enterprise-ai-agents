'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { EnterpriseIncidentDetail } from '@/components/incidents/EnterpriseIncidentDetail'

interface PageProps {
  params: { id: string }
}

export default function IncidentDetailPage({ params }: PageProps) {
  const router = useRouter()
  const incidentId = params.id
  const [incident, setIncident] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchIncident = async () => {
      try {
        setLoading(true)
        // Use relative URL to leverage Next.js API proxy
        const response = await fetch('/api/incidents')

        if (!response.ok) {
          throw new Error(`Failed to fetch: ${response.status}`)
        }

        const data = await response.json()
        const found = data.incidents?.find((inc: any) =>
          inc.incident_id === incidentId ||
          inc.number === incidentId ||
          inc.sys_id === incidentId
        )

        if (!found) {
          throw new Error(`Incident ${incidentId} not found`)
        }

        setIncident(found)
      } catch (err: any) {
        console.error('Fetch error:', err)
        setError(err.message || 'Failed to load')
      } finally {
        setLoading(false)
      }
    }

    fetchIncident()
  }, [incidentId])

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-900">
        <div className="flex flex-col items-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
          <p className="mt-4 text-gray-400">Loading {incidentId}...</p>
        </div>
      </div>
    )
  }

  if (error || !incident) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-900">
        <div className="text-center">
          <p className="text-red-400 mb-4">{error || 'Not found'}</p>
          <button
            onClick={() => router.push('/incidents')}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded"
          >
            Back to Incidents
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-900">
      <EnterpriseIncidentDetail incident={incident} />
    </div>
  )
}
