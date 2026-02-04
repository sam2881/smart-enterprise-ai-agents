'use client'

import React, { useState } from 'react'
import { X } from 'lucide-react'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'

interface CreateIncidentModalProps {
  isOpen: boolean
  onClose: () => void
  onSubmit?: (data: IncidentData) => void
  onSuccess?: () => void
}

interface IncidentData {
  title: string
  description: string
  severity: string
  category: string
}

export function CreateIncidentModal({ isOpen, onClose, onSubmit, onSuccess }: CreateIncidentModalProps) {
  const [formData, setFormData] = useState<IncidentData>({
    title: '',
    description: '',
    severity: 'medium',
    category: 'infrastructure',
  })

  if (!isOpen) return null

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (onSubmit) {
      onSubmit(formData)
    }
    if (onSuccess) {
      onSuccess()
    }
    onClose()
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className="relative bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-md p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Create Incident</h2>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700">
            <X className="w-5 h-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <Input
            label="Title"
            value={formData.title}
            onChange={(e) => setFormData({ ...formData, title: e.target.value })}
            placeholder="Enter incident title"
            required
          />

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Description
            </label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md
                dark:bg-gray-800 dark:text-white focus:ring-2 focus:ring-blue-500"
              rows={4}
              placeholder="Describe the incident"
              required
            />
          </div>

          <Select
            label="Severity"
            value={formData.severity}
            onChange={(e) => setFormData({ ...formData, severity: e.target.value })}
            options={[
              { value: 'low', label: 'Low' },
              { value: 'medium', label: 'Medium' },
              { value: 'high', label: 'High' },
              { value: 'critical', label: 'Critical' },
            ]}
          />

          <Select
            label="Category"
            value={formData.category}
            onChange={(e) => setFormData({ ...formData, category: e.target.value })}
            options={[
              { value: 'infrastructure', label: 'Infrastructure' },
              { value: 'application', label: 'Application' },
              { value: 'database', label: 'Database' },
              { value: 'network', label: 'Network' },
              { value: 'security', label: 'Security' },
            ]}
          />

          <div className="flex gap-3 pt-4">
            <Button type="button" variant="outline" onClick={onClose} className="flex-1">
              Cancel
            </Button>
            <Button type="submit" className="flex-1">
              Create Incident
            </Button>
          </div>
        </form>
      </div>
    </div>
  )
}

export default CreateIncidentModal
