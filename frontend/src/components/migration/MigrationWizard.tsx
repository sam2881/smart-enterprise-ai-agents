'use client'

import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Database,
  ArrowRight,
  CheckCircle,
  Loader2,
  AlertCircle,
  GitBranch,
  Code2,
} from 'lucide-react'
import type { MigrationJob, MigrationJobRequest } from '@/types/migration'

const API_BASE = process.env.NEXT_PUBLIC_DATA_AGENT_API || 'http://localhost:8001'

const STEPS = [
  { id: 1, label: 'Connect DB', icon: Database },
  { id: 2, label: 'Extract', icon: GitBranch },
  { id: 3, label: 'Review Graph', icon: GitBranch },
  { id: 4, label: 'Generate', icon: Code2 },
]

interface Props {
  onJobCreated: (jobId: string) => void
}

export function MigrationWizard({ onJobCreated }: Props) {
  const [step, setStep] = useState(1)
  const [form, setForm] = useState<MigrationJobRequest>({
    connection_code: '',
    schema_filter: '%',
    proc_name_pattern: '%',
    dtsx_source_path: '',
    created_by: '',
  })
  const [activeJobId, setActiveJobId] = useState<string | null>(null)

  const queryClient = useQueryClient()

  const createMutation = useMutation({
    mutationFn: async (req: MigrationJobRequest) => {
      const res = await fetch(`${API_BASE}/migration/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || 'Failed to create migration job')
      }
      return res.json() as Promise<MigrationJob>
    },
    onSuccess: (job) => {
      setActiveJobId(job.job_id)
      onJobCreated(job.job_id)
      setStep(2)
    },
  })

  // Poll job status while running
  const { data: jobStatus } = useQuery<MigrationJob>({
    queryKey: ['migration-job', activeJobId],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/migration/jobs/${activeJobId}`)
      if (!res.ok) throw new Error('Failed to fetch job status')
      return res.json()
    },
    enabled: !!activeJobId && step <= 3,
    refetchInterval: (query) => {
      const d = (query as any).state?.data as MigrationJob | undefined
      if (!d) return 2000
      return ['COMPLETE', 'FAILED'].includes(d.status) ? false : 2000
    },
  })

  // Advance wizard step when job status changes
  if (jobStatus) {
    if (step === 2 && jobStatus.status === 'GRAPH_BUILDING') setStep(3)
    if (step === 3 && jobStatus.status === 'COMPLETE') setStep(4)
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    createMutation.mutate(form)
  }

  return (
    <div className="bg-gray-900/60 border border-white/10 rounded-xl p-6">
      {/* Step indicator */}
      <div className="flex items-center gap-2 mb-8">
        {STEPS.map((s, idx) => (
          <div key={s.id} className="flex items-center gap-2">
            <div className={`flex items-center justify-center w-8 h-8 rounded-full text-xs font-semibold transition-all ${
              step > s.id
                ? 'bg-green-500/20 text-green-400 border border-green-500/30'
                : step === s.id
                ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30'
                : 'bg-gray-800 text-gray-600 border border-white/5'
            }`}>
              {step > s.id ? <CheckCircle className="w-4 h-4" /> : s.id}
            </div>
            <span className={`text-xs ${step >= s.id ? 'text-gray-300' : 'text-gray-600'}`}>
              {s.label}
            </span>
            {idx < STEPS.length - 1 && (
              <div className={`h-px w-8 ${step > s.id ? 'bg-green-500/40' : 'bg-white/10'}`} />
            )}
          </div>
        ))}
      </div>

      {/* Step 1: Connection form */}
      {step === 1 && (
        <form onSubmit={handleSubmit} className="space-y-4">
          <h3 className="text-sm font-semibold text-white mb-4">Connect to Legacy Database</h3>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Connection Code *</label>
            <input
              required
              value={form.connection_code}
              onChange={(e) => setForm({ ...form, connection_code: e.target.value })}
              placeholder="e.g. prod_sqlserver_dw"
              className="w-full px-3 py-2 rounded-lg bg-gray-800/50 text-white border border-white/10 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/50"
            />
            <p className="text-xs text-gray-600 mt-1">Must exist in connection_registry</p>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-gray-400 mb-1">Schema Filter</label>
              <input
                value={form.schema_filter}
                onChange={(e) => setForm({ ...form, schema_filter: e.target.value })}
                placeholder="% or dbo"
                className="w-full px-3 py-2 rounded-lg bg-gray-800/50 text-white border border-white/10 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/50"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-400 mb-1">Procedure Name Pattern</label>
              <input
                value={form.proc_name_pattern}
                onChange={(e) => setForm({ ...form, proc_name_pattern: e.target.value })}
                placeholder="% or usp_%"
                className="w-full px-3 py-2 rounded-lg bg-gray-800/50 text-white border border-white/10 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/50"
              />
            </div>
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">DTSX Source Path (optional)</label>
            <input
              value={form.dtsx_source_path || ''}
              onChange={(e) => setForm({ ...form, dtsx_source_path: e.target.value })}
              placeholder="gs://bucket/packages/etl.dtsx"
              className="w-full px-3 py-2 rounded-lg bg-gray-800/50 text-white border border-white/10 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/50"
            />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Created By</label>
            <input
              value={form.created_by || ''}
              onChange={(e) => setForm({ ...form, created_by: e.target.value })}
              placeholder="you@company.com"
              className="w-full px-3 py-2 rounded-lg bg-gray-800/50 text-white border border-white/10 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/50"
            />
          </div>
          {createMutation.error && (
            <div className="flex items-center gap-2 p-3 rounded-lg bg-red-500/10 border border-red-500/20">
              <AlertCircle className="w-4 h-4 text-red-400 shrink-0" />
              <span className="text-xs text-red-300">{(createMutation.error as Error).message}</span>
            </div>
          )}
          <button
            type="submit"
            disabled={createMutation.isPending || !form.connection_code}
            className="w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-sm font-medium transition-colors"
          >
            {createMutation.isPending ? (
              <><Loader2 className="w-4 h-4 animate-spin" /> Starting extraction...</>
            ) : (
              <><Database className="w-4 h-4" /> Start Extraction <ArrowRight className="w-4 h-4" /></>
            )}
          </button>
        </form>
      )}

      {/* Step 2: Extracting */}
      {step === 2 && jobStatus && (
        <div className="space-y-4">
          <h3 className="text-sm font-semibold text-white">Extracting Stored Procedures</h3>
          <StatusBar job={jobStatus} />
          <div className="text-xs text-gray-500 text-center">
            Job ID: <span className="text-gray-400 font-mono">{activeJobId}</span>
          </div>
        </div>
      )}

      {/* Step 3: Graph ready */}
      {step === 3 && jobStatus && (
        <div className="space-y-4">
          <h3 className="text-sm font-semibold text-white">Dependency Graph Ready</h3>
          <StatusBar job={jobStatus} />
          <div className="grid grid-cols-3 gap-3 text-center">
            <Stat label="Objects" value={jobStatus.extracted_objects} />
            <Stat label="Skipped" value={jobStatus.skipped_objects} />
            <Stat label="Failed" value={jobStatus.failed_objects} color="text-red-400" />
          </div>
          <p className="text-xs text-gray-500 text-center">
            Generating PySpark artifacts with Claude…
          </p>
        </div>
      )}

      {/* Step 4: Complete */}
      {step === 4 && jobStatus && (
        <div className="space-y-4">
          <div className="flex items-center gap-2">
            {jobStatus.status === 'COMPLETE' ? (
              <CheckCircle className="w-5 h-5 text-green-400" />
            ) : (
              <AlertCircle className="w-5 h-5 text-red-400" />
            )}
            <h3 className="text-sm font-semibold text-white">
              {jobStatus.status === 'COMPLETE' ? 'Migration Complete' : 'Migration Failed'}
            </h3>
          </div>
          <div className="grid grid-cols-3 gap-3 text-center">
            <Stat label="Objects" value={jobStatus.extracted_objects} />
            <Stat label="Artifacts" value={jobStatus.artifacts?.length ?? 0} color="text-blue-400" />
            <Stat label="Failed" value={jobStatus.failed_objects} color="text-red-400" />
          </div>
          {jobStatus.error_message && (
            <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/20 text-xs text-red-300">
              {jobStatus.error_message}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function StatusBar({ job }: { job: MigrationJob }) {
  const statusColors: Record<string, string> = {
    PENDING: 'text-gray-400',
    EXTRACTING: 'text-blue-400',
    GRAPH_BUILDING: 'text-purple-400',
    LLM_GENERATING: 'text-yellow-400',
    COMPLETE: 'text-green-400',
    FAILED: 'text-red-400',
  }

  const isRunning = !['COMPLETE', 'FAILED'].includes(job.status)

  return (
    <div className="flex items-center gap-2 p-3 rounded-lg bg-gray-800/40 border border-white/5">
      {isRunning && <Loader2 className="w-4 h-4 animate-spin text-blue-400" />}
      <span className={`text-xs font-medium ${statusColors[job.status] || 'text-gray-400'}`}>
        {job.status}
      </span>
      {job.total_objects > 0 && (
        <span className="ml-auto text-xs text-gray-500">
          {job.extracted_objects}/{job.total_objects} extracted
        </span>
      )}
    </div>
  )
}

function Stat({ label, value, color = 'text-white' }: { label: string; value: number; color?: string }) {
  return (
    <div className="p-3 rounded-lg bg-gray-800/40 border border-white/5">
      <div className={`text-lg font-bold ${color}`}>{value}</div>
      <div className="text-xs text-gray-500">{label}</div>
    </div>
  )
}
