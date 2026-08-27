'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ArrowRightLeft, RefreshCw, Clock, CheckCircle, AlertCircle, Loader2 } from 'lucide-react'
import { MigrationWizard } from '@/components/migration/MigrationWizard'
import { DependencyGraphViewer } from '@/components/migration/DependencyGraphViewer'
import type { MigrationJob } from '@/types/migration'

const API_BASE = process.env.NEXT_PUBLIC_DATA_AGENT_API || 'http://localhost:8001'

export default function MigrationPage() {
  const [activeJobId, setActiveJobId] = useState<string | null>(null)

  const { data: jobDetail, refetch: refetchJob } = useQuery<MigrationJob>({
    queryKey: ['migration-job-detail', activeJobId],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/migration/jobs/${activeJobId}?include_artifacts=true`)
      if (!res.ok) throw new Error('Failed to fetch job')
      return res.json()
    },
    enabled: !!activeJobId,
    refetchInterval: (query) => {
      const d = (query as any).state?.data as MigrationJob | undefined
      if (!d) return 3000
      return ['COMPLETE', 'FAILED'].includes(d.status) ? false : 3000
    },
  })

  const { data: jobList, refetch: refetchList } = useQuery<{ jobs: MigrationJob[] }>({
    queryKey: ['migration-jobs-list'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/migration/jobs?limit=20`)
      if (!res.ok) return { jobs: [] }
      return res.json()
    },
    refetchInterval: 15000,
  })

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      {/* Header */}
      <div className="border-b border-white/10 bg-gray-900/50">
        <div className="max-w-7xl mx-auto px-6 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-white flex items-center gap-3">
                <ArrowRightLeft className="w-7 h-7 text-purple-400" />
                Legacy Migration
              </h1>
              <p className="text-sm text-gray-400 mt-1">
                Migrate SSIS packages and stored procedures to PySpark + Airflow
              </p>
            </div>
            <button
              onClick={() => { refetchJob(); refetchList() }}
              className="px-4 py-2 rounded-lg bg-gray-800 text-gray-300 hover:bg-gray-700 flex items-center gap-2 text-sm"
            >
              <RefreshCw className="w-4 h-4" />
              Refresh
            </button>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-6 py-6 grid grid-cols-[380px,1fr] gap-6 items-start">
        {/* Left panel: wizard + recent jobs */}
        <div className="space-y-6">
          <MigrationWizard onJobCreated={(id) => setActiveJobId(id)} />

          {/* Recent jobs */}
          <div className="bg-gray-900/60 border border-white/10 rounded-xl p-4">
            <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">
              Recent Jobs
            </h3>
            {!jobList?.jobs?.length ? (
              <p className="text-xs text-gray-600 text-center py-4">No migration jobs yet</p>
            ) : (
              <div className="space-y-2">
                {jobList.jobs.map((job) => (
                  <button
                    key={job.job_id}
                    onClick={() => setActiveJobId(job.job_id)}
                    className={`w-full flex items-center gap-3 p-3 rounded-lg text-left transition-all ${
                      activeJobId === job.job_id
                        ? 'bg-purple-500/10 border border-purple-500/30'
                        : 'bg-gray-800/30 border border-white/5 hover:border-white/10'
                    }`}
                  >
                    <StatusIcon status={job.status} />
                    <div className="flex-1 min-w-0">
                      <div className="text-xs font-mono text-gray-300 truncate">{job.job_id.slice(0, 16)}…</div>
                      <div className="text-xs text-gray-600 mt-0.5 flex gap-2">
                        <span>{job.status}</span>
                        <span>·</span>
                        <span>{job.extracted_objects}/{job.total_objects} procs</span>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Right panel: graph + artifacts */}
        <div>
          {!activeJobId ? (
            <div className="rounded-xl border border-white/10 bg-gray-900/40 flex items-center justify-center h-96">
              <div className="text-center">
                <ArrowRightLeft className="w-12 h-12 text-gray-700 mx-auto mb-3" />
                <p className="text-gray-500">Select or create a migration job</p>
                <p className="text-xs text-gray-700 mt-1">
                  Use the wizard on the left to start extracting stored procedures
                </p>
              </div>
            </div>
          ) : !jobDetail ? (
            <div className="rounded-xl border border-white/10 bg-gray-900/40 flex items-center justify-center h-96">
              <Loader2 className="w-8 h-8 animate-spin text-gray-600" />
            </div>
          ) : (
            <div className="space-y-4">
              {/* Job summary bar */}
              <div className="rounded-xl bg-gray-900/60 border border-white/10 p-4 flex items-center gap-6">
                <StatusIcon status={jobDetail.status} large />
                <div>
                  <div className="text-sm font-semibold text-white">{jobDetail.status}</div>
                  <div className="text-xs text-gray-500 font-mono">{jobDetail.job_id}</div>
                </div>
                <div className="ml-auto flex gap-6 text-center">
                  <div>
                    <div className="text-lg font-bold text-white">{jobDetail.extracted_objects}</div>
                    <div className="text-xs text-gray-500">Extracted</div>
                  </div>
                  <div>
                    <div className="text-lg font-bold text-amber-400">{jobDetail.skipped_objects}</div>
                    <div className="text-xs text-gray-500">Skipped</div>
                  </div>
                  <div>
                    <div className="text-lg font-bold text-red-400">{jobDetail.failed_objects}</div>
                    <div className="text-xs text-gray-500">Failed</div>
                  </div>
                  <div>
                    <div className="text-lg font-bold text-blue-400">{jobDetail.artifacts?.length ?? 0}</div>
                    <div className="text-xs text-gray-500">Artifacts</div>
                  </div>
                </div>
              </div>

              {/* Dependency graph */}
              {jobDetail.dependency_graph && jobDetail.dependency_graph.node_count > 0 ? (
                <DependencyGraphViewer
                  graph={jobDetail.dependency_graph as any}
                  artifacts={jobDetail.artifacts || []}
                />
              ) : (
                <div className="rounded-xl border border-white/10 bg-gray-900/40 flex items-center justify-center h-64">
                  {['PENDING', 'EXTRACTING'].includes(jobDetail.status) ? (
                    <div className="text-center">
                      <Loader2 className="w-8 h-8 animate-spin text-blue-400 mx-auto mb-2" />
                      <p className="text-xs text-gray-500">Extracting procedures…</p>
                    </div>
                  ) : (
                    <p className="text-xs text-gray-600">No dependency graph data</p>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function StatusIcon({ status, large }: { status: string; large?: boolean }) {
  const size = large ? 'w-6 h-6' : 'w-4 h-4'
  if (status === 'COMPLETE') return <CheckCircle className={`${size} text-green-400`} />
  if (status === 'FAILED') return <AlertCircle className={`${size} text-red-400`} />
  if (['PENDING', 'EXTRACTING', 'GRAPH_BUILDING', 'LLM_GENERATING'].includes(status))
    return <Loader2 className={`${size} animate-spin text-blue-400`} />
  return <Clock className={`${size} text-gray-500`} />
}
