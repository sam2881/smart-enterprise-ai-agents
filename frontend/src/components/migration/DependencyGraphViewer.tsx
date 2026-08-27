'use client'

import { useEffect, useRef, useState } from 'react'
import { Code2, X, ChevronRight, Lock } from 'lucide-react'
import type { DependencyGraph, GraphNode, MigrationArtifact } from '@/types/migration'

interface Props {
  graph: DependencyGraph
  artifacts?: MigrationArtifact[]
}

const LEVEL_COLORS: Record<number, { bg: string; border: string; text: string }> = {
  0: { bg: 'bg-emerald-500/20', border: 'border-emerald-500/40', text: 'text-emerald-300' },
  1: { bg: 'bg-blue-500/20', border: 'border-blue-500/40', text: 'text-blue-300' },
  2: { bg: 'bg-purple-500/20', border: 'border-purple-500/40', text: 'text-purple-300' },
  3: { bg: 'bg-amber-500/20', border: 'border-amber-500/40', text: 'text-amber-300' },
}
const DEFAULT_COLOR = { bg: 'bg-gray-700/40', border: 'border-gray-500/40', text: 'text-gray-300' }

export function DependencyGraphViewer({ graph, artifacts = [] }: Props) {
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)

  // Group nodes by topo_level
  const byLevel: Record<number, GraphNode[]> = {}
  for (const node of graph.nodes) {
    if (!byLevel[node.topo_level]) byLevel[node.topo_level] = []
    byLevel[node.topo_level].push(node)
  }
  const levels = Object.keys(byLevel)
    .map(Number)
    .sort((a, b) => a - b)

  // Artifact lookup by object_id
  const artifactByFqn: Record<string, MigrationArtifact[]> = {}
  for (const art of artifacts) {
    const key = `${art.object_schema}.${art.object_name}`
    if (!artifactByFqn[key]) artifactByFqn[key] = []
    artifactByFqn[key].push(art)
  }

  const selectedArtifacts = selectedNode ? (artifactByFqn[selectedNode.id] || []) : []

  return (
    <div className="flex gap-4 h-full min-h-[500px]">
      {/* Graph panel */}
      <div className="flex-1 overflow-auto rounded-xl bg-gray-900/50 border border-white/10 p-4">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-sm font-semibold text-white">
            Dependency Graph
            <span className="ml-2 text-xs text-gray-500">
              {graph.node_count} nodes · {graph.edge_count} edges · depth {graph.max_depth}
            </span>
          </h3>
          {graph.has_cycles && (
            <span className="text-xs px-2 py-0.5 rounded-full bg-amber-500/20 text-amber-400 border border-amber-500/30">
              Cycles detected
            </span>
          )}
        </div>

        {/* Level legend */}
        <div className="flex gap-2 mb-4 flex-wrap">
          {levels.map((lvl) => {
            const color = LEVEL_COLORS[lvl] || DEFAULT_COLOR
            return (
              <span key={lvl} className={`text-xs px-2 py-0.5 rounded-full border ${color.bg} ${color.border} ${color.text}`}>
                Level {lvl} — runs {lvl === 0 ? 'first (leaf)' : `after level ${lvl - 1}`}
              </span>
            )
          })}
        </div>

        {/* Levels top to bottom — highest level (most upstream) first visually */}
        <div className="space-y-4">
          {[...levels].reverse().map((lvl) => {
            const color = LEVEL_COLORS[lvl] || DEFAULT_COLOR
            const nodes = byLevel[lvl]
            return (
              <div key={lvl}>
                <div className="text-xs text-gray-600 mb-2 font-mono">Level {lvl}</div>
                <div className="flex gap-2 flex-wrap">
                  {nodes.map((node) => (
                    <button
                      key={node.id}
                      onClick={() => setSelectedNode(node.id === selectedNode?.id ? null : node)}
                      className={`flex items-center gap-1.5 px-3 py-2 rounded-lg border text-xs font-mono transition-all ${color.bg} ${color.border} ${color.text} ${
                        selectedNode?.id === node.id ? 'ring-2 ring-white/30' : 'hover:brightness-125'
                      }`}
                    >
                      {node.is_encrypted && <Lock className="w-3 h-3 opacity-60" />}
                      <span>{node.schema}.</span>
                      <span className="font-semibold">{node.name}</span>
                      <span className="opacity-50 ml-1">[{node.object_type[0]}]</span>
                      {artifactByFqn[node.id]?.length > 0 && (
                        <Code2 className="w-3 h-3 text-green-400 ml-1" />
                      )}
                    </button>
                  ))}
                </div>
                {lvl > 0 && (
                  <div className="flex justify-center mt-2 text-gray-700">
                    <ChevronRight className="w-4 h-4 rotate-90" />
                  </div>
                )}
              </div>
            )
          })}
        </div>

        {/* Execution order */}
        {graph.execution_order.length > 0 && (
          <div className="mt-6 pt-4 border-t border-white/5">
            <p className="text-xs text-gray-500 mb-2">Execution order (leaf → root)</p>
            <div className="flex flex-wrap gap-1">
              {graph.execution_order.map((fqn, i) => (
                <span key={fqn} className="text-xs font-mono text-gray-500">
                  {i > 0 && <span className="mx-1 text-gray-700">→</span>}
                  {fqn}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Definition / artifact drawer */}
      {selectedNode && (
        <div className="w-96 rounded-xl bg-gray-900/50 border border-white/10 flex flex-col">
          <div className="flex items-start justify-between p-4 border-b border-white/5">
            <div>
              <h4 className="text-sm font-semibold text-white font-mono">{selectedNode.id}</h4>
              <div className="flex gap-2 mt-1 text-xs text-gray-500">
                <span>{selectedNode.object_type}</span>
                <span>·</span>
                <span>{selectedNode.db_platform}</span>
                <span>·</span>
                <span>Level {selectedNode.topo_level}</span>
              </div>
            </div>
            <button
              onClick={() => setSelectedNode(null)}
              className="p-1 rounded hover:bg-white/10 text-gray-500 hover:text-white"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          <div className="p-4 space-y-3 flex-1 overflow-auto">
            <div className="flex gap-4 text-xs text-gray-400">
              <span>{selectedNode.param_count} params</span>
              <span>{selectedNode.char_count.toLocaleString()} chars</span>
              {selectedNode.is_encrypted && (
                <span className="flex items-center gap-1 text-amber-400">
                  <Lock className="w-3 h-3" /> Encrypted
                </span>
              )}
            </div>

            {selectedArtifacts.length > 0 ? (
              <div className="space-y-3">
                <p className="text-xs text-gray-500 font-medium">{selectedArtifacts.length} artifact(s) generated</p>
                {selectedArtifacts.map((art) => (
                  <ArtifactCard key={art.artifact_id} artifact={art} />
                ))}
              </div>
            ) : (
              <div className="text-xs text-gray-600 italic">No artifacts generated yet</div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

function ArtifactCard({ artifact }: { artifact: MigrationArtifact }) {
  const [expanded, setExpanded] = useState(false)

  const confidenceColor =
    (artifact.confidence_score ?? 0) >= 0.8
      ? 'text-green-400'
      : (artifact.confidence_score ?? 0) >= 0.5
      ? 'text-yellow-400'
      : 'text-red-400'

  return (
    <div className="rounded-lg bg-gray-800/40 border border-white/5 overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-3 py-2 text-xs hover:bg-white/5 transition-colors"
      >
        <div className="flex items-center gap-2">
          <Code2 className="w-3.5 h-3.5 text-blue-400" />
          <span className="text-gray-300 font-medium">{artifact.artifact_type}</span>
        </div>
        <div className="flex items-center gap-3 text-gray-500">
          {artifact.confidence_score != null && (
            <span className={confidenceColor}>
              {(artifact.confidence_score * 100).toFixed(0)}% confidence
            </span>
          )}
          <span className={expanded ? 'rotate-90' : ''} style={{ display: 'inline-block', transition: 'transform 0.2s' }}>›</span>
        </div>
      </button>
      {expanded && artifact.artifact_content && (
        <pre className="text-[10px] text-gray-400 bg-gray-950/60 p-3 overflow-auto max-h-64 border-t border-white/5 leading-relaxed">
          {artifact.artifact_content}
        </pre>
      )}
    </div>
  )
}
