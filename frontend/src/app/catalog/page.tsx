'use client'

/**
 * Data Catalog Page
 *
 * Browse and search data assets, business glossary, and tag taxonomy.
 * Backed by data_asset, business_term, and tag_taxonomy tables.
 */

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Search,
  Database,
  Tag,
  BookOpen,
  Filter,
  RefreshCw,
  ExternalLink,
  Shield,
  Layers,
} from 'lucide-react'

// Types matching backend data_asset table
interface DataAsset {
  asset_id: string
  asset_name: string
  asset_type: string
  zone_level: string
  feed_id?: string
  domain?: string
  owner?: string
  row_count?: number
  size_bytes?: number
  description?: string
  tags?: string[]
  last_refreshed_at?: string
  is_active: boolean
}

interface BusinessTerm {
  term_id: string
  term_name: string
  definition: string
  domain?: string
  owner?: string
  synonyms?: string[]
}

type TabId = 'assets' | 'glossary' | 'tags'

const API_BASE = process.env.NEXT_PUBLIC_DATA_AGENT_API || 'http://localhost:8001'

const ZONE_COLORS: Record<string, string> = {
  RAW: 'bg-gray-500/20 text-gray-300 border-gray-500/30',
  BRONZE: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
  SILVER: 'bg-slate-400/20 text-slate-300 border-slate-400/30',
  GOLD: 'bg-yellow-500/20 text-yellow-300 border-yellow-500/30',
  LANDING: 'bg-gray-600/20 text-gray-400 border-gray-600/30',
}

const ASSET_TYPE_ICONS: Record<string, string> = {
  TABLE: 'grid',
  VIEW: 'eye',
  DATASET: 'database',
  FILE: 'file',
}

export default function CatalogPage() {
  const [activeTab, setActiveTab] = useState<TabId>('assets')
  const [searchQuery, setSearchQuery] = useState('')
  const [zoneFilter, setZoneFilter] = useState<string>('')
  const [domainFilter, setDomainFilter] = useState<string>('')

  // Fetch data assets
  const { data: assets = [], isLoading: assetsLoading, refetch: refetchAssets } = useQuery<DataAsset[]>({
    queryKey: ['catalog-assets', searchQuery, zoneFilter, domainFilter],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (searchQuery) params.set('query', searchQuery)
      if (zoneFilter) params.set('zone', zoneFilter)
      if (domainFilter) params.set('domain', domainFilter)
      const res = await fetch(`${API_BASE}/api/v2/catalog/assets?${params}`)
      if (!res.ok) return []
      return res.json()
    },
    enabled: activeTab === 'assets',
  })

  // Fetch business terms
  const { data: terms = [], isLoading: termsLoading } = useQuery<BusinessTerm[]>({
    queryKey: ['catalog-glossary', searchQuery],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (searchQuery) params.set('query', searchQuery)
      const res = await fetch(`${API_BASE}/api/v2/catalog/glossary?${params}`)
      if (!res.ok) return []
      return res.json()
    },
    enabled: activeTab === 'glossary',
  })

  const formatBytes = (bytes?: number) => {
    if (!bytes) return '-'
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`
    if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(1)} MB`
    return `${(bytes / 1073741824).toFixed(1)} GB`
  }

  const formatCount = (count?: number) => {
    if (!count) return '-'
    if (count < 1000) return count.toString()
    if (count < 1000000) return `${(count / 1000).toFixed(1)}K`
    return `${(count / 1000000).toFixed(1)}M`
  }

  const tabs = [
    { id: 'assets' as TabId, label: 'Data Assets', icon: Database, count: assets.length },
    { id: 'glossary' as TabId, label: 'Business Glossary', icon: BookOpen, count: terms.length },
    { id: 'tags' as TabId, label: 'Tag Taxonomy', icon: Tag, count: 0 },
  ]

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      {/* Header */}
      <div className="border-b border-white/10 bg-gray-900/50">
        <div className="max-w-7xl mx-auto px-6 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-white flex items-center gap-3">
                <Database className="w-7 h-7 text-blue-400" />
                Data Catalog
              </h1>
              <p className="text-sm text-gray-400 mt-1">
                Discover and explore data assets across all medallion zones
              </p>
            </div>
            <button
              onClick={() => refetchAssets()}
              className="px-4 py-2 rounded-lg bg-gray-800 text-gray-300 hover:bg-gray-700 flex items-center gap-2 text-sm"
            >
              <RefreshCw className="w-4 h-4" />
              Refresh
            </button>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-6 py-6">
        {/* Tabs */}
        <div className="flex gap-1 p-1 bg-gray-800/30 rounded-lg mb-6">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-2 rounded-md transition-all duration-200 flex items-center gap-2 text-sm ${
                activeTab === tab.id
                  ? 'bg-blue-600/20 text-blue-400 border border-blue-500/30'
                  : 'text-gray-400 hover:text-white hover:bg-gray-700/50'
              }`}
            >
              <tab.icon className="w-4 h-4" />
              {tab.label}
              {tab.count > 0 && (
                <span className="px-1.5 py-0.5 rounded-full bg-gray-700 text-xs">{tab.count}</span>
              )}
            </button>
          ))}
        </div>

        {/* Search & Filters */}
        <div className="flex gap-4 mb-6">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder={activeTab === 'assets' ? 'Search assets by name, domain, or description...' : 'Search business terms...'}
              className="w-full pl-10 pr-4 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
            />
          </div>

          {activeTab === 'assets' && (
            <>
              <select
                value={zoneFilter}
                onChange={(e) => setZoneFilter(e.target.value)}
                className="px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="">All Zones</option>
                <option value="RAW">Raw</option>
                <option value="BRONZE">Bronze</option>
                <option value="SILVER">Silver</option>
                <option value="GOLD">Gold</option>
              </select>

              <select
                value={domainFilter}
                onChange={(e) => setDomainFilter(e.target.value)}
                className="px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="">All Domains</option>
                <option value="sales">Sales</option>
                <option value="finance">Finance</option>
                <option value="customer">Customer</option>
                <option value="product">Product</option>
                <option value="operations">Operations</option>
              </select>
            </>
          )}
        </div>

        {/* Assets Tab */}
        {activeTab === 'assets' && (
          <div className="space-y-3">
            {assetsLoading ? (
              <div className="text-center py-12 text-gray-500">Loading assets...</div>
            ) : assets.length === 0 ? (
              <div className="text-center py-12">
                <Database className="w-12 h-12 text-gray-600 mx-auto mb-3" />
                <p className="text-gray-400">No data assets found</p>
                <p className="text-sm text-gray-600 mt-1">Assets are auto-registered when pipelines run</p>
              </div>
            ) : (
              <div className="grid gap-3">
                {assets.map((asset) => (
                  <div
                    key={asset.asset_id}
                    className="p-4 rounded-lg bg-gray-800/30 border border-white/5 hover:border-white/10 transition-all"
                  >
                    <div className="flex items-start justify-between">
                      <div className="flex-1">
                        <div className="flex items-center gap-3">
                          <h3 className="text-sm font-semibold text-white">{asset.asset_name}</h3>
                          <span className={`px-2 py-0.5 rounded-full text-xs border ${ZONE_COLORS[asset.zone_level] || 'bg-gray-600/20 text-gray-400'}`}>
                            {asset.zone_level}
                          </span>
                          <span className="px-2 py-0.5 rounded-full text-xs bg-gray-700/50 text-gray-400">
                            {asset.asset_type}
                          </span>
                        </div>
                        {asset.description && (
                          <p className="text-xs text-gray-500 mt-1">{asset.description}</p>
                        )}
                        <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                          {asset.domain && <span>Domain: {asset.domain}</span>}
                          {asset.owner && <span>Owner: {asset.owner}</span>}
                          {asset.feed_id && <span>Feed: {asset.feed_id}</span>}
                        </div>
                      </div>
                      <div className="text-right text-xs text-gray-500 space-y-1">
                        <div>Rows: {formatCount(asset.row_count)}</div>
                        <div>Size: {formatBytes(asset.size_bytes)}</div>
                        {asset.last_refreshed_at && (
                          <div>Updated: {new Date(asset.last_refreshed_at).toLocaleDateString()}</div>
                        )}
                      </div>
                    </div>
                    {asset.tags && asset.tags.length > 0 && (
                      <div className="flex gap-1.5 mt-2">
                        {asset.tags.map((tag) => (
                          <span key={tag} className="px-2 py-0.5 rounded-full text-xs bg-blue-500/10 text-blue-400 border border-blue-500/20">
                            {tag}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Glossary Tab */}
        {activeTab === 'glossary' && (
          <div className="space-y-3">
            {termsLoading ? (
              <div className="text-center py-12 text-gray-500">Loading glossary...</div>
            ) : terms.length === 0 ? (
              <div className="text-center py-12">
                <BookOpen className="w-12 h-12 text-gray-600 mx-auto mb-3" />
                <p className="text-gray-400">No business terms defined</p>
                <p className="text-sm text-gray-600 mt-1">Business glossary maps technical columns to business definitions</p>
              </div>
            ) : (
              <div className="grid gap-3">
                {terms.map((term) => (
                  <div
                    key={term.term_id}
                    className="p-4 rounded-lg bg-gray-800/30 border border-white/5"
                  >
                    <h3 className="text-sm font-semibold text-white">{term.term_name}</h3>
                    <p className="text-xs text-gray-400 mt-1">{term.definition}</p>
                    <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                      {term.domain && <span>Domain: {term.domain}</span>}
                      {term.owner && <span>Owner: {term.owner}</span>}
                      {term.synonyms && term.synonyms.length > 0 && (
                        <span>Synonyms: {term.synonyms.join(', ')}</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Tags Tab */}
        {activeTab === 'tags' && (
          <div className="text-center py-12">
            <Tag className="w-12 h-12 text-gray-600 mx-auto mb-3" />
            <p className="text-gray-400">Tag Taxonomy</p>
            <p className="text-sm text-gray-600 mt-1">
              Hierarchical classification: SENSITIVITY, DOMAIN, QUALITY, COMPLIANCE
            </p>
            <div className="grid grid-cols-4 gap-4 mt-6 max-w-2xl mx-auto">
              {['SENSITIVITY', 'DOMAIN', 'QUALITY', 'COMPLIANCE'].map((category) => (
                <div key={category} className="p-4 rounded-lg bg-gray-800/30 border border-white/5">
                  <Shield className="w-5 h-5 text-blue-400 mx-auto mb-2" />
                  <p className="text-xs font-medium text-gray-300">{category}</p>
                  <p className="text-xs text-gray-600 mt-1">
                    {category === 'SENSITIVITY' && 'PII, PHI, PCI, Public'}
                    {category === 'DOMAIN' && 'Sales, Finance, Customer'}
                    {category === 'QUALITY' && 'Certified, Validated, Raw'}
                    {category === 'COMPLIANCE' && 'GDPR, HIPAA, SOX'}
                  </p>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
