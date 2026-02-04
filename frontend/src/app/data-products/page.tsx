'use client'

/**
 * Data Products Page
 *
 * Data mesh product registry with subscription workflow.
 * Backed by data_product and data_product_subscription tables.
 */

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Package,
  Search,
  RefreshCw,
  Users,
  Clock,
  CheckCircle,
  AlertCircle,
  Star,
  ArrowRight,
} from 'lucide-react'

interface DataProduct {
  product_id: string
  product_name: string
  domain: string
  owner: string
  description?: string
  sla_freshness_hours?: number
  sla_quality_score?: number
  version: string
  status: 'DRAFT' | 'PUBLISHED' | 'DEPRECATED'
  output_assets?: string[]
  input_feeds?: string[]
  consumers?: Array<{ principal: string; access_level: string }>
  published_at?: string
  subscriber_count?: number
}

interface Subscription {
  subscription_id: string
  product_id: string
  product_name?: string
  subscriber: string
  status: 'PENDING' | 'APPROVED' | 'REJECTED'
  requested_at: string
  approved_by?: string
  approved_at?: string
}

const API_BASE = process.env.NEXT_PUBLIC_DATA_AGENT_API || 'http://localhost:8001'

const STATUS_STYLES: Record<string, string> = {
  DRAFT: 'bg-gray-500/20 text-gray-300 border-gray-500/30',
  PUBLISHED: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
  DEPRECATED: 'bg-red-500/20 text-red-300 border-red-500/30',
}

const SUB_STATUS_STYLES: Record<string, string> = {
  PENDING: 'bg-yellow-500/20 text-yellow-300 border-yellow-500/30',
  APPROVED: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
  REJECTED: 'bg-red-500/20 text-red-300 border-red-500/30',
}

type TabId = 'products' | 'subscriptions'

export default function DataProductsPage() {
  const [activeTab, setActiveTab] = useState<TabId>('products')
  const [searchQuery, setSearchQuery] = useState('')
  const [domainFilter, setDomainFilter] = useState<string>('')
  const queryClient = useQueryClient()

  // Fetch data products
  const { data: products = [], isLoading: productsLoading, refetch } = useQuery<DataProduct[]>({
    queryKey: ['data-products', domainFilter],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (domainFilter) params.set('domain', domainFilter)
      const res = await fetch(`${API_BASE}/api/v2/data-products?${params}`)
      if (!res.ok) return []
      return res.json()
    },
    enabled: activeTab === 'products',
  })

  // Fetch subscriptions
  const { data: subscriptions = [], isLoading: subsLoading } = useQuery<Subscription[]>({
    queryKey: ['data-product-subscriptions'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/v2/data-products/subscriptions`)
      if (!res.ok) return []
      return res.json()
    },
    enabled: activeTab === 'subscriptions',
  })

  // Subscribe mutation
  const subscribeMutation = useMutation({
    mutationFn: async ({ productId, subscriber }: { productId: string; subscriber: string }) => {
      const res = await fetch(`${API_BASE}/api/v2/data-products/${productId}/subscribe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ subscriber }),
      })
      if (!res.ok) throw new Error('Failed to subscribe')
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['data-products'] })
      queryClient.invalidateQueries({ queryKey: ['data-product-subscriptions'] })
    },
  })

  const filteredProducts = searchQuery
    ? products.filter(
        (p) =>
          p.product_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
          p.domain.toLowerCase().includes(searchQuery.toLowerCase()) ||
          p.description?.toLowerCase().includes(searchQuery.toLowerCase())
      )
    : products

  const tabs = [
    { id: 'products' as TabId, label: 'Products', icon: Package, count: products.length },
    { id: 'subscriptions' as TabId, label: 'Subscriptions', icon: Users, count: subscriptions.length },
  ]

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      {/* Header */}
      <div className="border-b border-white/10 bg-gray-900/50">
        <div className="max-w-7xl mx-auto px-6 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-white flex items-center gap-3">
                <Package className="w-7 h-7 text-purple-400" />
                Data Products
              </h1>
              <p className="text-sm text-gray-400 mt-1">
                Data mesh product registry — publish, discover, and subscribe
              </p>
            </div>
            <button
              onClick={() => refetch()}
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
                  ? 'bg-purple-600/20 text-purple-400 border border-purple-500/30'
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

        {/* Search */}
        {activeTab === 'products' && (
          <div className="flex gap-4 mb-6">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search products by name, domain, or description..."
                className="w-full pl-10 pr-4 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 focus:outline-none focus:ring-2 focus:ring-purple-500/50"
              />
            </div>
            <select
              value={domainFilter}
              onChange={(e) => setDomainFilter(e.target.value)}
              className="px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 focus:outline-none focus:ring-2 focus:ring-purple-500/50"
              style={{ colorScheme: 'dark' }}
            >
              <option value="">All Domains</option>
              <option value="sales">Sales</option>
              <option value="finance">Finance</option>
              <option value="customer">Customer</option>
              <option value="product">Product</option>
              <option value="operations">Operations</option>
            </select>
          </div>
        )}

        {/* Products Tab */}
        {activeTab === 'products' && (
          <div className="space-y-3">
            {productsLoading ? (
              <div className="text-center py-12 text-gray-500">Loading products...</div>
            ) : filteredProducts.length === 0 ? (
              <div className="text-center py-12">
                <Package className="w-12 h-12 text-gray-600 mx-auto mb-3" />
                <p className="text-gray-400">No data products found</p>
                <p className="text-sm text-gray-600 mt-1">
                  Publish products via the MetadataClient API
                </p>
              </div>
            ) : (
              <div className="grid gap-4">
                {filteredProducts.map((product) => (
                  <div
                    key={product.product_id}
                    className="p-5 rounded-lg bg-gray-800/30 border border-white/5 hover:border-white/10 transition-all"
                  >
                    <div className="flex items-start justify-between">
                      <div className="flex-1">
                        <div className="flex items-center gap-3">
                          <h3 className="text-sm font-semibold text-white">{product.product_name}</h3>
                          <span className={`px-2 py-0.5 rounded-full text-xs border ${STATUS_STYLES[product.status]}`}>
                            {product.status}
                          </span>
                          <span className="text-xs text-gray-500">v{product.version}</span>
                        </div>
                        {product.description && (
                          <p className="text-xs text-gray-400 mt-1">{product.description}</p>
                        )}
                        <div className="flex items-center gap-4 mt-3 text-xs text-gray-500">
                          <span className="flex items-center gap-1">
                            <Package className="w-3 h-3" />
                            Domain: {product.domain}
                          </span>
                          <span>Owner: {product.owner}</span>
                          {product.subscriber_count !== undefined && (
                            <span className="flex items-center gap-1">
                              <Users className="w-3 h-3" />
                              {product.subscriber_count} subscribers
                            </span>
                          )}
                          {product.published_at && (
                            <span className="flex items-center gap-1">
                              <Clock className="w-3 h-3" />
                              Published: {new Date(product.published_at).toLocaleDateString()}
                            </span>
                          )}
                        </div>
                      </div>
                      <div className="text-right space-y-2">
                        {product.sla_freshness_hours && (
                          <div className="text-xs text-gray-500">
                            SLA: {product.sla_freshness_hours}h freshness
                          </div>
                        )}
                        {product.sla_quality_score && (
                          <div className="text-xs text-gray-500 flex items-center gap-1 justify-end">
                            <Star className="w-3 h-3 text-yellow-400" />
                            Quality: {product.sla_quality_score}%
                          </div>
                        )}
                        {product.status === 'PUBLISHED' && (
                          <button
                            onClick={() =>
                              subscribeMutation.mutate({
                                productId: product.product_id,
                                subscriber: 'current-user@company.com',
                              })
                            }
                            disabled={subscribeMutation.isPending}
                            className="px-3 py-1 rounded-md bg-purple-600/20 text-purple-400 border border-purple-500/30 hover:bg-purple-600/30 text-xs flex items-center gap-1"
                          >
                            Subscribe <ArrowRight className="w-3 h-3" />
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Subscriptions Tab */}
        {activeTab === 'subscriptions' && (
          <div className="space-y-3">
            {subsLoading ? (
              <div className="text-center py-12 text-gray-500">Loading subscriptions...</div>
            ) : subscriptions.length === 0 ? (
              <div className="text-center py-12">
                <Users className="w-12 h-12 text-gray-600 mx-auto mb-3" />
                <p className="text-gray-400">No subscriptions</p>
                <p className="text-sm text-gray-600 mt-1">Subscribe to data products to get access</p>
              </div>
            ) : (
              <div className="grid gap-3">
                {subscriptions.map((sub) => (
                  <div
                    key={sub.subscription_id}
                    className="p-4 rounded-lg bg-gray-800/30 border border-white/5 flex items-center justify-between"
                  >
                    <div>
                      <h3 className="text-sm font-medium text-white">
                        {sub.product_name || sub.product_id}
                      </h3>
                      <div className="flex items-center gap-3 mt-1 text-xs text-gray-500">
                        <span>Subscriber: {sub.subscriber}</span>
                        <span>Requested: {new Date(sub.requested_at).toLocaleDateString()}</span>
                        {sub.approved_by && <span>Approved by: {sub.approved_by}</span>}
                      </div>
                    </div>
                    <span className={`px-2 py-0.5 rounded-full text-xs border ${SUB_STATUS_STYLES[sub.status]}`}>
                      {sub.status}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
