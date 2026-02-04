'use client'

import React, { useState } from 'react'
import { cn } from '@/lib/utils'
import { Textarea } from '@/components/ui/Textarea'

// Zone intent configuration for each medallion layer
export interface ZoneIntent {
  bronze: BronzeIntent
  silver: SilverIntent
  gold: GoldIntent
}

export interface BronzeIntent {
  ingestion_intent: string  // Free-text: "Load daily CSV files from SFTP"
  data_freshness: 'real-time' | 'near-real-time' | 'hourly' | 'daily' | 'weekly'
  source_priority: 'primary' | 'secondary' | 'backup'
  retention_policy?: string  // "Keep raw data for 90 days"
  special_handling?: string  // "Handle PII masking at ingestion"
}

export interface SilverIntent {
  transformation_intent: string  // Free-text: "Clean nulls, deduplicate by customer_id"
  business_rules: string  // Free-text: "Apply currency conversion, validate dates"
  quality_expectations: string  // Free-text: "99.5% completeness, no duplicate keys"
  deduplication_strategy?: 'first' | 'last' | 'merge' | 'custom'
  null_handling?: 'drop' | 'fill_default' | 'fill_previous' | 'flag'
}

export interface GoldIntent {
  modeling_intent: string  // Free-text: "Build customer 360 view with purchase history"
  business_context: string  // Free-text: "Support marketing analytics and segmentation"
  aggregation_grain: string  // Free-text: "Daily aggregates by customer and product category"
  kpi_definitions?: string  // Free-text: "LTV = sum(purchases) over 365 days"
  consumer_systems?: string  // Free-text: "Looker dashboards, ML feature store"
}

interface ZoneIntentPanelProps {
  value: Partial<ZoneIntent>
  onChange: (intent: ZoneIntent) => void
  className?: string
}

type ZoneTab = 'bronze' | 'silver' | 'gold'

const ZONE_CONFIGS: Record<ZoneTab, {
  label: string
  color: string
  bgColor: string
  borderColor: string
  description: string
  icon: string
}> = {
  bronze: {
    label: 'Bronze Zone',
    color: 'text-amber-400',
    bgColor: 'bg-amber-500/10',
    borderColor: 'border-amber-500/30',
    description: 'Raw data ingestion - Define what data to capture and how',
    icon: '🥉'
  },
  silver: {
    label: 'Silver Zone',
    color: 'text-gray-300',
    bgColor: 'bg-gray-800/500/10',
    borderColor: 'border-gray-500/30',
    description: 'Data cleaning & transformation - Define business rules and quality standards',
    icon: '🥈'
  },
  gold: {
    label: 'Gold Zone',
    color: 'text-yellow-400',
    bgColor: 'bg-amber-900/300/10',
    borderColor: 'border-yellow-500/30',
    description: 'Business-ready data - Define modeling intent and consumer context',
    icon: '🥇'
  }
}

export function ZoneIntentPanel({ value, onChange, className }: ZoneIntentPanelProps) {
  const [activeZone, setActiveZone] = useState<ZoneTab>('bronze')

  // Initialize with defaults
  const intent: ZoneIntent = {
    bronze: {
      ingestion_intent: '',
      data_freshness: 'daily',
      source_priority: 'primary',
      retention_policy: '',
      special_handling: '',
      ...value.bronze
    },
    silver: {
      transformation_intent: '',
      business_rules: '',
      quality_expectations: '',
      deduplication_strategy: 'last',
      null_handling: 'flag',
      ...value.silver
    },
    gold: {
      modeling_intent: '',
      business_context: '',
      aggregation_grain: '',
      kpi_definitions: '',
      consumer_systems: '',
      ...value.gold
    }
  }

  const updateBronze = (updates: Partial<BronzeIntent>) => {
    onChange({
      ...intent,
      bronze: { ...intent.bronze, ...updates }
    })
  }

  const updateSilver = (updates: Partial<SilverIntent>) => {
    onChange({
      ...intent,
      silver: { ...intent.silver, ...updates }
    })
  }

  const updateGold = (updates: Partial<GoldIntent>) => {
    onChange({
      ...intent,
      gold: { ...intent.gold, ...updates }
    })
  }

  const zoneConfig = ZONE_CONFIGS[activeZone]

  return (
    <div className={cn('space-y-4', className)}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-white">Zone Intent Configuration</h3>
          <p className="text-sm text-gray-400">
            Define natural language context for each data zone
          </p>
        </div>
      </div>

      {/* Zone Tabs */}
      <div className="flex gap-2 p-1 bg-gray-800/30 rounded-lg">
        {(Object.keys(ZONE_CONFIGS) as ZoneTab[]).map((zone) => {
          const config = ZONE_CONFIGS[zone]
          const isActive = activeZone === zone
          const hasContent = zone === 'bronze'
            ? !!intent.bronze.ingestion_intent
            : zone === 'silver'
            ? !!intent.silver.transformation_intent
            : !!intent.gold.modeling_intent

          return (
            <button
              key={zone}
              onClick={() => setActiveZone(zone)}
              className={cn(
                'flex-1 px-4 py-2.5 rounded-md transition-all duration-200',
                'flex items-center justify-center gap-2',
                isActive
                  ? `${config.bgColor} ${config.color} ${config.borderColor} border`
                  : 'text-gray-400 hover:text-white hover:bg-gray-700/50'
              )}
            >
              <span>{config.icon}</span>
              <span className="font-medium">{config.label}</span>
              {hasContent && (
                <span className="w-2 h-2 rounded-full bg-green-500" />
              )}
            </button>
          )
        })}
      </div>

      {/* Zone Description */}
      <div className={cn(
        'p-3 rounded-lg border',
        zoneConfig.bgColor,
        zoneConfig.borderColor
      )}>
        <p className={cn('text-sm', zoneConfig.color)}>
          {zoneConfig.description}
        </p>
      </div>

      {/* Bronze Zone Form */}
      {activeZone === 'bronze' && (
        <div className="space-y-4">
          <Textarea
            label="Ingestion Intent *"
            placeholder="Describe what data to capture and from where...&#10;Example: Load daily sales CSV files from SFTP server /data/sales/, process files matching pattern sales_*.csv"
            value={intent.bronze.ingestion_intent}
            onChange={(e) => updateBronze({ ingestion_intent: e.target.value })}
            rows={3}
            helperText="Natural language description of your data ingestion goals"
          />

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Data Freshness
              </label>
              <select
                value={intent.bronze.data_freshness}
                onChange={(e) => updateBronze({ data_freshness: e.target.value as BronzeIntent['data_freshness'] })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="real-time" className="bg-gray-800">Real-time (streaming)</option>
                <option value="near-real-time" className="bg-gray-800">Near Real-time (&lt;15 min)</option>
                <option value="hourly" className="bg-gray-800">Hourly</option>
                <option value="daily" className="bg-gray-800">Daily</option>
                <option value="weekly" className="bg-gray-800">Weekly</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Source Priority
              </label>
              <select
                value={intent.bronze.source_priority}
                onChange={(e) => updateBronze({ source_priority: e.target.value as BronzeIntent['source_priority'] })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="primary" className="bg-gray-800">Primary Source</option>
                <option value="secondary" className="bg-gray-800">Secondary Source</option>
                <option value="backup" className="bg-gray-800">Backup/Fallback</option>
              </select>
            </div>
          </div>

          <Textarea
            label="Retention Policy"
            placeholder="Example: Keep raw data for 90 days, archive to cold storage after 30 days"
            value={intent.bronze.retention_policy || ''}
            onChange={(e) => updateBronze({ retention_policy: e.target.value })}
            rows={2}
          />

          <Textarea
            label="Special Handling Notes"
            placeholder="Example: Mask PII fields at ingestion, handle UTF-8 encoding issues, skip corrupted files"
            value={intent.bronze.special_handling || ''}
            onChange={(e) => updateBronze({ special_handling: e.target.value })}
            rows={2}
          />
        </div>
      )}

      {/* Silver Zone Form */}
      {activeZone === 'silver' && (
        <div className="space-y-4">
          <Textarea
            label="Transformation Intent *"
            placeholder="Describe data cleaning and transformation goals...&#10;Example: Deduplicate records by customer_id keeping latest, standardize date formats to ISO 8601, convert currency to USD"
            value={intent.silver.transformation_intent}
            onChange={(e) => updateSilver({ transformation_intent: e.target.value })}
            rows={3}
            helperText="Natural language description of how data should be cleaned and transformed"
          />

          <Textarea
            label="Business Rules *"
            placeholder="Define business validation and transformation rules...&#10;Example: Orders with negative amounts are invalid, customer_status must be one of: active, inactive, suspended"
            value={intent.silver.business_rules}
            onChange={(e) => updateSilver({ business_rules: e.target.value })}
            rows={3}
            helperText="Business logic that must be applied during transformation"
          />

          <Textarea
            label="Quality Expectations *"
            placeholder="Define data quality requirements...&#10;Example: 99.5% record completeness, no duplicate primary keys, all dates within valid range"
            value={intent.silver.quality_expectations}
            onChange={(e) => updateSilver({ quality_expectations: e.target.value })}
            rows={2}
            helperText="Data quality thresholds and validation rules"
          />

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Deduplication Strategy
              </label>
              <select
                value={intent.silver.deduplication_strategy}
                onChange={(e) => updateSilver({ deduplication_strategy: e.target.value as SilverIntent['deduplication_strategy'] })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="first" className="bg-gray-800">Keep First</option>
                <option value="last" className="bg-gray-800">Keep Last (recommended)</option>
                <option value="merge" className="bg-gray-800">Merge Records</option>
                <option value="custom" className="bg-gray-800">Custom Logic</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">
                Null Handling
              </label>
              <select
                value={intent.silver.null_handling}
                onChange={(e) => updateSilver({ null_handling: e.target.value as SilverIntent['null_handling'] })}
                className="w-full px-3 py-2.5 rounded-lg bg-gray-800/50 text-white border border-white/10 hover:border-white/20 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
                style={{ colorScheme: 'dark' }}
              >
                <option value="drop" className="bg-gray-800">Drop Records</option>
                <option value="fill_default" className="bg-gray-800">Fill with Default</option>
                <option value="fill_previous" className="bg-gray-800">Fill with Previous</option>
                <option value="flag" className="bg-gray-800">Flag and Keep</option>
              </select>
            </div>
          </div>
        </div>
      )}

      {/* Gold Zone Form */}
      {activeZone === 'gold' && (
        <div className="space-y-4">
          <Textarea
            label="Modeling Intent *"
            placeholder="Describe the business-ready data model to build...&#10;Example: Build customer 360 view joining orders, support tickets, and marketing interactions"
            value={intent.gold.modeling_intent}
            onChange={(e) => updateGold({ modeling_intent: e.target.value })}
            rows={3}
            helperText="Natural language description of the target data model"
          />

          <Textarea
            label="Business Context *"
            placeholder="Describe who will use this data and why...&#10;Example: Marketing team needs customer segmentation for campaign targeting, Finance needs revenue attribution"
            value={intent.gold.business_context}
            onChange={(e) => updateGold({ business_context: e.target.value })}
            rows={3}
            helperText="Business use cases and stakeholder requirements"
          />

          <Textarea
            label="Aggregation Grain *"
            placeholder="Define the granularity of the output data...&#10;Example: Daily aggregates by customer and product category, monthly rollups by region"
            value={intent.gold.aggregation_grain}
            onChange={(e) => updateGold({ aggregation_grain: e.target.value })}
            rows={2}
            helperText="Time period and dimensional grain of aggregations"
          />

          <Textarea
            label="KPI Definitions"
            placeholder="Define key metrics and calculations...&#10;Example: LTV = sum(order_amount) over 365 days, Churn Risk = probability based on recency and frequency"
            value={intent.gold.kpi_definitions || ''}
            onChange={(e) => updateGold({ kpi_definitions: e.target.value })}
            rows={2}
          />

          <Textarea
            label="Consumer Systems"
            placeholder="List downstream systems that will consume this data...&#10;Example: Looker dashboards, ML feature store, Salesforce sync, Data Science notebooks"
            value={intent.gold.consumer_systems || ''}
            onChange={(e) => updateGold({ consumer_systems: e.target.value })}
            rows={2}
          />
        </div>
      )}

      {/* Validation Summary */}
      <div className="mt-6 p-4 bg-gray-800/30 rounded-lg border border-white/5">
        <h4 className="text-sm font-medium text-white mb-3">Configuration Status</h4>
        <div className="grid grid-cols-3 gap-4">
          {(Object.keys(ZONE_CONFIGS) as ZoneTab[]).map((zone) => {
            const config = ZONE_CONFIGS[zone]
            const isComplete = zone === 'bronze'
              ? !!intent.bronze.ingestion_intent
              : zone === 'silver'
              ? !!intent.silver.transformation_intent && !!intent.silver.business_rules
              : !!intent.gold.modeling_intent && !!intent.gold.business_context

            return (
              <div
                key={zone}
                className={cn(
                  'p-3 rounded-lg border',
                  isComplete
                    ? 'border-green-500/30 bg-green-500/10'
                    : 'border-gray-700 bg-gray-800/30'
                )}
              >
                <div className="flex items-center gap-2 mb-1">
                  <span>{config.icon}</span>
                  <span className={cn('text-sm font-medium', config.color)}>
                    {config.label}
                  </span>
                </div>
                <p className={cn(
                  'text-xs',
                  isComplete ? 'text-green-400' : 'text-gray-400'
                )}>
                  {isComplete ? 'Configured' : 'Pending'}
                </p>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

export default ZoneIntentPanel
