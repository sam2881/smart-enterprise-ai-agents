/**
 * Cost Estimation Utilities
 *
 * Estimates BigQuery and Dataproc costs based on pipeline configuration.
 * Part of Phase 4 - Nice-to-Have Features
 */

import { UnifiedPipelineInput, ProcessingMode } from '@/types/pipeline-canonical'

export interface CostEstimate {
  bigquery_storage_gb_month: number
  bigquery_storage_cost_usd: number
  bigquery_processing_tb: number
  bigquery_processing_cost_usd: number
  dataproc_cluster_hours: number
  dataproc_cluster_cost_usd: number
  total_monthly_cost_usd: number
  cost_breakdown: {
    storage: number
    processing: number
    compute: number
  }
  recommendations: string[]
}

// Pricing (as of 2026, US region)
const PRICING = {
  bigquery: {
    storage_per_gb_month: 0.020,  // $0.02 per GB/month (active storage)
    processing_per_tb: 5.00,  // $5 per TB processed
  },
  dataproc: {
    n1_standard_4_per_hour: 0.19,  // $0.19/hour per node
    preemptible_discount: 0.70,  // 70% discount for preemptible nodes
  }
}

/**
 * Estimate BigQuery storage cost
 */
function estimateBigQueryStorage(
  input: UnifiedPipelineInput,
  avgRowSizeBytes: number = 500,
  estimatedRowCount: number = 1000000
): { storage_gb: number; cost_usd: number } {
  const target = input.target
  if (!target) return { storage_gb: 0, cost_usd: 0 }

  // Estimate storage based on zones and row count
  const zones = ['landing', 'bronze', 'silver', 'gold', 'trusted']
  const zonesUsed = zones.filter(z => z === target.target_zone).length || 3  // Default 3 zones

  const storageBytes = estimatedRowCount * avgRowSizeBytes * zonesUsed
  const storageGB = storageBytes / (1024 ** 3)

  const cost = storageGB * PRICING.bigquery.storage_per_gb_month

  return { storage_gb: storageGB, cost_usd: cost }
}

/**
 * Estimate BigQuery processing cost
 */
function estimateBigQueryProcessing(
  input: UnifiedPipelineInput,
  estimatedRowCount: number = 1000000,
  avgRowSizeBytes: number = 500
): { processing_tb: number; cost_usd: number } {
  const execution = input.execution_policy
  if (!execution) return { processing_tb: 0, cost_usd: 0 }

  // Estimate daily queries based on schedule
  let dailyRuns = 1
  if (execution.schedule_interval) {
    if (execution.schedule_interval.includes('hour')) {
      dailyRuns = 24
    } else if (execution.schedule_interval === '@daily') {
      dailyRuns = 1
    } else if (execution.schedule_interval === '@weekly') {
      dailyRuns = 1 / 7
    }
  }

  // Estimate bytes scanned per run
  const bytesPerRun = estimatedRowCount * avgRowSizeBytes
  const bytesPerMonth = bytesPerRun * dailyRuns * 30

  // Convert to TB
  const processingTB = bytesPerMonth / (1024 ** 4)

  const cost = processingTB * PRICING.bigquery.processing_per_tb

  return { processing_tb: processingTB, cost_usd: cost }
}

/**
 * Estimate Dataproc cluster cost
 */
function estimateDataprocCost(
  input: UnifiedPipelineInput,
  estimatedJobDurationHours: number = 0.5
): { cluster_hours: number; cost_usd: number } {
  const execution = input.execution_policy
  if (!execution) return { cluster_hours: 0, cost_usd: 0 }

  // Estimate monthly runs
  let monthlyRuns = 30
  if (execution.schedule_interval) {
    if (execution.schedule_interval.includes('hour')) {
      monthlyRuns = 24 * 30
    } else if (execution.schedule_interval === '@daily') {
      monthlyRuns = 30
    } else if (execution.schedule_interval === '@weekly') {
      monthlyRuns = 4
    }
  }

  // Estimate cluster hours (assume 3-node cluster)
  const clusterHours = monthlyRuns * estimatedJobDurationHours * 3  // 3 nodes

  // Use preemptible pricing for batch jobs
  const nodeHourCost = execution.processing_mode === 'streaming'
    ? PRICING.dataproc.n1_standard_4_per_hour
    : PRICING.dataproc.n1_standard_4_per_hour * PRICING.dataproc.preemptible_discount

  const cost = clusterHours * nodeHourCost

  return { cluster_hours: clusterHours, cost_usd: cost }
}

/**
 * Generate cost recommendations
 */
function generateRecommendations(
  input: UnifiedPipelineInput,
  estimates: {
    storage_gb: number
    processing_tb: number
    cluster_hours: number
  }
): string[] {
  const recommendations: string[] = []

  // Storage recommendations
  if (estimates.storage_gb > 1000) {  // >1 TB
    recommendations.push('Consider partitioning and clustering tables to reduce storage costs')
    recommendations.push('Enable table expiration to automatically delete old data')
  }

  // Processing recommendations
  if (estimates.processing_tb > 10) {  // >10 TB/month
    recommendations.push('Use BigQuery BI Engine or materialized views to cache frequently accessed data')
    recommendations.push('Require partition filters in queries to scan less data')
  }

  // Compute recommendations
  if (estimates.cluster_hours > 500) {  // >500 hours/month
    recommendations.push('Consider using preemptible Dataproc nodes for 70% cost savings')
    recommendations.push('Optimize Spark jobs to reduce runtime (use broadcast joins, cache DataFrames)')
  }

  // Scheduling recommendations
  const execution = input.execution_policy
  if (execution?.schedule_interval?.includes('hour')) {
    recommendations.push('Hourly schedules can be expensive - consider batching to daily if possible')
  }

  // Write mode recommendations
  if (input.target?.write_mode === 'overwrite') {
    recommendations.push('OVERWRITE mode rewrites entire table - consider APPEND or MERGE for incremental data')
  }

  return recommendations
}

/**
 * Main cost estimation function
 */
export function estimatePipelineCost(
  input: UnifiedPipelineInput,
  options: {
    estimatedRowCount?: number
    avgRowSizeBytes?: number
    estimatedJobDurationHours?: number
  } = {}
): CostEstimate {
  const {
    estimatedRowCount = 1000000,
    avgRowSizeBytes = 500,
    estimatedJobDurationHours = 0.5
  } = options

  // Estimate storage
  const storage = estimateBigQueryStorage(input, avgRowSizeBytes, estimatedRowCount)

  // Estimate processing
  const processing = estimateBigQueryProcessing(input, estimatedRowCount, avgRowSizeBytes)

  // Estimate compute
  const compute = estimateDataprocCost(input, estimatedJobDurationHours)

  // Total cost
  const totalCost = storage.cost_usd + processing.cost_usd + compute.cost_usd

  // Generate recommendations
  const recommendations = generateRecommendations(input, {
    storage_gb: storage.storage_gb,
    processing_tb: processing.processing_tb,
    cluster_hours: compute.cluster_hours
  })

  return {
    bigquery_storage_gb_month: storage.storage_gb,
    bigquery_storage_cost_usd: storage.cost_usd,
    bigquery_processing_tb: processing.processing_tb,
    bigquery_processing_cost_usd: processing.cost_usd,
    dataproc_cluster_hours: compute.cluster_hours,
    dataproc_cluster_cost_usd: compute.cost_usd,
    total_monthly_cost_usd: totalCost,
    cost_breakdown: {
      storage: storage.cost_usd,
      processing: processing.cost_usd,
      compute: compute.cost_usd
    },
    recommendations
  }
}

/**
 * Format cost as USD string
 */
export function formatCost(cost: number): string {
  return `$${cost.toFixed(2)}`
}

/**
 * Get cost tier (low, medium, high)
 */
export function getCostTier(monthlyCost: number): 'low' | 'medium' | 'high' {
  if (monthlyCost < 100) return 'low'
  if (monthlyCost < 1000) return 'medium'
  return 'high'
}
