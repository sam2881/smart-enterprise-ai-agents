'use client'

/**
 * JoinDependencyBuilder - Configure join dependencies for Gold zone tables
 *
 * Features:
 * - Support for existing tables (validated joins)
 * - Support for future/placeholder tables (contract-based joins)
 * - Join type configuration (inner, left, right, full)
 * - Cardinality specification (1-1, 1-N, N-1, N-N)
 * - Grain validation display
 */

import React, { useState, useCallback } from 'react'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { cn } from '@/lib/utils'
import { JoinDependency, ColumnDefinition } from '@/types/pipeline-canonical'
import {
  Link,
  Plus,
  Trash2,
  AlertCircle,
  CheckCircle,
  Clock,
  Database,
  GitBranch,
  Key,
} from 'lucide-react'

// =============================================================================
// Types
// =============================================================================

interface JoinDependencyBuilderProps {
  dependencies: JoinDependency[]
  onChange: (dependencies: JoinDependency[]) => void
  leftEntityName: string
  availableColumns: ColumnDefinition[]
  className?: string
}

type JoinType = 'inner' | 'left' | 'right' | 'full' | 'cross'
type Cardinality = '1-1' | '1-N' | 'N-1' | 'N-N'

// =============================================================================
// Constants
// =============================================================================

const JOIN_TYPES: { value: JoinType; label: string; description: string }[] = [
  { value: 'inner', label: 'INNER JOIN', description: 'Only matching rows from both tables' },
  { value: 'left', label: 'LEFT JOIN', description: 'All rows from left, matching from right' },
  { value: 'right', label: 'RIGHT JOIN', description: 'All rows from right, matching from left' },
  { value: 'full', label: 'FULL JOIN', description: 'All rows from both tables' },
  { value: 'cross', label: 'CROSS JOIN', description: 'Cartesian product (use with caution)' },
]

const CARDINALITIES: { value: Cardinality; label: string; description: string }[] = [
  { value: '1-1', label: 'One-to-One', description: 'Each row matches exactly one row' },
  { value: '1-N', label: 'One-to-Many', description: 'One left row matches many right rows' },
  { value: 'N-1', label: 'Many-to-One', description: 'Many left rows match one right row' },
  { value: 'N-N', label: 'Many-to-Many', description: 'Many rows match many rows (rarely desired)' },
]

// =============================================================================
// Component
// =============================================================================

export function JoinDependencyBuilder({
  dependencies,
  onChange,
  leftEntityName,
  availableColumns,
  className,
}: JoinDependencyBuilderProps) {
  const [isAddingNew, setIsAddingNew] = useState(false)
  const [editingIndex, setEditingIndex] = useState<number | null>(null)

  // Add new dependency
  const handleAddDependency = useCallback((dependency: JoinDependency) => {
    onChange([...dependencies, dependency])
    setIsAddingNew(false)
  }, [dependencies, onChange])

  // Update existing dependency
  const handleUpdateDependency = useCallback((index: number, dependency: JoinDependency) => {
    const updated = [...dependencies]
    updated[index] = dependency
    onChange(updated)
    setEditingIndex(null)
  }, [dependencies, onChange])

  // Remove dependency
  const handleRemoveDependency = useCallback((index: number) => {
    onChange(dependencies.filter((_, i) => i !== index))
  }, [dependencies, onChange])

  return (
    <div className={cn('space-y-4', className)}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <GitBranch className="w-5 h-5 text-purple-400" />
          <h4 className="text-md font-semibold text-white dark:text-white">
            Join Dependencies
          </h4>
          <Badge variant="default" className="text-xs">
            {dependencies.length} configured
          </Badge>
        </div>
        {!isAddingNew && editingIndex === null && (
          <Button
            onClick={() => setIsAddingNew(true)}
            variant="outline"
            size="sm"
            className="gap-1"
          >
            <Plus className="w-4 h-4" />
            Add Join
          </Button>
        )}
      </div>

      {/* Info Text */}
      <p className="text-sm text-gray-400 dark:text-gray-400">
        Configure table joins for your Gold zone. You can join with existing tables or create
        placeholder contracts for tables that will be created by future pipelines.
      </p>

      {/* Existing Dependencies */}
      {dependencies.length > 0 && (
        <div className="space-y-3">
          {dependencies.map((dep, index) => (
            editingIndex === index ? (
              <JoinDependencyForm
                key={index}
                initialValue={dep}
                leftEntityName={leftEntityName}
                availableColumns={availableColumns}
                onSubmit={(updated) => handleUpdateDependency(index, updated)}
                onCancel={() => setEditingIndex(null)}
                isEditing
              />
            ) : (
              <JoinDependencyCard
                key={index}
                dependency={dep}
                leftEntityName={leftEntityName}
                onEdit={() => setEditingIndex(index)}
                onRemove={() => handleRemoveDependency(index)}
              />
            )
          ))}
        </div>
      )}

      {/* Add New Form */}
      {isAddingNew && (
        <JoinDependencyForm
          leftEntityName={leftEntityName}
          availableColumns={availableColumns}
          onSubmit={handleAddDependency}
          onCancel={() => setIsAddingNew(false)}
        />
      )}

      {/* Empty State */}
      {dependencies.length === 0 && !isAddingNew && (
        <div className="text-center py-8 border-2 border-dashed border-gray-200 dark:border-gray-700 rounded-lg">
          <Link className="w-8 h-8 mx-auto text-gray-400 mb-2" />
          <p className="text-sm text-gray-400 dark:text-gray-400">
            No join dependencies configured.
          </p>
          <p className="text-xs text-gray-400 dark:text-gray-400 mt-1">
            Click "Add Join" to configure table relationships.
          </p>
        </div>
      )}
    </div>
  )
}

// =============================================================================
// JoinDependencyCard - Display a configured join
// =============================================================================

interface JoinDependencyCardProps {
  dependency: JoinDependency
  leftEntityName: string
  onEdit: () => void
  onRemove: () => void
}

function JoinDependencyCard({
  dependency,
  leftEntityName,
  onEdit,
  onRemove,
}: JoinDependencyCardProps) {
  const isPlaceholder = dependency.right_entity?.future?.placeholder_contract
  const rightEntityName = dependency.right_entity?.table_name || 'Unknown'

  return (
    <div className="p-4 bg-gray-800/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-lg">
      <div className="flex items-start justify-between">
        <div className="flex-1">
          {/* Join Visualization */}
          <div className="flex items-center gap-2 mb-3">
            <div className="flex items-center gap-1.5 px-2 py-1 bg-blue-100 dark:bg-blue-900/30 rounded">
              <Database className="w-3.5 h-3.5 text-blue-600 dark:text-blue-400" />
              <span className="text-sm font-medium text-blue-700 dark:text-blue-300">
                {leftEntityName || dependency.left_entity}
              </span>
            </div>

            <Badge
              variant={dependency.join_type === 'inner' ? 'default' : 'info'}
              className="text-xs uppercase"
            >
              {dependency.join_type}
            </Badge>

            <div className={cn(
              'flex items-center gap-1.5 px-2 py-1 rounded',
              isPlaceholder
                ? 'bg-amber-100 dark:bg-amber-900/30'
                : 'bg-emerald-100 dark:bg-emerald-900/30'
            )}>
              {isPlaceholder ? (
                <Clock className="w-3.5 h-3.5 text-amber-600 dark:text-amber-400" />
              ) : (
                <CheckCircle className="w-3.5 h-3.5 text-emerald-600 dark:text-emerald-400" />
              )}
              <span className={cn(
                'text-sm font-medium',
                isPlaceholder
                  ? 'text-amber-700 dark:text-amber-300'
                  : 'text-emerald-700 dark:text-emerald-300'
              )}>
                {rightEntityName}
              </span>
            </div>
          </div>

          {/* Join Keys */}
          <div className="flex items-center gap-4 text-xs text-gray-400 dark:text-gray-400">
            <div className="flex items-center gap-1">
              <Key className="w-3 h-3" />
              <span>
                {dependency.left_keys.join(', ')} = {dependency.right_keys.join(', ')}
              </span>
            </div>
            <span className="text-gray-400">|</span>
            <span>Cardinality: {dependency.cardinality}</span>
            {dependency.expected_grain && (
              <>
                <span className="text-gray-400">|</span>
                <span>Grain: {dependency.expected_grain}</span>
              </>
            )}
          </div>

          {/* Placeholder Notice */}
          {isPlaceholder && dependency.right_entity?.future?.expected_pipeline_id && (
            <div className="mt-2 flex items-center gap-1.5 text-xs text-amber-600 dark:text-amber-400">
              <AlertCircle className="w-3.5 h-3.5" />
              <span>
                Waiting for pipeline: {dependency.right_entity.future.expected_pipeline_id}
              </span>
            </div>
          )}
        </div>

        {/* Actions */}
        <div className="flex items-center gap-1 ml-4">
          <Button
            onClick={onEdit}
            variant="ghost"
            size="sm"
            className="h-8 w-8 p-0"
          >
            <span className="sr-only">Edit</span>
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
            </svg>
          </Button>
          <Button
            onClick={onRemove}
            variant="ghost"
            size="sm"
            className="h-8 w-8 p-0 text-red-500 hover:text-red-700"
          >
            <span className="sr-only">Remove</span>
            <Trash2 className="w-4 h-4" />
          </Button>
        </div>
      </div>
    </div>
  )
}

// =============================================================================
// JoinDependencyForm - Add/Edit a join dependency
// =============================================================================

interface JoinDependencyFormProps {
  initialValue?: JoinDependency
  leftEntityName: string
  availableColumns: ColumnDefinition[]
  onSubmit: (dependency: JoinDependency) => void
  onCancel: () => void
  isEditing?: boolean
}

function JoinDependencyForm({
  initialValue,
  leftEntityName,
  availableColumns,
  onSubmit,
  onCancel,
  isEditing = false,
}: JoinDependencyFormProps) {
  // Form state
  const [joinType, setJoinType] = useState<JoinType>(initialValue?.join_type || 'left')
  const [leftKeys, setLeftKeys] = useState<string[]>(initialValue?.left_keys || [])
  const [rightEntityName, setRightEntityName] = useState(
    initialValue?.right_entity?.table_name || ''
  )
  const [rightKeys, setRightKeys] = useState<string[]>(initialValue?.right_keys || [])
  const [cardinality, setCardinality] = useState<Cardinality>(initialValue?.cardinality || '1-N')
  const [expectedGrain, setExpectedGrain] = useState(initialValue?.expected_grain || '')
  const [isExisting, setIsExisting] = useState(
    initialValue?.right_entity?.existing?.dataset ? true : !initialValue?.right_entity?.future
  )
  const [existingDataset, setExistingDataset] = useState(
    initialValue?.right_entity?.existing?.dataset || ''
  )
  const [existingTable, setExistingTable] = useState(
    initialValue?.right_entity?.existing?.table || ''
  )
  const [expectedPipelineId, setExpectedPipelineId] = useState(
    initialValue?.right_entity?.future?.expected_pipeline_id || ''
  )

  // Handle left key toggle
  const toggleLeftKey = (columnName: string) => {
    setLeftKeys((prev) =>
      prev.includes(columnName)
        ? prev.filter((k) => k !== columnName)
        : [...prev, columnName]
    )
  }

  // Handle right key input
  const handleRightKeysChange = (value: string) => {
    setRightKeys(value.split(',').map((k) => k.trim()).filter(Boolean))
  }

  // Handle submit
  const handleSubmit = () => {
    const dependency: JoinDependency = {
      join_type: joinType,
      left_entity: leftEntityName,
      left_keys: leftKeys,
      right_entity: {
        table_name: rightEntityName,
        ...(isExisting
          ? { existing: { dataset: existingDataset, table: existingTable, resolved_from_metadata: false } }
          : { future: { expected_pipeline_id: expectedPipelineId, placeholder_contract: true } }
        ),
      },
      right_keys: rightKeys,
      expected_grain: expectedGrain,
      cardinality: cardinality,
    }
    onSubmit(dependency)
  }

  const isValid =
    leftKeys.length > 0 &&
    rightEntityName &&
    rightKeys.length > 0 &&
    (isExisting ? existingDataset && existingTable : expectedPipelineId)

  return (
    <Card className="p-4 border-2 border-purple-200 dark:border-purple-800 bg-purple-50/50 dark:bg-purple-900/10">
      <h5 className="text-sm font-semibold text-white dark:text-white mb-4">
        {isEditing ? 'Edit Join Dependency' : 'Add Join Dependency'}
      </h5>

      <div className="space-y-4">
        {/* Join Type */}
        <div>
          <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">
            Join Type
          </label>
          <div className="grid grid-cols-5 gap-2">
            {JOIN_TYPES.map((type) => (
              <button
                key={type.value}
                onClick={() => setJoinType(type.value)}
                className={cn(
                  'px-3 py-2 text-xs font-medium rounded-lg border transition-colors',
                  joinType === type.value
                    ? 'bg-purple-100 dark:bg-purple-900/50 border-purple-300 dark:border-purple-700 text-purple-700 dark:text-purple-300'
                    : 'bg-gray-800 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-400 dark:text-gray-400 hover:border-purple-200'
                )}
                title={type.description}
              >
                {type.label}
              </button>
            ))}
          </div>
        </div>

        {/* Left Side Configuration */}
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">
              Left Entity (Your Table)
            </label>
            <div className="px-3 py-2 bg-gray-700 dark:bg-gray-800 rounded-lg text-sm text-gray-300 dark:text-gray-300">
              {leftEntityName || 'Not specified'}
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">
              Left Join Keys
            </label>
            <div className="flex flex-wrap gap-1.5">
              {availableColumns.map((col) => (
                <button
                  key={col.name}
                  onClick={() => toggleLeftKey(col.name)}
                  className={cn(
                    'px-2 py-1 text-xs rounded border transition-colors',
                    leftKeys.includes(col.name)
                      ? 'bg-blue-100 dark:bg-blue-900/50 border-blue-300 dark:border-blue-700 text-blue-700 dark:text-blue-300'
                      : 'bg-gray-800 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-400 dark:text-gray-400 hover:border-blue-600/50'
                  )}
                >
                  {col.name}
                </button>
              ))}
              {availableColumns.length === 0 && (
                <span className="text-xs text-gray-400">No columns defined in schema</span>
              )}
            </div>
          </div>
        </div>

        {/* Right Entity Type Toggle */}
        <div>
          <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">
            Right Entity Type
          </label>
          <div className="flex gap-2">
            <button
              onClick={() => setIsExisting(true)}
              className={cn(
                'flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-lg border transition-colors',
                isExisting
                  ? 'bg-emerald-100 dark:bg-emerald-900/50 border-emerald-300 dark:border-emerald-700 text-emerald-700 dark:text-emerald-300'
                  : 'bg-gray-800 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-400 dark:text-gray-400'
              )}
            >
              <CheckCircle className="w-4 h-4" />
              <span className="text-sm font-medium">Existing Table</span>
            </button>
            <button
              onClick={() => setIsExisting(false)}
              className={cn(
                'flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-lg border transition-colors',
                !isExisting
                  ? 'bg-amber-100 dark:bg-amber-900/50 border-amber-300 dark:border-amber-700 text-amber-700 dark:text-amber-300'
                  : 'bg-gray-800 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-400 dark:text-gray-400'
              )}
            >
              <Clock className="w-4 h-4" />
              <span className="text-sm font-medium">Future Table (Placeholder)</span>
            </button>
          </div>
        </div>

        {/* Right Side Configuration */}
        <div className="grid grid-cols-2 gap-4">
          <div>
            <Input
              label="Right Entity Name"
              placeholder="e.g., dim_customer"
              value={rightEntityName}
              onChange={(e) => setRightEntityName(e.target.value)}
              required
            />
          </div>

          <div>
            <Input
              label="Right Join Keys (comma-separated)"
              placeholder="e.g., customer_id"
              value={rightKeys.join(', ')}
              onChange={(e) => handleRightKeysChange(e.target.value)}
              required
            />
          </div>
        </div>

        {/* Existing Table Details */}
        {isExisting && (
          <div className="grid grid-cols-2 gap-4 p-3 bg-emerald-50 dark:bg-emerald-900/20 rounded-lg">
            <Input
              label="BigQuery Dataset"
              placeholder="e.g., analytics_gold"
              value={existingDataset}
              onChange={(e) => setExistingDataset(e.target.value)}
              required
            />
            <Input
              label="BigQuery Table"
              placeholder="e.g., dim_customer"
              value={existingTable}
              onChange={(e) => setExistingTable(e.target.value)}
              required
            />
          </div>
        )}

        {/* Future Table Details */}
        {!isExisting && (
          <div className="p-3 bg-amber-50 dark:bg-amber-900/20 rounded-lg">
            <Input
              label="Expected Pipeline Jira Ticket"
              placeholder="e.g., DATA-5678"
              value={expectedPipelineId}
              onChange={(e) => setExpectedPipelineId(e.target.value)}
              required
            />
            <p className="mt-2 text-xs text-amber-600 dark:text-amber-400">
              This creates a placeholder contract. The join will be validated when the referenced
              pipeline is created.
            </p>
          </div>
        )}

        {/* Cardinality & Grain */}
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 dark:text-gray-300 mb-2">
              Cardinality
            </label>
            <div className="grid grid-cols-2 gap-2">
              {CARDINALITIES.map((card) => (
                <button
                  key={card.value}
                  onClick={() => setCardinality(card.value)}
                  className={cn(
                    'px-3 py-2 text-xs font-medium rounded-lg border transition-colors',
                    cardinality === card.value
                      ? 'bg-purple-100 dark:bg-purple-900/50 border-purple-300 dark:border-purple-700 text-purple-700 dark:text-purple-300'
                      : 'bg-gray-800 dark:bg-gray-800 border-gray-200 dark:border-gray-700 text-gray-400 dark:text-gray-400 hover:border-purple-200'
                  )}
                  title={card.description}
                >
                  {card.label}
                </button>
              ))}
            </div>
          </div>

          <div>
            <Input
              label="Expected Grain (Optional)"
              placeholder="e.g., customer_id + date"
              value={expectedGrain}
              onChange={(e) => setExpectedGrain(e.target.value)}
            />
          </div>
        </div>

        {/* Form Actions */}
        <div className="flex justify-end gap-2 pt-2 border-t border-gray-200 dark:border-gray-700">
          <Button onClick={onCancel} variant="ghost" size="sm">
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            variant="primary"
            size="sm"
            disabled={!isValid}
          >
            {isEditing ? 'Update Join' : 'Add Join'}
          </Button>
        </div>
      </div>
    </Card>
  )
}

export default JoinDependencyBuilder
