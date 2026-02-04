/**
 * NestedSourceConfigForm - Configuration for nested/semi-structured data sources
 *
 * Supports: nested_json, nested_xml, nested_avro, nested_parquet
 * Features: Schema flattening, nested field extraction, array handling
 *
 * Part of Phase 2 - UI Backend Gap Analysis Remediation
 */

import React from 'react'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'

export interface NestedSourceConfig {
  storage_path: string
  file_pattern?: string
  root_element?: string  // For XML
  flatten_nested?: boolean
  flatten_level?: number  // Max nesting level to flatten (0 = no limit)
  array_handling?: 'explode' | 'json_string' | 'skip'
  nested_field_paths?: string[]  // JSON paths to extract (e.g., "data.items[*].id")
  schema_inference?: boolean
  sample_rows?: number  // Rows to sample for schema inference
  encoding?: string
}

interface NestedSourceConfigFormProps {
  config: Partial<NestedSourceConfig>
  onChange: (config: Partial<NestedSourceConfig>) => void
  sourceType: 'nested_json' | 'nested_xml' | 'nested_avro' | 'nested_parquet'
}

export const NestedSourceConfigForm: React.FC<NestedSourceConfigFormProps> = ({
  config,
  onChange,
  sourceType
}) => {
  const handleChange = (field: keyof NestedSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  const addNestedPath = () => {
    const paths = config.nested_field_paths || []
    onChange({ ...config, nested_field_paths: [...paths, ''] })
  }

  const updateNestedPath = (index: number, value: string) => {
    const paths = [...(config.nested_field_paths || [])]
    paths[index] = value
    onChange({ ...config, nested_field_paths: paths })
  }

  const removeNestedPath = (index: number) => {
    const paths = [...(config.nested_field_paths || [])]
    paths.splice(index, 1)
    onChange({ ...config, nested_field_paths: paths })
  }

  return (
    <div className="space-y-6">
      {/* Storage Path */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Storage Configuration</h3>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Storage Path *
            </label>
            <Input
              type="text"
              value={config.storage_path || ''}
              onChange={(e) => handleChange('storage_path', e.target.value)}
              placeholder="gs://bucket/path/to/nested-data/"
              required
            />
            <p className="text-xs text-gray-500 mt-1">
              GCS path containing {sourceType.replace('nested_', '').toUpperCase()} files
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              File Pattern
            </label>
            <Input
              type="text"
              value={config.file_pattern || ''}
              onChange={(e) => handleChange('file_pattern', e.target.value)}
              placeholder="*.json"
            />
            <p className="text-xs text-gray-500 mt-1">
              Glob pattern to match files (e.g., *.json, data_*.xml)
            </p>
          </div>

          {sourceType === 'nested_xml' && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Root Element
              </label>
              <Input
                type="text"
                value={config.root_element || ''}
                onChange={(e) => handleChange('root_element', e.target.value)}
                placeholder="items"
              />
              <p className="text-xs text-gray-500 mt-1">
                XML root element to start parsing from
              </p>
            </div>
          )}

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Encoding
            </label>
            <Select
              value={config.encoding || 'UTF-8'}
              onChange={(e) => handleChange('encoding', e.target.value)}
            >
              <option value="UTF-8">UTF-8</option>
              <option value="UTF-16">UTF-16</option>
              <option value="ISO-8859-1">ISO-8859-1</option>
              <option value="ASCII">ASCII</option>
            </Select>
          </div>
        </div>
      </Card>

      {/* Schema Flattening */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Schema Flattening</h3>
        <div className="space-y-4">
          <div className="flex items-center space-x-3">
            <input
              type="checkbox"
              id="flatten_nested"
              checked={config.flatten_nested || false}
              onChange={(e) => handleChange('flatten_nested', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="flatten_nested" className="text-sm font-medium text-gray-700">
              Flatten nested structures
            </label>
          </div>

          {config.flatten_nested && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Flatten Level (0 = no limit)
                </label>
                <Input
                  type="number"
                  min="0"
                  max="10"
                  value={config.flatten_level || 0}
                  onChange={(e) => handleChange('flatten_level', parseInt(e.target.value))}
                />
                <p className="text-xs text-gray-500 mt-1">
                  Maximum nesting depth to flatten. 0 means flatten all levels.
                </p>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Array Handling
                </label>
                <Select
                  value={config.array_handling || 'explode'}
                  onChange={(e) => handleChange('array_handling', e.target.value)}
                >
                  <option value="explode">Explode (create row per array element)</option>
                  <option value="json_string">Store as JSON string</option>
                  <option value="skip">Skip arrays</option>
                </Select>
                <p className="text-xs text-gray-500 mt-1">
                  How to handle nested arrays during flattening
                </p>
              </div>
            </>
          )}
        </div>
      </Card>

      {/* Nested Field Extraction */}
      <Card className="p-4">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-sm font-medium text-gray-900">
            Nested Field Extraction (Optional)
          </h3>
          <Button
            type="button"
            onClick={addNestedPath}
            className="text-sm px-3 py-1"
          >
            + Add Field Path
          </Button>
        </div>

        {(config.nested_field_paths || []).length === 0 ? (
          <p className="text-sm text-gray-500 italic">
            No field paths defined. All fields will be extracted.
          </p>
        ) : (
          <div className="space-y-2">
            {(config.nested_field_paths || []).map((path, index) => (
              <div key={index} className="flex items-center space-x-2">
                <Input
                  type="text"
                  value={path}
                  onChange={(e) => updateNestedPath(index, e.target.value)}
                  placeholder="data.items[*].id"
                  className="flex-1"
                />
                <Button
                  type="button"
                  onClick={() => removeNestedPath(index)}
                  className="text-red-600 hover:text-red-800 px-2 py-1"
                >
                  Remove
                </Button>
              </div>
            ))}
          </div>
        )}

        <p className="text-xs text-gray-500 mt-3">
          Use JSON path notation (e.g., "data.items[*].id", "user.profile.email")
        </p>
      </Card>

      {/* Schema Inference */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Schema Inference</h3>
        <div className="space-y-4">
          <div className="flex items-center space-x-3">
            <input
              type="checkbox"
              id="schema_inference"
              checked={config.schema_inference !== false}  // Default true
              onChange={(e) => handleChange('schema_inference', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="schema_inference" className="text-sm font-medium text-gray-700">
              Enable schema inference
            </label>
          </div>

          {config.schema_inference !== false && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Sample Rows for Inference
              </label>
              <Input
                type="number"
                min="100"
                max="100000"
                value={config.sample_rows || 10000}
                onChange={(e) => handleChange('sample_rows', parseInt(e.target.value))}
              />
              <p className="text-xs text-gray-500 mt-1">
                Number of rows to sample for automatic schema detection
              </p>
            </div>
          )}
        </div>
      </Card>

      {/* Help Text */}
      <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
        <div className="flex">
          <svg
            className="h-5 w-5 text-blue-600 mt-0.5 mr-2 flex-shrink-0"
            fill="currentColor"
            viewBox="0 0 20 20"
          >
            <path
              fillRule="evenodd"
              d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z"
              clipRule="evenodd"
            />
          </svg>
          <div className="text-sm text-blue-700">
            <p className="font-medium mb-1">Nested Source Tips:</p>
            <ul className="list-disc list-inside space-y-1 text-xs">
              <li>Enable flattening to convert nested JSON/XML to flat tables</li>
              <li>Use field paths to extract only specific nested fields</li>
              <li>Choose "explode" for arrays to create one row per element</li>
              <li>Schema inference samples files to detect data types automatically</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  )
}
