/**
 * SpecialSourceConfigForm - Configuration for special/advanced data sources
 *
 * Supports: special_iot, special_timeseries, special_geospatial, special_ml_features, special_open_data
 * Features: Time-series windows, geospatial indexing, feature engineering
 *
 * Part of Phase 2 - UI Backend Gap Analysis Remediation
 */

import React from 'react'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { Button } from '@/components/ui/Button'

export interface SpecialSourceConfig {
  storage_path: string
  file_pattern?: string

  // Time-series specific
  timestamp_column?: string
  time_window_size?: string  // e.g., "1 hour", "5 minutes"
  time_window_unit?: 'second' | 'minute' | 'hour' | 'day'
  aggregation_functions?: string[]  // sum, avg, min, max, count
  partition_by_time?: boolean

  // Geospatial specific
  latitude_column?: string
  longitude_column?: string
  geometry_column?: string  // WKT or GeoJSON column
  spatial_index?: boolean
  coordinate_system?: string  // e.g., "EPSG:4326" (WGS84)
  bounding_box?: {
    min_lat: number
    max_lat: number
    min_lon: number
    max_lon: number
  }

  // IoT specific
  device_id_column?: string
  sensor_type_column?: string
  measurement_column?: string
  quality_flag_column?: string
  filter_invalid_readings?: boolean

  // ML Features specific
  feature_columns?: string[]
  label_column?: string
  feature_engineering?: boolean
  normalize_features?: boolean

  // Open Data specific
  data_source_url?: string
  update_frequency?: string
  api_key_secret?: string
}

interface SpecialSourceConfigFormProps {
  config: Partial<SpecialSourceConfig>
  onChange: (config: Partial<SpecialSourceConfig>) => void
  sourceType: 'special_iot' | 'special_timeseries' | 'special_geospatial' | 'special_ml_features' | 'special_open_data'
}

export const SpecialSourceConfigForm: React.FC<SpecialSourceConfigFormProps> = ({
  config,
  onChange,
  sourceType
}) => {
  const handleChange = (field: keyof SpecialSourceConfig, value: any) => {
    onChange({ ...config, [field]: value })
  }

  const renderTimeSeriesFields = () => (
    <Card className="p-4">
      <h3 className="text-sm font-medium text-gray-900 mb-4">Time-Series Configuration</h3>
      <div className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Timestamp Column *
          </label>
          <Input
            type="text"
            value={config.timestamp_column || ''}
            onChange={(e) => handleChange('timestamp_column', e.target.value)}
            placeholder="event_time"
            required
          />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Time Window Size
            </label>
            <Input
              type="number"
              min="1"
              value={config.time_window_size || ''}
              onChange={(e) => handleChange('time_window_size', e.target.value)}
              placeholder="5"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Time Window Unit
            </label>
            <Select
              value={config.time_window_unit || 'minute'}
              onChange={(e) => handleChange('time_window_unit', e.target.value)}
            >
              <option value="second">Second</option>
              <option value="minute">Minute</option>
              <option value="hour">Hour</option>
              <option value="day">Day</option>
            </Select>
          </div>
        </div>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            id="partition_by_time"
            checked={config.partition_by_time || false}
            onChange={(e) => handleChange('partition_by_time', e.target.checked)}
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
          />
          <label htmlFor="partition_by_time" className="text-sm font-medium text-gray-700">
            Partition by time windows
          </label>
        </div>

        <p className="text-xs text-gray-500">
          Time-series data will be aggregated into {config.time_window_size || '5'} {config.time_window_unit || 'minute'} windows
        </p>
      </div>
    </Card>
  )

  const renderGeospatialFields = () => (
    <Card className="p-4">
      <h3 className="text-sm font-medium text-gray-900 mb-4">Geospatial Configuration</h3>
      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Latitude Column
            </label>
            <Input
              type="text"
              value={config.latitude_column || ''}
              onChange={(e) => handleChange('latitude_column', e.target.value)}
              placeholder="lat"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Longitude Column
            </label>
            <Input
              type="text"
              value={config.longitude_column || ''}
              onChange={(e) => handleChange('longitude_column', e.target.value)}
              placeholder="lon"
            />
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Geometry Column (WKT/GeoJSON)
          </label>
          <Input
            type="text"
            value={config.geometry_column || ''}
            onChange={(e) => handleChange('geometry_column', e.target.value)}
            placeholder="geometry"
          />
          <p className="text-xs text-gray-500 mt-1">
            Column containing WKT or GeoJSON geometry data
          </p>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Coordinate System
          </label>
          <Select
            value={config.coordinate_system || 'EPSG:4326'}
            onChange={(e) => handleChange('coordinate_system', e.target.value)}
          >
            <option value="EPSG:4326">WGS84 (EPSG:4326)</option>
            <option value="EPSG:3857">Web Mercator (EPSG:3857)</option>
            <option value="EPSG:2163">US National Atlas (EPSG:2163)</option>
          </Select>
        </div>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            id="spatial_index"
            checked={config.spatial_index || false}
            onChange={(e) => handleChange('spatial_index', e.target.checked)}
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
          />
          <label htmlFor="spatial_index" className="text-sm font-medium text-gray-700">
            Create spatial index for fast geospatial queries
          </label>
        </div>
      </div>
    </Card>
  )

  const renderIoTFields = () => (
    <Card className="p-4">
      <h3 className="text-sm font-medium text-gray-900 mb-4">IoT Device Configuration</h3>
      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Device ID Column *
            </label>
            <Input
              type="text"
              value={config.device_id_column || ''}
              onChange={(e) => handleChange('device_id_column', e.target.value)}
              placeholder="device_id"
              required
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Sensor Type Column
            </label>
            <Input
              type="text"
              value={config.sensor_type_column || ''}
              onChange={(e) => handleChange('sensor_type_column', e.target.value)}
              placeholder="sensor_type"
            />
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Measurement Column *
            </label>
            <Input
              type="text"
              value={config.measurement_column || ''}
              onChange={(e) => handleChange('measurement_column', e.target.value)}
              placeholder="value"
              required
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Quality Flag Column
            </label>
            <Input
              type="text"
              value={config.quality_flag_column || ''}
              onChange={(e) => handleChange('quality_flag_column', e.target.value)}
              placeholder="quality"
            />
          </div>
        </div>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            id="filter_invalid"
            checked={config.filter_invalid_readings !== false}
            onChange={(e) => handleChange('filter_invalid_readings', e.target.checked)}
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
          />
          <label htmlFor="filter_invalid" className="text-sm font-medium text-gray-700">
            Filter out invalid/anomalous readings
          </label>
        </div>
      </div>
    </Card>
  )

  const renderMLFeaturesFields = () => (
    <Card className="p-4">
      <h3 className="text-sm font-medium text-gray-900 mb-4">ML Features Configuration</h3>
      <div className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Feature Columns (comma-separated)
          </label>
          <Input
            type="text"
            value={(config.feature_columns || []).join(', ')}
            onChange={(e) => handleChange('feature_columns', e.target.value.split(',').map(s => s.trim()))}
            placeholder="age, income, credit_score"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Label Column (target variable)
          </label>
          <Input
            type="text"
            value={config.label_column || ''}
            onChange={(e) => handleChange('label_column', e.target.value)}
            placeholder="churn_flag"
          />
        </div>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            id="feature_engineering"
            checked={config.feature_engineering || false}
            onChange={(e) => handleChange('feature_engineering', e.target.checked)}
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
          />
          <label htmlFor="feature_engineering" className="text-sm font-medium text-gray-700">
            Enable automated feature engineering
          </label>
        </div>

        <div className="flex items-center space-x-3">
          <input
            type="checkbox"
            id="normalize_features"
            checked={config.normalize_features || false}
            onChange={(e) => handleChange('normalize_features', e.target.checked)}
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
          />
          <label htmlFor="normalize_features" className="text-sm font-medium text-gray-700">
            Normalize features (min-max scaling)
          </label>
        </div>
      </div>
    </Card>
  )

  const renderOpenDataFields = () => (
    <Card className="p-4">
      <h3 className="text-sm font-medium text-gray-900 mb-4">Open Data Configuration</h3>
      <div className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Data Source URL *
          </label>
          <Input
            type="url"
            value={config.data_source_url || ''}
            onChange={(e) => handleChange('data_source_url', e.target.value)}
            placeholder="https://data.example.gov/api/dataset"
            required
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Update Frequency
          </label>
          <Select
            value={config.update_frequency || 'daily'}
            onChange={(e) => handleChange('update_frequency', e.target.value)}
          >
            <option value="hourly">Hourly</option>
            <option value="daily">Daily</option>
            <option value="weekly">Weekly</option>
            <option value="monthly">Monthly</option>
          </Select>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            API Key Secret (optional)
          </label>
          <Input
            type="text"
            value={config.api_key_secret || ''}
            onChange={(e) => handleChange('api_key_secret', e.target.value)}
            placeholder="projects/PROJECT_ID/secrets/API_KEY/versions/latest"
          />
          <p className="text-xs text-gray-500 mt-1">
            GCP Secret Manager path for API authentication
          </p>
        </div>
      </div>
    </Card>
  )

  return (
    <div className="space-y-6">
      {/* Storage Path (common to all types) */}
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
              placeholder="gs://bucket/path/to/data/"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              File Pattern
            </label>
            <Input
              type="text"
              value={config.file_pattern || ''}
              onChange={(e) => handleChange('file_pattern', e.target.value)}
              placeholder="*.parquet"
            />
          </div>
        </div>
      </Card>

      {/* Type-specific fields */}
      {sourceType === 'special_timeseries' && renderTimeSeriesFields()}
      {sourceType === 'special_geospatial' && renderGeospatialFields()}
      {sourceType === 'special_iot' && renderIoTFields()}
      {sourceType === 'special_ml_features' && renderMLFeaturesFields()}
      {sourceType === 'special_open_data' && renderOpenDataFields()}
    </div>
  )
}
