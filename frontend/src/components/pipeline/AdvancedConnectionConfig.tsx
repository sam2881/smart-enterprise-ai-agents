/**
 * AdvancedConnectionConfig - Advanced connection settings for sources
 *
 * Features: SSL/TLS, custom auth, connection strings, schema registry
 * Part of Phase 3 - Medium Priority Enhancements
 */

import React from 'react'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { Textarea } from '@/components/ui/Textarea'

export interface AdvancedConnectionConfigData {
  // SSL/TLS Configuration
  use_ssl?: boolean
  ssl_cert_path?: string
  ssl_key_path?: string
  ssl_ca_path?: string
  verify_ssl?: boolean

  // Custom Authentication
  auth_method?: 'basic' | 'bearer' | 'api_key' | 'oauth2' | 'kerberos' | 'custom'
  custom_auth_header?: string
  custom_auth_value?: string

  // Connection Override
  connection_string?: string  // Manual connection string override
  connection_options?: Record<string, string>  // Key-value connection options

  // Schema Registry (for Avro/Confluent)
  schema_registry_url?: string
  schema_registry_api_key_secret?: string
  avro_schema_id?: number
  avro_schema_version?: string

  // Advanced Network
  proxy_host?: string
  proxy_port?: number
  timeout_seconds?: number
  max_retries?: number
  backoff_factor?: number
}

interface AdvancedConnectionConfigProps {
  config: Partial<AdvancedConnectionConfigData>
  onChange: (config: Partial<AdvancedConnectionConfigData>) => void
}

export const AdvancedConnectionConfig: React.FC<AdvancedConnectionConfigProps> = ({
  config,
  onChange
}) => {
  const handleChange = (field: keyof AdvancedConnectionConfigData, value: any) => {
    onChange({ ...config, [field]: value })
  }

  const handleConnectionOptionsChange = (optionsText: string) => {
    try {
      const lines = optionsText.split('\n').filter(line => line.trim())
      const options: Record<string, string> = {}
      lines.forEach(line => {
        const [key, ...valueParts] = line.split('=')
        if (key) {
          options[key.trim()] = valueParts.join('=').trim()
        }
      })
      handleChange('connection_options', options)
    } catch (e) {
      // Invalid format, ignore
    }
  }

  return (
    <div className="space-y-6">
      {/* SSL/TLS Configuration */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">SSL/TLS Configuration</h3>
        <div className="space-y-4">
          <div className="flex items-center space-x-3">
            <input
              type="checkbox"
              id="use_ssl"
              checked={config.use_ssl || false}
              onChange={(e) => handleChange('use_ssl', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="use_ssl" className="text-sm font-medium text-gray-700">
              Enable SSL/TLS encryption
            </label>
          </div>

          {config.use_ssl && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  SSL Certificate Path
                </label>
                <Input
                  type="text"
                  value={config.ssl_cert_path || ''}
                  onChange={(e) => handleChange('ssl_cert_path', e.target.value)}
                  placeholder="gs://bucket/certs/client-cert.pem"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  SSL Key Path
                </label>
                <Input
                  type="text"
                  value={config.ssl_key_path || ''}
                  onChange={(e) => handleChange('ssl_key_path', e.target.value)}
                  placeholder="gs://bucket/certs/client-key.pem"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  SSL CA Certificate Path
                </label>
                <Input
                  type="text"
                  value={config.ssl_ca_path || ''}
                  onChange={(e) => handleChange('ssl_ca_path', e.target.value)}
                  placeholder="gs://bucket/certs/ca-cert.pem"
                />
              </div>

              <div className="flex items-center space-x-3">
                <input
                  type="checkbox"
                  id="verify_ssl"
                  checked={config.verify_ssl !== false}
                  onChange={(e) => handleChange('verify_ssl', e.target.checked)}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                />
                <label htmlFor="verify_ssl" className="text-sm font-medium text-gray-700">
                  Verify SSL certificates
                </label>
              </div>
            </>
          )}
        </div>
      </Card>

      {/* Custom Authentication */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Custom Authentication</h3>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Authentication Method
            </label>
            <Select
              value={config.auth_method || 'basic'}
              onChange={(e) => handleChange('auth_method', e.target.value)}
            >
              <option value="basic">Basic (Username/Password)</option>
              <option value="bearer">Bearer Token</option>
              <option value="api_key">API Key</option>
              <option value="oauth2">OAuth 2.0</option>
              <option value="kerberos">Kerberos</option>
              <option value="custom">Custom Header</option>
            </Select>
          </div>

          {config.auth_method === 'custom' && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Custom Auth Header Name
                </label>
                <Input
                  type="text"
                  value={config.custom_auth_header || ''}
                  onChange={(e) => handleChange('custom_auth_header', e.target.value)}
                  placeholder="X-API-Key"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Custom Auth Value (or Secret Path)
                </label>
                <Input
                  type="text"
                  value={config.custom_auth_value || ''}
                  onChange={(e) => handleChange('custom_auth_value', e.target.value)}
                  placeholder="projects/PROJECT_ID/secrets/API_KEY/versions/latest"
                />
              </div>
            </>
          )}
        </div>
      </Card>

      {/* Connection String Override */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Connection Override</h3>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Manual Connection String (Optional)
            </label>
            <Input
              type="text"
              value={config.connection_string || ''}
              onChange={(e) => handleChange('connection_string', e.target.value)}
              placeholder="postgresql://user:password@host:5432/database?sslmode=require"
            />
            <p className="text-xs text-gray-500 mt-1">
              Override default connection with custom JDBC/connection string
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Connection Options (key=value per line)
            </label>
            <Textarea
              rows={4}
              value={Object.entries(config.connection_options || {})
                .map(([k, v]) => `${k}=${v}`)
                .join('\n')}
              onChange={(e) => handleConnectionOptionsChange(e.target.value)}
              placeholder="connectTimeout=30000\nreadTimeout=60000\nreWriteBatchedInserts=true"
            />
            <p className="text-xs text-gray-500 mt-1">
              Additional connection parameters (one per line)
            </p>
          </div>
        </div>
      </Card>

      {/* Schema Registry (Avro/Confluent) */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Schema Registry (Avro/Confluent)</h3>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Schema Registry URL
            </label>
            <Input
              type="url"
              value={config.schema_registry_url || ''}
              onChange={(e) => handleChange('schema_registry_url', e.target.value)}
              placeholder="https://schema-registry.example.com"
            />
          </div>

          {config.schema_registry_url && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Schema Registry API Key Secret
                </label>
                <Input
                  type="text"
                  value={config.schema_registry_api_key_secret || ''}
                  onChange={(e) => handleChange('schema_registry_api_key_secret', e.target.value)}
                  placeholder="projects/PROJECT_ID/secrets/SCHEMA_REGISTRY_KEY/versions/latest"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Avro Schema ID
                  </label>
                  <Input
                    type="number"
                    value={config.avro_schema_id || ''}
                    onChange={(e) => handleChange('avro_schema_id', parseInt(e.target.value))}
                    placeholder="12345"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Avro Schema Version
                  </label>
                  <Input
                    type="text"
                    value={config.avro_schema_version || ''}
                    onChange={(e) => handleChange('avro_schema_version', e.target.value)}
                    placeholder="v1"
                  />
                </div>
              </div>
            </>
          )}
        </div>
      </Card>

      {/* Advanced Network Settings */}
      <Card className="p-4">
        <h3 className="text-sm font-medium text-gray-900 mb-4">Advanced Network Settings</h3>
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Proxy Host
              </label>
              <Input
                type="text"
                value={config.proxy_host || ''}
                onChange={(e) => handleChange('proxy_host', e.target.value)}
                placeholder="proxy.example.com"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Proxy Port
              </label>
              <Input
                type="number"
                value={config.proxy_port || ''}
                onChange={(e) => handleChange('proxy_port', parseInt(e.target.value))}
                placeholder="8080"
              />
            </div>
          </div>

          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Timeout (seconds)
              </label>
              <Input
                type="number"
                min="1"
                value={config.timeout_seconds || 30}
                onChange={(e) => handleChange('timeout_seconds', parseInt(e.target.value))}
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Max Retries
              </label>
              <Input
                type="number"
                min="0"
                max="10"
                value={config.max_retries || 3}
                onChange={(e) => handleChange('max_retries', parseInt(e.target.value))}
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Backoff Factor
              </label>
              <Input
                type="number"
                step="0.1"
                min="0"
                max="10"
                value={config.backoff_factor || 2}
                onChange={(e) => handleChange('backoff_factor', parseFloat(e.target.value))}
              />
            </div>
          </div>

          <p className="text-xs text-gray-500">
            Exponential backoff: wait_time = backoff_factor ^ retry_number seconds
          </p>
        </div>
      </Card>
    </div>
  )
}
