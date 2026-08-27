-- Enterprise Agentic Platform — PostgreSQL initialization
-- Runs automatically when the container first starts

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS agent;
CREATE SCHEMA IF NOT EXISTS pipeline;
CREATE SCHEMA IF NOT EXISTS audit;

-- Incidents table (Incident Management System)
CREATE TABLE IF NOT EXISTS agent.incidents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    snow_incident_id VARCHAR(50) UNIQUE,
    title TEXT NOT NULL,
    description TEXT,
    priority VARCHAR(20),
    status VARCHAR(50) DEFAULT 'new',
    workflow_state VARCHAR(50),
    assigned_to VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}'
);

-- Approvals table
CREATE TABLE IF NOT EXISTS agent.approvals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id UUID REFERENCES agent.incidents(id),
    plan JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    requested_by VARCHAR(100),
    approved_by VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    decided_at TIMESTAMPTZ,
    notes TEXT
);

-- Pipeline runs table (Data Engineering Agent)
CREATE TABLE IF NOT EXISTS pipeline.runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dag_id VARCHAR(200),
    jira_ticket VARCHAR(50),
    created_by VARCHAR(100),
    input_type VARCHAR(50),
    input_payload JSONB NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    artifacts JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT
);

-- Audit log
CREATE TABLE IF NOT EXISTS audit.events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50),
    entity_id UUID,
    actor VARCHAR(100),
    payload JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_incidents_status ON agent.incidents(status);
CREATE INDEX IF NOT EXISTS idx_incidents_snow_id ON agent.incidents(snow_incident_id);
CREATE INDEX IF NOT EXISTS idx_approvals_status ON agent.approvals(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline.runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_jira ON pipeline.runs(jira_ticket);
CREATE INDEX IF NOT EXISTS idx_audit_events_type ON audit.events(event_type, created_at);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA agent TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pipeline TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA audit TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA agent TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA pipeline TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA audit TO admin;
