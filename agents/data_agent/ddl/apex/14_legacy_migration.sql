-- ═══════════════════════════════════════════════════════════════════════════
-- APEX DATA AGENT - LEGACY MIGRATION TABLES
-- Part 14: Migration Jobs, Objects, Lineage, Artifacts
--
-- Tracks the full lifecycle of migrating SSIS / legacy ETL packages to
-- modern Airflow + PySpark equivalents.
--
-- Flow:
--   platform_migration_job  →  platform_migration_object (one per SP/view/proc extracted)
--                 →  platform_migration_lineage (directed dependency graph edges)
--                 →  platform_migration_artifact (LLM-generated PySpark/Airflow code)
-- ═══════════════════════════════════════════════════════════════════════════

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 1. MIGRATION_JOB                                                         │
-- │    One row per migration request (DTSX + live DB or static .sql scan)   │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS platform_migration_job (
    job_id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id        UUID REFERENCES platform_connection_registry(connection_id),
    dtsx_source_path     VARCHAR(1000),           -- GCS path to .dtsx file, nullable
    schema_filter        VARCHAR(200) DEFAULT '%', -- SQL LIKE pattern for schema
    proc_name_pattern    VARCHAR(200) DEFAULT '%', -- SQL LIKE pattern for object names
    target_feed_group_id UUID REFERENCES platform_feed_group(feed_group_id),
    status               VARCHAR(30) NOT NULL DEFAULT 'PENDING'
                             CHECK (status IN (
                                 'PENDING', 'EXTRACTING', 'GRAPH_BUILDING',
                                 'LLM_GENERATING', 'COMPLETE', 'FAILED'
                             )),
    total_objects        INTEGER DEFAULT 0,
    extracted_objects    INTEGER DEFAULT 0,
    failed_objects       INTEGER DEFAULT 0,
    skipped_objects      INTEGER DEFAULT 0,       -- encrypted / not_found
    extraction_source    VARCHAR(20) NOT NULL DEFAULT 'LIVE_DB'
                             CHECK (extraction_source IN ('LIVE_DB', 'STATIC_FILES', 'HYBRID')),
    dtsx_summary         JSONB DEFAULT '{}',       -- output of DTSXParser.get_migration_summary()
    error_message        TEXT,
    started_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at         TIMESTAMP,
    created_by           VARCHAR(200),
    created_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mig_job_status     ON platform_migration_job(status);
CREATE INDEX IF NOT EXISTS idx_mig_job_conn       ON platform_migration_job(connection_id);
CREATE INDEX IF NOT EXISTS idx_mig_job_created    ON platform_migration_job(created_at DESC);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 2. MIGRATION_OBJECT                                                      │
-- │    One row per stored procedure / function / view extracted              │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS platform_migration_object (
    object_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id             UUID NOT NULL
                           REFERENCES platform_migration_job(job_id) ON DELETE CASCADE,
    object_schema      VARCHAR(200) NOT NULL,
    object_name        VARCHAR(400) NOT NULL,
    object_type        VARCHAR(30)  NOT NULL
                           CHECK (object_type IN (
                               'PROCEDURE', 'FUNCTION', 'VIEW',
                               'TRIGGER', 'PACKAGE', 'PACKAGE_BODY'
                           )),
    db_platform        VARCHAR(30)  NOT NULL
                           CHECK (db_platform IN ('SQLSERVER', 'ORACLE', 'POSTGRESQL')),
    -- Full procedure body (from OBJECT_DEFINITION / ALL_SOURCE / pg_get_functiondef)
    definition_text    TEXT,
    -- [{name, data_type, direction: INPUT|OUTPUT|INOUT, default_value}]
    parameter_list     JSONB NOT NULL DEFAULT '[]',
    -- Raw refs from sys.sql_expression_dependencies / ALL_DEPENDENCIES / pg_depend
    referenced_objects JSONB NOT NULL DEFAULT '[]',
    extraction_source  VARCHAR(20)  NOT NULL DEFAULT 'LIVE_DB'
                           CHECK (extraction_source IN ('LIVE_DB', 'STATIC_FILES')),
    char_count         INTEGER,
    is_encrypted       BOOLEAN NOT NULL DEFAULT false,
    extraction_status  VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                           CHECK (extraction_status IN (
                               'PENDING', 'EXTRACTED', 'ENCRYPTED',
                               'NOT_FOUND', 'FAILED'
                           )),
    error_message      TEXT,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (job_id, object_schema, object_name, object_type)
);

CREATE INDEX IF NOT EXISTS idx_mig_obj_job        ON platform_migration_object(job_id);
CREATE INDEX IF NOT EXISTS idx_mig_obj_status     ON platform_migration_object(extraction_status);
CREATE INDEX IF NOT EXISTS idx_mig_obj_name       ON platform_migration_object(object_schema, object_name);
CREATE INDEX IF NOT EXISTS idx_mig_obj_type       ON platform_migration_object(object_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 3. MIGRATION_LINEAGE                                                     │
-- │    Directed edges of the dependency graph                                │
-- │    parent calls / references child                                       │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS platform_migration_lineage (
    lineage_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id           UUID NOT NULL
                         REFERENCES platform_migration_job(job_id) ON DELETE CASCADE,
    parent_object_id UUID NOT NULL
                         REFERENCES platform_migration_object(object_id) ON DELETE CASCADE,
    child_object_id  UUID NOT NULL
                         REFERENCES platform_migration_object(object_id) ON DELETE CASCADE,
    reference_type   VARCHAR(30) NOT NULL DEFAULT 'CALLS'
                         CHECK (reference_type IN (
                             'CALLS', 'SELECTS_FROM', 'INSERTS_INTO',
                             'UPDATES', 'DELETES_FROM'
                         )),
    is_cross_schema  BOOLEAN NOT NULL DEFAULT false,
    -- 0 = deepest leaf (no outgoing calls); higher = depends on lower levels
    topo_level       INTEGER,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE  (job_id, parent_object_id, child_object_id, reference_type),
    CHECK   (parent_object_id <> child_object_id)   -- no self-references
);

CREATE INDEX IF NOT EXISTS idx_mig_lin_job     ON platform_migration_lineage(job_id);
CREATE INDEX IF NOT EXISTS idx_mig_lin_parent  ON platform_migration_lineage(parent_object_id);
CREATE INDEX IF NOT EXISTS idx_mig_lin_child   ON platform_migration_lineage(child_object_id);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 4. MIGRATION_ARTIFACT                                                    │
-- │    LLM-generated code for each migrated object                          │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS platform_migration_artifact (
    artifact_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    object_id         UUID NOT NULL
                          REFERENCES platform_migration_object(object_id) ON DELETE CASCADE,
    job_id            UUID NOT NULL
                          REFERENCES platform_migration_job(job_id) ON DELETE CASCADE,
    artifact_type     VARCHAR(30) NOT NULL
                          CHECK (artifact_type IN (
                              'PYSPARK_JOB', 'AIRFLOW_OPERATOR',
                              'SQL_DDL', 'GREAT_EXPECTATIONS', 'SPARK_SUBMIT_CONFIG'
                          )),
    artifact_path     VARCHAR(1000),    -- GCS path (for large artifacts stored externally)
    artifact_content  TEXT,            -- inline content (for small artifacts)
    llm_model         VARCHAR(100),
    llm_prompt_tokens INTEGER,
    llm_output_tokens INTEGER,
    -- LLM self-reported confidence: 0.000–1.000
    confidence_score  NUMERIC(4, 3)    CHECK (confidence_score BETWEEN 0 AND 1),
    -- Syntax/import validation result
    validation_status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                          CHECK (validation_status IN (
                              'PENDING', 'PASSED', 'FAILED', 'SKIPPED'
                          )),
    validation_errors JSONB NOT NULL DEFAULT '[]',
    generation_ms     INTEGER,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mig_art_object ON platform_migration_artifact(object_id);
CREATE INDEX IF NOT EXISTS idx_mig_art_job    ON platform_migration_artifact(job_id);
CREATE INDEX IF NOT EXISTS idx_mig_art_type   ON platform_migration_artifact(artifact_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ HELPFUL VIEWS                                                            │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE OR REPLACE VIEW v_migration_job_summary AS
SELECT
    j.job_id,
    j.status,
    j.extraction_source,
    j.total_objects,
    j.extracted_objects,
    j.failed_objects,
    j.skipped_objects,
    j.started_at,
    j.completed_at,
    j.created_by,
    EXTRACT(EPOCH FROM (COALESCE(j.completed_at, CURRENT_TIMESTAMP) - j.started_at))::INTEGER
        AS duration_seconds,
    COUNT(DISTINCT mo.object_id)  AS object_count,
    COUNT(DISTINCT ma.artifact_id) AS artifact_count,
    COUNT(DISTINCT ml.lineage_id)  AS lineage_edge_count,
    AVG(ma.confidence_score)       AS avg_confidence
FROM platform_migration_job j
LEFT JOIN platform_migration_object   mo ON mo.job_id = j.job_id
LEFT JOIN platform_migration_artifact ma ON ma.job_id = j.job_id
LEFT JOIN platform_migration_lineage  ml ON ml.job_id = j.job_id
GROUP BY j.job_id, j.status, j.extraction_source,
         j.total_objects, j.extracted_objects, j.failed_objects, j.skipped_objects,
         j.started_at, j.completed_at, j.created_by;
