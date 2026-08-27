"""
Data Pipeline Agent API Server.

WHY: Provides HTTP API for triggering pipeline generation workflows.
HOW: FastAPI server that receives pipeline intents and runs LangGraph workflow.

Endpoints:
- POST /pipelines - Start new pipeline generation
- GET /pipelines/{request_id} - Get pipeline status
- POST /pipelines/{request_id}/approve - Approve pending pipeline
- GET /health - Health check
"""

import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import structlog

from src.graphs.apex_workflow import (
    create_apex_workflow,
    create_initial_apex_state,
    run_apex_workflow_sync,
)
from src.config.settings import get_settings

# Initialize logger
logger = structlog.get_logger()

# Initialize FastAPI app
app = FastAPI(
    title="Data Pipeline Agent API",
    description="AI-powered data pipeline generation service",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for pipeline states (use Redis in production)
pipeline_states: Dict[str, Dict[str, Any]] = {}


# =============================================================================
# Request/Response Models
# =============================================================================


class PipelineIdentity(BaseModel):
    """Pipeline identification info."""
    pipeline_name: str
    project_name: str
    team: str = "data-engineering"


class SourceConfig(BaseModel):
    """Source system configuration."""
    source_type: str  # postgres, mysql, gcs, api, kafka
    connection_id: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    file_path: Optional[str] = None
    api_endpoint: Optional[str] = None


class TargetConfig(BaseModel):
    """Target system configuration."""
    target_type: str = "bigquery"
    dataset: str
    table: str
    write_mode: str = "append"  # append, overwrite, merge


class SchemaColumn(BaseModel):
    """Column definition."""
    name: str
    type: str
    mode: str = "NULLABLE"  # NULLABLE, REQUIRED, REPEATED
    description: Optional[str] = None


class SchemaDefinition(BaseModel):
    """Schema definition for the pipeline."""
    columns: list[SchemaColumn]
    partition_column: Optional[str] = None
    cluster_columns: Optional[list[str]] = None


class ExecutionPolicy(BaseModel):
    """Execution policy configuration."""
    schedule: str = "0 2 * * *"  # Cron expression
    retry_count: int = 3
    environment: str = "dev"  # dev, qa, prod
    timeout_minutes: int = 60


class PipelineIntentRequest(BaseModel):
    """Request to create a new pipeline."""
    pipeline_identity: PipelineIdentity
    source_config: SourceConfig
    target_config: TargetConfig
    schema_definition: SchemaDefinition
    execution_policy: ExecutionPolicy = Field(default_factory=ExecutionPolicy)
    jira_key: Optional[str] = None


class PipelineResponse(BaseModel):
    """Pipeline status response."""
    request_id: str
    status: str
    current_phase: str
    message: str
    created_at: str
    updated_at: str
    artifacts: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class ApprovalRequest(BaseModel):
    """Approval request body."""
    approved: bool
    approver: str
    notes: Optional[str] = None


# =============================================================================
# Conversion Layer: API Models → APEX Workflow
# =============================================================================


def convert_api_request_to_apex(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert PipelineIntentRequest to APEX workflow format.

    Maps user-friendly API models to metadata-driven APEX format.

    API Format:
        - pipeline_identity: {pipeline_name, project_name, team}
        - source_config: {source_type, connection_id, ...}
        - target_config: {target_type, dataset, table, write_mode}
        - schema_definition: {columns, partition_column, cluster_columns}
        - execution_policy: {schedule, retry_count, environment, timeout_minutes}

    APEX Format:
        - feed: {feed_id, feed_name, feed_type, domain, schedule}
        - source: {source_type, file_path, connection_id, ...}
        - bronze_schema: {columns}
        - target: {dataset, table, write_mode}
        - environment: str
    """
    pipeline = intent.get("pipeline_identity", {})
    source = intent.get("source_config", {})
    target = intent.get("target_config", {})
    schema = intent.get("schema_definition", {})
    policy = intent.get("execution_policy", {})

    # Generate APEX identifiers
    feed_id = f"FEED_{uuid.uuid4().hex[:8].upper()}"
    feed_name = pipeline.get("pipeline_name", "unknown_pipeline")
    domain = pipeline.get("project_name", "default")
    contract_id = f"CONTRACT_{uuid.uuid4().hex[:8].upper()}"
    source_id = f"SOURCE_{uuid.uuid4().hex[:8].upper()}"

    # Map source type to APEX format
    source_type_map = {
        "postgres": "DATABASE",
        "mysql": "DATABASE",
        "gcs": "FILE",
        "api": "API",
        "kafka": "KAFKA",
        # Add more mappings as needed
    }
    apex_source_type = source_type_map.get(
        source.get("source_type", "").lower(),
        "FILE"
    )

    # Map target type to contract type (use STANDARD for regular pipelines)
    # ContractType enum values: STANDARD, SCD2, DATA_VAULT, STAR_SCHEMA
    contract_type = "STANDARD"  # Default to STANDARD for regular pipelines

    # Build APEX format
    apex_request = {
        "feed": {
            "feed_id": feed_id,
            "feed_name": feed_name,
            "feed_type": "BATCH",
            "domain": domain,
            "schedule": policy.get("schedule", "@daily"),
            "is_active": True,
        },
        "contract": {
            "contract_id": contract_id,
            "contract_name": f"{feed_name}_contract",
            "feed_id": feed_id,
            "contract_type": contract_type,
            "version": 1,
            "schema_definition": schema,
            "is_active": True,
        },
        "source": {
            "source_id": source_id,
            "source_name": f"{feed_name}_source",
            "source_type": apex_source_type,
            "domain_id": domain,
            "connection_id": source.get("connection_id"),
            "is_active": True,
            "metadata": {
                "file_path": source.get("file_path"),
                "schema_name": source.get("schema_name"),
                "table_name": source.get("table_name"),
                "api_endpoint": source.get("api_endpoint"),
            },
        },
        "bronze_schema": {
            "columns": schema.get("columns", []),
            "partition_column": schema.get("partition_column"),
            "cluster_columns": schema.get("cluster_columns"),
        },
        "target": {
            "target_type": target.get("target_type", "bigquery"),
            "dataset": target.get("dataset"),
            "table": target.get("table"),
            "write_mode": target.get("write_mode", "append"),
        },
        "environment": policy.get("environment", "dev"),
        "jira_ticket": intent.get("jira_key"),
        "created_by": "api",
    }

    logger.info(
        "Converted API request to APEX format",
        feed_id=feed_id,
        feed_name=feed_name,
        contract_id=contract_id,
        source_type=apex_source_type,
        contract_type=contract_type,
    )

    return apex_request


# =============================================================================
# Background Task for Pipeline Execution
# =============================================================================


async def run_pipeline_background(request_id: str, intent_json: Dict[str, Any]):
    """Run pipeline workflow in background."""
    logger.info("Starting background pipeline", request_id=request_id)

    try:
        # Update state to running
        pipeline_states[request_id]["status"] = "running"
        pipeline_states[request_id]["current_phase"] = "validate_intent"
        pipeline_states[request_id]["updated_at"] = datetime.utcnow().isoformat()

        # Convert API format to APEX format
        apex_request = convert_api_request_to_apex(intent_json)

        # Run the pipeline
        result = run_apex_workflow_sync(apex_request, request_id)

        # Update state with result
        pipeline_states[request_id]["status"] = (
            "completed" if result.get("error_message") is None else "failed"
        )
        pipeline_states[request_id]["current_phase"] = result.get("current_phase", "unknown")
        pipeline_states[request_id]["updated_at"] = datetime.utcnow().isoformat()

        if result.get("error_message"):
            pipeline_states[request_id]["error"] = result.get("error_message")
        else:
            # Store artifacts
            pipeline_states[request_id]["artifacts"] = {
                "dag_content": result.get("generator_output", {}).get("dag_content"),
                "spark_jobs": result.get("generator_output", {}).get("spark_jobs"),
                "pr_url": result.get("deployer_output", {}).get("pr_url"),
            }

        logger.info(
            "Pipeline completed",
            request_id=request_id,
            status=pipeline_states[request_id]["status"],
        )

    except Exception as e:
        logger.error("Pipeline failed", request_id=request_id, error=str(e))
        pipeline_states[request_id]["status"] = "failed"
        pipeline_states[request_id]["error"] = str(e)
        pipeline_states[request_id]["updated_at"] = datetime.utcnow().isoformat()


# =============================================================================
# API Endpoints
# =============================================================================


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "data-pipeline-agent",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus metrics endpoint — scraped by prometheus service."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/pipelines", response_model=PipelineResponse)
async def create_pipeline(
    request: PipelineIntentRequest,
    background_tasks: BackgroundTasks,
):
    """
    Start a new pipeline generation workflow.

    This endpoint triggers the LangGraph workflow to:
    1. Validate the intent
    2. Plan the pipeline (detect changes, select templates)
    3. Generate artifacts (DAG, Spark jobs, SQL)
    4. Validate artifacts
    5. Wait for approval (if PROD)
    6. Deploy to Git repo
    """
    request_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    # Convert request to intent JSON
    intent_json = {
        "pipeline_identity": request.pipeline_identity.model_dump(),
        "source_config": request.source_config.model_dump(),
        "target_config": request.target_config.model_dump(),
        "schema_definition": request.schema_definition.model_dump(),
        "execution_policy": request.execution_policy.model_dump(),
        "jira_key": request.jira_key,
    }

    # Initialize state
    pipeline_states[request_id] = {
        "request_id": request_id,
        "status": "pending",
        "current_phase": "init",
        "message": "Pipeline creation started",
        "created_at": now,
        "updated_at": now,
        "intent_json": intent_json,
        "artifacts": None,
        "error": None,
    }

    # Start background task
    background_tasks.add_task(run_pipeline_background, request_id, intent_json)

    logger.info("Pipeline creation initiated", request_id=request_id)

    return PipelineResponse(
        request_id=request_id,
        status="pending",
        current_phase="init",
        message="Pipeline generation started",
        created_at=now,
        updated_at=now,
    )


@app.get("/pipelines/{request_id}", response_model=PipelineResponse)
async def get_pipeline_status(request_id: str):
    """Get the status of a pipeline generation request."""
    if request_id not in pipeline_states:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    state = pipeline_states[request_id]

    return PipelineResponse(
        request_id=request_id,
        status=state["status"],
        current_phase=state["current_phase"],
        message=f"Pipeline is {state['status']}",
        created_at=state["created_at"],
        updated_at=state["updated_at"],
        artifacts=state.get("artifacts"),
        error=state.get("error"),
    )


@app.post("/pipelines/{request_id}/approve", response_model=PipelineResponse)
async def approve_pipeline(request_id: str, approval: ApprovalRequest):
    """
    Approve or reject a pending pipeline.

    This is called when a pipeline requires human approval (e.g., PROD deployment).
    """
    if request_id not in pipeline_states:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    state = pipeline_states[request_id]

    if state["status"] != "pending_approval":
        raise HTTPException(
            status_code=400,
            detail=f"Pipeline is not pending approval (current status: {state['status']})",
        )

    # Update approval status
    state["approval"] = {
        "approved": approval.approved,
        "approver": approval.approver,
        "notes": approval.notes,
        "timestamp": datetime.utcnow().isoformat(),
    }

    if approval.approved:
        state["status"] = "deploying"
        state["message"] = f"Approved by {approval.approver}"
        # TODO: Resume workflow
    else:
        state["status"] = "rejected"
        state["message"] = f"Rejected by {approval.approver}: {approval.notes}"

    state["updated_at"] = datetime.utcnow().isoformat()

    logger.info(
        "Pipeline approval processed",
        request_id=request_id,
        approved=approval.approved,
        approver=approval.approver,
    )

    return PipelineResponse(
        request_id=request_id,
        status=state["status"],
        current_phase=state["current_phase"],
        message=state["message"],
        created_at=state["created_at"],
        updated_at=state["updated_at"],
        artifacts=state.get("artifacts"),
        error=state.get("error"),
    )


@app.get("/pipelines")
async def list_pipelines(
    status: Optional[str] = None,
    limit: int = 50,
):
    """List all pipelines, optionally filtered by status."""
    pipelines = []

    for request_id, state in pipeline_states.items():
        if status and state["status"] != status:
            continue

        pipelines.append(
            PipelineResponse(
                request_id=request_id,
                status=state["status"],
                current_phase=state["current_phase"],
                message=f"Pipeline is {state['status']}",
                created_at=state["created_at"],
                updated_at=state["updated_at"],
                artifacts=state.get("artifacts"),
                error=state.get("error"),
            )
        )

        if len(pipelines) >= limit:
            break

    return {"pipelines": pipelines, "total": len(pipelines)}


# =============================================================================
# Legacy Migration Endpoints
# =============================================================================


class MigrationJobRequest(BaseModel):
    """Request to start a stored procedure migration job."""
    connection_code: str = Field(..., description="platform_connection_registry.connection_code for the source DB")
    schema_filter: str = Field(default="%", description="SQL LIKE pattern for schema names")
    proc_name_pattern: str = Field(default="%", description="SQL LIKE pattern for procedure names")
    dtsx_source_path: Optional[str] = Field(default=None, description="GCS path to .dtsx file (optional)")
    target_feed_group_id: Optional[str] = Field(default=None, description="UUID of target feed group")
    created_by: Optional[str] = Field(default=None)


class MigrationObjectSummary(BaseModel):
    object_id: str
    object_schema: str
    object_name: str
    object_type: str
    db_platform: str
    extraction_status: str
    char_count: Optional[int] = None
    is_encrypted: bool = False


class MigrationJobResponse(BaseModel):
    job_id: str
    status: str
    extraction_source: str
    total_objects: int = 0
    extracted_objects: int = 0
    failed_objects: int = 0
    skipped_objects: int = 0
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_by: Optional[str] = None
    objects: Optional[list] = None
    artifacts: Optional[list] = None
    dependency_graph: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


async def _run_migration_background(job_id: str, intent_json: Dict[str, Any]) -> None:
    """Run LegacyMigrationAgent in a background task."""
    try:
        from src.agents.legacy_migration_agent import LegacyMigrationAgent

        state: Dict[str, Any] = {
            "request_id": job_id,
            "intent_json": intent_json,
        }
        agent = LegacyMigrationAgent()
        result = agent.run(state)
        logger.info("migration_background_complete", job_id=job_id, error=result.get("error_message"))
    except Exception as exc:
        logger.error("migration_background_failed", job_id=job_id, error=str(exc))
        try:
            from src.repository.migration_repository import MigrationRepository
            settings = get_settings()
            repo = MigrationRepository(settings.get_database_url_str())
            repo.update_job_status(job_id, "FAILED", error_message=str(exc), completed=True)
        except Exception:
            pass


@app.post("/migration/jobs", response_model=MigrationJobResponse)
async def create_migration_job(request: MigrationJobRequest, background_tasks: BackgroundTasks):
    """
    Start a new legacy stored procedure migration job.

    Parses the source DB (or .dtsx file) for stored procedures, builds a
    dependency graph, and generates PySpark + Airflow artifacts via LLM.
    The job runs asynchronously — poll GET /migration/jobs/{job_id} for status.
    """
    settings = get_settings()

    # Pre-create the job row so we can return job_id immediately
    from src.repository.migration_repository import MigrationRepository
    repo = MigrationRepository(settings.get_database_url_str())

    try:
        job_id = repo.create_migration_job(
            connection_id=None,
            dtsx_source_path=request.dtsx_source_path,
            schema_filter=request.schema_filter,
            proc_name_pattern=request.proc_name_pattern,
            extraction_source="LIVE_DB",
            created_by=request.created_by,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create migration job: {exc}")

    intent_json = {
        "source": {
            "source_type": "legacy_ssis",
            "connection_code": request.connection_code,
            "dtsx_config": {
                "connection_code": request.connection_code,
                "schema_filter": request.schema_filter,
                "proc_name_pattern": request.proc_name_pattern,
                "dtsx_source_path": request.dtsx_source_path,
                "migration_job_id": job_id,
            },
        },
        "created_by": request.created_by,
    }

    background_tasks.add_task(_run_migration_background, job_id, intent_json)
    logger.info("migration_job_created", job_id=job_id)

    return MigrationJobResponse(
        job_id=job_id,
        status="PENDING",
        extraction_source="LIVE_DB",
    )


@app.get("/migration/jobs/{job_id}", response_model=MigrationJobResponse)
async def get_migration_job(job_id: str, include_artifacts: bool = True):
    """Get full status, objects, graph, and artifacts for a migration job."""
    settings = get_settings()
    from src.repository.migration_repository import MigrationRepository
    repo = MigrationRepository(settings.get_database_url_str())

    try:
        job = repo.get_job(job_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    if not job:
        raise HTTPException(status_code=404, detail=f"Migration job {job_id} not found")

    objects = []
    artifacts = []
    dependency_graph: Optional[Dict[str, Any]] = None

    try:
        objects = repo.get_objects_for_job(job_id)
        if include_artifacts:
            artifacts = repo.get_artifacts_for_job(job_id)

        # Re-build dependency graph from persisted lineage (lightweight representation)
        if objects:
            dependency_graph = {
                "node_count": len(objects),
                "nodes": [
                    {
                        "id": f"{o['object_schema']}.{o['object_name']}",
                        "schema": o["object_schema"],
                        "name": o["object_name"],
                        "object_type": o["object_type"],
                        "extraction_status": o["extraction_status"],
                    }
                    for o in objects
                ],
            }
    except Exception as exc:
        logger.warning("migration_job_detail_fetch_failed", job_id=job_id, error=str(exc))

    return MigrationJobResponse(
        job_id=str(job["job_id"]),
        status=job["status"],
        extraction_source=job["extraction_source"],
        total_objects=job.get("total_objects", 0) or 0,
        extracted_objects=job.get("extracted_objects", 0) or 0,
        failed_objects=job.get("failed_objects", 0) or 0,
        skipped_objects=job.get("skipped_objects", 0) or 0,
        started_at=str(job["started_at"]) if job.get("started_at") else None,
        completed_at=str(job["completed_at"]) if job.get("completed_at") else None,
        created_by=job.get("created_by"),
        objects=objects,
        artifacts=artifacts,
        dependency_graph=dependency_graph,
        error_message=job.get("error_message"),
    )


@app.get("/migration/jobs")
async def list_migration_jobs(limit: int = 50):
    """List recent migration jobs with summary counts."""
    settings = get_settings()
    from src.repository.migration_repository import MigrationRepository
    repo = MigrationRepository(settings.get_database_url_str())
    try:
        jobs = repo.list_jobs(limit=limit)
        return {"jobs": jobs, "total": len(jobs)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# =============================================================================
# Startup/Shutdown Events
# =============================================================================


@app.on_event("startup")
async def startup_event():
    """Initialize on startup."""
    logger.info("Data Pipeline Agent API starting up")
    settings = get_settings()
    logger.info(
        "Configuration loaded",
        environment=settings.environment,
        gcp_project=settings.gcp_project_id,
    )


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Data Pipeline Agent API shutting down")


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8001")),
        reload=os.getenv("ENVIRONMENT", "dev") == "dev",
    )
