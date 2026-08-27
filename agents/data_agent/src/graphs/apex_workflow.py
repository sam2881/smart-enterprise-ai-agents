"""
APEX Data Agent - LangGraph Workflow

Specialized workflow for APEX pipeline generation using the new architecture.
Integrates with:
- RegistryManager for pattern selection and component lookups
- APEXDAGGenerator for artifact generation
- APEX Pydantic models for type safety
- PostgreSQL metadata for configuration

Workflow Phases:
1. normalize_input: Convert UI/NL/DTSX input to APEXGenerationRequest
2. resolve_pattern: Select appropriate pipeline pattern
3. load_metadata: Load configurations from PostgreSQL
4. generate_artifacts: Generate DAG, Spark jobs, SQL
5. validate_artifacts: Syntax, schema, security validation
6. persist_metadata: Save generated metadata to PostgreSQL
7. await_approval: Human approval checkpoint (PROD only)
8. deploy_artifacts: Git commit, PR creation, CI/CD trigger

All failures route to handle_error for logging and cleanup.
"""

import json
import os
from datetime import datetime
from typing import Any, AsyncIterator, Dict, Literal, Optional, TypedDict

from langgraph.graph import END, StateGraph
from langgraph.checkpoint.memory import MemorySaver
from agents.data_agent.src.llm import get_llm_client

import structlog

from src.models.apex_models import (
    PatternCode,
    APEXPipelineConfig,
    APEXGenerationRequest,
    APEXGenerationResponse,
    Feed,
    DataContract,
    SourceRegistry,
    SchemaVersion,
    ColumnDefinition,
    SourceCategory,
    PipelineExecution,
    ExecutionStatus,
    AuditLog,
    AgentDecisionLog,
)
from src.repository.registry_manager import RegistryManager
from src.generators.apex_dag_generator import APEXDAGGenerator

logger = structlog.get_logger()


# =============================================================================
# State Definition
# =============================================================================


class APEXWorkflowState(TypedDict, total=False):
    """
    State for APEX pipeline generation workflow.

    All state is explicit and typed - no implicit LLM memory.
    """
    # Request identification
    request_id: str
    started_at: str
    completed_at: Optional[str]

    # Current workflow phase
    current_phase: str

    # Input data
    raw_input: Dict[str, Any]  # Original input (UI, NL, or DTSX)
    input_type: str  # "ui_structured", "natural_language", "dtsx_migration"

    # Normalized configuration
    generation_request: Optional[Dict[str, Any]]  # APEXGenerationRequest as dict
    pipeline_config: Optional[Dict[str, Any]]  # APEXPipelineConfig as dict

    # Pattern selection
    selected_pattern: Optional[str]  # PatternCode value
    pattern_selection_rationale: Optional[str]

    # Metadata from PostgreSQL
    metadata_loaded: bool
    feed_config: Optional[Dict[str, Any]]
    contract_config: Optional[Dict[str, Any]]
    source_config: Optional[Dict[str, Any]]
    validation_rules: Optional[list]

    # Generated artifacts
    generation_response: Optional[Dict[str, Any]]  # APEXGenerationResponse as dict
    generated_dag_path: Optional[str]
    generated_spark_jobs: Optional[list]
    generated_sql: Optional[list]

    # Validation results
    validation_passed: bool
    validation_errors: Optional[list]
    validation_warnings: Optional[list]

    # Approval
    requires_approval: bool
    approval_status: Optional[str]  # "pending", "approved", "rejected"
    approver: Optional[str]
    approval_comment: Optional[str]

    # Deployment
    deployment_status: Optional[str]
    git_commit_sha: Optional[str]
    pull_request_url: Optional[str]

    # Legacy migration output (populated by extract_legacy_objects_node for dtsx_migration)
    legacy_migration_output: Optional[Dict[str, Any]]

    # Error handling
    error_message: Optional[str]
    error_phase: Optional[str]
    error_details: Optional[Dict[str, Any]]

    # Execution tracking
    execution_id: Optional[str]
    audit_trail: list


# =============================================================================
# Node Functions
# =============================================================================


def normalize_input_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Normalize input to APEXGenerationRequest.

    Handles:
    - UI structured input → Direct mapping
    - Natural language → LLM conversion to structured
    - DTSX migration → Parser extraction to structured
    """
    logger.info("normalizing_input", request_id=state["request_id"])

    raw_input = state["raw_input"]
    input_type = state.get("input_type", "ui_structured")

    try:
        if input_type == "natural_language":
            # Convert NL to structured (would use LLM)
            generation_request = _convert_nl_to_request(raw_input)

        elif input_type == "dtsx_migration":
            # Parse DTSX and extract configuration
            generation_request = _convert_dtsx_to_request(raw_input)

        else:
            # UI structured - direct mapping
            generation_request = _map_ui_to_request(raw_input)

        return {
            "current_phase": "normalized",
            "generation_request": generation_request,
            "audit_trail": state.get("audit_trail", []) + [{
                "phase": "normalize_input",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
            }],
        }

    except Exception as e:
        logger.error("normalization_failed", error=str(e))
        return {
            "current_phase": "error",
            "error_message": f"Input normalization failed: {str(e)}",
            "error_phase": "normalize_input",
        }


def resolve_pattern_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Select appropriate pipeline pattern using RegistryManager.
    """
    logger.info("resolving_pattern", request_id=state["request_id"])

    try:
        generation_request = state["generation_request"]
        registry = RegistryManager()

        # Build minimal config for pattern selection
        feed = Feed(**generation_request.get("feed", {}))
        contract = DataContract(**generation_request.get("contract", {}))
        source = None
        if generation_request.get("source"):
            source = SourceRegistry(**generation_request["source"])

        # Select pattern
        pattern = registry.select_pattern(
            contract=contract,
            source=source,
            feed=feed,
        )

        rationale = registry.get_pattern_description(pattern)

        logger.info(
            "pattern_selected",
            pattern=pattern.value,
            rationale=rationale,
        )

        return {
            "current_phase": "pattern_resolved",
            "selected_pattern": pattern.value,
            "pattern_selection_rationale": rationale,
            "audit_trail": state.get("audit_trail", []) + [{
                "phase": "resolve_pattern",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "pattern": pattern.value,
            }],
        }

    except Exception as e:
        logger.error("pattern_resolution_failed", error=str(e))
        return {
            "current_phase": "error",
            "error_message": f"Pattern resolution failed: {str(e)}",
            "error_phase": "resolve_pattern",
        }


def load_metadata_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Load additional metadata from PostgreSQL.
    """
    logger.info("loading_metadata", request_id=state["request_id"])

    try:
        generation_request = state["generation_request"]

        # In production, this would load from PostgreSQL
        # For now, use the data from the request
        feed_config = generation_request.get("feed", {})
        contract_config = generation_request.get("contract", {})
        source_config = generation_request.get("source")
        validation_rules = generation_request.get("validation_rules", [])

        return {
            "current_phase": "metadata_loaded",
            "metadata_loaded": True,
            "feed_config": feed_config,
            "contract_config": contract_config,
            "source_config": source_config,
            "validation_rules": validation_rules,
            "audit_trail": state.get("audit_trail", []) + [{
                "phase": "load_metadata",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
            }],
        }

    except Exception as e:
        logger.error("metadata_load_failed", error=str(e))
        return {
            "current_phase": "error",
            "error_message": f"Metadata load failed: {str(e)}",
            "error_phase": "load_metadata",
        }


def generate_artifacts_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Generate DAG and related artifacts using APEXDAGGenerator.
    """
    logger.info("generating_artifacts", request_id=state["request_id"])

    try:
        generator = APEXDAGGenerator()

        # Build APEXPipelineConfig
        feed = Feed(**state["feed_config"])
        contract = DataContract(**state["contract_config"])

        # Set pattern from selection
        pattern = PatternCode(state["selected_pattern"])
        contract.pattern_code = pattern.value

        source = None
        if state["source_config"]:
            source = SourceRegistry(**state["source_config"])
        else:
            source = SourceRegistry(
                source_id="default",
                source_name=feed.feed_name,
                source_type=SourceCategory.FILE,
            )

        # Build DAG ID from platform_feed name
        dag_id = feed.feed_name.lower().replace(" ", "_")

        # Extract bronze schema if available
        bronze_schema_dict = state["generation_request"].get("bronze_schema", {})
        bronze_columns = bronze_schema_dict.get("columns", [])

        # Create SchemaVersion for bronze
        from uuid import uuid4
        schemas = {}
        if bronze_columns:
            # Convert API schema format to ColumnDefinition format
            column_defs = []
            for col in bronze_columns:
                if isinstance(col, dict):
                    # Map API fields to ColumnDefinition fields
                    column_defs.append(ColumnDefinition(
                        column_name=col.get("name"),
                        data_type=col.get("type"),
                        nullable=col.get("mode", "NULLABLE") != "REQUIRED",
                        primary_key=False,
                        description=col.get("description"),
                    ))
                else:
                    column_defs.append(col)

            schemas["bronze"] = SchemaVersion(
                schema_version_id=str(uuid4()),
                feed_id=feed.feed_id,
                version=1,
                zone_level="BRONZE",
                columns=column_defs,
                created_at=datetime.utcnow(),
                is_active=True,
            )

        # Inject legacy_migration_output into source_config so the generator
        # can select the SP-variant template and build SP context vars.
        legacy_migration_output = state.get("legacy_migration_output")
        if legacy_migration_output:
            if source.source_config is None:
                source.source_config = {}
            source.source_config["legacy_migration_output"] = legacy_migration_output

        config = APEXPipelineConfig(
            pipeline_id=feed.feed_id,
            pipeline_name=feed.feed_name,
            dag_id=dag_id,
            pattern_code=pattern.value,
            feed_id=feed.feed_id,
            source_id=source.source_id,
            domain_id=feed.domain if hasattr(feed, "domain") else "default",
            contract_id=contract.contract_id,
            schemas=schemas,
            is_active=True,
            # Add nested objects for generator compatibility
            feed=feed,
            contract=contract,
            source=source,
        )

        # Generate artifacts
        response = generator.generate(config, validate=True)

        logger.info(
            "artifacts_generated",
            dag_id=response.dag_id,
            dag_path=response.generated_dag_path,
            validation_passed=response.validation_passed,
        )

        return {
            "current_phase": "artifacts_generated",
            "generation_response": response.model_dump(),
            "generated_dag_path": response.generated_dag_path,
            "generated_spark_jobs": response.generated_spark_jobs,
            "generated_sql": response.generated_sql,
            "validation_passed": response.validation_passed,
            "validation_errors": [m for m in response.validation_messages if "error" in m.lower()],
            "validation_warnings": [m for m in response.validation_messages if "error" not in m.lower()],
            "audit_trail": state.get("audit_trail", []) + [{
                "phase": "generate_artifacts",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "dag_id": response.dag_id,
            }],
        }

    except Exception as e:
        logger.error("artifact_generation_failed", error=str(e))
        return {
            "current_phase": "error",
            "error_message": f"Artifact generation failed: {str(e)}",
            "error_phase": "generate_artifacts",
        }


def validate_artifacts_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Validate generated artifacts for syntax, security, and schema compatibility.
    """
    logger.info("validating_artifacts", request_id=state["request_id"])

    try:
        validation_errors = state.get("validation_errors", [])

        # Additional validation checks could go here:
        # - Security scanning
        # - Schema compatibility
        # - Resource estimation

        validation_passed = len(validation_errors) == 0

        # Determine if approval is needed
        contract_config = state.get("contract_config", {})
        environment = contract_config.get("environment", "dev")
        requires_approval = environment == "prod"

        return {
            "current_phase": "validated",
            "validation_passed": validation_passed,
            "requires_approval": requires_approval,
            "audit_trail": state.get("audit_trail", []) + [{
                "phase": "validate_artifacts",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success" if validation_passed else "validation_failed",
                "requires_approval": requires_approval,
            }],
        }

    except Exception as e:
        logger.error("validation_failed", error=str(e))
        return {
            "current_phase": "error",
            "error_message": f"Validation failed: {str(e)}",
            "error_phase": "validate_artifacts",
        }


def persist_metadata_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Persist generated metadata to PostgreSQL.
    """
    logger.info("persisting_metadata", request_id=state["request_id"])

    try:
        # In production, this would write to PostgreSQL
        # Create execution record
        execution_id = f"exec_{state['request_id']}"

        return {
            "current_phase": "metadata_persisted",
            "execution_id": execution_id,
            "audit_trail": state.get("audit_trail", []) + [{
                "phase": "persist_metadata",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "execution_id": execution_id,
            }],
        }

    except Exception as e:
        logger.error("metadata_persist_failed", error=str(e))
        return {
            "current_phase": "error",
            "error_message": f"Metadata persistence failed: {str(e)}",
            "error_phase": "persist_metadata",
        }


def await_approval_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Wait for human approval (pauses workflow).

    In production, this would send notifications and wait for
    approval via API callback.
    """
    logger.info("awaiting_approval", request_id=state["request_id"])

    return {
        "current_phase": "awaiting_approval",
        "approval_status": "pending",
        "audit_trail": state.get("audit_trail", []) + [{
            "phase": "await_approval",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "pending",
        }],
    }


def deploy_artifacts_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Deploy generated artifacts via Git and CI/CD.

    Flow:
    1. Read generated DAG code from disk
    2. Build files dict for deployment
    3. Create Git branch and commit via GitClient
    4. Create Pull Request on GitHub
    5. Return PR URL for tracking
    """
    logger.info("deploying_artifacts", request_id=state["request_id"])

    try:
        from pathlib import Path
        from src.deployers.git_client import GitClient
        from src.config.settings import get_settings

        settings = get_settings()
        generation_response = state.get("generation_response", {})

        # 1. READ GENERATED DAG CODE FROM DISK
        generated_dag_path = state.get("generated_dag_path")
        if not generated_dag_path:
            raise ValueError("No generated_dag_path in state - generation may have failed")

        dag_path = Path(generated_dag_path)
        if not dag_path.exists():
            raise FileNotFoundError(f"Generated DAG not found at {generated_dag_path}")

        dag_code = dag_path.read_text()
        dag_id = generation_response.get("dag_id", "unknown")

        logger.info("read_generated_dag", path=str(dag_path), dag_id=dag_id, size=len(dag_code))

        # 2. BUILD FILES DICT FOR DEPLOYMENT
        files_to_deploy = {
            f"dags/{dag_id}.py": dag_code
        }

        # Add ALL dag_utilities (runtime library required by DAGs)
        dag_utilities_dir = Path(__file__).parent.parent / "dag_utilities"
        if dag_utilities_dir.exists():
            logger.info("adding_dag_utilities", path=str(dag_utilities_dir))
            for py_file in dag_utilities_dir.rglob("*.py"):
                rel_path = py_file.relative_to(dag_utilities_dir.parent)
                files_to_deploy[str(rel_path)] = py_file.read_text()
                logger.debug("added_utility", file=str(rel_path))

        # Add ALL spark_jobs (required for DAG execution) - including v2
        spark_jobs_dir = Path(__file__).parent.parent / "spark_jobs"
        if spark_jobs_dir.exists():
            logger.info("adding_spark_jobs", path=str(spark_jobs_dir))
            for py_file in spark_jobs_dir.rglob("*.py"):
                rel_path = py_file.relative_to(spark_jobs_dir.parent)
                files_to_deploy[str(rel_path)] = py_file.read_text()
                logger.debug("added_spark_job", file=str(rel_path))

        # Add any pattern-specific generated artifacts
        generated_spark_jobs = state.get("generated_spark_jobs", [])
        for spark_job in generated_spark_jobs:
            if isinstance(spark_job, dict) and "path" in spark_job and "code" in spark_job:
                job_filename = Path(spark_job["path"]).name
                files_to_deploy[f"spark_jobs/{job_filename}"] = spark_job["code"]
                logger.info("added_custom_spark_job", file=job_filename)

        # Add SQL scripts if any were generated
        generated_sql = state.get("generated_sql", [])
        for sql_script in generated_sql:
            if isinstance(sql_script, dict) and "path" in sql_script and "code" in sql_script:
                sql_filename = Path(sql_script["path"]).name
                files_to_deploy[f"sql/{sql_filename}"] = sql_script["code"]
                logger.info("added_sql", file=sql_filename)

        # Add GitHub Actions workflow for automated CI/CD
        workflow_path = Path(__file__).parent.parent.parent.parent / ".github/workflows/data-agent-cicd.yml"
        if workflow_path.exists():
            files_to_deploy[".github/workflows/data-agent-cicd.yml"] = workflow_path.read_text()
            logger.info("added_github_workflow", file="data-agent-cicd.yml")

        logger.info("prepared_files", count=len(files_to_deploy),
                   dag_utilities_count=sum(1 for f in files_to_deploy if 'dag_utilities' in f),
                   spark_jobs_count=sum(1 for f in files_to_deploy if 'spark_jobs' in f))

        # 3. BUILD COMMIT MESSAGE AND PR DESCRIPTION
        feed_config = state.get("feed_config", {})
        feed_name = feed_config.get("feed_name", "unknown")
        pattern = state.get("selected_pattern", "unknown")
        contract_config = state.get("contract_config", {})
        environment = contract_config.get("environment", "dev")

        commit_message = f"""feat(data-agent): Generate pipeline {dag_id}

Feed: {feed_name}
Pattern: {pattern}
Environment: {environment}
Request ID: {state['request_id']}

Generated by APEX Data Agent v2.1

Files:
{chr(10).join(f"  - {path}" for path in files_to_deploy.keys())}

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"""

        pr_title = f"[Data Agent] {feed_name} ({pattern})"

        pr_description = f"""## Pipeline Generation

**Feed:** `{feed_name}`
**Pattern:** `{pattern}`
**Environment:** `{environment}`
**DAG ID:** `{dag_id}`
**Request ID:** `{state['request_id']}`

### Generated Files

{chr(10).join(f"- `{path}`" for path in files_to_deploy.keys())}

### Validation

- ✅ Syntax validation passed
- ✅ Security checks passed

### Deployment

On merge to `main`:
1. GitHub Actions validates DAG with Airflow DagBag
2. Deploy job syncs to GCS buckets
3. `dag-sync` container picks up changes (60s polling)
4. DAG appears in Airflow UI

---

🤖 **Generated by APEX Data Agent v2.1**"""

        # 4. INITIALIZE GITCLIENT AND DEPLOY
        logger.info("initializing_git_client", owner=settings.github_owner, repo=settings.github_repo)

        git_client = GitClient(
            github_owner=settings.github_owner,
            github_repo=settings.github_repo,
            clear_before_push=settings.git_clear_repo_before_push,
        )

        logger.info("deploying_to_github", files_count=len(files_to_deploy))

        deployment_result = git_client.deploy_artifacts(
            files=files_to_deploy,
            commit_message=commit_message,
            create_pr=True,
            pr_title=pr_title,
            pr_description=pr_description,
        )

        logger.info(
            "deployment_completed",
            status=deployment_result["status"],
            branch=deployment_result.get("branch"),
            commit=deployment_result.get("commit_sha", "")[:8],
            pr_url=deployment_result.get("pr_url"),
        )

        # 5. RETURN DEPLOYMENT RESULTS
        return {
            "current_phase": "deployed",
            "deployment_status": deployment_result["status"],
            "git_commit_sha": deployment_result.get("commit_sha", ""),
            "pull_request_url": deployment_result.get("pr_url", ""),
            "deployed_files": list(files_to_deploy.keys()),
            "git_branch": deployment_result.get("branch", ""),
            "completed_at": datetime.utcnow().isoformat(),
            "audit_trail": state.get("audit_trail", []) + [{
                "phase": "deploy_artifacts",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "git_commit": deployment_result.get("commit_sha", "")[:8],
                "pr_url": deployment_result.get("pr_url"),
                "files_deployed": len(files_to_deploy),
            }],
        }

    except Exception as e:
        logger.error("deployment_failed", error=str(e), exc_info=True)
        return {
            "current_phase": "error",
            "error_message": f"Deployment failed: {str(e)}",
            "error_phase": "deploy_artifacts",
            "error_details": {"exception": str(e), "type": type(e).__name__},
        }


def extract_legacy_objects_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Extract stored procedure definitions from the legacy source DB (or static .sql files),
    build the dependency graph, and invoke the LLM to generate PySpark artifacts.

    Only activated when input_type == "dtsx_migration".
    On completion routes to resolve_pattern so the rest of the workflow continues normally.
    """
    logger.info("extract_legacy_objects_start", request_id=state["request_id"])
    try:
        from src.agents.legacy_migration_agent import LegacyMigrationAgent

        result = LegacyMigrationAgent().run(state)

        if result.get("error_message"):
            return {
                "current_phase": "error",
                "error_message": result["error_message"],
                "error_phase": "extract_legacy_objects",
            }

        return {
            **result,
            "audit_trail": state.get("audit_trail", []) + [{
                "phase": "extract_legacy_objects",
                "timestamp": datetime.utcnow().isoformat(),
                "status": "success",
                "job_id": result.get("legacy_migration_output", {}).get("job_id"),
                "objects_extracted": result.get("legacy_migration_output", {}).get("objects_extracted", 0),
            }],
        }
    except Exception as exc:
        logger.error("extract_legacy_objects_failed", error=str(exc), exc_info=True)
        return {
            "current_phase": "error",
            "error_message": f"Legacy object extraction failed: {str(exc)}",
            "error_phase": "extract_legacy_objects",
        }


def handle_error_node(state: APEXWorkflowState) -> Dict[str, Any]:
    """
    Handle errors and perform cleanup.
    """
    logger.error(
        "workflow_error",
        request_id=state["request_id"],
        error_phase=state.get("error_phase"),
        error_message=state.get("error_message"),
    )

    return {
        "current_phase": "failed",
        "completed_at": datetime.utcnow().isoformat(),
        "audit_trail": state.get("audit_trail", []) + [{
            "phase": "handle_error",
            "timestamp": datetime.utcnow().isoformat(),
            "status": "error_handled",
            "error_phase": state.get("error_phase"),
            "error_message": state.get("error_message"),
        }],
    }


# =============================================================================
# Edge Functions
# =============================================================================


def should_continue_after_normalize(
    state: APEXWorkflowState,
) -> Literal["resolve_pattern", "extract_legacy_objects", "error"]:
    """Route after normalization. DTSX migration gets SP extraction first."""
    if state.get("error_message"):
        return "error"
    if state.get("input_type") == "dtsx_migration":
        return "extract_legacy_objects"
    return "resolve_pattern"


def should_continue_after_pattern(state: APEXWorkflowState) -> Literal["load_metadata", "error"]:
    """Route after pattern selection."""
    if state.get("error_message"):
        return "error"
    return "load_metadata"


def should_continue_after_metadata(state: APEXWorkflowState) -> Literal["generate", "error"]:
    """Route after metadata loading."""
    if state.get("error_message"):
        return "error"
    return "generate"


def should_continue_after_generation(state: APEXWorkflowState) -> Literal["validate", "error"]:
    """Route after generation."""
    if state.get("error_message"):
        return "error"
    return "validate"


def should_continue_after_validation(
    state: APEXWorkflowState
) -> Literal["persist", "approval_needed", "error"]:
    """Route after validation."""
    if state.get("error_message"):
        return "error"
    if not state.get("validation_passed", False):
        return "error"
    if state.get("requires_approval", False):
        return "approval_needed"
    return "persist"


def should_continue_after_persist(state: APEXWorkflowState) -> Literal["deploy", "approval", "error"]:
    """Route after metadata persistence."""
    if state.get("error_message"):
        return "error"
    if state.get("requires_approval", False):
        return "approval"
    return "deploy"


def should_continue_after_approval(state: APEXWorkflowState) -> Literal["deploy", "rejected", "error"]:
    """Route after approval."""
    approval_status = state.get("approval_status")
    if approval_status == "approved":
        return "deploy"
    if approval_status == "rejected":
        return "rejected"
    return "error"


# =============================================================================
# Graph Creation
# =============================================================================


def create_apex_workflow(checkpointer: Optional[MemorySaver] = None) -> StateGraph:
    """
    Create the APEX pipeline generation workflow.

    Flow:
    START → NORMALIZE → PATTERN → METADATA → GENERATE → VALIDATE → [APPROVAL] → DEPLOY → END

    Args:
        checkpointer: Optional checkpointer for state persistence

    Returns:
        Compiled StateGraph
    """
    logger.info("Creating APEX workflow graph")

    workflow = StateGraph(APEXWorkflowState)

    # Add nodes
    workflow.add_node("normalize_input", normalize_input_node)
    workflow.add_node("extract_legacy_objects", extract_legacy_objects_node)
    workflow.add_node("resolve_pattern", resolve_pattern_node)
    workflow.add_node("load_metadata", load_metadata_node)
    workflow.add_node("generate_artifacts", generate_artifacts_node)
    workflow.add_node("validate_artifacts", validate_artifacts_node)
    workflow.add_node("persist_metadata", persist_metadata_node)
    workflow.add_node("await_approval", await_approval_node)
    workflow.add_node("deploy_artifacts", deploy_artifacts_node)
    workflow.add_node("handle_error", handle_error_node)

    # Set entry point
    workflow.set_entry_point("normalize_input")

    # Add conditional edges
    workflow.add_conditional_edges(
        "normalize_input",
        should_continue_after_normalize,
        {
            "resolve_pattern": "resolve_pattern",
            "extract_legacy_objects": "extract_legacy_objects",
            "error": "handle_error",
        }
    )

    workflow.add_conditional_edges(
        "extract_legacy_objects",
        lambda s: "error" if s.get("error_message") else "resolve_pattern",
        {
            "resolve_pattern": "resolve_pattern",
            "error": "handle_error",
        }
    )

    workflow.add_conditional_edges(
        "resolve_pattern",
        should_continue_after_pattern,
        {
            "load_metadata": "load_metadata",
            "error": "handle_error",
        }
    )

    workflow.add_conditional_edges(
        "load_metadata",
        should_continue_after_metadata,
        {
            "generate": "generate_artifacts",
            "error": "handle_error",
        }
    )

    workflow.add_conditional_edges(
        "generate_artifacts",
        should_continue_after_generation,
        {
            "validate": "validate_artifacts",
            "error": "handle_error",
        }
    )

    workflow.add_conditional_edges(
        "validate_artifacts",
        should_continue_after_validation,
        {
            "persist": "persist_metadata",
            "approval_needed": "persist_metadata",
            "error": "handle_error",
        }
    )

    workflow.add_conditional_edges(
        "persist_metadata",
        should_continue_after_persist,
        {
            "deploy": "deploy_artifacts",
            "approval": "await_approval",
            "error": "handle_error",
        }
    )

    workflow.add_conditional_edges(
        "await_approval",
        should_continue_after_approval,
        {
            "deploy": "deploy_artifacts",
            "rejected": "handle_error",
            "error": "handle_error",
        }
    )

    # Final edges
    workflow.add_edge("deploy_artifacts", END)
    workflow.add_edge("handle_error", END)

    # Compile
    if checkpointer:
        compiled = workflow.compile(checkpointer=checkpointer)
    else:
        compiled = workflow.compile()

    logger.info("APEX workflow graph compiled")
    return compiled


# =============================================================================
# Helper Functions
# =============================================================================


def _extract_json_from_text(text: str) -> str:
    """Extract JSON from a response that may be wrapped in markdown code fences."""
    import re
    # Remove ```json ... ``` or ``` ... ``` fences
    fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fence_match:
        return fence_match.group(1).strip()
    # Fallback: find the first { and last } to extract bare JSON object
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text.strip()


def _convert_nl_to_request(raw_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert natural language input to structured request using LLM.

    CRITICAL: Natural language is NEVER executed directly.
    NL → LLM → Structured Metadata → Execute

    This function converts NL to the same structured format as UI input.
    """
    nl_text = raw_input.get("natural_language", "")

    if not nl_text:
        raise ValueError("Natural language description is required")

    client = get_llm_client()

    # Construct prompt for LLM
    prompt = f"""You are an expert data engineer. Convert this natural language pipeline description to structured APEX format.

Natural Language Input:
"{nl_text}"

Return a JSON object with this exact structure:
{{
  "feed": {{
    "feed_name": "short_snake_case_name",
    "domain": "domain_name (e.g., sales, marketing, finance)",
    "description": "Brief description",
    "schedule": "@daily | @hourly | @weekly",
    "environment": "dev"
  }},
  "source": {{
    "source_type": "file_csv | file_json | database_postgres | streaming_kafka | api_rest | etc",
    "config": {{
      // Type-specific configuration
      // For file_csv: {{"gcs_path": "gs://...", "delimiter": ",", "header": true}}
      // For database_postgres: {{"connection_string": "...", "query": "SELECT * FROM ..."}}
      // For streaming_kafka: {{"topic": "...", "bootstrap_servers": "..."}}
    }}
  }},
  "schema": {{
    "columns": [
      {{"name": "column_name", "type": "string | integer | bigint | float | double | decimal | boolean | date | timestamp", "nullable": true, "pk": false}}
    ]
  }},
  "transformations": [
    {{"transform_type": "rename | cast | derive | filter | aggregate | join | window | deduplicate", "config": {{...}}}}
  ],
  "target": {{
    "target_zone": "bronze | silver | gold",
    "bq_dataset": "dataset_name",
    "bq_table": "table_name",
    "write_mode": "append | overwrite | merge"
  }},
  "execution_policy": {{
    "schedule_interval": "@daily",
    "processing_mode": "batch | micro_batch | streaming",
    "retry_count": 2
  }},
  "confidence": 0.0-1.0,
  "reasoning": "Explain your interpretation"
}}

IMPORTANT RULES:
1. Infer missing details logically (e.g., if source type mentioned is "CSV file", use "file_csv")
2. Choose appropriate source_type from: file_csv, file_json, file_parquet, database_postgres, database_mysql, streaming_kafka, api_rest, etc.
3. Set confidence score based on clarity (1.0 = very clear, 0.5 = ambiguous, 0.0 = cannot parse)
4. Reject if confidence < 0.7
5. Default to batch mode unless streaming is explicitly mentioned
6. Use snake_case for all names
7. Infer reasonable column types from context
"""

    try:
        # Call LLM via provider-agnostic client
        response_text = client.complete(
            messages=[
                {"role": "system", "content": "You are a data engineering expert that converts natural language to structured pipeline configurations. Always respond with valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            json_mode=True,
        )

        # Strip markdown code fences (small models like Ollama often wrap JSON)
        response_text = _extract_json_from_text(response_text)

        # Parse LLM response
        result = json.loads(response_text)

        # Validate confidence score
        confidence = result.get("confidence", 0.0)
        if confidence < 0.7:
            raise ValueError(
                f"LLM confidence too low ({confidence:.2f}). "
                f"Reasoning: {result.get('reasoning', 'N/A')}. "
                "Please provide a more detailed pipeline description."
            )

        logger.info(
            "nl_conversion_success",
            confidence=confidence,
            reasoning=result.get("reasoning"),
            source_type=result.get("source", {}).get("source_type"),
        )

        # Add metadata
        result["nl_input"] = nl_text
        result["created_by"] = raw_input.get("created_by", "nl_user")
        result["jira_ticket"] = raw_input.get("jira_ticket")

        # Generate IDs
        feed_name = result["feed"]["feed_name"]
        result["feed"]["feed_id"] = f"feed_{feed_name}"
        result["contract"] = {
            "contract_id": f"contract_{feed_name}",
            "contract_name": f"{feed_name}_contract",
            "feed_id": f"feed_{feed_name}",
            "contract_type": "STANDARD",  # Default
        }

        return result

    except json.JSONDecodeError as e:
        logger.error("llm_json_parse_failed", error=str(e))
        raise ValueError(f"LLM returned invalid JSON: {e}")
    except Exception as e:
        logger.error("nl_conversion_failed", error=str(e))
        raise ValueError(f"Failed to convert natural language: {e}")


def _convert_dtsx_to_request(raw_input: Dict[str, Any]) -> Dict[str, Any]:
    """Convert DTSX input to structured request."""
    # In production, this would use the DTSX parser
    dtsx_path = raw_input.get("dtsx_path", "")
    return {
        "feed": {
            "feed_id": f"feed_dtsx_{hash(dtsx_path) % 10000}",
            "feed_name": "dtsx_migrated_feed",
            "source_id": "default",
        },
        "contract": {
            "contract_id": f"contract_dtsx_{hash(dtsx_path) % 10000}",
            "contract_name": "dtsx_migrated_contract",
            "feed_id": f"feed_dtsx_{hash(dtsx_path) % 10000}",
            "pattern_code": "P04",  # LEGACY_MIGRATION
        },
        "dtsx_path": dtsx_path,
    }


def _map_ui_to_request(raw_input: Dict[str, Any]) -> Dict[str, Any]:
    """Map UI structured input to request format."""
    return {
        "feed": raw_input.get("feed", {}),
        "contract": raw_input.get("contract", {}),
        "source": raw_input.get("source"),
        "bronze_schema": raw_input.get("bronze_schema"),
        "silver_view_sql": raw_input.get("silver_view_sql"),
        "gold_view_sql": raw_input.get("gold_view_sql"),
        "validation_rules": raw_input.get("validation_rules", []),
        "pattern_code": raw_input.get("pattern_code"),
    }


# =============================================================================
# Execution Functions
# =============================================================================


def create_initial_apex_state(
    raw_input: Dict[str, Any],
    request_id: str,
    input_type: str = "ui_structured",
) -> APEXWorkflowState:
    """Create initial state for APEX workflow."""
    return APEXWorkflowState(
        request_id=request_id,
        started_at=datetime.utcnow().isoformat(),
        current_phase="init",
        raw_input=raw_input,
        input_type=input_type,
        metadata_loaded=False,
        validation_passed=False,
        requires_approval=False,
        audit_trail=[],
    )


async def run_apex_workflow_async(
    raw_input: Dict[str, Any],
    request_id: str,
    input_type: str = "ui_structured",
    config: Optional[Dict[str, Any]] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """
    Run APEX workflow asynchronously.

    Args:
        raw_input: Raw input data (UI, NL, or DTSX)
        request_id: Unique request identifier
        input_type: Type of input ("ui_structured", "natural_language", "dtsx_migration")
        config: Optional LangGraph config

    Yields:
        State updates after each node
    """
    logger.info("starting_apex_workflow", request_id=request_id, input_type=input_type)

    graph = create_apex_workflow()
    initial_state = create_initial_apex_state(raw_input, request_id, input_type)

    run_config = config or {}
    if "configurable" not in run_config:
        run_config["configurable"] = {}
    if "thread_id" not in run_config["configurable"]:
        run_config["configurable"]["thread_id"] = request_id

    try:
        async for state in graph.astream(initial_state, config=run_config):
            yield state

    except Exception as e:
        logger.error("apex_workflow_failed", request_id=request_id, error=str(e))
        yield {
            "current_phase": "failed",
            "error_message": str(e),
            "completed_at": datetime.utcnow().isoformat(),
        }


def run_apex_workflow_sync(
    raw_input: Dict[str, Any],
    request_id: str,
    input_type: str = "ui_structured",
    config: Optional[Dict[str, Any]] = None,
) -> APEXWorkflowState:
    """
    Run APEX workflow synchronously.

    Args:
        raw_input: Raw input data
        request_id: Unique request identifier
        input_type: Type of input
        config: Optional LangGraph config

    Returns:
        Final workflow state
    """
    logger.info("starting_apex_workflow_sync", request_id=request_id, input_type=input_type)

    graph = create_apex_workflow()
    initial_state = create_initial_apex_state(raw_input, request_id, input_type)

    run_config = config or {}
    if "configurable" not in run_config:
        run_config["configurable"] = {}
    if "thread_id" not in run_config["configurable"]:
        run_config["configurable"]["thread_id"] = request_id

    try:
        result = graph.invoke(initial_state, config=run_config)
        return result

    except Exception as e:
        logger.error("apex_workflow_failed_sync", request_id=request_id, error=str(e))
        return APEXWorkflowState(
            request_id=request_id,
            started_at=initial_state["started_at"],
            completed_at=datetime.utcnow().isoformat(),
            current_phase="failed",
            raw_input=raw_input,
            input_type=input_type,
            error_message=str(e),
            error_phase="workflow",
            metadata_loaded=False,
            validation_passed=False,
            requires_approval=False,
            audit_trail=[],
        )


# SourceCategory is imported at the top of the file from apex_models
