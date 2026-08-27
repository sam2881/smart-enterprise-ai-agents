"""
LegacyMigrationAgent — Orchestrates the full SSIS → PySpark migration pipeline.

Steps executed in order:
  1. Create migration_job row in DB
  2. Extract SP definitions via StoredProcReader (live DB → static fallback)
  3. Build DependencyGraph → persist lineage
  4. Call LLM (per proc) via stored_proc_migration.md prompt → save artifacts
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import structlog

from src.agents.base_agent import BaseAgent, requires_state_keys
from src.config.settings import get_settings

try:
    from agents.servicenow_agent.src.security import secure_llm_call, data_agent_plugin_chain
    _DATA_CHAIN = data_agent_plugin_chain()
    _SECURITY_ENABLED = True
except ImportError:
    _SECURITY_ENABLED = False
    def secure_llm_call(chain=None):  # type: ignore[misc]
        def _noop(func):
            return func
        return _noop
from src.dag_utilities.sources.database_source import get_jdbc_connection
from src.parsers.dependency_graph import DependencyGraph
from src.parsers.stored_proc_reader import StoredProcDefinition, StoredProcReader
from src.repository.migration_repository import MigrationRepository

logger = structlog.get_logger(__name__)


class LegacyMigrationAgent(BaseAgent):
    """
    Orchestrates stored procedure extraction, dependency graph construction,
    and LLM-driven PySpark code generation for a DTSX migration job.
    """

    def __init__(self) -> None:
        super().__init__("legacy_migration")

    @requires_state_keys("intent_json", "request_id")
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        settings = get_settings()
        intent = state["intent_json"]
        request_id = state["request_id"]
        source_config = intent.get("source", {})
        dtsx_config = source_config.get("dtsx_config", {})
        target_config = intent.get("target", {})
        created_by = intent.get("created_by", "system")

        log = logger.bind(request_id=request_id)
        log.info("legacy_migration_start")

        # ── GCS bucket mode (DTSX + SQL files in one place) ──────────────────
        # When legacy_gcs_bucket is provided, all files are read from GCS.
        # Stored procedure .sql files in the bucket replace the live-DB / static
        # fallback path entirely.
        legacy_gcs_bucket = dtsx_config.get("legacy_gcs_bucket")
        dtsx_summaries: List[Dict[str, Any]] = []
        gcs_sp_definitions: List[StoredProcDefinition] = []

        if legacy_gcs_bucket:
            log.info("gcs_bucket_mode", bucket=legacy_gcs_bucket)
            dtsx_summaries, gcs_sp_definitions = self._read_gcs_bucket(legacy_gcs_bucket, log)

        # Resolve connection record (same pattern as database_source.get_jdbc_connection)
        connection_code = dtsx_config.get("connection_code") or source_config.get("connection_code")
        connection_record = self._resolve_connection(connection_code, settings)

        # Repository uses the platform APEX DB (not the source DB)
        repo = MigrationRepository(settings.get_database_url_str())

        # ----------------------------------------------------------------
        # Step 1: Create migration_job
        # ----------------------------------------------------------------
        dtsx_summary = dtsx_config.get("dtsx_summary", {})
        schema_filter = dtsx_config.get("schema_filter", "%")
        proc_name_pattern = dtsx_config.get("proc_name_pattern", "%")
        dtsx_source_path = dtsx_config.get("dtsx_source_path") or legacy_gcs_bucket
        if legacy_gcs_bucket and gcs_sp_definitions:
            extraction_source = "GCS_BUCKET"
        else:
            extraction_source = "LIVE_DB" if connection_record else "STATIC_FILES"

        job_id = repo.create_migration_job(
            connection_id=connection_record.get("connection_id") if connection_record else None,
            dtsx_source_path=dtsx_source_path,
            schema_filter=schema_filter,
            proc_name_pattern=proc_name_pattern,
            extraction_source=extraction_source,
            dtsx_summary=dtsx_summary,
            created_by=created_by,
        )
        log = log.bind(job_id=job_id)
        repo.update_job_status(job_id, "EXTRACTING")

        # ----------------------------------------------------------------
        # Step 2: Extract stored procedure definitions
        # ----------------------------------------------------------------
        definitions: List[StoredProcDefinition] = []
        actual_source = "STATIC_FILES"

        # GCS bucket takes priority — .sql files from the bucket are the source
        if gcs_sp_definitions:
            definitions = gcs_sp_definitions
            actual_source = "GCS_BUCKET"
            log.info("using_gcs_sp_definitions", count=len(definitions))

        elif connection_record:
            try:
                reader = StoredProcReader(
                    connection_record=connection_record,
                    timeout_seconds=settings.sp_extraction_timeout_seconds,
                    max_definition_length=settings.sp_max_definition_length,
                )
                definitions = reader.fetch_definitions(
                    schema_filter=schema_filter,
                    name_filter=proc_name_pattern,
                )
                actual_source = "LIVE_DB"
                log.info("live_extraction_complete", count=len(definitions))
            except Exception as exc:
                log.warning("live_extraction_failed_falling_back", error=str(exc))
                definitions = []

        if not definitions:
            # Static fallback
            static_root = settings.static_sql_scan_root
            if static_root and Path(static_root).exists():
                # Use a dummy reader (platform comes from connection_record or default SQLSERVER)
                platform = (connection_record or {}).get("db_platform", "SQLSERVER")
                reader = StoredProcReader(
                    connection_record={"db_platform": platform},
                    timeout_seconds=settings.sp_extraction_timeout_seconds,
                    max_definition_length=settings.sp_max_definition_length,
                )
                definitions = reader.static_fallback(
                    sql_root_dir=Path(static_root),
                    name_filter=proc_name_pattern,
                )
                actual_source = "STATIC_FILES"
                log.info("static_extraction_complete", count=len(definitions))
            else:
                log.warning("no_extraction_source_available", job_id=job_id)

        # Persist objects
        skipped: List[str] = []
        object_ids: Dict[str, str] = {}
        for proc in definitions:
            status = "ENCRYPTED" if proc.is_encrypted else (
                "EXTRACTED" if proc.definition_text else "NOT_FOUND"
            )
            if proc.is_encrypted:
                skipped.append(proc.fqn)
            try:
                oid = repo.upsert_migration_object(
                    job_id=job_id,
                    schema=proc.schema,
                    name=proc.name,
                    object_type=proc.object_type,
                    db_platform=proc.db_platform,
                    definition_text=proc.definition_text,
                    parameter_list=[p.to_dict() for p in proc.parameters],
                    referenced_objects=[r.to_dict() for r in proc.referenced_objects],
                    extraction_source=actual_source,
                    char_count=proc.char_count,
                    is_encrypted=proc.is_encrypted,
                    extraction_status=status,
                    error_message=proc.error_message,
                )
                object_ids[proc.fqn] = oid
            except Exception as exc:
                log.warning("object_persist_failed", fqn=proc.fqn, error=str(exc))

        repo.update_job_status(
            job_id, "GRAPH_BUILDING",
            total_objects=len(definitions),
            extracted_objects=len(definitions) - len(skipped),
            skipped_objects=len(skipped),
        )

        # ----------------------------------------------------------------
        # Step 3: Build dependency graph
        # ----------------------------------------------------------------
        graph = DependencyGraph()
        graph.load_from_definitions(definitions)
        graph.assign_topo_levels()
        execution_order = graph.get_execution_order()
        execution_batches = graph.get_execution_batches()

        lineage_rows = graph.to_lineage_rows(job_id)
        # object_id_map needs real UUIDs from DB
        db_object_id_map = repo.get_object_id_map(job_id)
        lineage_inserted = repo.persist_lineage(lineage_rows, db_object_id_map)
        log.info("lineage_persisted", edges=lineage_inserted)

        repo.update_job_status(job_id, "LLM_GENERATING")

        # ----------------------------------------------------------------
        # Step 4: LLM generation (one call per extractable proc)
        # ----------------------------------------------------------------
        artifacts: List[Dict[str, Any]] = []
        failed_objects = 0

        for proc in definitions:
            if proc.is_encrypted or not proc.definition_text:
                continue
            try:
                artifact = self._generate_pyspark(
                    proc=proc,
                    graph=graph,
                    job_id=job_id,
                    target_config=target_config,
                    repo=repo,
                    object_ids=db_object_id_map,
                    settings=settings,
                    log=log,
                    dtsx_summaries=dtsx_summaries,
                )
                artifacts.append(artifact)
            except Exception as exc:
                log.error("llm_generation_failed", fqn=proc.fqn, error=str(exc))
                failed_objects += 1

        repo.update_job_status(
            job_id, "COMPLETE",
            extracted_objects=len(artifacts),
            failed_objects=failed_objects,
            completed=True,
        )

        log.info(
            "legacy_migration_complete",
            objects_extracted=len(definitions),
            artifacts_generated=len(artifacts),
            skipped=len(skipped),
            failed=failed_objects,
        )

        return {
            "legacy_migration_output": {
                "job_id": job_id,
                "objects_extracted": len(definitions),
                "dependency_graph": graph.to_dict(),
                "sp_execution_order": execution_order,
                "sp_execution_batches": {str(k): v for k, v in execution_batches.items()},
                "llm_artifacts": artifacts,
                "extraction_source": actual_source,
                "skipped_objects": skipped,
            },
            "current_phase": "legacy_extracted",
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_connection(
        self, connection_code: Optional[str], settings: Any
    ) -> Optional[Dict[str, Any]]:
        """
        Resolve connection details from connection_registry + Secret Manager.
        Mirrors the pattern in database_source.get_jdbc_connection().
        Returns None if no connection_code is provided or lookup fails.
        """
        if not connection_code:
            return None
        try:
            class _FakeMetadataClient:
                """Minimal stub that reads connection_registry via psycopg2."""
                def __init__(self, db_url: str) -> None:
                    self._db_url = db_url

                def get_connection(self, code: str) -> Optional[Dict[str, Any]]:
                    import psycopg2
                    conn = psycopg2.connect(self._db_url)
                    cur = conn.cursor()
                    try:
                        cur.execute(
                            """SELECT connection_id, connection_code, db_type, host, port,
                                      database_name, username, credentials_secret_path,
                                      extra_params
                               FROM connection_registry
                               WHERE connection_code = %s AND is_active = true
                               LIMIT 1""",
                            (code,),
                        )
                        row = cur.fetchone()
                        if not row:
                            return None
                        return {
                            "connection_id": str(row[0]),
                            "connection_code": row[1],
                            "db_platform": (row[2] or "").upper(),
                            "host": row[3] or "",
                            "port": row[4],
                            "database": row[5] or "",
                            "user": row[6] or "",
                            "secret_name": row[7],
                            "extra_params": row[8] or {},
                        }
                    finally:
                        conn.close()

                def get_secret(self, secret_name: str) -> Dict[str, str]:
                    try:
                        from google.cloud import secretmanager
                        client = secretmanager.SecretManagerServiceClient()
                        response = client.access_secret_version(
                            request={"name": f"{secret_name}/versions/latest"}
                        )
                        return json.loads(response.payload.data.decode("UTF-8"))
                    except Exception:
                        return {}

            client = _FakeMetadataClient(settings.get_database_url_str())
            return get_jdbc_connection(connection_code, client)
        except Exception as exc:
            logger.warning("connection_resolve_failed", connection_code=connection_code, error=str(exc))
            return None

    def _generate_pyspark(
        self,
        proc: StoredProcDefinition,
        graph: DependencyGraph,
        job_id: str,
        target_config: Dict[str, Any],
        repo: MigrationRepository,
        object_ids: Dict[str, str],
        settings: Any,
        log: Any,
        dtsx_summaries: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Call the LLM with stored_proc_migration.md prompt and save the artifact."""
        from jinja2 import Environment, FileSystemLoader

        topo_levels = graph._levels
        adj = graph._adj

        proc_topo_level = topo_levels.get(proc.fqn, 0)
        upstream = list(adj.get(proc.fqn, set()))
        downstream = [fqn for fqn, deps in adj.items() if proc.fqn in deps]

        # dtsx_context: pass all SSIS package summaries from the bucket so the LLM
        # understands the full SSIS package structure (connections, transforms, etc.)
        dtsx_context: Any = None
        if dtsx_summaries:
            dtsx_context = dtsx_summaries[0] if len(dtsx_summaries) == 1 else dtsx_summaries

        payload = {
            "procedure": proc.to_dict(),
            "dependencies": [r.to_dict() for r in proc.referenced_objects],
            "dependency_graph_context": {
                "topo_level": proc_topo_level,
                "execution_batch": proc_topo_level,
                "upstream_procs": upstream,
                "downstream_procs": downstream,
            },
            "dtsx_context": dtsx_context,
            "target": target_config,
            "migration_job_id": job_id,
        }

        # Render prompt
        prompts_dir = settings.prompts_dir
        env = Environment(loader=FileSystemLoader(str(prompts_dir)))
        template = env.get_template("stored_proc_migration.md")
        prompt_text = template.render(payload=payload, **payload.get("target", {}))

        # Call Anthropic API
        t0 = time.monotonic()
        llm_response = self._call_llm(prompt_text, settings)
        generation_ms = int((time.monotonic() - t0) * 1000)

        # Parse JSON response
        try:
            parsed = json.loads(llm_response)
        except json.JSONDecodeError:
            # Extract JSON from markdown block if present
            import re
            m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", llm_response, re.DOTALL)
            parsed = json.loads(m.group(1)) if m else {"pyspark_job": llm_response, "confidence_score": 0.0}

        pyspark_code = parsed.get("pyspark_job", "")
        confidence = float(parsed.get("confidence_score", 0.0))

        object_id = object_ids.get(proc.fqn, "")
        if not object_id:
            log.warning("object_id_not_found_for_artifact", fqn=proc.fqn)
            return parsed

        artifact_id = repo.save_artifact(
            object_id=object_id,
            job_id=job_id,
            artifact_type="PYSPARK_JOB",
            artifact_content=pyspark_code,
            llm_model=settings.llm_model or "claude-sonnet-4-6",
            confidence_score=confidence,
            validation_status="PENDING",
            generation_ms=generation_ms,
        )

        # Also save Airflow operator config as a second artifact
        airflow_cfg = parsed.get("airflow_operator")
        if airflow_cfg:
            repo.save_artifact(
                object_id=object_id,
                job_id=job_id,
                artifact_type="AIRFLOW_OPERATOR",
                artifact_content=json.dumps(airflow_cfg, indent=2),
                llm_model=settings.llm_model or "claude-sonnet-4-6",
                confidence_score=confidence,
                validation_status="PENDING",
                generation_ms=generation_ms,
            )

        log.info("artifact_generated", fqn=proc.fqn, confidence=confidence, ms=generation_ms)
        return {
            "fqn": proc.fqn,
            "artifact_id": artifact_id,
            "confidence_score": confidence,
            "migration_notes": parsed.get("migration_notes", ""),
            "unsupported_features": parsed.get("unsupported_features", []),
        }

    def _read_gcs_bucket(
        self,
        bucket_path: str,
        log: Any,
    ) -> Tuple[List[Dict[str, Any]], List[StoredProcDefinition]]:
        """
        Read a GCS prefix that holds .dtsx and .sql files for legacy modernisation.

        .dtsx files → parsed with DTSXParser → migration summary dicts
        .sql  files → read as StoredProcDefinition objects (one per file)

        Schema is inferred from the filename: "dbo.usp_LoadSales.sql" gives
        schema="dbo", name="usp_LoadSales". Plain names ("usp_LoadSales.sql")
        default to schema="dbo".

        Returns:
            (dtsx_summaries, sp_definitions) — either list may be empty.
        """
        try:
            from google.cloud import storage
        except ImportError:
            log.warning("google-cloud-storage not installed; cannot read GCS bucket")
            return [], []

        from src.parsers.dtsx_parser import DTSXParser

        clean = bucket_path.removeprefix("gs://")
        bucket_name, _, prefix = clean.partition("/")
        prefix = (prefix.rstrip("/") + "/") if prefix else ""

        gcs_client = storage.Client()
        blobs = list(gcs_client.list_blobs(bucket_name, prefix=prefix or None))

        dtsx_summaries: List[Dict[str, Any]] = []
        sp_definitions: List[StoredProcDefinition] = []
        parser = DTSXParser()

        for blob in blobs:
            blob_lower = blob.name.lower()
            gcs_path = f"gs://{bucket_name}/{blob.name}"

            if blob_lower.endswith(".dtsx"):
                try:
                    package = parser.parse_from_gcs(gcs_path)
                    dtsx_summaries.append(parser.get_migration_summary(package))
                    log.info("dtsx_parsed", path=gcs_path, package=package.name)
                except Exception as exc:
                    log.warning("dtsx_parse_failed", path=gcs_path, error=str(exc))

            elif blob_lower.endswith(".sql"):
                try:
                    text = blob.download_as_text(encoding="utf-8")
                    stem = Path(blob.name).stem
                    if "." in stem:
                        schema_part, _, name_part = stem.partition(".")
                    else:
                        schema_part, name_part = "dbo", stem
                    sp_definitions.append(
                        StoredProcDefinition(
                            schema=schema_part,
                            name=name_part,
                            db_platform="SQLSERVER",
                            object_type="PROCEDURE",
                            definition_text=text,
                            char_count=len(text),
                            extraction_source="GCS_BUCKET",
                        )
                    )
                    log.info("sp_read", path=gcs_path, fqn=f"{schema_part}.{name_part}")
                except Exception as exc:
                    log.warning("sp_read_failed", path=gcs_path, error=str(exc))

        log.info(
            "gcs_bucket_read_complete",
            bucket=bucket_path,
            dtsx_files=len(dtsx_summaries),
            sql_files=len(sp_definitions),
        )
        return dtsx_summaries, sp_definitions

    @secure_llm_call(_DATA_CHAIN if _SECURITY_ENABLED else None)
    def _call_llm(self, prompt: str, settings: Any) -> str:
        """Call the configured LLM provider and return the raw text response."""
        from agents.data_agent.src.llm import LLMClientFactory

        client = LLMClientFactory.create(settings)
        return client.complete(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=8192,
        )
