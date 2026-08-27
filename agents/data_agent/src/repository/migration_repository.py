"""
MigrationRepository — PostgreSQL writes for migration_* tables.

All raw SQL lives here; no SQL in agents or parsers.
Follows the same psycopg2 pattern as FeedRepository.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


class MigrationRepository:
    """CRUD for migration_job, migration_object, migration_lineage, migration_artifact."""

    def __init__(self, connection_string: str) -> None:
        self._connection_string = connection_string

    def _get_conn(self):
        import psycopg2
        return psycopg2.connect(self._connection_string)

    # ------------------------------------------------------------------
    # migration_job
    # ------------------------------------------------------------------

    def create_migration_job(
        self,
        connection_id: Optional[str],
        dtsx_source_path: Optional[str],
        schema_filter: str = "%",
        proc_name_pattern: str = "%",
        target_feed_group_id: Optional[str] = None,
        extraction_source: str = "LIVE_DB",
        dtsx_summary: Optional[Dict[str, Any]] = None,
        created_by: Optional[str] = None,
    ) -> str:
        """Insert a new migration_job row and return its job_id (UUID string)."""
        job_id = str(uuid.uuid4())
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO migration_job (
                    job_id, connection_id, dtsx_source_path, schema_filter,
                    proc_name_pattern, target_feed_group_id, status,
                    extraction_source, dtsx_summary, created_by
                ) VALUES (%s, %s, %s, %s, %s, %s, 'PENDING', %s, %s, %s)
                """,
                (
                    job_id,
                    connection_id,
                    dtsx_source_path,
                    schema_filter,
                    proc_name_pattern,
                    target_feed_group_id,
                    extraction_source,
                    json.dumps(dtsx_summary or {}),
                    created_by,
                ),
            )
            conn.commit()
            logger.info("migration_job_created", job_id=job_id)
            return job_id
        finally:
            conn.close()

    def update_job_status(
        self,
        job_id: str,
        status: str,
        total_objects: Optional[int] = None,
        extracted_objects: Optional[int] = None,
        failed_objects: Optional[int] = None,
        skipped_objects: Optional[int] = None,
        error_message: Optional[str] = None,
        completed: bool = False,
    ) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            sets = ["status = %s", "updated_at = NOW()"]
            values: List[Any] = [status]
            if total_objects is not None:
                sets.append("total_objects = %s"); values.append(total_objects)
            if extracted_objects is not None:
                sets.append("extracted_objects = %s"); values.append(extracted_objects)
            if failed_objects is not None:
                sets.append("failed_objects = %s"); values.append(failed_objects)
            if skipped_objects is not None:
                sets.append("skipped_objects = %s"); values.append(skipped_objects)
            if error_message is not None:
                sets.append("error_message = %s"); values.append(error_message)
            if completed:
                sets.append("completed_at = NOW()")
            values.append(job_id)
            cur.execute(
                f"UPDATE migration_job SET {', '.join(sets)} WHERE job_id = %s",
                values,
            )
            conn.commit()
        finally:
            conn.close()

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT job_id, connection_id, dtsx_source_path, schema_filter,
                       proc_name_pattern, status, total_objects, extracted_objects,
                       failed_objects, skipped_objects, extraction_source,
                       dtsx_summary, error_message, started_at, completed_at,
                       created_by, created_at
                FROM migration_job WHERE job_id = %s
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [
                "job_id", "connection_id", "dtsx_source_path", "schema_filter",
                "proc_name_pattern", "status", "total_objects", "extracted_objects",
                "failed_objects", "skipped_objects", "extraction_source",
                "dtsx_summary", "error_message", "started_at", "completed_at",
                "created_by", "created_at",
            ]
            return dict(zip(cols, row))
        finally:
            conn.close()

    def list_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT job_id, status, extraction_source, total_objects,
                       extracted_objects, failed_objects, started_at, completed_at,
                       created_by
                FROM migration_job ORDER BY created_at DESC LIMIT %s
                """,
                (limit,),
            )
            cols = [
                "job_id", "status", "extraction_source", "total_objects",
                "extracted_objects", "failed_objects", "started_at",
                "completed_at", "created_by",
            ]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # migration_object
    # ------------------------------------------------------------------

    def upsert_migration_object(
        self,
        job_id: str,
        schema: str,
        name: str,
        object_type: str,
        db_platform: str,
        definition_text: Optional[str] = None,
        parameter_list: Optional[List[Dict[str, Any]]] = None,
        referenced_objects: Optional[List[Dict[str, Any]]] = None,
        extraction_source: str = "LIVE_DB",
        char_count: int = 0,
        is_encrypted: bool = False,
        extraction_status: str = "EXTRACTED",
        error_message: Optional[str] = None,
    ) -> str:
        """Upsert a migration_object row and return its object_id."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO migration_object (
                    job_id, object_schema, object_name, object_type, db_platform,
                    definition_text, parameter_list, referenced_objects,
                    extraction_source, char_count, is_encrypted,
                    extraction_status, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id, object_schema, object_name, object_type) DO UPDATE SET
                    definition_text   = EXCLUDED.definition_text,
                    parameter_list    = EXCLUDED.parameter_list,
                    referenced_objects = EXCLUDED.referenced_objects,
                    extraction_source = EXCLUDED.extraction_source,
                    char_count        = EXCLUDED.char_count,
                    is_encrypted      = EXCLUDED.is_encrypted,
                    extraction_status = EXCLUDED.extraction_status,
                    error_message     = EXCLUDED.error_message
                RETURNING object_id
                """,
                (
                    job_id, schema, name, object_type, db_platform,
                    definition_text,
                    json.dumps(parameter_list or []),
                    json.dumps(referenced_objects or []),
                    extraction_source, char_count, is_encrypted,
                    extraction_status, error_message,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return str(row[0])
        finally:
            conn.close()

    def get_objects_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT object_id, object_schema, object_name, object_type,
                       db_platform, extraction_status, char_count, is_encrypted,
                       error_message, created_at
                FROM migration_object WHERE job_id = %s
                ORDER BY object_schema, object_name
                """,
                (job_id,),
            )
            cols = [
                "object_id", "object_schema", "object_name", "object_type",
                "db_platform", "extraction_status", "char_count", "is_encrypted",
                "error_message", "created_at",
            ]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    def get_object_id_map(self, job_id: str) -> Dict[str, str]:
        """Return {fqn: object_id} for all objects in this job."""
        objects = self.get_objects_for_job(job_id)
        return {
            f"{o['object_schema']}.{o['object_name']}": str(o["object_id"])
            for o in objects
        }

    # ------------------------------------------------------------------
    # migration_lineage
    # ------------------------------------------------------------------

    def persist_lineage(
        self,
        lineage_rows: List[Dict[str, Any]],
        object_id_map: Dict[str, str],
    ) -> int:
        """
        Insert lineage edges.

        lineage_rows come from DependencyGraph.to_lineage_rows() and use
        fqn strings; object_id_map translates them to real UUIDs.
        Rows where either endpoint is not in object_id_map are skipped.
        Returns count of rows inserted.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        inserted = 0
        try:
            for row in lineage_rows:
                parent_id = object_id_map.get(row["parent_fqn"])
                child_id = object_id_map.get(row["child_fqn"])
                if not parent_id or not child_id:
                    continue
                cur.execute(
                    """
                    INSERT INTO migration_lineage (
                        job_id, parent_object_id, child_object_id,
                        reference_type, is_cross_schema, topo_level
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_id, parent_object_id, child_object_id, reference_type)
                    DO UPDATE SET topo_level = EXCLUDED.topo_level
                    """,
                    (
                        row["job_id"],
                        parent_id,
                        child_id,
                        row.get("reference_type", "CALLS"),
                        row.get("is_cross_schema", False),
                        row.get("topo_level", 0),
                    ),
                )
                inserted += 1
            conn.commit()
            logger.info("lineage_persisted", job_id=lineage_rows[0]["job_id"] if lineage_rows else "?", rows=inserted)
            return inserted
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # migration_artifact
    # ------------------------------------------------------------------

    def save_artifact(
        self,
        object_id: str,
        job_id: str,
        artifact_type: str,
        artifact_content: Optional[str] = None,
        artifact_path: Optional[str] = None,
        llm_model: Optional[str] = None,
        llm_prompt_tokens: Optional[int] = None,
        llm_output_tokens: Optional[int] = None,
        confidence_score: Optional[float] = None,
        validation_status: str = "PENDING",
        validation_errors: Optional[List[Any]] = None,
        generation_ms: Optional[int] = None,
    ) -> str:
        """Insert a migration_artifact row and return its artifact_id."""
        artifact_id = str(uuid.uuid4())
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO migration_artifact (
                    artifact_id, object_id, job_id, artifact_type,
                    artifact_content, artifact_path,
                    llm_model, llm_prompt_tokens, llm_output_tokens,
                    confidence_score, validation_status, validation_errors,
                    generation_ms
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    artifact_id, object_id, job_id, artifact_type,
                    artifact_content, artifact_path,
                    llm_model, llm_prompt_tokens, llm_output_tokens,
                    confidence_score, validation_status,
                    json.dumps(validation_errors or []),
                    generation_ms,
                ),
            )
            conn.commit()
            logger.info("artifact_saved", artifact_id=artifact_id, artifact_type=artifact_type)
            return artifact_id
        finally:
            conn.close()

    def get_artifacts_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT a.artifact_id, a.object_id, a.artifact_type,
                       a.artifact_content, a.artifact_path,
                       a.llm_model, a.confidence_score,
                       a.validation_status, a.generation_ms, a.created_at,
                       mo.object_schema, mo.object_name
                FROM migration_artifact a
                JOIN migration_object mo ON mo.object_id = a.object_id
                WHERE a.job_id = %s
                ORDER BY a.created_at
                """,
                (job_id,),
            )
            cols = [
                "artifact_id", "object_id", "artifact_type",
                "artifact_content", "artifact_path",
                "llm_model", "confidence_score",
                "validation_status", "generation_ms", "created_at",
                "object_schema", "object_name",
            ]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
