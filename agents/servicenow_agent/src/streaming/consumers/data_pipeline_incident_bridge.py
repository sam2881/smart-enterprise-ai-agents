"""
DataPipelineIncidentBridge - Kafka Consumer that Bridges Data Failures to Incident Management

WHY: The Data Engineering Agent and Incident Management Agent are separate systems
     with separate Kafka topics. When a data pipeline fails, the Incident Management
     Agent doesn't know about it — and vice versa. A real support team would immediately
     create a ticket when a critical data pipeline fails.

     This bridge is the connective tissue: it listens to Data Agent Kafka topics
     (pipeline.failed, pipeline.sla_missed, pipeline.health_report) and automatically
     creates incidents in the Incident Management workflow.

     This closes the feedback loop:
       Data Agent deploys pipeline
       → PipelineMonitoringAgent watches first 5 runs
       → Pipeline fails with schema_mismatch (not auto-remediable)
       → PipelineMonitoringAgent publishes pipeline.failed
       → DataPipelineIncidentBridge picks it up
       → Creates incident.created with full context (dag_id, run_id, error logs, team)
       → Governor processes it through FAST 9-agent workflow
       → IncidentIntelligenceAgent classifies as dag_failure
       → RiskAgent computes blast radius (which Gold zone tables are affected?)
       → Human approves remediation
       → Fix deployed, pipeline re-runs successfully

ALSO HANDLES:
  - pipeline.sla_missed → P3 incident with SLA context
  - pipeline.config_update → applies Spark config changes to the deployed DAG
  - pipeline.healthy (5 consecutive) → close any open pipeline incident automatically
  - Deduplication: same pipeline.dag_id should not create >1 open incident

CONSUMER GROUP: data-pipeline-bridge (separate from event-orchestrator)
TOPICS CONSUMED: pipeline.failed, pipeline.sla_missed, pipeline.config_update, pipeline.healthy
TOPICS PUBLISHED: incident.created (→ incident management workflow)
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import hashlib

import structlog

logger = structlog.get_logger(__name__)


# Severity mapping from pipeline failure patterns to incident severity
FAILURE_PATTERN_SEVERITY = {
    "schema_mismatch": "P2",          # Data contract broken — affects Gold zone consumers
    "permission_denied": "P2",         # Platform issue — may affect multiple pipelines
    "data_quality_fail": "P2",         # Data quality breach — Gold zone data is wrong
    "out_of_memory": "P3",             # Resource issue — pipeline down but recoverable
    "timeout": "P3",
    "source_unavailable": "P3",        # Source system down — SLA at risk
    "unknown": "P3",
}

# Categories map directly to IncidentIntelligenceAgent's 15 RCA patterns
FAILURE_PATTERN_TO_RCA_HINT = {
    "schema_mismatch": "dag_failure",
    "permission_denied": "permission_denied",
    "data_quality_fail": "dag_failure",
    "out_of_memory": "memory_exhaustion",
    "timeout": "timeout",
    "source_unavailable": "connectivity_failure",
    "unknown": "dag_failure",
}


class DataPipelineIncidentBridge:
    """
    Kafka consumer bridge: data pipeline events → incident.created events.
    Runs as a standalone consumer process (alongside EventOrchestrator).
    """

    def __init__(
        self,
        kafka_consumer=None,
        kafka_producer=None,
        redis_client=None,
        dedup_window_minutes: int = 60,
    ):
        self.consumer = kafka_consumer
        self.producer = kafka_producer
        self.redis = redis_client
        self.dedup_window_seconds = dedup_window_minutes * 60
        self._running = False

        # Tracks dag_id -> open incident_id for auto-close on recovery
        self._open_pipeline_incidents: Dict[str, str] = {}

    async def start(self) -> None:
        """Start consuming pipeline events."""
        self._running = True
        logger.info("data_pipeline_bridge_start")

        TOPICS = [
            "pipeline.failed",
            "pipeline.sla_missed",
            "pipeline.config_update",
            "pipeline.healthy",
            "pipeline.health_report",
        ]

        if self.consumer:
            self.consumer.subscribe(TOPICS)

        while self._running:
            if self.consumer:
                try:
                    messages = self.consumer.poll(timeout_ms=1000, max_records=10)
                    for topic_partition, records in messages.items():
                        for record in records:
                            await self._handle_message(
                                topic=record.topic,
                                payload=record.value,
                            )
                except Exception as e:
                    logger.error("bridge_consumer_error", error=str(e))
                    await asyncio.sleep(5)
            else:
                await asyncio.sleep(1)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Message routing
    # ------------------------------------------------------------------

    async def _handle_message(self, topic: str, payload: Dict[str, Any]) -> None:
        """Route incoming pipeline event to the appropriate handler."""
        try:
            if topic == "pipeline.failed":
                await self._handle_pipeline_failed(payload)
            elif topic == "pipeline.sla_missed":
                await self._handle_pipeline_sla_missed(payload)
            elif topic == "pipeline.config_update":
                await self._handle_pipeline_config_update(payload)
            elif topic == "pipeline.healthy":
                await self._handle_pipeline_healthy(payload)
            elif topic == "pipeline.health_report":
                await self._handle_pipeline_health_report(payload)
        except Exception as e:
            logger.error("bridge_message_handler_error", topic=topic, error=str(e))

    # ------------------------------------------------------------------
    # Handler: pipeline.failed → incident.created
    # ------------------------------------------------------------------

    async def _handle_pipeline_failed(self, payload: Dict[str, Any]) -> None:
        pipeline_id = payload.get("pipeline_id", "")
        dag_id = payload.get("dag_id", "")
        run_id = payload.get("run_id", "")
        failed_tasks = payload.get("failed_tasks", [])
        team = payload.get("team", "data-engineering")
        jira_ticket = payload.get("jira_ticket", "")

        log = logger.bind(dag_id=dag_id, run_id=run_id, pipeline_id=pipeline_id)

        # Deduplication: don't create duplicate incidents for same DAG
        dedup_key = f"pipeline_incident:{dag_id}"
        if await self._is_deduplicated(dedup_key):
            log.info("bridge_deduplicated", reason="incident already open for this dag")
            return

        # Determine primary failure pattern
        primary_pattern = "unknown"
        primary_log_snippet = ""
        if failed_tasks:
            primary_pattern = failed_tasks[0].get("failure_pattern", "unknown")
            primary_log_snippet = failed_tasks[0].get("log_snippet", "")

        severity = FAILURE_PATTERN_SEVERITY.get(primary_pattern, "P3")
        rca_hint = FAILURE_PATTERN_TO_RCA_HINT.get(primary_pattern, "dag_failure")

        # Build rich incident description
        task_summary = "\n".join(
            f"  - Task '{t['task_id']}': {t['failure_pattern']} — {t['log_snippet'][:200]}"
            for t in failed_tasks[:5]
        )
        description = (
            f"Airflow DAG '{dag_id}' (run '{run_id}') failed on pipeline '{pipeline_id}'.\n\n"
            f"Failure pattern: {primary_pattern}\n"
            f"Affected tasks ({len(failed_tasks)}):\n{task_summary}\n\n"
            f"Error excerpt:\n{primary_log_snippet[:500]}"
        )
        if jira_ticket:
            description += f"\n\nOriginating Jira ticket: {jira_ticket}"

        incident_id = self._make_incident_id(dag_id, run_id)

        incident = {
            "incident_id": incident_id,
            "source": "data_pipeline_bridge",
            "title": f"Data Pipeline Failure: {dag_id}",
            "description": description,
            "severity": severity,
            "affected_services": [dag_id, pipeline_id],
            "affected_team": team,
            "rca_hint": rca_hint,           # Pre-fill for IncidentIntelligenceAgent
            "pipeline_context": {
                "pipeline_id": pipeline_id,
                "dag_id": dag_id,
                "run_id": run_id,
                "jira_ticket": jira_ticket,
                "failed_tasks": failed_tasks,
            },
            "create_servicenow_ticket": True,
            "auto_classify": True,
            "classification": "dag_failure",  # Skip classification — we know the type
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        await self._publish_incident(incident)
        await self._mark_deduplicated(dedup_key)
        self._open_pipeline_incidents[dag_id] = incident_id

        log.warning(
            "pipeline_incident_created",
            incident_id=incident_id,
            severity=severity,
            pattern=primary_pattern,
        )

    # ------------------------------------------------------------------
    # Handler: pipeline.sla_missed → P3 incident
    # ------------------------------------------------------------------

    async def _handle_pipeline_sla_missed(self, payload: Dict[str, Any]) -> None:
        dag_id = payload.get("dag_id", "")
        run_id = payload.get("run_id", "")
        elapsed = payload.get("elapsed_minutes", 0.0)
        sla_deadline = payload.get("sla_deadline_minutes", 60)
        team = payload.get("team", "data-engineering")

        dedup_key = f"pipeline_sla:{dag_id}"
        if await self._is_deduplicated(dedup_key):
            return

        incident_id = self._make_incident_id(dag_id, f"sla_{run_id}")
        incident = {
            "incident_id": incident_id,
            "source": "data_pipeline_bridge",
            "title": f"Data Pipeline SLA Breach: {dag_id}",
            "description": (
                f"Airflow DAG '{dag_id}' (run '{run_id}') has been running for "
                f"{elapsed:.1f} minutes, exceeding the SLA deadline of {sla_deadline} minutes. "
                f"Downstream consumers may experience data freshness issues."
            ),
            "severity": "P3",
            "affected_services": [dag_id],
            "affected_team": team,
            "classification": "timeout",
            "pipeline_context": {
                "dag_id": dag_id,
                "run_id": run_id,
                "elapsed_minutes": elapsed,
                "sla_deadline_minutes": sla_deadline,
            },
            "create_servicenow_ticket": False,  # SLA miss = monitoring, not full ticket
            "auto_classify": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        await self._publish_incident(incident)
        await self._mark_deduplicated(dedup_key)
        logger.warning("pipeline_sla_incident_created", dag_id=dag_id, elapsed=elapsed)

    # ------------------------------------------------------------------
    # Handler: pipeline.config_update → apply Spark config change
    # ------------------------------------------------------------------

    async def _handle_pipeline_config_update(self, payload: Dict[str, Any]) -> None:
        """
        Auto-remediation from PipelineMonitoringAgent: Spark memory increase, timeout bump, etc.
        Publishes pipeline.reconfigure event for DeployerAgent to apply.
        """
        pipeline_id = payload.get("pipeline_id", "")
        dag_id = payload.get("dag_id", "")
        change_type = payload.get("change_type", "")
        new_config = payload.get("new_config", {})
        reason = payload.get("reason", "")

        logger.info(
            "pipeline_config_update",
            dag_id=dag_id,
            change_type=change_type,
            new_config=new_config,
        )

        # Publish reconfigure event for the data agent to pick up and redeploy
        reconfigure_event = {
            "pipeline_id": pipeline_id,
            "dag_id": dag_id,
            "change_type": change_type,
            "new_config": new_config,
            "reason": reason,
            "auto_approved": True,         # Low-risk config changes don't need approval
            "initiated_by": "pipeline_monitoring_auto_remediation",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if self.producer:
            try:
                await self.producer.publish_event(
                    topic="pipeline.reconfigure",
                    event=reconfigure_event,
                    key=pipeline_id,
                )
            except Exception as e:
                logger.error("pipeline_reconfigure_publish_failed", error=str(e))

    # ------------------------------------------------------------------
    # Handler: pipeline.healthy → auto-close open pipeline incident
    # ------------------------------------------------------------------

    async def _handle_pipeline_healthy(self, payload: Dict[str, Any]) -> None:
        dag_id = payload.get("dag_id", "")
        run_number = payload.get("run_number", 0)

        # Auto-close incident after 3 consecutive successful runs
        if run_number >= 3 and dag_id in self._open_pipeline_incidents:
            incident_id = self._open_pipeline_incidents[dag_id]
            await self._auto_close_incident(incident_id, dag_id, run_number)
            del self._open_pipeline_incidents[dag_id]
            # Clear dedup key so new failures create fresh incidents
            if self.redis:
                try:
                    self.redis.delete(f"pipeline_incident:{dag_id}")
                except Exception:
                    pass
            logger.info("pipeline_incident_auto_closed", dag_id=dag_id, incident_id=incident_id)

    async def _handle_pipeline_health_report(self, payload: Dict[str, Any]) -> None:
        """Log health report for audit; no incident action needed here."""
        dag_id = payload.get("dag_id", "")
        health = payload.get("overall_health", "unknown")
        logger.info(
            "pipeline_health_report_received",
            dag_id=dag_id,
            health=health,
            runs=payload.get("runs_observed", 0),
            recommendations=len(payload.get("recommendations", [])),
        )

    # ------------------------------------------------------------------
    # Incident publication
    # ------------------------------------------------------------------

    async def _publish_incident(self, incident: Dict[str, Any]) -> None:
        """Publish incident.created to Kafka for the EventOrchestrator to pick up."""
        if self.producer:
            try:
                await self.producer.publish_event(
                    topic="incident.created",
                    event=incident,
                    key=incident.get("incident_id", ""),
                )
            except Exception as e:
                logger.error("bridge_incident_publish_failed", error=str(e))
        else:
            logger.info("bridge_incident_simulated", incident_id=incident.get("incident_id"))

    async def _auto_close_incident(
        self, incident_id: str, dag_id: str, run_number: int
    ) -> None:
        """Auto-close a pipeline incident when pipeline recovers."""
        if self.producer:
            try:
                await self.producer.publish_event(
                    topic="incident.auto_closed",
                    event={
                        "incident_id": incident_id,
                        "reason": f"Pipeline '{dag_id}' completed {run_number} consecutive successful runs",
                        "closed_by": "data_pipeline_bridge",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                    key=incident_id,
                )
            except Exception as e:
                logger.error("bridge_auto_close_failed", error=str(e))

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _make_incident_id(self, dag_id: str, run_id: str) -> str:
        """Generate a deterministic incident ID from DAG + run context."""
        raw = f"pipeline:{dag_id}:{run_id}:{int(time.time() // self.dedup_window_seconds)}"
        return "INC-PIPE-" + hashlib.sha256(raw.encode()).hexdigest()[:12].upper()

    async def _is_deduplicated(self, key: str) -> bool:
        if not self.redis:
            return False
        try:
            return self.redis.exists(key) > 0
        except Exception:
            return False

    async def _mark_deduplicated(self, key: str) -> None:
        if not self.redis:
            return
        try:
            self.redis.setex(key, self.dedup_window_seconds, "1")
        except Exception:
            pass
