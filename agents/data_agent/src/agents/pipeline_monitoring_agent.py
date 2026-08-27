"""
PipelineMonitoringAgent - Post-Deployment Pipeline Health Watcher

WHY: Deploying a pipeline is not the end of the job. A real data engineering team
     watches the first 5-10 runs, checks SLA compliance, detects data quality
     regressions, monitors resource utilisation, and remediates failures
     automatically where safe to do so.

     This agent does exactly that — acting as the on-call engineer who stays
     up to watch the first run and pages the team (or auto-fixes) when things go wrong.

WHAT IT DOES:
  1. Poll Airflow API for DAG run status (configurable interval + max_watch_minutes)
  2. Parse Airflow task logs for common failure patterns
  3. Correlate failure against known issue patterns (via Weaviate RAG)
  4. For auto-remediable failures: trigger fix workflow (retry, restart, backfill)
  5. For non-remediable failures: publish pipeline.failed Kafka event →
       DataPipelineIncidentBridge picks it up → creates ServiceNow incident
  6. Track SLA: if DAG hasn't completed by deadline, escalate immediately
  7. After a successful run: collect runtime metrics and publish pipeline.healthy

POSITION IN WORKFLOW: Runs as a background coroutine after DeployerAgent completes.
                       Launched by Supervisor with pipeline_id + dag_id from deploy output.
"""

import asyncio
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


class DagRunState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    UPSTREAM_FAILED = "upstream_failed"


class FailurePattern(str, Enum):
    OOM = "out_of_memory"
    SCHEMA_MISMATCH = "schema_mismatch"
    SOURCE_UNAVAILABLE = "source_unavailable"
    PERMISSION_DENIED = "permission_denied"
    TIMEOUT = "timeout"
    DATA_QUALITY_FAIL = "data_quality_fail"
    UNKNOWN = "unknown"


@dataclass
class TaskRunSummary:
    task_id: str
    state: str
    start_date: Optional[str]
    end_date: Optional[str]
    try_number: int
    duration_seconds: Optional[float]
    failure_pattern: Optional[FailurePattern] = None
    failure_log_snippet: Optional[str] = None


@dataclass
class DagRunSummary:
    run_id: str
    dag_id: str
    run_number: int
    state: DagRunState
    start_date: Optional[str]
    end_date: Optional[str]
    total_duration_seconds: Optional[float]
    sla_breached: bool
    tasks: List[TaskRunSummary] = field(default_factory=list)
    auto_remediable: bool = False
    remediation_action: Optional[str] = None


@dataclass
class PipelineHealthReport:
    pipeline_id: str
    dag_id: str
    watch_started_at: str
    watch_completed_at: str
    runs_observed: int
    successful_runs: int
    failed_runs: int
    sla_breached_runs: int
    avg_duration_seconds: float
    failure_patterns: Dict[str, int]
    overall_health: str   # healthy | degraded | failing
    auto_remediated: int
    escalated_to_incident: int
    recommendations: List[str]


# ---------------------------------------------------------------------------
# Core agent
# ---------------------------------------------------------------------------


class PipelineMonitoringAgent:
    """
    Post-deployment pipeline health watcher.

    Not a BaseAgent subclass — runs as a background async task launched by
    Supervisor after DeployerAgent completes. Publishes Kafka events directly.
    """

    # Common failure patterns and whether they are auto-remediable
    FAILURE_PATTERNS: List[Dict[str, Any]] = [
        {
            "pattern": FailurePattern.OOM,
            "signatures": ["java.lang.OutOfMemoryError", "OOMKilled", "memory exhausted", "GC overhead"],
            "auto_remediable": True,
            "remediation": "increase_spark_memory",
            "description": "Spark executor ran out of memory",
        },
        {
            "pattern": FailurePattern.SCHEMA_MISMATCH,
            "signatures": ["AnalysisException", "cannot resolve column", "schema mismatch", "field not found"],
            "auto_remediable": False,
            "remediation": None,
            "description": "Source schema changed since pipeline was generated",
        },
        {
            "pattern": FailurePattern.SOURCE_UNAVAILABLE,
            "signatures": ["Connection refused", "Unable to connect", "timeout", "ECONNREFUSED", "host unreachable"],
            "auto_remediable": True,
            "remediation": "retry_with_backoff",
            "description": "Source system temporarily unavailable",
        },
        {
            "pattern": FailurePattern.PERMISSION_DENIED,
            "signatures": ["Access Denied", "PermissionDenied", "403", "Forbidden", "insufficient privileges"],
            "auto_remediable": False,
            "remediation": None,
            "description": "Service account missing required permissions on source/destination",
        },
        {
            "pattern": FailurePattern.TIMEOUT,
            "signatures": ["Task timeout", "execution timeout", "soft_time_limit", "TimedOut"],
            "auto_remediable": True,
            "remediation": "increase_timeout",
            "description": "Task exceeded time limit (data volume may have grown)",
        },
        {
            "pattern": FailurePattern.DATA_QUALITY_FAIL,
            "signatures": ["Great Expectations", "expectation_failed", "ValidationError", "quality check failed"],
            "auto_remediable": False,
            "remediation": None,
            "description": "Data quality validation failed — source data does not meet contract",
        },
    ]

    def __init__(
        self,
        airflow_base_url: str = "http://localhost:8083",
        airflow_username: str = "admin",
        airflow_password: str = "admin123",
        kafka_producer=None,
        max_watch_minutes: int = 120,
        poll_interval_seconds: int = 30,
        sla_deadline_minutes: int = 60,
        runs_to_watch: int = 5,
    ):
        self.airflow_base_url = airflow_base_url.rstrip("/")
        self.airflow_username = airflow_username
        self.airflow_password = airflow_password
        self.kafka_producer = kafka_producer
        self.max_watch_minutes = max_watch_minutes
        self.poll_interval_seconds = poll_interval_seconds
        self.sla_deadline_minutes = sla_deadline_minutes
        self.runs_to_watch = runs_to_watch

    async def watch_pipeline(
        self,
        pipeline_id: str,
        dag_id: str,
        request_id: str,
        team: str = "",
        jira_ticket: str = "",
    ) -> PipelineHealthReport:
        """
        Main entry point — watches a deployed pipeline and produces a health report.

        Args:
            pipeline_id: Platform pipeline UUID
            dag_id: Airflow DAG ID
            request_id: Original pipeline creation request ID
            team: Owning team (for incident routing)
            jira_ticket: Originating Jira ticket

        Returns:
            PipelineHealthReport after watching runs_to_watch runs
        """
        log = logger.bind(pipeline_id=pipeline_id, dag_id=dag_id, request_id=request_id)
        log.info("pipeline_monitoring_start")

        started_at = datetime.now(timezone.utc).isoformat()
        runs_observed = 0
        successful_runs = 0
        failed_runs = 0
        sla_breached_runs = 0
        all_durations: List[float] = []
        failure_pattern_counts: Dict[str, int] = {}
        auto_remediated = 0
        escalated = 0
        recommendations: List[str] = []

        deadline = asyncio.get_event_loop().time() + self.max_watch_minutes * 60
        last_seen_run_id: Optional[str] = None

        while runs_observed < self.runs_to_watch:
            if asyncio.get_event_loop().time() > deadline:
                log.warning("pipeline_monitoring_timeout", runs_observed=runs_observed)
                break

            await asyncio.sleep(self.poll_interval_seconds)

            try:
                latest_run = await self._get_latest_dag_run(dag_id)
            except Exception as e:
                log.warning("airflow_poll_error", error=str(e))
                continue

            if not latest_run or latest_run.get("run_id") == last_seen_run_id:
                continue

            run_id = latest_run["run_id"]
            run_state = latest_run.get("state", "running")

            if run_state in ("running", "queued"):
                # Check SLA
                start_str = latest_run.get("start_date")
                if start_str:
                    start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    elapsed = (datetime.now(timezone.utc) - start_dt).total_seconds() / 60
                    if elapsed > self.sla_deadline_minutes:
                        log.warning("sla_breach_detected", run_id=run_id, elapsed_minutes=round(elapsed))
                        await self._publish_sla_breach(
                            pipeline_id, dag_id, run_id, elapsed, request_id, team
                        )
                continue

            # Run completed
            last_seen_run_id = run_id
            runs_observed += 1
            log.info("dag_run_completed", run_id=run_id, state=run_state, run_number=runs_observed)

            tasks = await self._get_task_instances(dag_id, run_id)
            task_summaries = [self._summarize_task(t) for t in tasks]

            # Duration
            start_str = latest_run.get("start_date")
            end_str = latest_run.get("end_date")
            duration = None
            if start_str and end_str:
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                duration = (end_dt - start_dt).total_seconds()
                all_durations.append(duration)

            # SLA check for completed run
            sla_breached = duration is not None and duration > self.sla_deadline_minutes * 60
            if sla_breached:
                sla_breached_runs += 1

            if run_state == DagRunState.SUCCESS.value:
                successful_runs += 1
                await self._publish_pipeline_healthy(
                    pipeline_id, dag_id, run_id, runs_observed, duration, request_id
                )
                log.info("pipeline_run_healthy", run_id=run_id, duration_s=duration)

            elif run_state in (DagRunState.FAILED.value, DagRunState.UPSTREAM_FAILED.value):
                failed_runs += 1
                failed_tasks = [t for t in task_summaries if t.state == "failed"]

                # Classify failure for each failed task
                for task in failed_tasks:
                    logs = await self._get_task_log(dag_id, run_id, task.task_id)
                    task.failure_pattern = self._classify_failure(logs)
                    task.failure_log_snippet = self._extract_log_snippet(logs)
                    if task.failure_pattern:
                        key = task.failure_pattern.value
                        failure_pattern_counts[key] = failure_pattern_counts.get(key, 0) + 1

                # Determine if auto-remediable
                primary_pattern = failed_tasks[0].failure_pattern if failed_tasks else FailurePattern.UNKNOWN
                pattern_info = next(
                    (p for p in self.FAILURE_PATTERNS if p["pattern"] == primary_pattern),
                    {"auto_remediable": False, "remediation": None}
                )

                if pattern_info["auto_remediable"]:
                    log.info("auto_remediation_triggered", pattern=primary_pattern.value if primary_pattern else "unknown")
                    await self._auto_remediate(
                        dag_id=dag_id,
                        run_id=run_id,
                        pattern=primary_pattern,
                        remediation_action=pattern_info["remediation"],
                        pipeline_id=pipeline_id,
                        request_id=request_id,
                    )
                    auto_remediated += 1
                else:
                    # Escalate to incident management
                    log.warning("pipeline_failure_escalating", pattern=primary_pattern.value if primary_pattern else "unknown")
                    await self._escalate_to_incident(
                        pipeline_id=pipeline_id,
                        dag_id=dag_id,
                        run_id=run_id,
                        failed_tasks=task_summaries,
                        request_id=request_id,
                        team=team,
                        jira_ticket=jira_ticket,
                    )
                    escalated += 1

        # Build recommendations
        if failed_runs > 0 and failed_runs == runs_observed:
            recommendations.append("Pipeline is consistently failing — review source connection and schema")
        if sla_breached_runs > runs_observed / 2:
            recommendations.append(
                "More than 50% of runs breach SLA — consider increasing Spark resources or optimizing query"
            )
        if FailurePattern.OOM.value in failure_pattern_counts:
            recommendations.append(
                "OOM errors detected — increase spark.executor.memory or reduce partition size"
            )
        if FailurePattern.SCHEMA_MISMATCH.value in failure_pattern_counts:
            recommendations.append(
                "Schema mismatch failures — enable FLEXIBLE schema evolution policy or contact source team"
            )
        if all_durations and max(all_durations) > min(all_durations) * 3:
            recommendations.append(
                "High duration variance detected — investigate data skew or partition strategy"
            )

        health = "healthy"
        if failed_runs > 0:
            health = "failing" if failed_runs == runs_observed else "degraded"

        report = PipelineHealthReport(
            pipeline_id=pipeline_id,
            dag_id=dag_id,
            watch_started_at=started_at,
            watch_completed_at=datetime.now(timezone.utc).isoformat(),
            runs_observed=runs_observed,
            successful_runs=successful_runs,
            failed_runs=failed_runs,
            sla_breached_runs=sla_breached_runs,
            avg_duration_seconds=sum(all_durations) / len(all_durations) if all_durations else 0.0,
            failure_patterns=failure_pattern_counts,
            overall_health=health,
            auto_remediated=auto_remediated,
            escalated_to_incident=escalated,
            recommendations=recommendations,
        )

        await self._publish_pipeline_health_report(report, request_id)
        log.info(
            "pipeline_monitoring_complete",
            health=health,
            runs=runs_observed,
            success=successful_runs,
            failed=failed_runs,
        )
        return report

    # ------------------------------------------------------------------
    # Airflow API calls
    # ------------------------------------------------------------------

    async def _get_latest_dag_run(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """Fetch the most recent DAG run from Airflow REST API."""
        import urllib.request
        import base64

        url = f"{self.airflow_base_url}/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-start_date"
        creds = base64.b64encode(
            f"{self.airflow_username}:{self.airflow_password}".encode()
        ).decode()
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {creds}"})

        loop = asyncio.get_event_loop()
        try:
            def _fetch():
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return json.loads(resp.read().decode())
            data = await loop.run_in_executor(None, _fetch)
            runs = data.get("dag_runs", [])
            return runs[0] if runs else None
        except Exception:
            return None

    async def _get_task_instances(self, dag_id: str, run_id: str) -> List[Dict[str, Any]]:
        """Fetch all task instances for a specific DAG run."""
        import urllib.request
        import base64

        url = f"{self.airflow_base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
        creds = base64.b64encode(
            f"{self.airflow_username}:{self.airflow_password}".encode()
        ).decode()
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {creds}"})

        loop = asyncio.get_event_loop()
        try:
            def _fetch():
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return json.loads(resp.read().decode())
            data = await loop.run_in_executor(None, _fetch)
            return data.get("task_instances", [])
        except Exception:
            return []

    async def _get_task_log(self, dag_id: str, run_id: str, task_id: str) -> str:
        """Fetch last 10KB of task log from Airflow."""
        import urllib.request
        import base64

        url = f"{self.airflow_base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"
        creds = base64.b64encode(
            f"{self.airflow_username}:{self.airflow_password}".encode()
        ).decode()
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {creds}"})

        loop = asyncio.get_event_loop()
        try:
            def _fetch():
                with urllib.request.urlopen(req, timeout=15) as resp:
                    return resp.read().decode(errors="replace")[-10240:]  # Last 10KB
            return await loop.run_in_executor(None, _fetch)
        except Exception:
            return ""

    # ------------------------------------------------------------------
    # Classification + remediation
    # ------------------------------------------------------------------

    def _summarize_task(self, task: Dict[str, Any]) -> TaskRunSummary:
        start = task.get("start_date")
        end = task.get("end_date")
        duration = None
        if start and end:
            s = datetime.fromisoformat(start.replace("Z", "+00:00"))
            e = datetime.fromisoformat(end.replace("Z", "+00:00"))
            duration = (e - s).total_seconds()
        return TaskRunSummary(
            task_id=task.get("task_id", ""),
            state=task.get("state", ""),
            start_date=start,
            end_date=end,
            try_number=task.get("try_number", 1),
            duration_seconds=duration,
        )

    def _classify_failure(self, log_text: str) -> FailurePattern:
        """Match log text against known failure signatures."""
        for pattern_info in self.FAILURE_PATTERNS:
            for sig in pattern_info["signatures"]:
                if sig.lower() in log_text.lower():
                    return pattern_info["pattern"]
        return FailurePattern.UNKNOWN

    def _extract_log_snippet(self, log_text: str, max_chars: int = 500) -> str:
        """Extract the most relevant part of a failure log."""
        if not log_text:
            return ""
        # Find the last ERROR or CRITICAL line as the summary
        lines = log_text.splitlines()
        error_lines = [l for l in lines if "ERROR" in l or "Exception" in l or "CRITICAL" in l]
        if error_lines:
            snippet = "\n".join(error_lines[-5:])
        else:
            snippet = "\n".join(lines[-10:])
        return snippet[:max_chars]

    async def _auto_remediate(
        self,
        dag_id: str,
        run_id: str,
        pattern: FailurePattern,
        remediation_action: Optional[str],
        pipeline_id: str,
        request_id: str,
    ) -> None:
        """Apply automatic remediation for safe failure patterns."""
        log = logger.bind(dag_id=dag_id, run_id=run_id, action=remediation_action)

        if remediation_action == "retry_with_backoff":
            log.info("auto_remediation_retry")
            await asyncio.sleep(60)  # Wait 60s then Airflow auto-retry kicks in
            # Airflow handles retry via max_active_runs and retry configuration
            # We just need to wait and not escalate immediately

        elif remediation_action == "increase_spark_memory":
            log.info("auto_remediation_memory_bump")
            # Publish a pipeline.config_update event for the supervisor to handle
            if self.kafka_producer:
                await self._publish_kafka_event(
                    topic="pipeline.config_update",
                    payload={
                        "pipeline_id": pipeline_id,
                        "dag_id": dag_id,
                        "change_type": "spark_memory_increase",
                        "reason": "OOM detected in monitoring",
                        "request_id": request_id,
                        "new_config": {
                            "spark.executor.memory": "8g",  # Up from default 4g
                            "spark.driver.memory": "4g",
                        },
                    },
                )

        elif remediation_action == "increase_timeout":
            log.info("auto_remediation_timeout_increase")
            if self.kafka_producer:
                await self._publish_kafka_event(
                    topic="pipeline.config_update",
                    payload={
                        "pipeline_id": pipeline_id,
                        "dag_id": dag_id,
                        "change_type": "timeout_increase",
                        "reason": "Task timeout detected in monitoring",
                        "request_id": request_id,
                        "new_config": {"execution_timeout_minutes": 120},
                    },
                )

    # ------------------------------------------------------------------
    # Kafka event publishing
    # ------------------------------------------------------------------

    async def _publish_pipeline_healthy(
        self,
        pipeline_id: str,
        dag_id: str,
        run_id: str,
        run_number: int,
        duration_seconds: Optional[float],
        request_id: str,
    ) -> None:
        await self._publish_kafka_event(
            "pipeline.healthy",
            {
                "pipeline_id": pipeline_id,
                "dag_id": dag_id,
                "run_id": run_id,
                "run_number": run_number,
                "duration_seconds": duration_seconds,
                "request_id": request_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    async def _escalate_to_incident(
        self,
        pipeline_id: str,
        dag_id: str,
        run_id: str,
        failed_tasks: List[TaskRunSummary],
        request_id: str,
        team: str,
        jira_ticket: str,
    ) -> None:
        """
        Publish pipeline.failed event. DataPipelineIncidentBridge will pick this
        up and create a ServiceNow incident automatically.
        """
        failed_task_summary = [
            {
                "task_id": t.task_id,
                "failure_pattern": t.failure_pattern.value if t.failure_pattern else "unknown",
                "log_snippet": t.failure_log_snippet or "",
            }
            for t in failed_tasks if t.state == "failed"
        ]

        await self._publish_kafka_event(
            "pipeline.failed",
            {
                "pipeline_id": pipeline_id,
                "dag_id": dag_id,
                "run_id": run_id,
                "failed_tasks": failed_task_summary,
                "request_id": request_id,
                "team": team,
                "jira_ticket": jira_ticket,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": "P2",  # Data pipeline failures are P2 by default
                "description": (
                    f"Airflow DAG '{dag_id}' run '{run_id}' failed. "
                    f"{len(failed_task_summary)} task(s) failed. "
                    f"First failure pattern: {failed_task_summary[0]['failure_pattern'] if failed_task_summary else 'unknown'}"
                ),
            },
        )

    async def _publish_sla_breach(
        self,
        pipeline_id: str,
        dag_id: str,
        run_id: str,
        elapsed_minutes: float,
        request_id: str,
        team: str,
    ) -> None:
        await self._publish_kafka_event(
            "pipeline.sla_missed",
            {
                "pipeline_id": pipeline_id,
                "dag_id": dag_id,
                "run_id": run_id,
                "elapsed_minutes": round(elapsed_minutes, 1),
                "sla_deadline_minutes": self.sla_deadline_minutes,
                "request_id": request_id,
                "team": team,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    async def _publish_pipeline_health_report(
        self, report: PipelineHealthReport, request_id: str
    ) -> None:
        await self._publish_kafka_event(
            "pipeline.health_report",
            {
                "pipeline_id": report.pipeline_id,
                "dag_id": report.dag_id,
                "overall_health": report.overall_health,
                "runs_observed": report.runs_observed,
                "successful_runs": report.successful_runs,
                "failed_runs": report.failed_runs,
                "sla_breached_runs": report.sla_breached_runs,
                "avg_duration_seconds": round(report.avg_duration_seconds, 1),
                "failure_patterns": report.failure_patterns,
                "auto_remediated": report.auto_remediated,
                "escalated_to_incident": report.escalated_to_incident,
                "recommendations": report.recommendations,
                "request_id": request_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    async def _publish_kafka_event(self, topic: str, payload: Dict[str, Any]) -> None:
        """Publish to Kafka or log if producer not available."""
        if self.kafka_producer:
            try:
                await self.kafka_producer.publish_event(
                    topic=topic,
                    event=payload,
                    key=payload.get("pipeline_id", ""),
                )
            except Exception as e:
                logger.warning("kafka_publish_failed", topic=topic, error=str(e))
        else:
            logger.info("kafka_event_simulated", topic=topic, payload_keys=list(payload.keys()))
