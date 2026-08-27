"""
L1SupportAgent — SOP-Based Pre-AI Triage

WHY:
  Every incident (DAG failure, GCP VM down, connectivity blip) currently
  burns the full 12-node AI workflow: Swarm RAG → LLM plan → Judge → approval.
  For well-known, deterministic failure patterns (timeout, OOM, stopped VM)
  this is wasteful and adds latency. L1 handles these with zero LLM calls.

HOW:
  Runs as node_l1_triage, inserted after node_classify in the LangGraph
  workflow. Loads sop_registry.json, pattern-matches the classified incident,
  and executes the SOP action (DAG retry via Airflow MCP, VM start via GCP
  Compute API). On success → short-circuits to close_ticket. On failure or
  no match → falls through to the existing AI workflow unchanged.

ESCALATION RULES (never attempt L1):
  - Severity P1 / P2
  - Any immediate_escalate_pattern keyword in classification or error text
  - Active L1 attempts exhausted (max_l1_attempts reached)

REUSES:
  - kafka_producer for Airflow MCP events (same Topics as workflow)
  - google.cloud.compute_v1 for GCP VM ops (same lib as _execute_gcp_remediation)
  - audit_logger for EU AI Act compliance trail

METRICS (Prometheus):
  - aiagent_l1_attempts_total{sop_id, classification, outcome}
  - aiagent_l1_resolution_seconds{sop_id}
  - aiagent_l1_escalations_total{reason}
"""
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import structlog

from agents.servicenow_agent.src.streaming.kafka_producer import get_producer
from agents.servicenow_agent.src.streaming.schemas import Topics
from agents.servicenow_agent.src.governance.audit_logger import (
    audit_logger, AuditEventType, RiskLevel
)

logger = structlog.get_logger(__name__)

_SOP_REGISTRY_PATH = Path(__file__).parent.parent / "data" / "sop_registry.json"


# ---------------------------------------------------------------------------
# Public result keys added to WorkflowState
# ---------------------------------------------------------------------------
L1_ATTEMPTED   = "l1_attempted"
L1_SOP_ID      = "l1_sop_id"
L1_SOP_NAME    = "l1_sop_name"
L1_OUTCOME     = "l1_outcome"   # "resolved" | "escalated" | "no_match" | "failed"
L1_RESOLVED    = "l1_resolved"
L1_NOTES       = "l1_attempt_notes"


class L1SupportAgent:
    """
    Deterministic, SOP-driven incident resolver.
    Called once per incident from node_l1_triage.
    """

    def __init__(self):
        self._registry = self._load_registry()
        self._producer = get_producer()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate and attempt L1 resolution.

        Returns a dict of keys to merge into WorkflowState.
        Never raises — all failures set l1_outcome="escalated".
        """
        incident_id  = state.get("incident_id", "")
        classification = state.get("classification", "")
        severity       = state.get("severity", "3")
        parsed_context = state.get("parsed_context", {})

        logger.info("l1_triage_start", incident_id=incident_id,
                    classification=classification, severity=severity)

        # ── 1. Fast-path: immediate escalation conditions ──────────────
        escalate_reason = self._should_immediately_escalate(
            classification, severity, parsed_context
        )
        if escalate_reason:
            logger.info("l1_immediate_escalate", incident_id=incident_id,
                        reason=escalate_reason)
            self._emit_metric("escalated", "immediate_escalation")
            return self._escalate_result(escalate_reason)

        # ── 2. Match SOP ───────────────────────────────────────────────
        sop = self._match_sop(classification, severity, parsed_context)
        if not sop:
            logger.info("l1_no_sop_match", incident_id=incident_id,
                        classification=classification)
            self._emit_metric("no_match", "no_sop_match")
            return self._no_match_result()

        logger.info("l1_sop_matched", incident_id=incident_id,
                    sop_id=sop["id"], sop_name=sop["name"])

        # ── 3. Execute SOP (with retry loop up to max_l1_attempts) ────
        start_ts = time.monotonic()
        result = await self._run_sop(sop, state)
        duration = time.monotonic() - start_ts

        # ── 4. Audit trail ─────────────────────────────────────────────
        audit_logger.log(
            event_type=AuditEventType.MODEL_DECISION,
            actor="l1-support-agent",
            actor_type="system",
            action=f"l1_sop_{sop['id']}_{result['outcome']}",
            resource=incident_id,
            resource_type="incident",
            risk_level=RiskLevel.LOW,
            details={
                "sop_id": sop["id"],
                "classification": classification,
                "severity": severity,
                "outcome": result["outcome"],
                "duration_seconds": round(duration, 2),
                "notes": result.get("notes", ""),
            }
        )

        self._emit_metric(result["outcome"], sop["id"])

        if result["outcome"] == "resolved":
            logger.info("l1_resolved", incident_id=incident_id,
                        sop_id=sop["id"], duration=round(duration, 2))
            # Publish L1 attempted event (for dashboards)
            await self._publish_l1_event(state, sop, "resolved", result.get("notes", ""))
            return self._resolved_result(sop, result["notes"], state)
        else:
            logger.info("l1_escalated", incident_id=incident_id,
                        sop_id=sop["id"], reason=result.get("notes", ""))
            await self._publish_l1_event(state, sop, "escalated", result.get("notes", ""))
            return self._escalate_result(result.get("notes", "L1 SOP action failed"),
                                         sop_id=sop["id"], sop_name=sop["name"])

    # ------------------------------------------------------------------
    # SOP matching
    # ------------------------------------------------------------------

    def _should_immediately_escalate(
        self,
        classification: str,
        severity: str,
        parsed_context: Dict
    ) -> Optional[str]:
        """Return reason string if this incident must bypass L1, else None."""
        reg = self._registry

        # Severity gate
        if severity in reg.get("immediate_escalate_severities", []):
            return f"severity {severity} is P1/P2 — L1 only handles P3/P4"

        # Pattern gate — check classification AND error text
        escalate_patterns = reg.get("immediate_escalate_patterns", [])
        combined_text = (
            f"{classification} "
            f"{parsed_context.get('description', '')} "
            f"{parsed_context.get('error_message', '')}"
        ).lower()

        for pattern in escalate_patterns:
            if pattern.lower() in combined_text:
                return f"escalation pattern '{pattern}' detected in incident text"

        return None

    def _match_sop(
        self,
        classification: str,
        severity: str,
        parsed_context: Dict
    ) -> Optional[Dict]:
        """
        Return the highest-priority matching SOP or None.

        Matching logic (all must pass):
          1. classification overlaps sop.classifications
          2. At least one error_keyword found in combined incident text
        Priority: first match wins (registry is ordered by specificity).
        """
        combined_text = (
            f"{classification} "
            f"{parsed_context.get('description', '')} "
            f"{parsed_context.get('error_message', '')} "
            f"{parsed_context.get('category', '')}"
        ).lower()

        for sop in self._registry.get("sops", []):
            # Classification overlap
            sop_classifications = [c.lower() for c in sop.get("classifications", [])]
            if not any(c in combined_text for c in sop_classifications):
                continue

            # Keyword match
            keywords = [k.lower() for k in sop.get("error_keywords", [])]
            if not any(kw in combined_text for kw in keywords):
                continue

            return sop

        return None

    # ------------------------------------------------------------------
    # SOP execution
    # ------------------------------------------------------------------

    async def _run_sop(
        self, sop: Dict, state: Dict
    ) -> Dict[str, Any]:
        """
        Execute the SOP action with retry loop.
        Returns {"outcome": "resolved"|"failed", "notes": str}
        """
        max_attempts = sop.get("max_l1_attempts", 1)
        wait_before  = sop.get("wait_seconds_before_retry", 0)
        action       = sop.get("action", "")

        for attempt in range(1, max_attempts + 1):
            logger.info("l1_sop_attempt", sop_id=sop["id"],
                        attempt=attempt, max=max_attempts, action=action)

            if wait_before > 0 and attempt > 1:
                # Cap wait at 90s so the node doesn't block too long
                actual_wait = min(wait_before, 90)
                logger.info("l1_pre_retry_wait", seconds=actual_wait)
                await asyncio.sleep(actual_wait)
            elif wait_before > 0 and action == "wait_and_retry":
                actual_wait = min(wait_before, 90)
                await asyncio.sleep(actual_wait)

            exec_result = await self._dispatch_action(action, sop, state)
            success = exec_result.get("success", False)

            if success:
                # Brief settle time before verification
                verify_after = min(sop.get("verify_after_seconds", 30), 30)
                await asyncio.sleep(verify_after)
                verified = await self._verify(action, sop, state)
                if verified:
                    return {
                        "outcome": "resolved",
                        "notes": (
                            f"L1 {sop['id']} resolved on attempt {attempt}/{max_attempts}. "
                            f"Action: {action}. {exec_result.get('message', '')}"
                        )
                    }
                logger.warning("l1_verify_failed", sop_id=sop["id"], attempt=attempt)
            else:
                logger.warning("l1_action_failed", sop_id=sop["id"],
                               attempt=attempt, error=exec_result.get("error", ""))

        return {
            "outcome": "failed",
            "notes": (
                f"L1 {sop['id']} exhausted {max_attempts} attempt(s) without resolution. "
                f"Last error: {exec_result.get('error', 'unknown')}"
            )
        }

    async def _dispatch_action(
        self, action: str, sop: Dict, state: Dict
    ) -> Dict[str, Any]:
        """Route to concrete action handler."""
        if action in ("airflow_retry", "airflow_retry_with_config", "wait_and_retry"):
            return await self._action_airflow_retry(sop, state)
        elif action == "gcp_vm_start":
            return await self._action_gcp_vm_start(sop, state)
        else:
            return {"success": False, "error": f"unknown action '{action}'"}

    # ------------------------------------------------------------------
    # Action: Airflow DAG retry (via Kafka → Airflow MCP)
    # ------------------------------------------------------------------

    async def _action_airflow_retry(
        self, sop: Dict, state: Dict
    ) -> Dict[str, Any]:
        """
        Publish airflow.retry_dag to Kafka. Airflow MCP consumes and retries.
        Mirrors retry_dag_via_mcp() in langgraph_workflow.py.
        """
        incident_id    = state.get("incident_id", "")
        correlation_id = state.get("correlation_id", "")
        parsed         = state.get("parsed_context", {})
        cfg            = sop.get("action_config", {})

        dag_id     = parsed.get("dag_id") or self._extract_dag_id(parsed)
        dag_run_id = parsed.get("dag_run_id", "")
        task_id    = parsed.get("failed_task_id")

        if not dag_id:
            return {"success": False, "error": "Could not extract dag_id from incident context"}

        event = {
            "event_type": "airflow.retry_dag",
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "incident_id": incident_id,
            "correlation_id": correlation_id,
            "l1_sop_id": sop["id"],
            "conf": {
                cfg.get("conf_key", "l1_retry"): True,
                "l1_reason": cfg.get("conf_reason", sop["id"]),
                **({"executor_memory": cfg["executor_memory"]} if "executor_memory" in cfg else {}),
                **({"driver_memory":   cfg["driver_memory"]}   if "driver_memory"   in cfg else {}),
                **({"timeout_override": cfg["timeout_override"]} if "timeout_override" in cfg else {}),
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        try:
            ok = await self._producer.publish_event(
                topic=Topics.AIRFLOW_RETRY_DAG,
                event=event,
                key=correlation_id or incident_id
            )
            if ok:
                logger.info("l1_airflow_retry_published",
                            dag_id=dag_id, sop_id=sop["id"])
                return {"success": True, "dag_id": dag_id,
                        "message": f"Retry command published for dag_id={dag_id}"}
            return {"success": False, "error": "Kafka publish returned False"}
        except Exception as exc:
            logger.error("l1_airflow_retry_failed", error=str(exc))
            return {"success": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # Action: GCP VM start (via Compute API)
    # ------------------------------------------------------------------

    async def _action_gcp_vm_start(
        self, sop: Dict, state: Dict
    ) -> Dict[str, Any]:
        """
        Start a TERMINATED/STOPPED GCP VM.
        Replicates _execute_gcp_remediation() from langgraph_workflow.py.
        """
        parsed    = state.get("parsed_context", {})
        extracted = state.get("extracted_params", {})

        instance_name = extracted.get("instance_name") or self._extract_vm_name(parsed)
        zone          = extracted.get("zone")          or self._extract_zone(parsed)
        project       = extracted.get("project")       or os.getenv("GCP_PROJECT_ID", "")

        if not instance_name:
            return {"success": False, "error": "Could not extract VM instance name"}
        if not zone:
            return {"success": False, "error": f"Could not extract zone for VM '{instance_name}'"}
        if not project:
            return {"success": False, "error": "GCP_PROJECT_ID not set"}

        try:
            from google.cloud import compute_v1

            client    = compute_v1.InstancesClient()
            instance  = client.get(project=project, zone=zone, instance=instance_name)
            cur_state = instance.status

            if cur_state == "RUNNING":
                return {"success": True, "message": f"VM '{instance_name}' already RUNNING"}

            op = client.start(project=project, zone=zone, instance=instance_name)
            op_client = compute_v1.ZoneOperationsClient()
            op_result = op_client.wait(project=project, zone=zone, operation=op.name)

            if op_result.status == compute_v1.Operation.Status.DONE and not op_result.error:
                return {"success": True, "instance_name": instance_name, "zone": zone,
                        "message": f"VM '{instance_name}' start issued (was {cur_state})"}

            errors = "; ".join(e.message for e in (op_result.error.errors or []))
            return {"success": False, "error": f"GCP op failed: {errors}"}

        except ImportError:
            return {"success": False,
                    "error": "google-cloud-compute not installed — cannot execute GCP L1"}
        except Exception as exc:
            logger.error("l1_gcp_vm_start_error", instance=instance_name, error=str(exc))
            return {"success": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    async def _verify(self, action: str, sop: Dict, state: Dict) -> bool:
        """
        Post-action health check.
        For Airflow retries: publication success is sufficient (MCP handles execution).
        For GCP VM: poll current VM status.
        """
        if action == "gcp_vm_start":
            return await self._verify_gcp_vm(state)
        # For Airflow retries, trust the MCP (we published the command successfully)
        return True

    async def _verify_gcp_vm(self, state: Dict) -> bool:
        parsed    = state.get("parsed_context", {})
        extracted = state.get("extracted_params", {})
        instance_name = extracted.get("instance_name") or self._extract_vm_name(parsed)
        zone          = extracted.get("zone")          or self._extract_zone(parsed)
        project       = extracted.get("project")       or os.getenv("GCP_PROJECT_ID", "")

        try:
            from google.cloud import compute_v1
            client   = compute_v1.InstancesClient()
            instance = client.get(project=project, zone=zone, instance=instance_name)
            return instance.status == "RUNNING"
        except Exception as exc:
            logger.warning("l1_gcp_verify_error", error=str(exc))
            return False

    # ------------------------------------------------------------------
    # Kafka event
    # ------------------------------------------------------------------

    async def _publish_l1_event(
        self, state: Dict, sop: Dict, outcome: str, notes: str
    ) -> None:
        try:
            await self._producer.publish_event(
                topic=Topics.INCIDENT_L1_ATTEMPTED,
                event={
                    "event_type": "incident.l1_attempted",
                    "incident_id": state.get("incident_id", ""),
                    "correlation_id": state.get("correlation_id", ""),
                    "sop_id": sop["id"],
                    "sop_name": sop["name"],
                    "outcome": outcome,
                    "notes": notes,
                    "classification": state.get("classification", ""),
                    "severity": state.get("severity", ""),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                },
                key=state.get("incident_id", "")
            )
        except Exception as exc:
            logger.warning("l1_event_publish_failed", error=str(exc))

    # ------------------------------------------------------------------
    # Result builders
    # ------------------------------------------------------------------

    def _resolved_result(
        self, sop: Dict, notes: str, state: Dict
    ) -> Dict[str, Any]:
        """State updates when L1 resolves the incident."""
        return {
            L1_ATTEMPTED: True,
            L1_SOP_ID:    sop["id"],
            L1_SOP_NAME:  sop["name"],
            L1_OUTCOME:   "resolved",
            L1_RESOLVED:  True,
            L1_NOTES:     notes,
            # close_ticket node reads these:
            "fix_verified": True,
            "verification_reason": f"L1 SOP {sop['id']} auto-resolved: {notes}",
            "resolution_summary": (
                f"[L1 AUTO-RESOLVED] {sop['name']} ({sop['id']}) — "
                f"resolved without AI workflow intervention. {notes}"
            ),
            # Synthetic plan so close_ticket's resolution_notes are populated
            "plan": {
                "script_id": sop["id"],
                "action_type": sop.get("action", "l1_sop"),
                "sop_doc": sop.get("sop_doc", ""),
                "steps": [{"step": 1, "action": sop["name"]}],
                "environment": state.get("parsed_context", {}).get("environment", "production"),
                "confidence": 1.0,
            },
            "rag_confidence": 1.0,
            "judge_score": {"quality_score": 10.0, "safety_passed": True,
                            "reasoning": "L1 SOP — deterministic action, no LLM required"},
            "judge_passed": True,
            "status": "approved",
        }

    def _escalate_result(
        self,
        reason: str,
        sop_id: str = "",
        sop_name: str = ""
    ) -> Dict[str, Any]:
        """State updates when L1 escalates to the AI workflow."""
        return {
            L1_ATTEMPTED: bool(sop_id),
            L1_SOP_ID:    sop_id,
            L1_SOP_NAME:  sop_name,
            L1_OUTCOME:   "escalated",
            L1_RESOLVED:  False,
            L1_NOTES:     reason,
        }

    def _no_match_result(self) -> Dict[str, Any]:
        return {
            L1_ATTEMPTED: False,
            L1_SOP_ID:    "",
            L1_SOP_NAME:  "",
            L1_OUTCOME:   "no_match",
            L1_RESOLVED:  False,
            L1_NOTES:     "No SOP pattern matched — routing to AI workflow",
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_registry(self) -> Dict:
        try:
            with open(_SOP_REGISTRY_PATH, "r") as f:
                reg = json.load(f)
            logger.info("l1_sop_registry_loaded", sops=len(reg.get("sops", [])))
            return reg
        except Exception as exc:
            logger.error("l1_sop_registry_load_failed", error=str(exc))
            return {"sops": [], "immediate_escalate_severities": ["P1", "P2", "1", "2"],
                    "immediate_escalate_patterns": []}

    def _extract_dag_id(self, parsed: Dict) -> Optional[str]:
        import re
        full_text = f"{parsed.get('description', '')} {parsed.get('error_message', '')}"
        patterns  = [
            r"dag\s*['\"]([a-zA-Z0-9_-]+)['\"]",
            r"dag_id[=:\s]+([a-zA-Z0-9_-]+)",
            r"dag\s+([a-zA-Z0-9_-]+)\s+failed",
            r"\[airflow\]\s*dag\s*['\"]?([a-zA-Z0-9_-]+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, full_text, re.IGNORECASE)
            if m:
                return m.group(1)
        return None

    def _extract_vm_name(self, parsed: Dict) -> Optional[str]:
        import re
        full_text = f"{parsed.get('description', '')} {parsed.get('short_description', '')}"
        patterns  = [
            r"VM instance\s+([a-zA-Z0-9_-]+)\s+is",
            r"VM\s*['\"]([a-zA-Z0-9_-]+)['\"]",
            r"instance[_\s]+name[=:\s]+([a-zA-Z0-9_-]+)",
            r"[Ii]nstance\s+([a-zA-Z0-9][a-zA-Z0-9_-]{2,})\s+(?:is|was|has)",
        ]
        for pattern in patterns:
            m = re.search(pattern, full_text)
            if m:
                return m.group(1)
        return None

    def _extract_zone(self, parsed: Dict) -> Optional[str]:
        import re
        full_text = f"{parsed.get('description', '')} {parsed.get('error_message', '')}"
        m = re.search(
            r"((?:us|europe|asia|australia|southamerica|northamerica)-[a-z]+\d+-[a-z])",
            full_text
        )
        if m:
            return m.group(1)
        m2 = re.search(r"[Zz]one[=:\s]+([a-z]+-[a-z]+\d+-[a-z])", full_text)
        return m2.group(1) if m2 else None

    def _emit_metric(self, outcome: str, label: str) -> None:
        try:
            from agents.servicenow_agent.src.orchestrator.metrics import (
                L1_ATTEMPTS, L1_ESCALATIONS
            )
            L1_ATTEMPTS.labels(sop_id=label, outcome=outcome).inc()
            if outcome in ("escalated", "failed", "no_match"):
                L1_ESCALATIONS.labels(reason=label).inc()
        except Exception:
            pass  # metrics are best-effort
