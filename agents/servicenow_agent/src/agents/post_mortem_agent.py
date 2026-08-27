"""
PostMortemAgent - Automated Post-Incident Analysis and Report Generation

WHY: Every real support team runs a post-mortem after a significant incident.
     It's the most important feedback loop in SRE: understand what broke, why,
     what was the customer impact, what we did to fix it, and what we're doing
     to prevent it from happening again.

     Without post-mortems, the same incidents repeat. With them, the team
     learns and the system improves over time.

     This agent generates a blameless post-mortem report automatically after
     every P1/P2 incident closes. The human team then reviews, edits, and approves.
     It eliminates the 2-3 hours of log archaeology that usually precede a post-mortem.

WHAT IT PRODUCES (matching SRE industry standard post-mortem template):
  1. Incident timeline (reconstructed from Kafka event log + audit table)
  2. Root cause analysis (from IncidentIntelligenceAgent's RCA + LLM synthesis)
  3. Customer impact analysis (who was affected, for how long, severity)
  4. Detection gap analysis (how long before we detected? could we have detected sooner?)
  5. Contributing factors (not just root cause — everything that made it worse)
  6. What went well (the things that WORKED: auto-rollback, fast detection, etc.)
  7. Action items with owners and due dates (concrete, not vague)
  8. Similar past incidents (from Weaviate/Neo4j knowledge graph)
  9. Reliability metrics (MTTR, time-to-detect, time-to-mitigate)
  10. Knowledge base contribution (the fix is indexed for future RAG retrieval)

OUTPUT FORMATS:
  - Markdown document (committed to the runbooks repo via GitHub MCP)
  - Jira ticket with post-mortem content (via Jira MCP)
  - Kafka event: incident.postmortem_ready (UI shows review link)

TRIGGER:
  - Called by Governor in Phase 7 (after CLOSED state)
  - Runs for P1 and P2 incidents only (configurable)
  - Also runs for P3 if same incident type has recurred 3+ times in 30 days
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class TimelineEvent:
    """A single event in the incident timeline."""
    timestamp: str
    event_type: str
    actor: str            # System component or human name
    description: str
    duration_from_start_minutes: float


@dataclass
class ActionItem:
    """A concrete corrective action from the post-mortem."""
    title: str
    description: str
    category: str         # prevent_recurrence | improve_detection | improve_response | technical_debt
    owner_team: str
    priority: str         # P1/P2/P3
    due_date: str
    ticket_id: Optional[str] = None  # Created Jira ticket


@dataclass
class PostMortemReport:
    """Complete post-mortem report."""
    incident_id: str
    severity: str
    title: str
    executive_summary: str
    timeline: List[TimelineEvent]
    root_cause: str
    contributing_factors: List[str]
    customer_impact: str
    detection_gap_minutes: float      # Time from incident start to first alert
    time_to_mitigate_minutes: float   # From alert to service restored
    time_to_resolve_minutes: float    # Full ticket closure
    what_went_well: List[str]
    what_went_wrong: List[str]
    action_items: List[ActionItem]
    similar_past_incidents: List[Dict[str, str]]
    runbook_update_needed: bool
    runbook_reference: Optional[str]
    generated_at: str
    generated_by: str = "PostMortemAgent v1.0"
    reviewed: bool = False
    approved_by: Optional[str] = None


class PostMortemAgent:
    """
    Generates blameless post-mortem reports from closed incidents.

    Reads the full incident audit trail from PostgreSQL and synthesizes
    a structured post-mortem using LLM + rule-based analysis.
    """

    # Minimum severity thresholds for auto post-mortem
    AUTO_POSTMORTEM_SEVERITIES = {"P1", "P2"}

    def __init__(
        self,
        db_connection_string: str = "",
        kafka_producer=None,
        github_mcp_client=None,
        jira_mcp_client=None,
        llm_client=None,
        weaviate_client=None,
    ):
        self.db_conn_str = db_connection_string
        self.kafka_producer = kafka_producer
        self.github_mcp = github_mcp_client
        self.jira_mcp = jira_mcp_client
        self.llm = llm_client
        self.weaviate = weaviate_client

    async def generate(
        self,
        incident_id: str,
        incident_context: Dict[str, Any],
        risk_assessment: Dict[str, Any],
        remediation_plan: Dict[str, Any],
        execution_result: Dict[str, Any],
        verification_result: Dict[str, Any],
        approval_payload: Dict[str, Any],
        learning_feedback: Dict[str, Any],
    ) -> PostMortemReport:
        """
        Generate a full post-mortem from a closed incident's context.

        All parameters are the typed contracts from the FAST agents, available
        in the Governor's final state after incident closure.
        """
        log = logger.bind(incident_id=incident_id)
        log.info("post_mortem_generation_start")

        severity = incident_context.get("severity", "P2")

        # Load full event timeline from audit log
        timeline = await self._reconstruct_timeline(incident_id)

        # Compute reliability metrics
        created_at = incident_context.get("created_at", "")
        first_alert_at = self._find_event_timestamp(timeline, "incident.created")
        resolved_at = self._find_event_timestamp(timeline, "incident.closed")
        execution_started_at = self._find_event_timestamp(timeline, "remediation.started")

        detection_gap = self._compute_duration_minutes(created_at, first_alert_at)
        time_to_mitigate = self._compute_duration_minutes(first_alert_at, execution_started_at)
        time_to_resolve = self._compute_duration_minutes(first_alert_at, resolved_at)

        # Root cause from Intelligence Agent
        root_cause = incident_context.get("root_cause", "Root cause not determined automatically")
        rca_confidence = incident_context.get("confidence_score", 0.0)

        # Contributing factors analysis
        contributing_factors = self._analyze_contributing_factors(
            incident_context, risk_assessment, execution_result
        )

        # Customer impact
        blast_radius = risk_assessment.get("blast_radius", {})
        customer_impact = self._format_customer_impact(blast_radius, time_to_resolve)

        # What went well / wrong
        what_went_well = self._assess_what_went_well(
            detection_gap, time_to_resolve, execution_result, verification_result
        )
        what_went_wrong = self._assess_what_went_wrong(
            detection_gap, execution_result, approval_payload, incident_context
        )

        # Action items (rule-based + LLM-synthesized)
        action_items = await self._generate_action_items(
            incident_context=incident_context,
            root_cause=root_cause,
            what_went_wrong=what_went_wrong,
            execution_result=execution_result,
            detection_gap=detection_gap,
        )

        # Similar past incidents from knowledge base
        similar = await self._find_similar_past_incidents(incident_context)

        # Executive summary
        exec_summary = await self._generate_executive_summary(
            incident_id=incident_id,
            severity=severity,
            root_cause=root_cause,
            customer_impact=customer_impact,
            time_to_resolve=time_to_resolve,
            action_items=action_items,
        )

        # Check if runbook needs update
        runbook_ref = remediation_plan.get("runbook_reference")
        runbook_update_needed = (
            execution_result.get("required_deviation_from_runbook", False)
            or rca_confidence < 0.7
            or not runbook_ref
        )

        report = PostMortemReport(
            incident_id=incident_id,
            severity=severity,
            title=f"Post-Mortem: {incident_context.get('title', incident_id)}",
            executive_summary=exec_summary,
            timeline=timeline,
            root_cause=root_cause,
            contributing_factors=contributing_factors,
            customer_impact=customer_impact,
            detection_gap_minutes=detection_gap,
            time_to_mitigate_minutes=time_to_mitigate,
            time_to_resolve_minutes=time_to_resolve,
            what_went_well=what_went_well,
            what_went_wrong=what_went_wrong,
            action_items=action_items,
            similar_past_incidents=similar,
            runbook_update_needed=runbook_update_needed,
            runbook_reference=runbook_ref,
            generated_at=datetime.now(timezone.utc).isoformat(),
        )

        # Persist and publish
        markdown = self._render_markdown(report)
        await self._commit_to_runbooks_repo(incident_id, markdown)
        await self._create_jira_ticket(incident_id, report)
        await self._publish_postmortem_ready(incident_id, report)

        log.info(
            "post_mortem_generation_complete",
            action_items=len(action_items),
            timeline_events=len(timeline),
            ttd_minutes=round(detection_gap, 1),
            ttr_minutes=round(time_to_resolve, 1),
        )
        return report

    # ------------------------------------------------------------------
    # Timeline reconstruction
    # ------------------------------------------------------------------

    async def _reconstruct_timeline(self, incident_id: str) -> List[TimelineEvent]:
        """
        Reconstruct incident timeline from the audit_events PostgreSQL table.
        Events are ordered chronologically and annotated with human-readable descriptions.
        """
        events: List[TimelineEvent] = []

        EVENT_DESCRIPTIONS = {
            "incident.created": "Incident detected and published to Kafka",
            "incident.received": "EventOrchestrator picked up incident, LangGraph started",
            "incident.enriched": "Classification complete — incident type and severity assigned",
            "incident.plan_generated": "Remediation plan generated by LLM",
            "incident.requires_approval": "Plan submitted for human approval",
            "incident.approved": "Human approved remediation plan",
            "incident.rejected": "Human rejected plan — new plan generation triggered",
            "remediation.started": "Execution agent triggered remediation",
            "remediation.executed": "Remediation action completed",
            "remediation.failed": "Remediation action failed — auto-rollback initiated",
            "remediation.rollback": "Auto-rollback executed",
            "incident.verified": "Verification checks passed — service restored",
            "incident.closed": "Ticket closed in ServiceNow",
        }

        try:
            import psycopg2
            if not self.db_conn_str:
                return []
            conn = psycopg2.connect(self.db_conn_str)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT event_type, actor, created_at, details
                    FROM audit.audit_events
                    WHERE entity_id = %s
                    ORDER BY created_at ASC
                """, [incident_id])
                rows = cur.fetchall()
                conn.close()

            if not rows:
                return events

            start_time = rows[0][2]
            for row in rows:
                event_type, actor, ts, details = row
                delta = (ts - start_time).total_seconds() / 60
                events.append(TimelineEvent(
                    timestamp=ts.isoformat(),
                    event_type=event_type,
                    actor=actor or "system",
                    description=EVENT_DESCRIPTIONS.get(event_type, event_type),
                    duration_from_start_minutes=round(delta, 1),
                ))
        except Exception as e:
            logger.warning("timeline_reconstruction_failed", error=str(e))

        return events

    def _find_event_timestamp(self, timeline: List[TimelineEvent], event_type: str) -> str:
        for event in timeline:
            if event.event_type == event_type:
                return event.timestamp
        return ""

    def _compute_duration_minutes(self, start: str, end: str) -> float:
        if not start or not end:
            return 0.0
        try:
            s = datetime.fromisoformat(start.replace("Z", "+00:00"))
            e = datetime.fromisoformat(end.replace("Z", "+00:00"))
            return (e - s).total_seconds() / 60
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Analysis helpers
    # ------------------------------------------------------------------

    def _analyze_contributing_factors(
        self,
        incident_context: Dict[str, Any],
        risk_assessment: Dict[str, Any],
        execution_result: Dict[str, Any],
    ) -> List[str]:
        """
        Identify contributing factors beyond the root cause.
        These are things that made the incident worse or prolonged it.
        """
        factors: List[str] = []

        # High blast radius = dependencies amplified impact
        blast_users = risk_assessment.get("blast_radius", {}).get("estimated_users", 0)
        if blast_users > 1000:
            factors.append(
                f"High dependency blast radius: {blast_users:,} users affected due to downstream service coupling"
            )

        # Low confidence in RCA = we weren't sure what caused it
        rca_conf = incident_context.get("confidence_score", 1.0)
        if rca_conf < 0.7:
            factors.append(
                f"Low RCA confidence ({rca_conf:.0%}): insufficient observability made root cause determination uncertain"
            )

        # Execution needed rollback = first plan was wrong
        if execution_result.get("rolled_back", False):
            factors.append(
                "First remediation plan required rollback: initial fix was incorrect or incomplete"
            )

        # No runbook matched = novel failure mode
        rca_source = incident_context.get("rca_source", "")
        if rca_source == "llm_fallback":
            factors.append(
                "No matching runbook found: LLM generated ad-hoc remediation plan (novel failure mode)"
            )

        # SLA urgency
        sla_remaining_pct = risk_assessment.get("sla_remaining_pct", 100)
        if sla_remaining_pct < 30:
            factors.append(
                f"Incident occurred with only {sla_remaining_pct:.0f}% of SLA time remaining"
            )

        return factors or ["No significant contributing factors identified beyond root cause"]

    def _format_customer_impact(self, blast_radius: Dict[str, Any], ttr_minutes: float) -> str:
        estimated_users = blast_radius.get("estimated_users", 0)
        affected_services = blast_radius.get("affected_services", [])
        if not estimated_users and not affected_services:
            return "Customer impact unknown — blast radius data unavailable."

        hours = int(ttr_minutes // 60)
        mins = int(ttr_minutes % 60)
        duration_str = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"

        parts = []
        if estimated_users:
            parts.append(f"~{estimated_users:,} users affected")
        if affected_services:
            parts.append(f"{len(affected_services)} dependent services impacted: {', '.join(affected_services[:5])}")
        parts.append(f"Service degraded for {duration_str}")

        return ". ".join(parts) + "."

    def _assess_what_went_well(
        self,
        detection_gap: float,
        ttr: float,
        execution_result: Dict[str, Any],
        verification_result: Dict[str, Any],
    ) -> List[str]:
        well: List[str] = []

        if detection_gap < 5:
            well.append(f"Fast detection: incident detected within {detection_gap:.1f} minutes")
        if ttr < 30:
            well.append(f"Fast resolution: service restored in {ttr:.0f} minutes total")
        if execution_result.get("rolled_back") is False and execution_result.get("success"):
            well.append("Remediation succeeded on first attempt — no rollback needed")
        if verification_result.get("all_checks_passed"):
            well.append("All verification health checks passed, confirming full service recovery")
        if execution_result.get("auto_remediated"):
            well.append("Auto-remediation succeeded without requiring human execution")

        return well or ["Standard incident response process followed"]

    def _assess_what_went_wrong(
        self,
        detection_gap: float,
        execution_result: Dict[str, Any],
        approval_payload: Dict[str, Any],
        incident_context: Dict[str, Any],
    ) -> List[str]:
        wrong: List[str] = []

        if detection_gap > 30:
            wrong.append(
                f"Slow detection: {detection_gap:.0f} minutes elapsed before incident was detected. "
                f"Consider adding proactive monitoring for this metric."
            )
        if execution_result.get("rolled_back"):
            wrong.append(
                "Initial remediation required rollback. Review plan generation quality for this incident type."
            )
        approval_timeout = approval_payload.get("timed_out", False)
        if approval_timeout:
            wrong.append(
                "Approval timed out — escalation chain was triggered. Consider lower approval tier for this risk level."
            )
        if incident_context.get("duplicate_of"):
            wrong.append(
                f"This incident was a duplicate of {incident_context['duplicate_of']}. "
                f"Review deduplication window settings."
            )
        if incident_context.get("rca_source") == "llm_fallback":
            wrong.append(
                "No runbook matched this incident — relied on LLM ad-hoc generation. "
                "Document this resolution as a new runbook."
            )

        return wrong or ["No significant process gaps identified"]

    async def _generate_action_items(
        self,
        incident_context: Dict[str, Any],
        root_cause: str,
        what_went_wrong: List[str],
        execution_result: Dict[str, Any],
        detection_gap: float,
    ) -> List[ActionItem]:
        """Generate concrete, prioritized action items."""
        items: List[ActionItem] = []
        from datetime import timedelta

        due_soon = (datetime.now(timezone.utc) + timedelta(days=14)).strftime("%Y-%m-%d")
        due_quarter = (datetime.now(timezone.utc) + timedelta(days=90)).strftime("%Y-%m-%d")

        # Always: add/update runbook if none matched
        if incident_context.get("rca_source") == "llm_fallback":
            items.append(ActionItem(
                title=f"Document runbook for {incident_context.get('classification', 'this incident type')}",
                description=(
                    f"This incident used LLM-generated remediation because no matching runbook existed. "
                    f"Document the successful resolution steps as a permanent runbook. "
                    f"Root cause: {root_cause[:200]}"
                ),
                category="prevent_recurrence",
                owner_team=incident_context.get("affected_team", "platform-team"),
                priority="P2",
                due_date=due_soon,
            ))

        # Detection gap action
        if detection_gap > 15:
            items.append(ActionItem(
                title=f"Add proactive alerting for {incident_context.get('classification', 'this failure type')}",
                description=(
                    f"Detection took {detection_gap:.0f} minutes. "
                    f"Add Prometheus alert rule or ProactiveMonitoringAgent metric rule to detect this "
                    f"pattern earlier. Target: detect within 5 minutes."
                ),
                category="improve_detection",
                owner_team="platform-sre",
                priority="P2",
                due_date=due_soon,
            ))

        # Rollback needed
        if execution_result.get("rolled_back"):
            items.append(ActionItem(
                title="Improve remediation plan accuracy for this failure type",
                description=(
                    "The initial remediation plan was incorrect and required rollback. "
                    "Review the judge evaluation thresholds and RCA patterns for this incident type. "
                    "Consider adding this failure mode to the 15-pattern RCA library."
                ),
                category="improve_response",
                owner_team="platform-ai",
                priority="P2",
                due_date=due_soon,
            ))

        # LLM-generated action items from root cause analysis
        if self.llm and root_cause:
            llm_items = await self._llm_generate_action_items(root_cause, incident_context)
            items.extend(llm_items)

        # Default: always add a "review and verify" item
        items.append(ActionItem(
            title="Verify monitoring coverage for affected services",
            description=(
                f"Review Grafana dashboards for all services affected by this incident "
                f"({', '.join(incident_context.get('affected_services', ['unknown']))}) "
                f"and ensure all key SLIs are monitored."
            ),
            category="improve_detection",
            owner_team="platform-sre",
            priority="P3",
            due_date=due_quarter,
        ))

        return items

    async def _llm_generate_action_items(
        self, root_cause: str, incident_context: Dict[str, Any]
    ) -> List[ActionItem]:
        """Use LLM to generate additional context-specific action items."""
        if not self.llm:
            return []

        prompt = f"""
You are an SRE writing a post-mortem action items list. Based on the following incident root cause,
generate 2-3 specific, actionable items to prevent recurrence. Each item must have a concrete technical action.

Root cause: {root_cause}
Incident classification: {incident_context.get('classification', 'unknown')}
Affected services: {', '.join(incident_context.get('affected_services', []))}

Return a JSON array of objects with fields:
  title (string), description (string), category (prevent_recurrence|improve_detection|improve_response|technical_debt),
  owner_team (string), priority (P1|P2|P3)
"""
        try:
            from datetime import timedelta
            due_date = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")

            response = self.llm.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            # Extract JSON from response
            import re
            match = re.search(r'\[.*\]', raw, re.DOTALL)
            if match:
                items_data = json.loads(match.group())
                return [
                    ActionItem(
                        title=d.get("title", ""),
                        description=d.get("description", ""),
                        category=d.get("category", "prevent_recurrence"),
                        owner_team=d.get("owner_team", "platform-team"),
                        priority=d.get("priority", "P3"),
                        due_date=due_date,
                    )
                    for d in items_data
                ]
        except Exception as e:
            logger.warning("llm_action_items_failed", error=str(e))
        return []

    async def _find_similar_past_incidents(
        self, incident_context: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Query Weaviate for similar resolved incidents."""
        if not self.weaviate:
            return []
        try:
            description = incident_context.get("description", "")
            classification = incident_context.get("classification", "")
            query_text = f"{classification}: {description}"[:500]

            result = (
                self.weaviate.query
                .get("ResolvedIncident", ["incident_id", "title", "root_cause", "resolution_notes", "resolved_at"])
                .with_near_text({"concepts": [query_text]})
                .with_limit(3)
                .do()
            )
            incidents = result.get("data", {}).get("Get", {}).get("ResolvedIncident", [])
            return [
                {
                    "incident_id": i.get("incident_id", ""),
                    "title": i.get("title", ""),
                    "root_cause": i.get("root_cause", ""),
                    "resolved_at": i.get("resolved_at", ""),
                }
                for i in incidents
            ]
        except Exception as e:
            logger.warning("similar_incidents_lookup_failed", error=str(e))
            return []

    async def _generate_executive_summary(
        self,
        incident_id: str,
        severity: str,
        root_cause: str,
        customer_impact: str,
        time_to_resolve: float,
        action_items: List[ActionItem],
    ) -> str:
        hours = int(time_to_resolve // 60)
        mins = int(time_to_resolve % 60)
        duration = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"
        p1_actions = [a for a in action_items if a.priority in ("P1", "P2")]

        summary = (
            f"On {datetime.now(timezone.utc).strftime('%Y-%m-%d')}, a {severity} incident "
            f"(ID: {incident_id}) impacted production services. {customer_impact} "
            f"The incident was resolved in {duration}. "
            f"Root cause: {root_cause[:300]}. "
        )
        if p1_actions:
            summary += f"{len(p1_actions)} high-priority action items have been identified to prevent recurrence."

        return summary

    # ------------------------------------------------------------------
    # Output: Markdown, GitHub, Jira, Kafka
    # ------------------------------------------------------------------

    def _render_markdown(self, report: PostMortemReport) -> str:
        """Render the post-mortem as a Markdown document."""
        lines = [
            f"# Post-Mortem: {report.title}",
            f"",
            f"**Incident ID:** `{report.incident_id}`  ",
            f"**Severity:** {report.severity}  ",
            f"**Generated:** {report.generated_at}  ",
            f"**Status:** {'✅ Approved' if report.reviewed else '⏳ Pending Review'}  ",
            f"",
            f"---",
            f"",
            f"## Executive Summary",
            f"",
            report.executive_summary,
            f"",
            f"---",
            f"",
            f"## Impact",
            f"",
            report.customer_impact,
            f"",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Time to Detect | {report.detection_gap_minutes:.1f} min |",
            f"| Time to Mitigate | {report.time_to_mitigate_minutes:.1f} min |",
            f"| Time to Resolve | {report.time_to_resolve_minutes:.1f} min |",
            f"",
            f"---",
            f"",
            f"## Timeline",
            f"",
            f"| T+ (min) | Event | Actor | Description |",
            f"|----------|-------|-------|-------------|",
        ]
        for event in report.timeline:
            lines.append(
                f"| +{event.duration_from_start_minutes:.1f} | `{event.event_type}` "
                f"| {event.actor} | {event.description} |"
            )

        lines += [
            f"",
            f"---",
            f"",
            f"## Root Cause",
            f"",
            report.root_cause,
            f"",
            f"## Contributing Factors",
            f"",
        ]
        for factor in report.contributing_factors:
            lines.append(f"- {factor}")

        lines += [
            f"",
            f"---",
            f"",
            f"## What Went Well",
            f"",
        ]
        for item in report.what_went_well:
            lines.append(f"- ✅ {item}")

        lines += [
            f"",
            f"## What Went Wrong",
            f"",
        ]
        for item in report.what_went_wrong:
            lines.append(f"- ❌ {item}")

        if report.similar_past_incidents:
            lines += [
                f"",
                f"---",
                f"",
                f"## Similar Past Incidents",
                f"",
                f"| Incident ID | Title | Root Cause | Resolved |",
                f"|-------------|-------|-----------|---------|",
            ]
            for p in report.similar_past_incidents:
                lines.append(
                    f"| {p['incident_id']} | {p['title']} | {p['root_cause'][:60]}... | {p['resolved_at'][:10]} |"
                )

        lines += [
            f"",
            f"---",
            f"",
            f"## Action Items",
            f"",
            f"| # | Title | Category | Team | Priority | Due |",
            f"|---|-------|----------|------|----------|-----|",
        ]
        for i, action in enumerate(report.action_items, 1):
            lines.append(
                f"| {i} | {action.title} | {action.category} "
                f"| {action.owner_team} | {action.priority} | {action.due_date} |"
            )

        if report.runbook_update_needed:
            lines += [
                f"",
                f"---",
                f"",
                f"## Runbook Update Required",
                f"",
                f"⚠️ This incident revealed a gap in existing runbooks. "
                f"The resolution steps should be documented in the runbooks repository before closing this post-mortem.",
            ]
            if report.runbook_reference:
                lines.append(f"Existing runbook to update: `{report.runbook_reference}`")

        lines += [
            f"",
            f"---",
            f"",
            f"*Generated automatically by PostMortemAgent v1.0. Review and approve before publishing.*",
        ]

        return "\n".join(lines)

    async def _commit_to_runbooks_repo(self, incident_id: str, markdown: str) -> None:
        """Commit the post-mortem to the runbooks GitHub repo via GitHub MCP."""
        if not self.github_mcp:
            logger.info("post_mortem_github_skipped", reason="no github_mcp configured")
            return
        try:
            filename = f"post-mortems/{datetime.now().strftime('%Y-%m')}/{incident_id}.md"
            await self.github_mcp.create_file(
                path=filename,
                content=markdown,
                message=f"auto: post-mortem for incident {incident_id}",
                branch="main",
            )
            logger.info("post_mortem_committed", file=filename)
        except Exception as e:
            logger.warning("post_mortem_github_failed", error=str(e))

    async def _create_jira_ticket(self, incident_id: str, report: PostMortemReport) -> None:
        """Create a Jira story for the post-mortem review."""
        if not self.jira_mcp:
            return
        try:
            await self.jira_mcp.create_issue(
                project="PLAT",
                issue_type="Story",
                summary=f"Post-Mortem Review: {report.title}",
                description=report.executive_summary,
                priority=report.severity,
                labels=["post-mortem", "incident-review"],
            )
        except Exception as e:
            logger.warning("post_mortem_jira_failed", error=str(e))

    async def _publish_postmortem_ready(self, incident_id: str, report: PostMortemReport) -> None:
        """Publish Kafka event so UI can show post-mortem review link."""
        payload = {
            "incident_id": incident_id,
            "post_mortem_title": report.title,
            "severity": report.severity,
            "action_items_count": len(report.action_items),
            "time_to_resolve_minutes": report.time_to_resolve_minutes,
            "runbook_update_needed": report.runbook_update_needed,
            "generated_at": report.generated_at,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self.kafka_producer:
            try:
                await self.kafka_producer.publish_event(
                    topic="incident.postmortem_ready",
                    event=payload,
                    key=incident_id,
                )
            except Exception as e:
                logger.warning("post_mortem_kafka_failed", error=str(e))
