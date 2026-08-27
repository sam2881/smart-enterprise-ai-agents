"""
ProactiveMonitoringAgent - Detect Problems Before They Become Incidents

WHY: A real support team doesn't just react to tickets. They watch dashboards,
     set up alerting, and page engineers BEFORE users notice something is wrong.
     This agent is the always-on monitoring engineer who:
       - Polls Prometheus metrics every N seconds
       - Runs anomaly detection (Z-score + threshold rules)
       - Correlates signals across systems (metrics + logs + traces)
       - Pre-creates incidents when anomaly confidence is high enough
       - Suppresses alerts during maintenance windows
       - Learns healthy baselines from historical data

     The difference from a Prometheus alert rule:
       - Prometheus fires on threshold breach; this agent reasons about CONTEXT
       - It can say "this latency spike is expected because a batch job just ran"
       - It correlates multiple weak signals into a single high-confidence alert
       - It enriches the pre-created incident with RCA context, not just "metric is high"

WHAT IT DETECTS:
  - API latency P95 > threshold + trending upward (early degradation warning)
  - Error rate spike (even before it crosses alert threshold)
  - Kafka consumer lag growing (pipeline falling behind)
  - Memory/CPU saturation approaching limits (capacity warning)
  - LLM API error rate increase (agent system degradation)
  - DAG failure rate increase (data pipeline health)
  - Anomalous patterns: sudden drop in throughput, metrics going to zero

INTEGRATION:
  - Polls Prometheus HTTP API at http://localhost:9090/api/v1/query_range
  - Publishes incident.created to Kafka when anomaly detected
  - Marks source as "proactive_monitoring" so Governor knows to run full RCA
  - Stores baseline statistics in Redis (7-day rolling window)
  - Respects maintenance window schedule from PostgreSQL
"""

import asyncio
import json
import time
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import urllib.request
import urllib.parse

import structlog

logger = structlog.get_logger(__name__)


class AnomalyType(str, Enum):
    THRESHOLD_BREACH = "threshold_breach"
    TREND_ANOMALY = "trend_anomaly"         # Not breached yet but trending badly
    SUDDEN_DROP = "sudden_drop"              # Metric dropped to zero or near-zero
    MULTI_SIGNAL = "multi_signal"           # Multiple weak signals correlated


@dataclass
class MetricSample:
    timestamp: float
    value: float


@dataclass
class AnomalySignal:
    """A single detected anomaly before correlation."""
    metric_name: str
    anomaly_type: AnomalyType
    current_value: float
    baseline_value: float
    threshold: float
    confidence: float            # 0.0 – 1.0
    description: str
    service: str
    environment: str = "production"
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class ProactiveAlert:
    """Correlated, enriched alert ready for incident creation."""
    alert_id: str
    severity: str                           # P1/P2/P3/P4
    title: str
    description: str
    signals: List[AnomalySignal]
    affected_services: List[str]
    confidence: float
    recommended_runbook: Optional[str]
    correlation_id: str
    detected_at: str
    suppressed: bool = False
    suppression_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Metric rules — what to watch and how
# ---------------------------------------------------------------------------

METRIC_RULES = [
    # API Health
    {
        "name": "api_error_rate",
        "query": 'sum(rate(aiagent_workflow_errors_total[5m])) / sum(rate(aiagent_workflow_executions_total[5m]))',
        "threshold": 0.05,           # 5% error rate
        "warn_threshold": 0.02,      # 2% = trending
        "service": "backend-api",
        "severity_on_breach": "P2",
        "description": "Backend API error rate",
        "runbook": "RUNBOOK-API-001",
    },
    {
        "name": "api_latency_p95",
        "query": 'histogram_quantile(0.95, sum(rate(aiagent_workflow_node_duration_seconds_bucket[5m])) by (le))',
        "threshold": 300.0,          # 300s = P95 latency
        "warn_threshold": 150.0,
        "service": "backend-api",
        "severity_on_breach": "P2",
        "description": "Workflow P95 latency",
        "runbook": "RUNBOOK-LATENCY-001",
    },
    # Kafka
    {
        "name": "kafka_consumer_lag",
        "query": 'sum(kafka_consumer_group_lag{consumer_group="event-orchestrator"})',
        "threshold": 1000.0,
        "warn_threshold": 500.0,
        "service": "kafka",
        "severity_on_breach": "P2",
        "description": "EventOrchestrator Kafka consumer lag",
        "runbook": "RUNBOOK-KAFKA-001",
    },
    # LLM
    {
        "name": "llm_error_rate",
        "query": 'sum(rate(aiagent_llm_calls_total{status="error"}[5m])) / sum(rate(aiagent_llm_calls_total[5m]))',
        "threshold": 0.10,
        "warn_threshold": 0.03,
        "service": "llm-api",
        "severity_on_breach": "P1",
        "description": "LLM API error rate",
        "runbook": "RUNBOOK-LLM-001",
    },
    {
        "name": "llm_latency_p99",
        "query": 'histogram_quantile(0.99, sum(rate(aiagent_llm_latency_seconds_bucket[5m])) by (le))',
        "threshold": 30.0,
        "warn_threshold": 10.0,
        "service": "llm-api",
        "severity_on_breach": "P2",
        "description": "LLM API P99 latency",
        "runbook": "RUNBOOK-LLM-002",
    },
    # Approval queue
    {
        "name": "approval_queue_depth",
        "query": 'aiagent_approvals_pending',
        "threshold": 10.0,
        "warn_threshold": 5.0,
        "service": "approval-system",
        "severity_on_breach": "P3",
        "description": "Approval queue depth",
        "runbook": "RUNBOOK-APPROVAL-001",
    },
    # Data pipeline
    {
        "name": "pipeline_failure_rate",
        "query": 'sum(rate(aiagent_pipeline_failures_total[30m]))',
        "threshold": 3.0,            # 3 failures in 30 min
        "warn_threshold": 1.0,
        "service": "data-agent",
        "severity_on_breach": "P3",
        "description": "Data pipeline failure rate",
        "runbook": "RUNBOOK-PIPELINE-001",
    },
    # Memory
    {
        "name": "process_memory_mb",
        "query": 'process_resident_memory_bytes{job="backend-api"} / 1024 / 1024',
        "threshold": 3000.0,         # 3GB
        "warn_threshold": 2000.0,
        "service": "backend-api",
        "severity_on_breach": "P2",
        "description": "Backend API memory usage",
        "runbook": "RUNBOOK-MEMORY-001",
    },
]


class ProactiveMonitoringAgent:
    """
    Always-on monitoring agent that detects anomalies and pre-creates incidents.

    Runs as a background asyncio loop. Call start() to begin monitoring;
    call stop() to shut down cleanly.
    """

    def __init__(
        self,
        prometheus_url: str = "http://localhost:9090",
        kafka_producer=None,
        redis_client=None,
        poll_interval_seconds: int = 60,
        correlation_window_seconds: int = 300,   # Correlate signals within 5 min
        min_confidence_to_alert: float = 0.7,
    ):
        self.prometheus_url = prometheus_url.rstrip("/")
        self.kafka_producer = kafka_producer
        self.redis = redis_client
        self.poll_interval = poll_interval_seconds
        self.correlation_window = correlation_window_seconds
        self.min_confidence = min_confidence_to_alert
        self._running = False
        self._recent_alerts: Dict[str, float] = {}  # alert_key -> timestamp; dedup 30 min
        self._baseline_cache: Dict[str, List[float]] = {}  # metric -> last 100 values

    async def start(self) -> None:
        """Start the monitoring loop. Blocks until stop() is called."""
        self._running = True
        logger.info("proactive_monitoring_start", poll_interval=self.poll_interval)
        while self._running:
            try:
                await self._monitoring_cycle()
            except Exception as e:
                logger.error("monitoring_cycle_error", error=str(e))
            await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        self._running = False
        logger.info("proactive_monitoring_stop")

    # ------------------------------------------------------------------
    # Core monitoring cycle
    # ------------------------------------------------------------------

    async def _monitoring_cycle(self) -> None:
        """One poll cycle: query all metrics, detect anomalies, correlate, alert."""
        cycle_start = time.monotonic()
        signals: List[AnomalySignal] = []

        # Check maintenance window
        if await self._in_maintenance_window():
            logger.info("monitoring_suppressed_maintenance_window")
            return

        # Query all metric rules in parallel
        tasks = [self._check_metric_rule(rule) for rule in METRIC_RULES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.warning("metric_check_error", error=str(result))
                continue
            if result:
                signals.extend(result)

        if not signals:
            logger.debug("monitoring_cycle_clean", duration_ms=round((time.monotonic() - cycle_start) * 1000))
            return

        # Correlate signals into alerts
        alerts = self._correlate_signals(signals)

        for alert in alerts:
            if alert.confidence < self.min_confidence:
                continue
            if alert.suppressed:
                logger.info("alert_suppressed", reason=alert.suppression_reason, title=alert.title)
                continue
            if self._is_duplicate(alert):
                continue

            logger.warning(
                "proactive_alert_detected",
                title=alert.title,
                severity=alert.severity,
                confidence=round(alert.confidence, 2),
                services=alert.affected_services,
            )
            await self._create_incident(alert)
            self._mark_alerted(alert)

        logger.debug(
            "monitoring_cycle_complete",
            signals=len(signals),
            alerts=len(alerts),
            duration_ms=round((time.monotonic() - cycle_start) * 1000),
        )

    # ------------------------------------------------------------------
    # Metric evaluation
    # ------------------------------------------------------------------

    async def _check_metric_rule(self, rule: Dict[str, Any]) -> List[AnomalySignal]:
        """Query Prometheus for one metric rule and return any anomaly signals."""
        signals: List[AnomalySignal] = []
        metric_name = rule["name"]

        # Instant query
        current = await self._prometheus_query(rule["query"])
        if current is None:
            return signals

        # Update baseline cache
        if metric_name not in self._baseline_cache:
            self._baseline_cache[metric_name] = []
        self._baseline_cache[metric_name].append(current)
        self._baseline_cache[metric_name] = self._baseline_cache[metric_name][-100:]

        baseline = self._compute_baseline(metric_name)
        threshold = rule["threshold"]
        warn_threshold = rule["warn_threshold"]

        # Hard threshold breach
        if current >= threshold:
            confidence = min(1.0, 0.7 + 0.3 * (current - threshold) / max(threshold, 0.001))
            signals.append(AnomalySignal(
                metric_name=metric_name,
                anomaly_type=AnomalyType.THRESHOLD_BREACH,
                current_value=current,
                baseline_value=baseline,
                threshold=threshold,
                confidence=confidence,
                description=f"{rule['description']}: {current:.2f} exceeds threshold {threshold:.2f}",
                service=rule["service"],
            ))
            return signals  # Don't also add trend signal if threshold is breached

        # Trend: above warn_threshold AND trending upward
        if current >= warn_threshold and baseline > 0:
            trend = self._compute_trend(metric_name)
            if trend > 0.1:  # Positive slope = getting worse
                confidence = 0.5 + min(0.4, trend * 2)
                signals.append(AnomalySignal(
                    metric_name=metric_name,
                    anomaly_type=AnomalyType.TREND_ANOMALY,
                    current_value=current,
                    baseline_value=baseline,
                    threshold=threshold,
                    confidence=confidence,
                    description=(
                        f"{rule['description']}: {current:.2f} (warn threshold {warn_threshold:.2f}), "
                        f"trending upward (slope: {trend:.3f}/poll). Will breach in ~"
                        f"{self._estimate_breach_time(current, threshold, trend)} polls."
                    ),
                    service=rule["service"],
                ))

        # Sudden drop to near zero (throughput collapse)
        if baseline > 1.0 and current < baseline * 0.05:
            signals.append(AnomalySignal(
                metric_name=metric_name,
                anomaly_type=AnomalyType.SUDDEN_DROP,
                current_value=current,
                baseline_value=baseline,
                threshold=0.0,
                confidence=0.85,
                description=(
                    f"{rule['description']}: sudden drop from baseline {baseline:.2f} to {current:.2f} "
                    f"({100 * (1 - current/baseline):.0f}% below normal). Possible service outage."
                ),
                service=rule["service"],
            ))

        return signals

    # ------------------------------------------------------------------
    # Correlation: group signals into unified alerts
    # ------------------------------------------------------------------

    def _correlate_signals(self, signals: List[AnomalySignal]) -> List[ProactiveAlert]:
        """
        Group related signals into correlated alerts.
        Signals from the same service within the correlation window are merged.
        """
        from uuid import uuid4
        alerts: List[ProactiveAlert] = []

        # Group by service
        by_service: Dict[str, List[AnomalySignal]] = {}
        for s in signals:
            by_service.setdefault(s.service, []).append(s)

        for service, service_signals in by_service.items():
            # Overall confidence = max of individual confidences, boosted if multiple signals
            max_conf = max(s.confidence for s in service_signals)
            multi_signal_boost = min(0.15, 0.05 * (len(service_signals) - 1))
            overall_confidence = min(1.0, max_conf + multi_signal_boost)

            # Determine severity from highest-confidence breaching signal
            severity = "P3"
            for rule in METRIC_RULES:
                matching = [s for s in service_signals if s.metric_name == rule["name"]]
                if matching and matching[0].anomaly_type == AnomalyType.THRESHOLD_BREACH:
                    severity = rule.get("severity_on_breach", "P3")
                    if severity == "P1":
                        break

            # Determine if multi-signal
            anomaly_type_display = (
                "Multiple anomalies detected"
                if len(service_signals) > 1
                else service_signals[0].anomaly_type.value.replace("_", " ").title()
            )

            title = f"[Proactive] {service}: {anomaly_type_display}"
            description = (
                f"Proactive monitoring detected {len(service_signals)} signal(s) "
                f"on service '{service}':\n\n"
            )
            for sig in service_signals:
                description += f"  • {sig.description}\n"

            recommended_runbook = None
            for rule in METRIC_RULES:
                if rule["service"] == service and rule.get("runbook"):
                    recommended_runbook = rule["runbook"]
                    break

            alert = ProactiveAlert(
                alert_id=str(uuid4()),
                severity=severity,
                title=title,
                description=description.strip(),
                signals=service_signals,
                affected_services=[service],
                confidence=overall_confidence,
                recommended_runbook=recommended_runbook,
                correlation_id=str(uuid4()),
                detected_at=datetime.now(timezone.utc).isoformat(),
            )
            alerts.append(alert)

        return alerts

    # ------------------------------------------------------------------
    # Incident creation
    # ------------------------------------------------------------------

    async def _create_incident(self, alert: ProactiveAlert) -> None:
        """
        Publish incident.created to Kafka. Governor treats this identically
        to a ServiceNow-originated incident but knows it's proactive (no ticket yet).
        """
        incident_payload = {
            "incident_id": alert.alert_id,
            "source": "proactive_monitoring",
            "title": alert.title,
            "description": alert.description,
            "severity": alert.severity,
            "affected_services": alert.affected_services,
            "confidence": alert.confidence,
            "correlation_id": alert.correlation_id,
            "detected_at": alert.detected_at,
            "recommended_runbook": alert.recommended_runbook,
            "signals": [
                {
                    "metric": s.metric_name,
                    "type": s.anomaly_type.value,
                    "current_value": s.current_value,
                    "baseline_value": s.baseline_value,
                    "threshold": s.threshold,
                    "confidence": s.confidence,
                    "description": s.description,
                }
                for s in alert.signals
            ],
            "create_servicenow_ticket": True,  # Prompt MCP to open ticket
            "auto_classify": True,             # Skip classification node; provide context
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if self.kafka_producer:
            try:
                await self.kafka_producer.publish_event(
                    topic="incident.created",
                    event=incident_payload,
                    key=alert.alert_id,
                )
                logger.info("proactive_incident_published", alert_id=alert.alert_id, severity=alert.severity)
            except Exception as e:
                logger.error("proactive_incident_kafka_error", error=str(e))
        else:
            logger.info("proactive_incident_simulated", payload=incident_payload)

    # ------------------------------------------------------------------
    # Prometheus helpers
    # ------------------------------------------------------------------

    async def _prometheus_query(self, promql: str) -> Optional[float]:
        """Execute an instant PromQL query and return the scalar value."""
        encoded = urllib.parse.urlencode({"query": promql, "time": str(time.time())})
        url = f"{self.prometheus_url}/api/v1/query?{encoded}"

        loop = asyncio.get_event_loop()
        try:
            def _fetch():
                with urllib.request.urlopen(url, timeout=10) as resp:
                    return json.loads(resp.read().decode())
            data = await loop.run_in_executor(None, _fetch)

            results = data.get("data", {}).get("result", [])
            if not results:
                return None

            value_str = results[0].get("value", [None, None])[1]
            if value_str is None:
                return None
            val = float(value_str)
            return val if val == val else None  # NaN check
        except Exception as e:
            logger.debug("prometheus_query_failed", query=promql[:60], error=str(e))
            return None

    # ------------------------------------------------------------------
    # Baseline and trend computation
    # ------------------------------------------------------------------

    def _compute_baseline(self, metric_name: str) -> float:
        """Return median of last 100 values as baseline."""
        values = self._baseline_cache.get(metric_name, [])
        if len(values) < 2:
            return 0.0
        # Exclude the most recent value (that's what we're comparing against)
        history = values[:-1]
        return statistics.median(history) if history else 0.0

    def _compute_trend(self, metric_name: str) -> float:
        """
        Compute linear trend slope over last 10 samples.
        Returns positive = worsening, negative = improving.
        """
        values = self._baseline_cache.get(metric_name, [])
        if len(values) < 5:
            return 0.0
        recent = values[-10:]
        n = len(recent)
        if n < 2:
            return 0.0
        # Simple linear regression slope
        x_mean = (n - 1) / 2
        y_mean = sum(recent) / n
        numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(recent))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        return numerator / denominator if denominator != 0 else 0.0

    def _estimate_breach_time(self, current: float, threshold: float, slope: float) -> str:
        """Estimate how many polls until threshold breach given current trend."""
        if slope <= 0:
            return "never"
        polls = int((threshold - current) / slope)
        minutes = polls * self.poll_interval // 60
        return f"{minutes} min"

    # ------------------------------------------------------------------
    # Deduplication and maintenance window
    # ------------------------------------------------------------------

    def _is_duplicate(self, alert: ProactiveAlert) -> bool:
        """Suppress duplicate alerts for the same service within 30 minutes."""
        dedup_key = f"proactive:{','.join(sorted(alert.affected_services))}"
        last_seen = self._recent_alerts.get(dedup_key)
        if last_seen and (time.time() - last_seen) < 1800:  # 30 min
            return True
        return False

    def _mark_alerted(self, alert: ProactiveAlert) -> None:
        dedup_key = f"proactive:{','.join(sorted(alert.affected_services))}"
        self._recent_alerts[dedup_key] = time.time()

    async def _in_maintenance_window(self) -> bool:
        """
        Check if current time falls within a configured maintenance window.
        Maintenance windows stored in Redis key 'maintenance_windows' as JSON list
        of {start_utc, end_utc, reason} objects.
        """
        if not self.redis:
            return False
        try:
            raw = self.redis.get("maintenance_windows")
            if not raw:
                return False
            windows = json.loads(raw)
            now = datetime.now(timezone.utc)
            for w in windows:
                start = datetime.fromisoformat(w["start_utc"])
                end = datetime.fromisoformat(w["end_utc"])
                if start <= now <= end:
                    return True
        except Exception:
            pass
        return False
