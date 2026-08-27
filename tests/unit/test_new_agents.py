"""
Unit tests for agents added in the 2026-06-22 session.
External I/O (Kafka, Redis, Airflow, Prometheus) is fully mocked.

Import pattern (same as existing tests):
  - Temporarily insert agent root onto sys.path so `from src.x import ...` resolves.
  - Clear sys.modules['src*'] before each switch to avoid cross-agent contamination.
"""
import sys
import os
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_DATA_AGENT = os.path.join(_REPO, "agents", "data_agent")
_SNOW_AGENT = os.path.join(_REPO, "agents", "servicenow_agent")


def _activate(agent_root: str) -> None:
    """Put agent_root first on sys.path, evict any cached 'src.*' modules."""
    for key in list(sys.modules):
        if key == "src" or key.startswith("src."):
            del sys.modules[key]
    if agent_root in sys.path:
        sys.path.remove(agent_root)
    sys.path.insert(0, agent_root)


# ──────────────────────────────────────────────────────────────────────────────
# ConnectionTestAgent
# ──────────────────────────────────────────────────────────────────────────────

class TestConnectionTestAgentSkipLogic:
    def setup_method(self):
        _activate(_DATA_AGENT)

    def _skip_types(self):
        from src.agents.connection_test_agent import ConnectionTestAgent
        return ConnectionTestAgent.SKIP_SOURCE_TYPES

    def test_legacy_cobol_skipped(self):     assert "legacy_cobol" in self._skip_types()
    def test_file_ebcdic_skipped(self):      assert "file_ebcdic" in self._skip_types()
    def test_file_fixed_width_skipped(self): assert "file_fixed_width" in self._skip_types()
    def test_database_postgres_not_skipped(self): assert "database_postgres" not in self._skip_types()
    def test_file_csv_not_skipped(self):     assert "file_csv" not in self._skip_types()
    def test_streaming_kafka_not_skipped(self): assert "streaming_kafka" not in self._skip_types()


class TestConnectionTestAgentPartitioning:
    def setup_method(self):
        _activate(_DATA_AGENT)

    def _rec(self, source_type, row_count, keys):
        from src.agents.connection_test_agent import ConnectionTestAgent, SchemaDiscoveryResult
        disc = SchemaDiscoveryResult(
            columns={"id": "INTEGER", "created_at": "TIMESTAMP"},
            row_count_estimate=row_count, sample_rows=0, source_timezone=None,
            partitioning_keys=keys, nullable_columns=[],
        )
        return ConnectionTestAgent()._recommend_partitioning(source_type, disc, {"partition_by": "ingestion_date"})

    def test_small_table_no_partition(self):
        r = self._rec("file_csv", 50_000, ["created_at"])
        assert r.recommended_strategy == "no_partitioning"

    def test_medium_table_date_partition(self):
        r = self._rec("database_postgres", 5_000_000, ["created_at"])
        assert r.recommended_strategy == "ingestion_date"

    def test_large_table_composite_partition(self):
        r = self._rec("database_postgres", 500_000_000, ["id", "created_at"])
        assert "ingestion_date" in r.recommended_strategy

    def test_very_large_bucketed(self):
        r = self._rec("database_postgres", 2_000_000_000, ["id"])
        assert r.recommended_strategy == "ingestion_date + bucket_by_id"


class TestConnectionTestAgentSchemaDiff:
    def setup_method(self):
        _activate(_DATA_AGENT)

    def _diff(self, requested, live):
        from src.agents.connection_test_agent import ConnectionTestAgent
        return ConnectionTestAgent()._diff_schemas(requested, live)

    def test_matching_schemas_no_diff(self):
        d = self._diff({"id": "INTEGER", "name": "VARCHAR"}, {"id": "INTEGER", "name": "VARCHAR"})
        assert d.columns_in_config_not_in_source == []
        assert d.columns_in_source_not_in_config == []
        assert d.type_mismatches == {}

    def test_missing_column_detected(self):
        d = self._diff({"id": "INTEGER", "email": "VARCHAR"}, {"id": "INTEGER"})
        assert "email" in d.columns_in_config_not_in_source

    def test_extra_column_detected(self):
        d = self._diff({"id": "INTEGER"}, {"id": "INTEGER", "extra": "VARCHAR"})
        assert "extra" in d.columns_in_source_not_in_config

    def test_type_mismatch_detected(self):
        d = self._diff({"id": "INTEGER"}, {"id": "VARCHAR"})
        assert "id" in d.type_mismatches

    def test_missing_column_means_nonempty_diff(self):
        d = self._diff({"id": "INTEGER", "critical": "VARCHAR"}, {"id": "INTEGER"})
        assert "critical" in d.columns_in_config_not_in_source


# ──────────────────────────────────────────────────────────────────────────────
# PipelineMonitoringAgent
# ──────────────────────────────────────────────────────────────────────────────

class TestPipelineMonitoringFailureClassification:
    def setup_method(self):
        _activate(_DATA_AGENT)

    def _classify(self, log_text: str):
        from src.agents.pipeline_monitoring_agent import PipelineMonitoringAgent
        return PipelineMonitoringAgent()._classify_failure(log_text)

    def test_oom_java_classified(self):
        # Use an exact OOM signature from FAILURE_PATTERNS
        result = self._classify("java.lang.OutOfMemoryError: Java heap space")
        assert result.value == "out_of_memory"

    def test_oom_killed_classified(self):
        result = self._classify("OOMKilled: container exceeded memory limit")
        assert result.value == "out_of_memory"

    def test_schema_mismatch_classified(self):
        result = self._classify("AnalysisException: cannot resolve column 'user_id'")
        assert result.value == "schema_mismatch"

    def test_permission_denied_classified(self):
        # Use exact signature: "Access Denied" (capital A D)
        result = self._classify("Access Denied for user on bucket gs://prod-data")
        assert result.value == "permission_denied"

    def test_timeout_soft_limit_classified(self):
        # "soft_time_limit" is in TIMEOUT signatures and NOT in SOURCE_UNAVAILABLE
        result = self._classify("SoftTimeLimitExceeded: soft_time_limit reached")
        assert result.value == "timeout"

    def test_unknown_for_unrecognised_log(self):
        result = self._classify("Unexpected segmentation fault in worker process")
        assert result.value == "unknown"


class TestPipelineMonitoringAutoRemediation:
    def setup_method(self):
        _activate(_DATA_AGENT)

    def _patterns(self):
        from src.agents.pipeline_monitoring_agent import PipelineMonitoringAgent
        return PipelineMonitoringAgent.FAILURE_PATTERNS

    def test_oom_auto_remediable(self):
        p = next(x for x in self._patterns() if x["pattern"].value == "out_of_memory")
        assert p["auto_remediable"] is True

    def test_schema_mismatch_not_remediable(self):
        p = next(x for x in self._patterns() if x["pattern"].value == "schema_mismatch")
        assert p["auto_remediable"] is False

    def test_permission_denied_not_remediable(self):
        p = next(x for x in self._patterns() if x["pattern"].value == "permission_denied")
        assert p["auto_remediable"] is False

    def test_timeout_auto_remediable(self):
        p = next(x for x in self._patterns() if x["pattern"].value == "timeout")
        assert p["auto_remediable"] is True

    def test_data_quality_not_remediable(self):
        p = next(x for x in self._patterns() if x["pattern"].value == "data_quality_fail")
        assert p["auto_remediable"] is False


# ──────────────────────────────────────────────────────────────────────────────
# ProactiveMonitoringAgent
# ──────────────────────────────────────────────────────────────────────────────

class TestProactiveMonitoringRules:
    def setup_method(self):
        _activate(_SNOW_AGENT)

    def test_all_rules_have_required_fields(self):
        from src.agents.proactive_monitoring_agent import METRIC_RULES
        required = {"name", "query", "threshold", "service", "severity_on_breach"}
        for rule in METRIC_RULES:
            assert required <= set(rule.keys()), f"{rule.get('name')} missing fields"

    def test_at_least_eight_rules(self):
        from src.agents.proactive_monitoring_agent import METRIC_RULES
        assert len(METRIC_RULES) >= 8

    def test_severity_values_valid(self):
        from src.agents.proactive_monitoring_agent import METRIC_RULES
        for rule in METRIC_RULES:
            assert rule["severity_on_breach"] in {"P1", "P2", "P3", "P4"}


class TestProactiveMonitoringDedup:
    def setup_method(self):
        _activate(_SNOW_AGENT)

    def _make_alert(self, service="api-gw"):
        from src.agents.proactive_monitoring_agent import ProactiveAlert
        return ProactiveAlert(
            alert_id="test-1", severity="P2",
            title="High error rate", description="Error rate is high",
            signals=[], affected_services=[service],
            confidence=0.9, recommended_runbook="RUNBOOK-001",
            correlation_id="corr-1", detected_at=datetime.now(timezone.utc).isoformat(),
        )

    def test_not_duplicate_initially(self):
        from src.agents.proactive_monitoring_agent import ProactiveMonitoringAgent
        agent = ProactiveMonitoringAgent()
        assert agent._is_duplicate(self._make_alert()) is False

    def test_duplicate_after_mark_alerted(self):
        from src.agents.proactive_monitoring_agent import ProactiveMonitoringAgent
        agent = ProactiveMonitoringAgent()
        alert = self._make_alert()
        agent._mark_alerted(alert)
        assert agent._is_duplicate(alert) is True


# ──────────────────────────────────────────────────────────────────────────────
# DataPipelineIncidentBridge
# ──────────────────────────────────────────────────────────────────────────────

class TestDataPipelineIncidentBridge:
    def setup_method(self):
        _activate(_SNOW_AGENT)

    def _bridge(self):
        from src.streaming.consumers.data_pipeline_incident_bridge import DataPipelineIncidentBridge
        mock_redis = MagicMock()
        mock_redis.exists.return_value = 0
        return DataPipelineIncidentBridge(kafka_producer=AsyncMock(), redis_client=mock_redis)

    @pytest.mark.asyncio
    async def test_failed_creates_incident(self):
        b = self._bridge()
        await b._handle_pipeline_failed({
            "pipeline_id": "p", "dag_id": "sales_daily", "run_id": "r",
            "failed_tasks": [{"task_id": "t", "failure_pattern": "schema_mismatch", "log_snippet": "col"}],
            "team": "de",
        })
        b.producer.publish_event.assert_called_once()
        assert b.producer.publish_event.call_args.kwargs["topic"] == "incident.created"

    @pytest.mark.asyncio
    async def test_schema_mismatch_severity_p2(self):
        b = self._bridge()
        await b._handle_pipeline_failed({
            "pipeline_id": "p", "dag_id": "d", "run_id": "r",
            "failed_tasks": [{"task_id": "t", "failure_pattern": "schema_mismatch", "log_snippet": "x"}],
            "team": "de",
        })
        assert b.producer.publish_event.call_args.kwargs["event"]["severity"] == "P2"

    @pytest.mark.asyncio
    async def test_oom_severity_p3(self):
        b = self._bridge()
        await b._handle_pipeline_failed({
            "pipeline_id": "p", "dag_id": "d", "run_id": "r",
            "failed_tasks": [{"task_id": "t", "failure_pattern": "out_of_memory", "log_snippet": "heap"}],
            "team": "de",
        })
        assert b.producer.publish_event.call_args.kwargs["event"]["severity"] == "P3"

    @pytest.mark.asyncio
    async def test_dedup_suppresses_second_event(self):
        from src.streaming.consumers.data_pipeline_incident_bridge import DataPipelineIncidentBridge
        mock_redis = MagicMock()
        mock_redis.exists.return_value = 1  # already seen
        b = DataPipelineIncidentBridge(kafka_producer=AsyncMock(), redis_client=mock_redis)
        await b._handle_pipeline_failed({"pipeline_id": "p", "dag_id": "d", "run_id": "r", "failed_tasks": [], "team": "t"})
        b.producer.publish_event.assert_not_called()

    @pytest.mark.asyncio
    async def test_sla_missed_publishes_p3(self):
        b = self._bridge()
        await b._handle_pipeline_sla_missed({
            "dag_id": "d", "run_id": "r", "elapsed_minutes": 95.0,
            "sla_deadline_minutes": 60, "team": "de",
        })
        assert b.producer.publish_event.call_args.kwargs["event"]["severity"] == "P3"

    @pytest.mark.asyncio
    async def test_healthy_run3_closes_incident(self):
        b = self._bridge()
        b._open_pipeline_incidents["sales_daily"] = "INC-PIPE-ABC"
        await b._handle_pipeline_healthy({"dag_id": "sales_daily", "run_number": 3})
        assert b.producer.publish_event.call_args.kwargs["topic"] == "incident.auto_closed"
        assert "sales_daily" not in b._open_pipeline_incidents

    @pytest.mark.asyncio
    async def test_healthy_run2_no_close(self):
        b = self._bridge()
        b._open_pipeline_incidents["sales_daily"] = "INC-PIPE-ABC"
        await b._handle_pipeline_healthy({"dag_id": "sales_daily", "run_number": 2})
        b.producer.publish_event.assert_not_called()

    def test_incident_id_deterministic(self):
        b = self._bridge()
        assert b._make_incident_id("dag_a", "run_1") == b._make_incident_id("dag_a", "run_1")

    def test_incident_id_prefix(self):
        b = self._bridge()
        assert b._make_incident_id("x", "y").startswith("INC-PIPE-")

    def test_different_dags_different_ids(self):
        b = self._bridge()
        assert b._make_incident_id("dag_a", "r") != b._make_incident_id("dag_b", "r")

    @pytest.mark.asyncio
    async def test_config_update_republishes_reconfigure(self):
        b = self._bridge()
        await b._handle_pipeline_config_update({
            "pipeline_id": "p", "dag_id": "d",
            "change_type": "spark_memory", "new_config": {"mem": "4g"}, "reason": "OOM",
        })
        assert b.producer.publish_event.call_args.kwargs["topic"] == "pipeline.reconfigure"


class TestBridgeSeverityMappings:
    def setup_method(self):
        _activate(_SNOW_AGENT)

    def test_critical_patterns_p2(self):
        from src.streaming.consumers.data_pipeline_incident_bridge import FAILURE_PATTERN_SEVERITY
        for p in ["schema_mismatch", "permission_denied", "data_quality_fail"]:
            assert FAILURE_PATTERN_SEVERITY[p] == "P2"

    def test_recoverable_patterns_p3(self):
        from src.streaming.consumers.data_pipeline_incident_bridge import FAILURE_PATTERN_SEVERITY
        for p in ["out_of_memory", "timeout", "source_unavailable", "unknown"]:
            assert FAILURE_PATTERN_SEVERITY[p] == "P3"

    def test_every_pattern_has_rca_hint(self):
        from src.streaming.consumers.data_pipeline_incident_bridge import (
            FAILURE_PATTERN_SEVERITY, FAILURE_PATTERN_TO_RCA_HINT,
        )
        for p in FAILURE_PATTERN_SEVERITY:
            assert p in FAILURE_PATTERN_TO_RCA_HINT
