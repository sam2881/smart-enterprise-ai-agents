"""Shared pytest fixtures for the Enterprise Agentic Platform test suite."""
import pytest
from unittest.mock import MagicMock, patch
from typing import Dict, Any


@pytest.fixture
def mock_openai_client():
    """Mock OpenAI client returning deterministic responses."""
    client = MagicMock()
    completion = MagicMock()
    completion.choices = [MagicMock()]
    completion.choices[0].message.content = '{"result": "test", "confidence": 0.85}'
    completion.usage.prompt_tokens = 100
    completion.usage.completion_tokens = 50
    client.chat.completions.create = MagicMock(return_value=completion)
    return client


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer."""
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.flush = MagicMock(return_value=0)
    return producer


@pytest.fixture
def sample_incident_state() -> Dict[str, Any]:
    """Sample incident AgentState dict for testing LangGraph node patterns."""
    return {
        "incident_id": "INC0001234",
        "raw_payload": {
            "number": "INC0001234",
            "short_description": "Database CPU at 95% on prod-db-01",
            "severity": "2",
            "category": "infrastructure",
        },
        "system_name": "prod-db-01",
        "error_type": "HIGH_CPU",
        "severity": "HIGH",
        "incident_type": "INFRASTRUCTURE",
        "confidence_score": 0.85,
        "remediation_plan": None,
        "approval_status": None,
        "error_message": None,
        "error_agent": None,
    }


@pytest.fixture
def sample_pipeline_config() -> Dict[str, Any]:
    """Sample UnifiedPipelineInput dict for testing data agent."""
    return {
        "input_type": "ui_structured",
        "created_by": "test@example.com",
        "jira_ticket": "DATA-1234",
        "pipeline": {
            "dag_id": "test_csv_pipeline",
            "domain": "sales",
            "environment": "dev",
        },
        "source": {
            "source_type": "file_csv",
            "file_config": {
                "gcs_path": "gs://test-bucket/data.csv",
                "delimiter": ",",
                "header": True,
            },
        },
        "schema": {
            "columns": [
                {"name": "customer_id", "type": "string", "nullable": False},
                {"name": "amount", "type": "decimal", "nullable": False},
            ]
        },
        "target": {
            "target_zone": "gold",
            "bq_dataset": "sales_data",
            "bq_table": "daily_sales",
            "write_mode": "append",
        },
    }
