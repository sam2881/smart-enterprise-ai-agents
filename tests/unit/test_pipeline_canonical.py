"""Tests for UnifiedPipelineInput canonical model validation.

Tests Pydantic validation of the canonical pipeline input model from
agents/data_agent/src/models/canonical.py

Key behaviours under test:
- Required field: created_by
- model_validator enforces at least one of: pipeline/source, natural_language, dtsx_path
- model_validator auto-corrects input_type to NATURAL_LANGUAGE / DTSX_MIGRATION
- extra="forbid" rejects unknown fields
- jira_ticket is optional
- Enum values accepted as both string literals and InputType members

These models mirror frontend TypeScript types in
frontend/src/types/pipeline-canonical.ts
"""
import sys
import os
import pytest

# Add the data_agent package root so absolute imports resolve
_DATA_AGENT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "agents", "data_agent")
)
if _DATA_AGENT_ROOT not in sys.path:
    sys.path.insert(0, _DATA_AGENT_ROOT)

try:
    from src.models.canonical import UnifiedPipelineInput, InputType
    MODELS_AVAILABLE = True
    _IMPORT_ERROR_MSG = ""
except (ImportError, ModuleNotFoundError) as _e:
    MODELS_AVAILABLE = False
    _IMPORT_ERROR_MSG = str(_e)

pytestmark = pytest.mark.skipif(
    not MODELS_AVAILABLE,
    reason=f"data_agent models not importable: {_IMPORT_ERROR_MSG}",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_nl_input(**overrides):
    """Minimal natural-language UnifiedPipelineInput kwargs."""
    base = {
        "created_by": "test@example.com",
        "natural_language": "Load daily CSV from GCS and clean nulls",
    }
    base.update(overrides)
    return base


def _make_structured_input(**overrides):
    """Minimal structured (UI) UnifiedPipelineInput kwargs."""
    base = {
        "created_by": "test@example.com",
        "source": {"source_type": "file_csv"},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Required fields
# ---------------------------------------------------------------------------

class TestRequiredFields:
    def test_missing_created_by_raises(self):
        with pytest.raises(Exception):  # pydantic.ValidationError
            UnifiedPipelineInput(natural_language="Load data")

    def test_missing_all_content_raises(self):
        """No pipeline, source, natural_language, or dtsx_path → validation error."""
        with pytest.raises(Exception):
            UnifiedPipelineInput(created_by="user@test.com")

    def test_created_by_stored_correctly(self):
        m = UnifiedPipelineInput(**_make_nl_input(created_by="samrat@example.com"))
        assert m.created_by == "samrat@example.com"


# ---------------------------------------------------------------------------
# Input type auto-detection via model_validator
# ---------------------------------------------------------------------------

class TestInputTypeAutoDetection:
    def test_natural_language_sets_nl_type(self):
        """model_validator should override input_type to NATURAL_LANGUAGE."""
        m = UnifiedPipelineInput(**_make_nl_input())
        assert m.input_type == InputType.NATURAL_LANGUAGE

    def test_dtsx_path_sets_dtsx_type(self):
        """model_validator should override input_type to DTSX_MIGRATION."""
        m = UnifiedPipelineInput(
            created_by="user@test.com",
            dtsx_path="gs://bucket/package.dtsx",
        )
        assert m.input_type == InputType.DTSX_MIGRATION

    def test_structured_source_keeps_ui_structured_type(self):
        """Providing only source keeps the default UI_STRUCTURED type."""
        m = UnifiedPipelineInput(**_make_structured_input())
        assert m.input_type == InputType.UI_STRUCTURED

    def test_nl_overrides_explicit_ui_structured(self):
        """Even if caller passes input_type=ui_structured, NL content overrides it."""
        m = UnifiedPipelineInput(
            input_type="ui_structured",
            created_by="user@test.com",
            natural_language="Build a Kafka pipeline",
        )
        assert m.input_type == InputType.NATURAL_LANGUAGE

    def test_pipeline_field_counts_as_structured(self):
        """pipeline dict alone satisfies has_structured check."""
        m = UnifiedPipelineInput(
            created_by="user@test.com",
            pipeline={"dag_id": "my_dag", "environment": "dev"},
        )
        assert m.input_type == InputType.UI_STRUCTURED


# ---------------------------------------------------------------------------
# Optional fields
# ---------------------------------------------------------------------------

class TestOptionalFields:
    def test_jira_ticket_defaults_to_none(self):
        m = UnifiedPipelineInput(**_make_nl_input())
        assert m.jira_ticket is None

    def test_jira_ticket_stored_when_provided(self):
        m = UnifiedPipelineInput(**_make_nl_input(jira_ticket="DATA-9999"))
        assert m.jira_ticket == "DATA-9999"

    def test_request_id_auto_generated(self):
        m = UnifiedPipelineInput(**_make_nl_input())
        assert m.request_id is not None
        assert len(m.request_id) > 0

    def test_two_instances_get_different_request_ids(self):
        m1 = UnifiedPipelineInput(**_make_nl_input())
        m2 = UnifiedPipelineInput(**_make_nl_input())
        assert m1.request_id != m2.request_id

    def test_dtsx_package_name_optional(self):
        m = UnifiedPipelineInput(
            created_by="user@test.com",
            dtsx_path="gs://bucket/pkg.dtsx",
            dtsx_package_name="MyPackage",
        )
        assert m.dtsx_package_name == "MyPackage"

    def test_transformations_defaults_to_none(self):
        m = UnifiedPipelineInput(**_make_nl_input())
        assert m.transformations is None

    def test_quality_rules_defaults_to_none(self):
        m = UnifiedPipelineInput(**_make_nl_input())
        assert m.quality_rules is None


# ---------------------------------------------------------------------------
# Full structured input (mirrors conftest sample_pipeline_config)
# ---------------------------------------------------------------------------

class TestStructuredInput:
    def test_full_structured_config_accepted(self, sample_pipeline_config):
        """Full sample from conftest fixture should instantiate without error."""
        m = UnifiedPipelineInput(**sample_pipeline_config)
        assert m.input_type == InputType.UI_STRUCTURED
        assert m.created_by == "test@example.com"
        assert m.jira_ticket == "DATA-1234"

    def test_source_dict_preserved(self, sample_pipeline_config):
        m = UnifiedPipelineInput(**sample_pipeline_config)
        assert m.source["source_type"] == "file_csv"

    def test_target_dict_preserved(self, sample_pipeline_config):
        m = UnifiedPipelineInput(**sample_pipeline_config)
        assert m.target["target_zone"] == "gold"

    def test_pipeline_dict_preserved(self, sample_pipeline_config):
        m = UnifiedPipelineInput(**sample_pipeline_config)
        assert m.pipeline["dag_id"] == "test_csv_pipeline"


# ---------------------------------------------------------------------------
# extra="forbid" — unknown fields rejected
# ---------------------------------------------------------------------------

class TestExtraFieldsForbidden:
    def test_unknown_field_raises(self):
        with pytest.raises(Exception):
            UnifiedPipelineInput(
                created_by="user@test.com",
                natural_language="load data",
                nonexistent_field="should fail",
            )


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_model_dump_returns_dict(self, sample_pipeline_config):
        m = UnifiedPipelineInput(**sample_pipeline_config)
        data = m.model_dump()
        assert isinstance(data, dict)

    def test_model_dump_input_type_is_string(self, sample_pipeline_config):
        """Enum values should serialise to their string values."""
        m = UnifiedPipelineInput(**sample_pipeline_config)
        data = m.model_dump()
        assert data["input_type"] == "ui_structured"

    def test_model_dump_preserves_created_by(self, sample_pipeline_config):
        m = UnifiedPipelineInput(**sample_pipeline_config)
        data = m.model_dump()
        assert data["created_by"] == "test@example.com"

    def test_model_dump_json_mode(self, sample_pipeline_config):
        """model_dump(mode='json') should also work for JSON-safe output."""
        m = UnifiedPipelineInput(**sample_pipeline_config)
        data = m.model_dump(mode="json")
        assert data["input_type"] == "ui_structured"
