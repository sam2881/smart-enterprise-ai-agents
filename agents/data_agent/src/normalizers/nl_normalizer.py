"""
Natural Language Normalizer - Converts NL input to PipelineMetadata via LLM.

IMPORTANT: NL is NEVER executed directly.
It is ALWAYS converted to structured PipelineMetadata first.
"""

import json
import structlog
from typing import Any, Dict, List, Optional

from ..models.canonical import PipelineMetadata, UnifiedPipelineInput, InputType
from ..models.pipeline import PipelineConfig, Environment
from ..models.source import SourceConfig, SourceType, SourceFormat, FileSourceConfig
from ..models.schema import SchemaConfig, ColumnDefinition, DataType
from ..models.target import TargetConfig, TargetZone, WriteMode
from ..models.transformation import TransformConfig, TransformType
from ..models.quality import QualityRule, QualityRuleType, Severity
from ..models.execution import ExecutionPolicy

from .base import BaseNormalizer, NormalizerResult

logger = structlog.get_logger(__name__)


# System prompt for NL to structured conversion
NL_TO_METADATA_PROMPT = """You are a data engineering assistant that converts natural language
pipeline descriptions into structured JSON metadata.

IMPORTANT RULES:
1. Output ONLY valid JSON matching the schema below
2. Infer reasonable defaults when information is missing
3. Generate valid snake_case identifiers for dag_id, product_code
4. Map described sources to appropriate source_type enum values
5. Generate column definitions based on described data
6. Create appropriate quality rules based on data types

OUTPUT SCHEMA:
{
    "pipeline": {
        "dag_id": "string (snake_case, unique identifier)",
        "pipeline_name": "string (human readable name)",
        "domain": "string (sales, finance, hr, marketing, etc.)",
        "product_code": "string (snake_case)",
        "description": "string",
        "owner_team": "string (default: data-engineering)",
        "owner_email": "string (email format)"
    },
    "source": {
        "source_type": "file_csv|file_json|database_postgres|streaming_kafka|api_rest",
        "source_format": "csv|json|parquet|avro",
        "source_bucket": "string (for file sources)",
        "source_prefix": "string (path within bucket)",
        "file_pattern": "string (glob pattern)"
    },
    "schema": {
        "columns": [
            {
                "name": "string (snake_case)",
                "type": "string|integer|float|timestamp|boolean|date",
                "nullable": true/false,
                "pk": true/false,
                "description": "string"
            }
        ],
        "primary_keys": ["column_names"]
    },
    "target": {
        "target_zone": "bronze|silver|gold",
        "bq_dataset": "string",
        "bq_table": "string",
        "write_mode": "append|overwrite|merge"
    },
    "transformations": [
        {
            "transform_type": "rename|cast|derive|filter|aggregate|join|window|scd",
            "transform_order": 0,
            "zone": "bronze|silver|gold",
            "config": {}
        }
    ],
    "joins": [
        {
            "right_table": "table_name_to_join",
            "join_type": "inner|left|right|full",
            "join_keys": [{"left": "col_from_source", "right": "col_from_right_table"}],
            "join_order": 1
        }
    ],
    "quality_rules": [
        {
            "rule_name": "string",
            "rule_type": "not_null|unique|range|regex",
            "column_name": "string",
            "severity": "warning|error|critical"
        }
    ],
    "execution_policy": {
        "schedule_interval": "@daily|@hourly|cron expression",
        "requires_human_approval": false
    }
}

USER INPUT:
{user_input}

OUTPUT (JSON only, no explanation):"""


class NLInputNormalizer(BaseNormalizer):
    """
    Normalizes natural language input to PipelineMetadata via LLM.

    The NL description is converted to structured JSON first,
    then parsed into PipelineMetadata.
    """

    def __init__(self, llm_client: Optional[Any] = None):
        """
        Initialize NL normalizer.

        Args:
            llm_client: LLM client for NL processing (OpenAI, Anthropic, etc.)
        """
        self.llm_client = llm_client

    def can_handle(self, input_data: UnifiedPipelineInput) -> bool:
        """Check if input is natural language."""
        return (
            input_data.input_type == InputType.NATURAL_LANGUAGE and
            input_data.natural_language is not None
        )

    async def normalize(self, input_data: UnifiedPipelineInput) -> NormalizerResult:
        """
        Normalize NL input to PipelineMetadata.

        Process:
        1. Send NL to LLM with structured prompt
        2. Parse LLM JSON response
        3. Validate parsed structure
        4. Convert to PipelineMetadata

        Args:
            input_data: NL input

        Returns:
            NormalizerResult with PipelineMetadata
        """
        processing_log = []
        errors = []
        warnings = []

        try:
            nl_input = input_data.natural_language
            if not nl_input:
                return NormalizerResult(
                    success=False,
                    errors=["No natural language input provided"],
                )

            # 1. Convert NL to structured JSON via LLM
            processing_log.append({"step": "nl_to_json", "status": "started"})
            structured_json, confidence, tokens = await self._convert_nl_to_json(nl_input)
            processing_log.append({
                "step": "nl_to_json",
                "status": "completed",
                "confidence": confidence,
                "tokens_used": tokens,
            })

            if not structured_json:
                return NormalizerResult(
                    success=False,
                    errors=["Failed to convert natural language to structured format"],
                    processing_log=processing_log,
                )

            # 2. Create unified input from parsed JSON
            processing_log.append({"step": "create_unified_input", "status": "started"})
            unified_input = UnifiedPipelineInput(
                input_type=InputType.UI_STRUCTURED,
                created_by=input_data.created_by,
                jira_ticket=input_data.jira_ticket,
                pipeline=structured_json.get("pipeline"),
                source=structured_json.get("source"),
                schema=structured_json.get("schema"),
                target=structured_json.get("target"),
                transformations=structured_json.get("transformations"),
                joins=structured_json.get("joins"),
                quality_rules=structured_json.get("quality_rules"),
                execution_policy=structured_json.get("execution_policy"),
            )
            processing_log.append({"step": "create_unified_input", "status": "completed"})

            # 3. Use UI normalizer to convert to PipelineMetadata
            processing_log.append({"step": "normalize_structured", "status": "started"})
            from .ui_normalizer import UIInputNormalizer
            ui_normalizer = UIInputNormalizer()
            result = await ui_normalizer.normalize(unified_input)
            processing_log.append({"step": "normalize_structured", "status": "completed"})

            # 4. Add NL-specific metadata
            if result.success and result.metadata:
                result.metadata.nl_input = nl_input
                result.metadata.nl_processing_log = processing_log

            # Add confidence and token info
            result.confidence_score = confidence
            result.llm_tokens_used = tokens
            result.processing_log = processing_log

            logger.info(
                "nl_normalization_complete",
                success=result.success,
                confidence=confidence,
                tokens=tokens,
            )

            return result

        except Exception as e:
            logger.error("nl_normalization_failed", error=str(e))
            errors.append(f"NL normalization failed: {str(e)}")
            return NormalizerResult(
                success=False,
                errors=errors,
                processing_log=processing_log,
            )

    async def _convert_nl_to_json(
        self, nl_input: str
    ) -> tuple[Optional[Dict[str, Any]], float, int]:
        """
        Convert natural language to structured JSON via LLM.

        Args:
            nl_input: Natural language description

        Returns:
            Tuple of (parsed_json, confidence_score, tokens_used)
        """
        prompt = NL_TO_METADATA_PROMPT.format(user_input=nl_input)

        if self.llm_client is None:
            # Fallback: Use simple heuristic parsing for demo
            return self._fallback_nl_parse(nl_input)

        try:
            # Call LLM
            response = await self.llm_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a data engineering assistant."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                response_format={"type": "json_object"},
            )

            content = response.choices[0].message.content
            tokens = response.usage.total_tokens

            # Parse JSON
            structured = json.loads(content)

            # Calculate confidence based on completeness
            confidence = self._calculate_confidence(structured)

            return structured, confidence, tokens

        except json.JSONDecodeError as e:
            logger.error("llm_json_parse_failed", error=str(e))
            return None, 0.0, 0
        except Exception as e:
            logger.error("llm_call_failed", error=str(e))
            return None, 0.0, 0

    def _fallback_nl_parse(
        self, nl_input: str
    ) -> tuple[Optional[Dict[str, Any]], float, int]:
        """
        Fallback NL parsing using heuristics when LLM is unavailable.

        This is a simple keyword-based parser for demo purposes.
        """
        nl_lower = nl_input.lower()

        # Extract domain
        domain = "default"
        for d in ["sales", "finance", "hr", "marketing", "operations", "customer"]:
            if d in nl_lower:
                domain = d
                break

        # Extract source type
        source_type = "file_csv"
        if "kafka" in nl_lower:
            source_type = "streaming_kafka"
        elif "postgres" in nl_lower or "database" in nl_lower:
            source_type = "database_postgres"
        elif "api" in nl_lower or "rest" in nl_lower:
            source_type = "api_rest"
        elif "json" in nl_lower:
            source_type = "file_json"
        elif "parquet" in nl_lower:
            source_type = "file_parquet"

        # Extract write mode
        write_mode = "append"
        if "overwrite" in nl_lower or "replace" in nl_lower:
            write_mode = "overwrite"
        elif "merge" in nl_lower or "upsert" in nl_lower:
            write_mode = "merge"

        # Generate dag_id from first meaningful words
        words = [w for w in nl_lower.split() if len(w) > 3 and w.isalpha()][:3]
        dag_id = "_".join(words) if words else "generated_pipeline"

        # Build basic structure
        structured = {
            "pipeline": {
                "dag_id": dag_id,
                "pipeline_name": dag_id.replace("_", " ").title(),
                "domain": domain,
                "product_code": dag_id.split("_")[0],
                "description": nl_input[:500],
                "owner_team": "data-engineering",
                "owner_email": "data-eng@company.com",
            },
            "source": {
                "source_type": source_type,
                "source_format": source_type.split("_")[-1] if source_type.startswith("file_") else "json",
                "source_bucket": f"{domain}-raw-data",
                "source_prefix": f"{dag_id}/",
                "file_pattern": "*",
            },
            "schema": {
                "columns": [
                    {"name": "id", "type": "string", "nullable": False, "pk": True},
                    {"name": "created_at", "type": "timestamp", "nullable": False},
                    {"name": "data", "type": "string", "nullable": True},
                ],
                "primary_keys": ["id"],
            },
            "target": {
                "target_zone": "gold",
                "bq_dataset": domain,
                "bq_table": dag_id,
                "write_mode": write_mode,
            },
            "transformations": [],
            "quality_rules": [
                {
                    "rule_name": "id_not_null",
                    "rule_type": "not_null",
                    "column_name": "id",
                    "severity": "error",
                },
            ],
            "execution_policy": {
                "schedule_interval": "@daily",
                "requires_human_approval": False,
            },
        }

        # Low confidence for fallback parsing
        return structured, 0.5, 0

    def _calculate_confidence(self, structured: Dict[str, Any]) -> float:
        """Calculate confidence score based on completeness."""
        score = 0.0
        max_score = 10.0

        # Check required sections
        if structured.get("pipeline", {}).get("dag_id"):
            score += 2.0
        if structured.get("source", {}).get("source_type"):
            score += 2.0
        if structured.get("schema", {}).get("columns"):
            score += 2.0
        if structured.get("target", {}).get("bq_table"):
            score += 2.0
        if structured.get("execution_policy"):
            score += 1.0
        if structured.get("quality_rules"):
            score += 1.0

        return score / max_score
