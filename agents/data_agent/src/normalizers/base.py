"""
Base normalizer interface and result model.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ..models.canonical import PipelineMetadata, UnifiedPipelineInput


class NormalizerResult(BaseModel):
    """Result of normalization process."""

    success: bool
    metadata: Optional[PipelineMetadata] = None
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    processing_log: List[Dict[str, Any]] = Field(default_factory=list)

    # For NL processing
    confidence_score: Optional[float] = None
    llm_tokens_used: Optional[int] = None


class BaseNormalizer(ABC):
    """
    Abstract base class for input normalizers.

    All normalizers convert input to canonical PipelineMetadata.
    """

    @abstractmethod
    async def normalize(self, input_data: UnifiedPipelineInput) -> NormalizerResult:
        """
        Normalize input to PipelineMetadata.

        Args:
            input_data: Unified input from UI, NL, or DTSX

        Returns:
            NormalizerResult with PipelineMetadata or errors
        """
        pass

    @abstractmethod
    def can_handle(self, input_data: UnifiedPipelineInput) -> bool:
        """
        Check if this normalizer can handle the given input.

        Args:
            input_data: Input to check

        Returns:
            True if this normalizer can process the input
        """
        pass

    def validate_output(self, metadata: PipelineMetadata) -> List[str]:
        """
        Validate the output metadata meets minimum requirements.

        Args:
            metadata: Generated metadata to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Check required fields
        if not metadata.pipeline.dag_id:
            errors.append("dag_id is required")
        if not metadata.pipeline.domain:
            errors.append("domain is required")
        if not metadata.source.source_type:
            errors.append("source_type is required")
        if not metadata.schema.columns:
            errors.append("At least one column is required in schema")
        if not metadata.target.bq_dataset:
            errors.append("bq_dataset is required")
        if not metadata.target.bq_table:
            errors.append("bq_table is required")

        return errors
