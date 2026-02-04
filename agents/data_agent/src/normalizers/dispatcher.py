"""
Input Dispatcher - Routes input to appropriate normalizer.

Entry point for all pipeline input processing.
"""

import structlog
from typing import List, Optional

from ..models.canonical import UnifiedPipelineInput, InputType

from .base import BaseNormalizer, NormalizerResult
from .ui_normalizer import UIInputNormalizer
from .nl_normalizer import NLInputNormalizer
from .dtsx_normalizer import DTSXNormalizer

logger = structlog.get_logger(__name__)


class InputDispatcher:
    """
    Routes input to the appropriate normalizer.

    Dispatcher pattern ensures:
    1. All input flows through a single entry point
    2. Correct normalizer is selected based on input type
    3. Consistent error handling and logging
    """

    def __init__(self, llm_client: Optional[any] = None):
        """
        Initialize dispatcher with normalizers.

        Args:
            llm_client: Optional LLM client for NL processing
        """
        self.normalizers: List[BaseNormalizer] = [
            UIInputNormalizer(),
            NLInputNormalizer(llm_client=llm_client),
            DTSXNormalizer(),
        ]
        self.llm_client = llm_client

    async def dispatch(self, input_data: UnifiedPipelineInput) -> NormalizerResult:
        """
        Dispatch input to appropriate normalizer.

        Args:
            input_data: Unified input from any source

        Returns:
            NormalizerResult with PipelineMetadata or errors
        """
        logger.info(
            "dispatch_started",
            input_type=input_data.input_type.value,
            request_id=input_data.request_id,
        )

        # Find appropriate normalizer
        for normalizer in self.normalizers:
            if normalizer.can_handle(input_data):
                logger.info(
                    "normalizer_selected",
                    normalizer=normalizer.__class__.__name__,
                )
                result = await normalizer.normalize(input_data)

                logger.info(
                    "dispatch_complete",
                    success=result.success,
                    errors_count=len(result.errors),
                    warnings_count=len(result.warnings),
                )

                return result

        # No normalizer found
        error_msg = f"No normalizer found for input type: {input_data.input_type}"
        logger.error("no_normalizer_found", input_type=input_data.input_type.value)

        return NormalizerResult(
            success=False,
            errors=[error_msg],
        )

    def get_supported_input_types(self) -> List[InputType]:
        """Get list of supported input types."""
        return [
            InputType.UI_STRUCTURED,
            InputType.NATURAL_LANGUAGE,
            InputType.DTSX_MIGRATION,
        ]


# Convenience function for direct use
async def normalize_input(
    input_data: UnifiedPipelineInput,
    llm_client: Optional[any] = None,
) -> NormalizerResult:
    """
    Normalize any input type to PipelineMetadata.

    This is the main entry point for input processing.

    Args:
        input_data: Input from UI, NL, or DTSX
        llm_client: Optional LLM client for NL processing

    Returns:
        NormalizerResult with canonical PipelineMetadata
    """
    dispatcher = InputDispatcher(llm_client=llm_client)
    return await dispatcher.dispatch(input_data)
