"""
Input normalizers for converting various input types to canonical PipelineMetadata.

Normalizers handle:
1. UI Structured Input → PipelineMetadata
2. Natural Language → PipelineMetadata (via LLM)
3. DTSX Migration → PipelineMetadata (via parser)
"""

from .base import BaseNormalizer, NormalizerResult
from .ui_normalizer import UIInputNormalizer
from .nl_normalizer import NLInputNormalizer
from .dtsx_normalizer import DTSXNormalizer
from .dispatcher import InputDispatcher

__all__ = [
    "BaseNormalizer",
    "NormalizerResult",
    "UIInputNormalizer",
    "NLInputNormalizer",
    "DTSXNormalizer",
    "InputDispatcher",
]
