"""
Cross-Encoder Reranker Stub
This is a placeholder implementation that disables reranking.
For production, install sentence-transformers and implement proper reranking.
"""
import structlog
from typing import List, Any, Dict, NamedTuple

logger = structlog.get_logger()


class RerankedResult(NamedTuple):
    """Result from reranking operation"""
    text: str
    score: float
    metadata: Dict[str, Any]


class CrossEncoderReranker:
    """Stub implementation of CrossEncoderReranker that passes results through without reranking"""

    def __init__(self, model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"):
        logger.warning("cross_encoder_reranker_stub_initialized",
                      message="Reranking disabled - using stub implementation")
        self.model_name = model_name

    def rerank(
        self,
        query: str,
        candidates: List[Dict[str, Any]],
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """Pass-through reranking - returns top_k candidates without scoring"""
        logger.debug("rerank_passthrough",
                    query=query,
                    input_count=len(candidates),
                    output_count=min(top_k, len(candidates)))
        return candidates[:top_k]


def reranker_with_fallback(
    query: str,
    candidates: List[Dict[str, Any]],
    top_k: int = 5,
    fallback_enabled: bool = True
) -> List[Dict[str, Any]]:
    """Reranker function with fallback - stub implementation"""
    logger.debug("reranker_with_fallback_passthrough",
                query=query,
                input_count=len(candidates),
                output_count=min(top_k, len(candidates)))
    return candidates[:top_k]


# Singleton instance and aliases
cross_encoder_reranker = CrossEncoderReranker()
RerankerWithFallback = CrossEncoderReranker  # Alias for backward compatibility
