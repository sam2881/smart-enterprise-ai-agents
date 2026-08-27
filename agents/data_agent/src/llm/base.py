"""
BaseLLMClient — provider-agnostic interface for all LLM calls.

Any concrete provider (OpenAI, Anthropic, Ollama, …) implements this ABC so
callers never depend on a specific SDK.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class LLMMessage:
    role: str   # "user" | "assistant" | "system"
    content: str

    def to_dict(self) -> Dict[str, str]:
        return {"role": self.role, "content": self.content}


class BaseLLMClient(ABC):
    """Unified interface for every LLM provider."""

    @abstractmethod
    def complete(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int = 4096,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        """Synchronous text completion. Returns the assistant message text.

        Args:
            json_mode: Hint the provider to return valid JSON. Providers that
                       natively support JSON output mode (OpenAI, Ollama) will
                       enable it; others (Anthropic) will ignore the flag and
                       the caller should parse defensively.
        """

    @abstractmethod
    async def acomplete(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int = 4096,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        """Async text completion. Returns the assistant message text."""
