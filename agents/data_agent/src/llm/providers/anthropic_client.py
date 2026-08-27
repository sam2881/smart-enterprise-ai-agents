"""
Anthropic provider — wraps anthropic.Anthropic and anthropic.AsyncAnthropic.
"""
from __future__ import annotations

from typing import Dict, List

from agents.data_agent.src.llm.base import BaseLLMClient


class AnthropicLLMClient(BaseLLMClient):
    """Wraps the Anthropic SDK (sync + async)."""

    def __init__(self, api_key: str, model: str, timeout: int = 120) -> None:
        import anthropic

        self._sync_client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self._async_client = anthropic.AsyncAnthropic(api_key=api_key, timeout=timeout)
        self._model = model

    def complete(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int = 4096,
        temperature: float = 0.0,
        json_mode: bool = False,  # Anthropic has no native JSON mode; caller parses defensively
    ) -> str:
        response = self._sync_client.messages.create(
            model=self._model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return response.content[0].text

    async def acomplete(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int = 4096,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        response = await self._async_client.messages.create(
            model=self._model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return response.content[0].text
