"""
OpenAI provider — also used for Ollama (OpenAI-compatible endpoint).

When base_url is set to http://localhost:11434/v1 and api_key="ollama",
this client talks to a local Ollama instance instead of OpenAI's servers.
No other code changes are required for Ollama support.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from agents.data_agent.src.llm.base import BaseLLMClient


class OpenAILLMClient(BaseLLMClient):
    """
    Wraps openai.OpenAI and openai.AsyncOpenAI.

    Set base_url to an Ollama endpoint to use local models:
        OpenAILLMClient(
            api_key="ollama",
            model="llama3.2:3b",
            base_url="http://localhost:11434/v1",
        )
    """

    def __init__(
        self,
        api_key: str,
        model: str,
        base_url: Optional[str] = None,
        timeout: int = 120,
    ) -> None:
        import openai

        kwargs = {"api_key": api_key, "timeout": timeout}
        if base_url:
            kwargs["base_url"] = base_url

        self._sync_client = openai.OpenAI(**kwargs)
        self._async_client = openai.AsyncOpenAI(**kwargs)
        self._model = model

    def complete(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int = 4096,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        kwargs: dict = dict(
            model=self._model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}
        response = self._sync_client.chat.completions.create(**kwargs)
        return response.choices[0].message.content or ""

    async def acomplete(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: int = 4096,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        kwargs: dict = dict(
            model=self._model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}
        response = await self._async_client.chat.completions.create(**kwargs)
        return response.choices[0].message.content or ""
