"""
LLMClientFactory — creates the right BaseLLMClient from settings.

Add a new provider here; no other code needs to change.

Supported providers:
    openai    → OpenAILLMClient (api.openai.com)
    anthropic → AnthropicLLMClient (api.anthropic.com)
    ollama    → OpenAILLMClient pointed at a local Ollama endpoint
                (Ollama exposes an OpenAI-compatible REST API at /v1)

Ollama quick-start:
    # 1. Install: https://ollama.ai
    # 2. Pull a model:
    #        ollama pull llama3.2:3b        # 2 GB, fast
    #        ollama pull gemma3:4b          # 3 GB, Google's model
    #        ollama pull mistral:7b         # 4.5 GB, best quality small model
    # 3. Set in .env:
    #        LLM_PROVIDER=ollama
    #        LLM_MODEL=llama3.2:3b
    #        OLLAMA_BASE_URL=http://localhost:11434/v1
"""
from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from agents.data_agent.src.llm.base import BaseLLMClient

if TYPE_CHECKING:
    from agents.data_agent.src.config.settings import Settings


class LLMClientFactory:
    """Factory that constructs a provider-specific BaseLLMClient from Settings."""

    @staticmethod
    def create(settings: "Settings") -> BaseLLMClient:
        """
        Return the correct LLM client for the configured provider.

        Args:
            settings: Application settings (provider, model, api keys, etc.)

        Returns:
            A concrete BaseLLMClient ready to use.

        Raises:
            ValueError: If the provider is unknown.
            ImportError: If the required SDK package is not installed.
        """
        provider = settings.llm_provider
        model = settings.llm_model
        timeout = settings.llm_timeout

        if provider == "ollama":
            from agents.data_agent.src.llm.providers.openai_client import OpenAILLMClient

            return OpenAILLMClient(
                api_key="ollama",
                model=model,
                base_url=settings.ollama_base_url,
                timeout=timeout,
            )

        if provider == "openai":
            from agents.data_agent.src.llm.providers.openai_client import OpenAILLMClient

            api_key = settings.get_llm_api_key()
            if not api_key:
                raise ValueError("OPENAI_API_KEY is required when llm_provider=openai")
            return OpenAILLMClient(api_key=api_key, model=model, timeout=timeout)

        if provider == "anthropic":
            from agents.data_agent.src.llm.providers.anthropic_client import AnthropicLLMClient

            api_key = settings.get_llm_api_key()
            if not api_key:
                raise ValueError("ANTHROPIC_API_KEY is required when llm_provider=anthropic")
            return AnthropicLLMClient(api_key=api_key, model=model, timeout=timeout)

        if provider == "google":
            raise NotImplementedError(
                "Google provider is not yet implemented. "
                "Use llm_provider=openai or llm_provider=ollama."
            )

        raise ValueError(
            f"Unknown llm_provider '{provider}'. "
            "Valid values: openai, anthropic, ollama, google"
        )


@lru_cache(maxsize=1)
def get_llm_client() -> BaseLLMClient:
    """Cached singleton LLM client for the current process."""
    from agents.data_agent.src.config.settings import get_settings

    return LLMClientFactory.create(get_settings())
