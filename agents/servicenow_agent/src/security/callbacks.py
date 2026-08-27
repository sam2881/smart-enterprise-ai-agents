"""
Security callbacks and decorators for LLM call interception.

Three interception mechanisms:

  1. @secure_llm_call — decorator for direct OpenAI/Anthropic SDK calls.
     Works with both sync and async functions:
       - Async → awaits plugin_chain.run_pre() / run_post() (full chain incl. Model Armor)
       - Sync  → calls plugin_chain.run_pre_sync() / run_post_sync() (all except Model Armor)

  2. SecurityCallbackHandler — LangChain BaseCallbackHandler.
     Attach to ChatAnthropic / ChatOpenAI:
       llm = ChatAnthropic(callbacks=[SecurityCallbackHandler(chain)])
     Hooks implemented (per langchain_core.callbacks.base):
       on_llm_start, on_chat_model_start, on_llm_end, on_llm_error,
       on_tool_start, on_tool_end

  3. make_security_before_model_callback / make_security_after_model_callback
     Google ADK (google-adk>=2.3.0) callback factories.
     Official ADK signature:
       before_model_callback(callback_context, llm_request) -> Optional[LlmResponse]
       after_model_callback(callback_context, llm_response) -> LlmResponse
     Returning LlmResponse from before_model_callback BLOCKS the LLM call.
     Returning None lets it proceed.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import inspect
import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import structlog

from .plugins import PluginChain, SecurityViolation, incident_plugin_chain

logger = structlog.get_logger(__name__)
_std_logger = logging.getLogger(__name__)


# ── Prompt/response extraction helpers ────────────────────────────────────────

def _extract_prompt_text(args: tuple, kwargs: dict) -> str:
    """
    Best-effort extraction of a prompt string from function call arguments.

    Checks kwargs first (prompt, messages, content, text, description),
    then positional args (first str > 10 chars, or incident dict fields).
    """
    for key in ("prompt", "messages", "content", "text", "description"):
        val = kwargs.get(key)
        if isinstance(val, str):
            return val
        if isinstance(val, list):
            parts = []
            for m in val:
                if isinstance(m, dict):
                    parts.append(m.get("content", "") or "")
                elif isinstance(m, str):
                    parts.append(m)
            combined = " ".join(p for p in parts if p)
            if combined:
                return combined

    for arg in args:
        if isinstance(arg, str) and len(arg) > 10:
            return arg
        if isinstance(arg, dict):
            combined = " ".join(filter(None, [
                str(arg.get("short_description", "")),
                str(arg.get("description", "")),
                str(arg.get("content", "")),
            ])).strip()
            if combined:
                return combined

    return ""


def _extract_response_text(result: Any) -> str:
    """
    Best-effort extraction of a text string from an LLM response object.

    Handles: str, dict (JSON-serialised), OpenAI ChatCompletion,
    Anthropic Message, plain objects with .content attribute.
    """
    if isinstance(result, str):
        return result
    if isinstance(result, dict):
        return json.dumps(result)
    # OpenAI ChatCompletion → result.choices[0].message.content
    if hasattr(result, "choices"):
        try:
            return result.choices[0].message.content or ""
        except (IndexError, AttributeError):
            pass
    # Anthropic Message → result.content (list of ContentBlock)
    if hasattr(result, "content"):
        content = result.content
        if isinstance(content, list):
            return " ".join(getattr(c, "text", "") for c in content if getattr(c, "text", None))
        if isinstance(content, str):
            return content
    return str(result)


def _run_async_safe(coro):
    """
    Run a coroutine from a synchronous call site.

    If no event loop is running: uses asyncio.run() directly.
    If a loop IS running (e.g. called from within uvicorn): executes the
    coroutine in a dedicated thread pool to avoid deadlock.
    """
    try:
        asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result(timeout=30)
    except RuntimeError:
        return asyncio.run(coro)


# ── @secure_llm_call decorator ────────────────────────────────────────────────

def secure_llm_call(plugin_chain: Optional[PluginChain] = None):
    """
    Decorator that wraps any LLM function with the security plugin chain.

    Usage (module level — create chain once, reuse across calls):
        _CHAIN = incident_plugin_chain()

        @secure_llm_call(_CHAIN)
        def sync_llm_func(incident: dict) -> dict: ...

        @secure_llm_call(_CHAIN)
        async def async_llm_func(prompt: str) -> str: ...

    On SecurityViolation: the exception propagates to the caller. The LLM
    function is NOT invoked. Callers should handle SecurityViolation or let
    it surface as a 400/422 error via FastAPI exception handlers.
    """
    def decorator(func: Callable) -> Callable:
        chain = plugin_chain or incident_plugin_chain()

        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs) -> Any:
                context: Dict[str, Any] = {
                    "function": func.__name__,
                    "timestamp": datetime.utcnow().isoformat(),
                }
                prompt = _extract_prompt_text(args, kwargs)
                try:
                    await chain.run_pre(prompt, context)
                except SecurityViolation:
                    logger.warning(
                        "llm_call_blocked_pre",
                        function=func.__name__,
                    )
                    raise

                result = await func(*args, **kwargs)

                try:
                    await chain.run_post(_extract_response_text(result), context)
                except SecurityViolation:
                    logger.warning(
                        "llm_response_blocked_post",
                        function=func.__name__,
                    )
                    raise

                return result

            return async_wrapper

        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs) -> Any:
                context: Dict[str, Any] = {
                    "function": func.__name__,
                    "timestamp": datetime.utcnow().isoformat(),
                }
                prompt = _extract_prompt_text(args, kwargs)
                try:
                    chain.run_pre_sync(prompt, context)
                except SecurityViolation:
                    logger.warning(
                        "llm_call_blocked_pre",
                        function=func.__name__,
                    )
                    raise

                result = func(*args, **kwargs)

                try:
                    chain.run_post_sync(_extract_response_text(result), context)
                except SecurityViolation:
                    logger.warning(
                        "llm_response_blocked_post",
                        function=func.__name__,
                    )
                    raise

                return result

            return sync_wrapper

    return decorator


# ── LangChain SecurityCallbackHandler ─────────────────────────────────────────

try:
    from langchain_core.callbacks.base import BaseCallbackHandler  # type: ignore
    from langchain_core.outputs import LLMResult  # type: ignore
    _LANGCHAIN_AVAILABLE = True
except ImportError:
    _LANGCHAIN_AVAILABLE = False
    BaseCallbackHandler = object  # type: ignore
    LLMResult = object  # type: ignore


class SecurityCallbackHandler(BaseCallbackHandler):  # type: ignore[misc]
    """
    LangChain/LangGraph callback handler for security interception.

    Implements all LLM, tool, and chain hooks from
    langchain_core.callbacks.base.BaseCallbackHandler.

    Attach to a ChatAnthropic or ChatOpenAI model:
        handler = SecurityCallbackHandler(chain=incident_plugin_chain())
        llm = ChatAnthropic(
            model="claude-sonnet-4-6",
            callbacks=[handler],
        )

    Uses the sync plugin path (run_pre_sync / run_post_sync) because
    LangChain callback hooks are called synchronously by the framework.
    """

    def __init__(self, chain: Optional[PluginChain] = None) -> None:
        self._chain = chain or incident_plugin_chain()

    def on_llm_start(
        self,
        serialized: Dict[str, Any],
        prompts: List[str],
        *,
        run_id: Any,
        parent_run_id: Any = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        context = {
            "function": serialized.get("name", "llm"),
            "timestamp": datetime.utcnow().isoformat(),
            "run_id": str(run_id),
        }
        self._chain.run_pre_sync(" ".join(prompts), context)

    def on_chat_model_start(
        self,
        serialized: Dict[str, Any],
        messages: List[List[Any]],
        *,
        run_id: Any,
        parent_run_id: Any = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        context = {
            "function": serialized.get("name", "chat_model"),
            "timestamp": datetime.utcnow().isoformat(),
            "run_id": str(run_id),
        }
        all_content: List[str] = []
        for batch in messages:
            for msg in batch:
                content = getattr(msg, "content", "")
                if isinstance(content, str):
                    all_content.append(content)
                elif isinstance(content, list):
                    for part in content:
                        if isinstance(part, dict):
                            all_content.append(part.get("text", "") or "")
        self._chain.run_pre_sync(" ".join(all_content), context)

    def on_llm_end(
        self,
        response: "LLMResult",
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        context = {
            "function": "llm_end",
            "timestamp": datetime.utcnow().isoformat(),
            "run_id": str(run_id),
        }
        try:
            text = response.generations[0][0].text if response.generations else ""
        except (IndexError, AttributeError):
            text = ""
        self._chain.run_post_sync(text, context)

    def on_llm_error(
        self,
        error: BaseException,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        _std_logger.error("SecurityCallbackHandler.on_llm_error: %s", error)

    def on_tool_start(
        self,
        serialized: Dict[str, Any],
        input_str: str,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        inputs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        context = {
            "function": f"tool:{serialized.get('name', 'unknown')}",
            "timestamp": datetime.utcnow().isoformat(),
            "run_id": str(run_id),
        }
        self._chain.run_pre_sync(input_str or "", context)

    def on_tool_end(
        self,
        output: str,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        context = {
            "function": "tool_end",
            "timestamp": datetime.utcnow().isoformat(),
            "run_id": str(run_id),
        }
        self._chain.run_post_sync(output or "", context)

    def on_chain_start(
        self,
        serialized: Dict[str, Any],
        inputs: Dict[str, Any],
        *,
        run_id: Any,
        parent_run_id: Any = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        pass  # Chain-level interception not required; handled at LLM level

    def on_chain_end(
        self,
        outputs: Dict[str, Any],
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        pass


# ── Google ADK callback factories ─────────────────────────────────────────────

def make_security_before_model_callback(chain: Optional[PluginChain] = None):
    """
    Factory for a Google ADK before_model_callback (google-adk>=2.3.0).

    Official ADK signature:
        async def before_model_callback(
            callback_context: CallbackContext,
            llm_request: LlmRequest,
        ) -> Optional[LlmResponse]

    Semantics (per ADK docs):
      - Return None    → LLM call proceeds normally
      - Return LlmResponse → LLM call is BLOCKED; returned response is used instead

    Usage:
        from google.adk import Agent
        agent = Agent(
            before_model_callback=make_security_before_model_callback(
                incident_plugin_chain()
            ),
            ...
        )
    """
    security_chain = chain or incident_plugin_chain()

    async def before_model_callback(callback_context: Any, llm_request: Any) -> Any:
        # Extract text from LlmRequest.contents
        contents = getattr(llm_request, "contents", []) or []
        parts_text: List[str] = []
        for item in contents:
            # Each item may be a Content object with .parts, or a plain dict
            parts = getattr(item, "parts", None) or (
                item.get("parts", []) if isinstance(item, dict) else []
            )
            for part in (parts or []):
                text = getattr(part, "text", None) or (
                    part.get("text", "") if isinstance(part, dict) else ""
                )
                if text:
                    parts_text.append(text)
        prompt_text = " ".join(parts_text)

        context: Dict[str, Any] = {
            "function": "adk_before_model",
            "timestamp": datetime.utcnow().isoformat(),
            "adk": True,
        }

        try:
            await security_chain.run_pre(prompt_text, context)
            return None  # allow the LLM call to proceed

        except SecurityViolation as exc:
            logger.warning(
                "adk_before_model_blocked",
                reason=exc.reason,
                plugin=exc.plugin,
            )
            try:
                from google.adk.agents.types import LlmResponse, Content, Part  # type: ignore
                return LlmResponse(
                    content=Content(
                        parts=[Part(text=f"Request blocked by security policy: {exc.reason}")],
                        role="model",
                    )
                )
            except ImportError:
                # google-adk not installed — re-raise so the caller can handle it
                raise

    return before_model_callback


def make_security_after_model_callback(chain: Optional[PluginChain] = None):
    """
    Factory for a Google ADK after_model_callback (google-adk>=2.3.0).

    Official ADK signature:
        async def after_model_callback(
            callback_context: CallbackContext,
            llm_response: LlmResponse,
        ) -> LlmResponse

    Always returns the (possibly original) LlmResponse.
    Logs a warning and passes through on SecurityViolation rather than blocking,
    because the response has already been generated.

    Usage:
        agent = Agent(
            after_model_callback=make_security_after_model_callback(
                incident_plugin_chain()
            ),
            ...
        )
    """
    security_chain = chain or incident_plugin_chain()

    async def after_model_callback(callback_context: Any, llm_response: Any) -> Any:
        content = getattr(llm_response, "content", None)
        parts = getattr(content, "parts", []) if content else []
        response_text = " ".join(
            getattr(p, "text", "") for p in (parts or [])
            if getattr(p, "text", None)
        )

        context: Dict[str, Any] = {
            "function": "adk_after_model",
            "timestamp": datetime.utcnow().isoformat(),
            "adk": True,
        }

        try:
            await security_chain.run_post(response_text, context)
        except SecurityViolation as exc:
            logger.warning(
                "adk_after_model_response_flagged",
                reason=exc.reason,
                plugin=exc.plugin,
            )

        return llm_response

    return after_model_callback
