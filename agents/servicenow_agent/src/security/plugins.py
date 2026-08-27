"""
Security plugin architecture for pre/post LLM call interception.

Each plugin implements pre_llm() / post_llm() (async) and
pre_llm_sync() / post_llm_sync() (sync) so the same chain works in both
async LangGraph nodes (llm_judge.py, nl_transform_processor.py) and the
sync OpenAI call sites in llm_intelligence.py.

Execution order in incident_plugin_chain():
  pre:   RateLimit → PIIRedaction → Guardrails → ModelArmor → Audit
  post:  RateLimit → PIIRedaction → Guardrails → ModelArmor → Audit
         (post_llm on each plugin, in same list order)

ModelArmor is async-only; its sync stubs are no-ops so it is transparent
to sync callers (coverage comes from the FastAPI SecurityMiddleware at the
HTTP boundary for sync flows).
"""
from __future__ import annotations

import hashlib
import re
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import structlog

logger = structlog.get_logger(__name__)


# ── Exception ─────────────────────────────────────────────────────────────────

class SecurityViolation(Exception):
    """Raised when a plugin blocks an LLM call."""

    def __init__(self, reason: str, plugin: str):
        self.reason = reason
        self.plugin = plugin
        super().__init__(f"[{plugin}] {reason}")


# ── Base class ────────────────────────────────────────────────────────────────

class BaseSecurityPlugin(ABC):
    name: str = "base"
    enabled: bool = True

    @abstractmethod
    async def pre_llm(self, prompt: str, context: Dict[str, Any]) -> str:
        """Sanitize/validate prompt BEFORE LLM call. May raise SecurityViolation."""

    @abstractmethod
    async def post_llm(self, response: str, context: Dict[str, Any]) -> str:
        """Validate/log response AFTER LLM call. May raise SecurityViolation."""

    def pre_llm_sync(self, prompt: str, context: Dict[str, Any]) -> str:
        """Sync variant for non-async callers. Default: pass through."""
        return prompt

    def post_llm_sync(self, response: str, context: Dict[str, Any]) -> str:
        """Sync variant for non-async callers. Default: pass through."""
        return response


# ── Plugin implementations ────────────────────────────────────────────────────

class RateLimitPlugin(BaseSecurityPlugin):
    """
    Rate-limits LLM calls using the RateLimiter already inside LLMGuardrails.
    Checked synchronously (in-process counter — no I/O).
    """
    name = "rate_limit"

    def __init__(self) -> None:
        self._limiter = None
        try:
            from agents.servicenow_agent.src.guardrails.llm_guardrails import guardrails
            self._limiter = guardrails.rate_limiter
        except ImportError:
            try:
                from guardrails.llm_guardrails import guardrails  # type: ignore
                self._limiter = guardrails.rate_limiter
            except ImportError:
                logger.warning("rate_limit_plugin: LLMGuardrails not importable; rate-limiting disabled")

    async def pre_llm(self, prompt: str, context: Dict[str, Any]) -> str:
        return self.pre_llm_sync(prompt, context)

    def pre_llm_sync(self, prompt: str, context: Dict[str, Any]) -> str:
        if not self._limiter:
            return prompt
        identifier = context.get("user_id") or context.get("function", "default")
        allowed, reason = self._limiter.check(str(identifier))
        if not allowed:
            raise SecurityViolation(reason, self.name)
        return prompt

    async def post_llm(self, response: str, context: Dict[str, Any]) -> str:
        return response

    def post_llm_sync(self, response: str, context: Dict[str, Any]) -> str:
        return response


class PIIRedactionPlugin(BaseSecurityPlugin):
    """
    Redacts 6 PII types from LLM prompt text using the same regex patterns
    as PIIDetector (pii_detection.py), applied to free-form strings rather
    than Spark DataFrames. PIIMasker.mask_column() is NOT used here — it
    requires a Spark DataFrame context.

    Types redacted: SSN, Credit Card, Email, Phone, IP Address, ZIP Code.
    """
    name = "pii_redaction"

    _PATTERNS: List[tuple] = [
        (re.compile(r'\b\d{3}-\d{2}-\d{4}\b|\b\d{9}\b'), "[SSN_REDACTED]"),
        (re.compile(r'\b(?:\d{4}[-\s]?){3}\d{4}\b'), "[CREDIT_CARD_REDACTED]"),
        (re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'), "[EMAIL_REDACTED]"),
        (re.compile(r'\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'), "[PHONE_REDACTED]"),
        (re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b'), "[IP_REDACTED]"),
        (re.compile(r'\b\d{5}(?:-\d{4})?\b'), "[ZIP_REDACTED]"),
    ]

    async def pre_llm(self, prompt: str, context: Dict[str, Any]) -> str:
        return self.pre_llm_sync(prompt, context)

    def pre_llm_sync(self, prompt: str, context: Dict[str, Any]) -> str:
        result = prompt
        for pattern, replacement in self._PATTERNS:
            result = pattern.sub(replacement, result)
        if result != prompt:
            logger.info("pii_redacted_from_prompt", function=context.get("function"))
        return result

    async def post_llm(self, response: str, context: Dict[str, Any]) -> str:
        return response

    def post_llm_sync(self, response: str, context: Dict[str, Any]) -> str:
        return response


class GuardrailsPlugin(BaseSecurityPlugin):
    """
    Wraps existing LLMGuardrails (InputValidator + ContentModerator + OutputValidator).
    Runs synchronously — regex + in-process checks, no I/O.
    """
    name = "guardrails"

    def __init__(self) -> None:
        self._guardrails = None
        try:
            from agents.servicenow_agent.src.guardrails.llm_guardrails import guardrails
            self._guardrails = guardrails
        except ImportError:
            try:
                from guardrails.llm_guardrails import guardrails  # type: ignore
                self._guardrails = guardrails
            except ImportError:
                logger.warning("guardrails_plugin: LLMGuardrails not importable")

    async def pre_llm(self, prompt: str, context: Dict[str, Any]) -> str:
        return self.pre_llm_sync(prompt, context)

    def pre_llm_sync(self, prompt: str, context: Dict[str, Any]) -> str:
        if not self._guardrails:
            return prompt
        result = self._guardrails.validate_input(prompt, context="general")
        if not result.passed:
            raise SecurityViolation(
                f"guardrails_blocked: {'; '.join(result.issues)}", self.name
            )
        return result.sanitized_content or prompt

    async def post_llm(self, response: str, context: Dict[str, Any]) -> str:
        return self.post_llm_sync(response, context)

    def post_llm_sync(self, response: str, context: Dict[str, Any]) -> str:
        if not self._guardrails:
            return response
        result = self._guardrails.validate_output(response)
        if not result.passed:
            logger.warning(
                "guardrails_output_issues",
                issues=result.issues,
                function=context.get("function"),
            )
        return response


class ModelArmorPlugin(BaseSecurityPlugin):
    """
    Screens prompts and responses via Google Cloud Model Armor API.

    Async-only: sync stubs are no-ops (Model Armor is a remote API call).
    Blocked when filter_match_state == MATCH_FOUND.
    Fails open (never blocks) when ENVIRONMENT=local or credentials absent.
    """
    name = "model_armor"

    def __init__(self) -> None:
        from .model_armor import get_screener
        self._screener = get_screener()

    async def pre_llm(self, prompt: str, context: Dict[str, Any]) -> str:
        result = await self._screener.sanitize_prompt(prompt)
        if result.blocked:
            logger.warning(
                "model_armor_blocked_prompt",
                reason=result.reason,
                function=context.get("function"),
            )
            raise SecurityViolation(result.reason, self.name)
        return prompt

    async def post_llm(self, response: str, context: Dict[str, Any]) -> str:
        result = await self._screener.sanitize_response(response)
        if result.blocked:
            logger.warning("model_armor_blocked_response", reason=result.reason)
            raise SecurityViolation(result.reason, self.name)
        return response

    # Sync stubs — Model Armor screening is skipped in sync contexts.
    # Coverage for sync callers comes from GuardrailsPlugin + FastAPI SecurityMiddleware.
    def pre_llm_sync(self, prompt: str, context: Dict[str, Any]) -> str:
        return prompt

    def post_llm_sync(self, response: str, context: Dict[str, Any]) -> str:
        return response


class AuditPlugin(BaseSecurityPlugin):
    """
    Logs LLM call initiation and completion to the immutable audit trail.
    Uses existing AuditLogger (audit_logger.py) with AI_DECISION event type.
    """
    name = "audit"

    def __init__(self) -> None:
        self._audit_logger = None
        self._AuditEventType = None
        self._RiskLevel = None
        try:
            from agents.servicenow_agent.src.governance.audit_logger import (
                audit_logger, AuditEventType, RiskLevel,
            )
            self._audit_logger = audit_logger
            self._AuditEventType = AuditEventType
            self._RiskLevel = RiskLevel
        except ImportError:
            try:
                from governance.audit_logger import (  # type: ignore
                    audit_logger, AuditEventType, RiskLevel,
                )
                self._audit_logger = audit_logger
                self._AuditEventType = AuditEventType
                self._RiskLevel = RiskLevel
            except ImportError:
                logger.warning("audit_plugin: AuditLogger not importable; audit logging disabled")

    @staticmethod
    def _hash(text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()[:16]

    def _log(self, action: str, context: Dict[str, Any], details: Dict[str, Any]) -> None:
        if not self._audit_logger:
            return
        try:
            self._audit_logger.log(
                event_type=self._AuditEventType.AI_DECISION,
                actor="security-plugin-chain",
                actor_type="ai",
                action=action,
                resource=context.get("incident_id", "unknown"),
                resource_type="llm_call",
                details=details,
                risk_level=self._RiskLevel.LOW,
            )
        except Exception as exc:
            logger.warning("audit_plugin_log_failed", error=str(exc))

    async def pre_llm(self, prompt: str, context: Dict[str, Any]) -> str:
        return self.pre_llm_sync(prompt, context)

    def pre_llm_sync(self, prompt: str, context: Dict[str, Any]) -> str:
        context["_audit_start"] = time.monotonic()
        self._log(
            f"llm_call_initiated:{context.get('function', 'unknown')}",
            context,
            {
                "prompt_hash": self._hash(prompt),
                "function": context.get("function"),
                "timestamp": context.get("timestamp"),
            },
        )
        return prompt

    async def post_llm(self, response: str, context: Dict[str, Any]) -> str:
        return self.post_llm_sync(response, context)

    def post_llm_sync(self, response: str, context: Dict[str, Any]) -> str:
        elapsed_ms = round((time.monotonic() - context.get("_audit_start", time.monotonic())) * 1000, 2)
        self._log(
            f"llm_call_completed:{context.get('function', 'unknown')}",
            context,
            {
                "response_hash": self._hash(response),
                "function": context.get("function"),
                "latency_ms": elapsed_ms,
            },
        )
        return response


# ── PluginChain ────────────────────────────────────────────────────────────────

class PluginChain:
    """
    Runs an ordered sequence of security plugins before and after every LLM call.

    Async API  (async callers):  run_pre() / run_post()
    Sync API   (sync callers):   run_pre_sync() / run_post_sync()

    The first plugin that raises SecurityViolation stops the chain.
    """

    def __init__(self, plugins: List[BaseSecurityPlugin]) -> None:
        self.plugins = [p for p in plugins if p.enabled]

    async def run_pre(self, prompt: str, context: Dict[str, Any]) -> str:
        current = prompt
        for plugin in self.plugins:
            current = await plugin.pre_llm(current, context)
        return current

    async def run_post(self, response: str, context: Dict[str, Any]) -> str:
        current = response
        for plugin in self.plugins:
            current = await plugin.post_llm(current, context)
        return current

    def run_pre_sync(self, prompt: str, context: Dict[str, Any]) -> str:
        current = prompt
        for plugin in self.plugins:
            current = plugin.pre_llm_sync(current, context)
        return current

    def run_post_sync(self, response: str, context: Dict[str, Any]) -> str:
        current = response
        for plugin in self.plugins:
            current = plugin.post_llm_sync(current, context)
        return current


# ── Pre-built chains ───────────────────────────────────────────────────────────

def incident_plugin_chain() -> PluginChain:
    """
    Full 5-plugin chain for the incident management agent (strictest).

    Pre-call order: RateLimit → PIIRedaction → Guardrails → ModelArmor → Audit
    Post-call order: same list order (RateLimit/PIIRedaction are no-ops post)
    """
    return PluginChain([
        RateLimitPlugin(),
        PIIRedactionPlugin(),
        GuardrailsPlugin(),
        ModelArmorPlugin(),
        AuditPlugin(),
    ])


def data_agent_plugin_chain() -> PluginChain:
    """
    3-plugin chain for the data engineering agent.

    No Model Armor — data pipeline transforms are internal/trusted inputs.
    """
    return PluginChain([
        PIIRedactionPlugin(),
        GuardrailsPlugin(),
        AuditPlugin(),
    ])
