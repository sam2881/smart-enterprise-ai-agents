"""
Security module — Model Armor, plugin chain, and LLM call callbacks.

Quick-start:
    from agents.servicenow_agent.src.security import (
        secure_llm_call,
        incident_plugin_chain,
        data_agent_plugin_chain,
        SecurityCallbackHandler,
        make_security_before_model_callback,
        make_security_after_model_callback,
    )
"""
from .model_armor import ModelArmorScreener, ScreenResult, get_screener
from .plugins import (
    BaseSecurityPlugin,
    ModelArmorPlugin,
    GuardrailsPlugin,
    PIIRedactionPlugin,
    AuditPlugin,
    RateLimitPlugin,
    PluginChain,
    SecurityViolation,
    incident_plugin_chain,
    data_agent_plugin_chain,
)
from .callbacks import (
    secure_llm_call,
    SecurityCallbackHandler,
    make_security_before_model_callback,
    make_security_after_model_callback,
)

__all__ = [
    # model_armor
    "ModelArmorScreener",
    "ScreenResult",
    "get_screener",
    # plugins
    "BaseSecurityPlugin",
    "ModelArmorPlugin",
    "GuardrailsPlugin",
    "PIIRedactionPlugin",
    "AuditPlugin",
    "RateLimitPlugin",
    "PluginChain",
    "SecurityViolation",
    "incident_plugin_chain",
    "data_agent_plugin_chain",
    # callbacks
    "secure_llm_call",
    "SecurityCallbackHandler",
    "make_security_before_model_callback",
    "make_security_after_model_callback",
]
