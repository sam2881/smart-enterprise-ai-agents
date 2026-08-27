"""
Exception classes for the Data Engineering Agent.

Provides a hierarchy of typed exceptions for different agent operations:
- DataAgentError: Base exception for all data agent errors
- PlanningError, TemplateSelectionError: Planner agent
- GenerationError, TemplateNotFoundError, TemplateRenderError: Generator agent
- ValidationError: Validator agent
- DeploymentError, GitError, CICDError: Deployer agent
"""


class DataAgentError(Exception):
    """Base exception for the Data Agent."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.details = details or {}


# ── Planner Agent Exceptions ──────────────────────────────────────────────


class PlanningError(DataAgentError):
    """Raised when execution plan generation fails."""


class TemplateSelectionError(DataAgentError):
    """Raised when no suitable template can be selected for the pipeline."""


# ── Generator Agent Exceptions ────────────────────────────────────────────


class GenerationError(DataAgentError):
    """Raised when code generation fails."""


class TemplateNotFoundError(GenerationError):
    """Raised when a required Jinja2 template is not found."""


class TemplateRenderError(GenerationError):
    """Raised when a Jinja2 template fails to render."""


# ── Validator Agent Exceptions ────────────────────────────────────────────


class ValidationError(DataAgentError):
    """Raised when artifact validation fails."""


# ── Connection Test Agent Exceptions ─────────────────────────────────────


class ConnectionTestError(DataAgentError):
    """Raised when source connection validation fails."""


# ── Deployer Agent Exceptions ────────────────────────────────────────────


class DeploymentError(DataAgentError):
    """Raised when deployment of generated artifacts fails."""


class GitError(DeploymentError):
    """Raised when a Git operation fails."""


class CICDError(DeploymentError):
    """Raised when CI/CD pipeline trigger or check fails."""
