"""
Google Cloud Model Armor wrapper (v0.4.0).

Official API reference:
  google.cloud.modelarmor_v1.services.model_armor.ModelArmorAsyncClient
  Methods: sanitize_user_prompt() / sanitize_model_response()
  Block signal: resp.filter_match_state.name == "MATCH_FOUND"

Template configured via Terraform (secret: model-armor-template-{env}).
Falls back gracefully when ENVIRONMENT=local or credentials are absent.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

_LOCAL_ENVS = {"local", "test", "development", ""}

_TEMPLATE_FMT = (
    "projects/{project_id}/locations/{location}/templates/{template_id}"
)


@dataclass
class ScreenResult:
    blocked: bool
    reason: str = ""


class ModelArmorScreener:
    """
    Async wrapper around ModelArmorAsyncClient.

    Env vars consumed:
      GCP_PROJECT_ID          — GCP project (required for cloud)
      MODEL_ARMOR_LOCATION    — defaults to us-central1
      MODEL_ARMOR_TEMPLATE_ID — template ID stored in Secret Manager
      ENVIRONMENT             — if "local" / "test", screening is skipped
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        location: Optional[str] = None,
        template_id: Optional[str] = None,
    ):
        self._project_id = project_id or os.getenv("GCP_PROJECT_ID", "")
        self._location = location or os.getenv("MODEL_ARMOR_LOCATION", "us-central1")
        self._template_id = template_id or os.getenv("MODEL_ARMOR_TEMPLATE_ID", "")
        self._client = None

        env = os.getenv("ENVIRONMENT", "local").lower()
        self._enabled = (
            env not in _LOCAL_ENVS
            and bool(self._project_id)
            and bool(self._template_id)
        )
        if not self._enabled:
            logger.debug(
                "ModelArmorScreener disabled (env=%s, project=%s, template=%s)",
                env,
                bool(self._project_id),
                bool(self._template_id),
            )

    @property
    def _template_name(self) -> str:
        return _TEMPLATE_FMT.format(
            project_id=self._project_id,
            location=self._location,
            template_id=self._template_id,
        )

    def _get_client(self):
        if self._client is None:
            try:
                from google.cloud.modelarmor_v1 import ModelArmorAsyncClient  # type: ignore
                self._client = ModelArmorAsyncClient()
            except ImportError:
                logger.warning(
                    "google-cloud-modelarmor not installed; Model Armor disabled. "
                    "Add google-cloud-modelarmor>=0.4.0 to requirements.txt"
                )
                self._enabled = False
        return self._client

    async def sanitize_prompt(self, text: str) -> ScreenResult:
        """
        Screen user prompt BEFORE sending to LLM.

        Calls ModelArmorAsyncClient.sanitize_user_prompt() with:
          SanitizeUserPromptRequest(name=<template>, user_prompt_data=UserPromptData(text=text))

        Returns ScreenResult(blocked=True) when filter_match_state == MATCH_FOUND.
        Returns ScreenResult(blocked=False) on any error or when disabled (fail-open).
        """
        if not self._enabled:
            return ScreenResult(blocked=False)

        client = self._get_client()
        if client is None:
            return ScreenResult(blocked=False)

        try:
            from google.cloud.modelarmor_v1.types import (  # type: ignore
                SanitizeUserPromptRequest,
                UserPromptData,
            )

            resp = await client.sanitize_user_prompt(
                SanitizeUserPromptRequest(
                    name=self._template_name,
                    user_prompt_data=UserPromptData(text=text),
                )
            )
            blocked = resp.filter_match_state.name == "MATCH_FOUND"
            logger.debug("model_armor_prompt_screen", blocked=blocked)
            return ScreenResult(
                blocked=blocked,
                reason="prompt_injection_or_jailbreak_detected" if blocked else "",
            )
        except Exception as exc:
            logger.warning("model_armor_prompt_screen_failed: %s", exc)
            return ScreenResult(blocked=False)

    async def sanitize_response(self, text: str) -> ScreenResult:
        """
        Screen LLM response AFTER receiving it.

        Calls ModelArmorAsyncClient.sanitize_model_response() with:
          SanitizeModelResponseRequest(name=<template>, model_response_data=ModelResponseData(text=text))

        Returns ScreenResult(blocked=True) when filter_match_state == MATCH_FOUND.
        """
        if not self._enabled:
            return ScreenResult(blocked=False)

        client = self._get_client()
        if client is None:
            return ScreenResult(blocked=False)

        try:
            from google.cloud.modelarmor_v1.types import (  # type: ignore
                SanitizeModelResponseRequest,
                ModelResponseData,
            )

            resp = await client.sanitize_model_response(
                SanitizeModelResponseRequest(
                    name=self._template_name,
                    model_response_data=ModelResponseData(text=text),
                )
            )
            blocked = resp.filter_match_state.name == "MATCH_FOUND"
            logger.debug("model_armor_response_screen", blocked=blocked)
            return ScreenResult(
                blocked=blocked,
                reason="harmful_response_detected" if blocked else "",
            )
        except Exception as exc:
            logger.warning("model_armor_response_screen_failed: %s", exc)
            return ScreenResult(blocked=False)


# Module-level singleton — created lazily on first use
_screener: Optional[ModelArmorScreener] = None


def get_screener() -> ModelArmorScreener:
    global _screener
    if _screener is None:
        _screener = ModelArmorScreener()
    return _screener
