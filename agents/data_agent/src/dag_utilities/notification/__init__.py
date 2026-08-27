"""
DAG Utilities Notification Module

Notification utilities for APEX DAG patterns.
"""

from typing import List, Optional, Dict, Any
import json


class EmailNotifier:
    """
    Email notification sender.

    Sends email notifications for pipeline events.
    """

    def __init__(self, smtp_config: Dict[str, Any] = None):
        """
        Initialize EmailNotifier.

        Args:
            smtp_config: SMTP configuration dictionary
        """
        self.smtp_config = smtp_config or {}

    def send(
        self,
        to: List[str],
        subject: str,
        body: str,
        html: bool = False
    ) -> bool:
        """
        Send email notification.

        Args:
            to: List of recipient email addresses
            subject: Email subject
            body: Email body
            html: Whether body is HTML

        Returns:
            True if sent successfully
        """
        # In production, would use SMTP or SendGrid
        # Reference implementation

        try:
            # Log for debugging
            print(f"Email to {to}: {subject}")
            return True
        except Exception:
            return False

    def send_pipeline_failure(
        self,
        to: List[str],
        dag_id: str,
        execution_id: str,
        error: str
    ) -> bool:
        """
        Send pipeline failure notification.

        Args:
            to: Recipient list
            dag_id: DAG ID
            execution_id: Execution ID
            error: Error message

        Returns:
            True if sent
        """
        subject = f"[APEX] Pipeline Failed: {dag_id}"
        body = f"""
Pipeline Failure Alert

DAG ID: {dag_id}
Execution ID: {execution_id}

Error:
{error}

Please investigate.
"""
        return self.send(to, subject, body)

    def send_pipeline_success(
        self,
        to: List[str],
        dag_id: str,
        execution_id: str,
        metrics: Dict[str, Any]
    ) -> bool:
        """
        Send pipeline success notification.

        Args:
            to: Recipient list
            dag_id: DAG ID
            execution_id: Execution ID
            metrics: Execution metrics

        Returns:
            True if sent
        """
        subject = f"[APEX] Pipeline Succeeded: {dag_id}"
        body = f"""
Pipeline Success

DAG ID: {dag_id}
Execution ID: {execution_id}

Metrics:
{json.dumps(metrics, indent=2)}
"""
        return self.send(to, subject, body)


class SlackNotifier:
    """
    Slack notification sender.

    Sends Slack messages for pipeline events.
    """

    def __init__(self, webhook_url: str = None):
        """
        Initialize SlackNotifier.

        Args:
            webhook_url: Slack webhook URL
        """
        self.webhook_url = webhook_url

    def send(
        self,
        message: str,
        channel: str = None,
        attachments: List[Dict] = None
    ) -> bool:
        """
        Send Slack message.

        Args:
            message: Message text
            channel: Optional channel override
            attachments: Optional message attachments

        Returns:
            True if sent
        """
        if not self.webhook_url:
            return False

        try:
            import requests

            payload = {
                "text": message,
            }

            if channel:
                payload["channel"] = channel

            if attachments:
                payload["attachments"] = attachments

            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )

            return response.status_code == 200

        except Exception:
            return False


__all__ = [
    "EmailNotifier",
    "SlackNotifier",
]
