"""
Pub/Sub adapter — drop-in replacement for the Kafka producer/consumer.
Active when PUBSUB_ENABLED=true; callers never import this directly.
Use get_producer() and get_consumer() from this module's factory functions.
"""
import asyncio
import json
import logging
import os
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any, AsyncIterator, Callable, Dict, Optional

logger = logging.getLogger(__name__)

_TOPIC_MAP: Dict[str, str] = {
    "incident.created": "incident-created",
    "incident.enriched": "incident-enriched",
    "incident.plan_generated": "incident-plan-generated",
    "incident.requires_approval": "incident-requires-approval",
    "incident.approved": "incident-approved",
    "incident.rejected": "incident-rejected",
    "incident.executed": "incident-executed",
    "incident.verified": "incident-verified",
    "incident.close_requested": "incident-close-requested",
    "incident.close_execute": "incident-close-execute",
    "incident.closed": "incident-closed",
    "incident.postmortem_ready": "incident-postmortem-ready",
    "pipeline.requested": "pipeline-requested",
    "pipeline.planned": "pipeline-planned",
    "pipeline.generated": "pipeline-generated",
    "pipeline.validated": "pipeline-validated",
    "pipeline.requires_approval": "pipeline-requires-approval",
    "pipeline.approved": "pipeline-approved",
    "pipeline.rejected": "pipeline-rejected",
    "pipeline.deploy_execute": "pipeline-deploy-execute",
    "pipeline.deployed": "pipeline-deployed",
    "pipeline.completed": "pipeline-completed",
    "pipeline.failed": "pipeline-failed",
    "pipeline.mr_created": "pipeline-mr-created",
}


def _normalize_topic(topic: str) -> str:
    """Convert dot-notation topic names to hyphenated Pub/Sub topic names."""
    return _TOPIC_MAP.get(topic, topic.replace(".", "-"))


class PubSubProducer:
    """Async Pub/Sub producer. Interface mirrors the existing Kafka publish_event() usage."""

    def __init__(self) -> None:
        from google.cloud import pubsub_v1  # noqa: PLC0415
        self._client = pubsub_v1.PublisherClient()
        self._project = os.environ["GCP_PROJECT_ID"]

    def _topic_path(self, topic: str) -> str:
        return self._client.topic_path(self._project, _normalize_topic(topic))

    async def publish_event(self, topic: str, event: Dict[str, Any], **attrs: str) -> str:
        data = json.dumps(event, default=str).encode("utf-8")
        loop = asyncio.get_event_loop()
        future = self._client.publish(
            self._topic_path(topic),
            data,
            **{k: str(v) for k, v in attrs.items()},
        )
        try:
            message_id: str = await loop.run_in_executor(None, future.result, 30)
            logger.debug("Published to %s, message_id=%s", topic, message_id)
            return message_id
        except FuturesTimeoutError:
            logger.error("Pub/Sub publish timeout on topic %s", topic)
            raise

    async def close(self) -> None:
        self._client.transport.close()


class PubSubConsumer:
    """Streaming pull consumer. Calls handler for each message then acks."""

    def __init__(self, subscription_id: str, project_id: Optional[str] = None) -> None:
        from google.cloud import pubsub_v1  # noqa: PLC0415
        self._client = pubsub_v1.SubscriberClient()
        project = project_id or os.environ["GCP_PROJECT_ID"]
        self._subscription_path = self._client.subscription_path(project, subscription_id)

    def subscribe(
        self,
        handler: Callable[[Dict[str, Any]], None],
        max_messages: int = 10,
    ) -> None:
        """Blocking streaming pull. Run in a thread or process."""
        def _callback(message: Any) -> None:
            try:
                payload = json.loads(message.data.decode("utf-8"))
                handler(payload)
                message.ack()
            except Exception:
                logger.exception("Handler failed for message %s — nacking", message.message_id)
                message.nack()

        from google.cloud.pubsub_v1.types import FlowControl  # noqa: PLC0415
        flow_control = FlowControl(max_messages=max_messages)
        streaming_pull = self._client.subscribe(
            self._subscription_path,
            callback=_callback,
            flow_control=flow_control,
        )
        logger.info("Listening on %s", self._subscription_path)
        try:
            streaming_pull.result()
        except Exception:
            streaming_pull.cancel()
            streaming_pull.result()

    async def close(self) -> None:
        self._client.transport.close()


def get_producer() -> "PubSubProducer | Any":
    """
    Return a PubSubProducer when PUBSUB_ENABLED=true, otherwise fall back to
    the existing Kafka producer so local dev still works unchanged.
    """
    if os.environ.get("PUBSUB_ENABLED", "false").lower() == "true":
        return PubSubProducer()
    # Lazy import to avoid requiring kafka-python in GCP containers
    from backend.streaming.kafka_producer import KafkaEventProducer  # noqa: PLC0415
    return KafkaEventProducer()


def get_consumer(subscription_or_topic: str) -> "PubSubConsumer | Any":
    """
    Return a PubSubConsumer when PUBSUB_ENABLED=true, otherwise fall back to
    the existing Kafka consumer.
    """
    if os.environ.get("PUBSUB_ENABLED", "false").lower() == "true":
        return PubSubConsumer(subscription_or_topic)
    from backend.streaming.kafka_consumer import KafkaEventConsumer  # noqa: PLC0415
    return KafkaEventConsumer(subscription_or_topic)
