"""
Airflow MCP Server - Event-Driven DAG Orchestration.

WHY: Provides MCP interface for Airflow operations via Kafka events.
     LangGraph publishes airflow.trigger_dag → This server executes via Airflow REST API.
     This decouples LangGraph from direct Airflow API calls.

HOW:
  1. Consumes Kafka topic: airflow.trigger_dag
  2. Executes DAG trigger via Airflow REST API
  3. Monitors DAG run status
  4. Publishes result to: airflow.dag_completed

FLOW:
  LangGraph → Kafka (airflow.trigger_dag) → Airflow MCP → Airflow REST API
  Airflow MCP → Kafka (airflow.dag_completed) → LangGraph resumes

RULES:
  1. All Airflow operations go through this MCP server
  2. LangGraph never calls Airflow REST API directly
  3. Results published to Kafka for event-driven architecture
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional
import structlog
import httpx

# ---------------------------------------------------------------------------
# Prometheus metrics for Airflow MCP
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

    AIRFLOW_MCP_DAG_TRIGGERS = Counter(
        "aiagent_airflow_mcp_dag_triggers_total",
        "Total DAG trigger attempts via Airflow MCP",
        ["dag_id", "status"],
    )
    AIRFLOW_MCP_DAG_LATENCY = Histogram(
        "aiagent_airflow_mcp_dag_latency_seconds",
        "Airflow MCP DAG trigger-to-completion latency",
        ["dag_id"],
        buckets=[1, 5, 10, 30, 60, 120, 300, 600],
    )
    AIRFLOW_MCP_EVENTS = Counter(
        "aiagent_airflow_mcp_events_total",
        "Total Kafka events processed by Airflow MCP",
        ["topic", "status"],
    )
    _PROM_OK = True
except ImportError:
    _PROM_OK = False

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Inline Kafka producer (avoids import from backend which isn't on PYTHONPATH
# inside the MCP container)
# ---------------------------------------------------------------------------

class _InlineKafkaProducer:
    """Lightweight Kafka producer for the Airflow MCP server."""

    def __init__(self):
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ).replace("kafka://", "")
        self.mock_mode = os.getenv("MOCK_KAFKA", "false").lower() == "true"
        self.producer = None

        if self.mock_mode:
            logger.info("kafka_producer_mock_mode")
            return

        try:
            from kafka import KafkaProducer as _KP

            self.producer = _KP(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info("kafka_producer_initialized", servers=self.bootstrap_servers)
        except Exception as e:
            logger.error("kafka_producer_init_failed", error=str(e))
            self.mock_mode = True

    async def publish_event(self, topic: str, event: dict, key: str = None) -> bool:
        if "timestamp" not in event:
            event["timestamp"] = datetime.utcnow().isoformat()
        if self.mock_mode or not self.producer:
            logger.info("kafka_mock_publish", topic=topic, key=key)
            return True
        try:
            future = self.producer.send(topic=topic, value=event, key=key)
            future.get(timeout=10)
            return True
        except Exception as e:
            logger.error("kafka_publish_error", topic=topic, error=str(e))
            return False


_airflow_producer = _InlineKafkaProducer()


def get_producer():
    return _airflow_producer


class AirflowMCPServer:
    """
    Airflow MCP Server for event-driven DAG orchestration.

    Consumes:
        - airflow.trigger_dag: Trigger a DAG run
        - airflow.get_dag_status: Get status of a DAG run
        - airflow.retry_dag: Retry a failed DAG

    Publishes:
        - airflow.dag_completed: DAG run completed (success or failure)
    """

    def __init__(self):
        self.airflow_url = os.getenv("AIRFLOW_API_URL", "http://localhost:8083")
        self.airflow_username = os.getenv("AIRFLOW_USERNAME", "admin")
        self.airflow_password = os.getenv("AIRFLOW_PASSWORD", "admin")
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

        # Topics
        self.trigger_topic = "airflow.trigger_dag"
        self.completed_topic = "airflow.dag_completed"
        self.status_topic = "airflow.get_dag_status"
        self.retry_topic = "airflow.retry_dag"

        # HTTP client for Airflow API
        self._http_client: Optional[httpx.AsyncClient] = None

        # Kafka producer
        self._producer = None

        logger.info(
            "AirflowMCPServer initialized",
            airflow_url=self.airflow_url,
            kafka_servers=self.kafka_servers
        )

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client for Airflow API."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.airflow_url,
                auth=(self.airflow_username, self.airflow_password),
                timeout=30.0
            )
        return self._http_client

    async def _get_producer(self):
        """Get Kafka producer."""
        if self._producer is None:
            self._producer = get_producer()
        return self._producer

    async def trigger_dag(
        self,
        dag_id: str,
        conf: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
        correlation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Trigger a DAG run via Airflow REST API.

        Args:
            dag_id: ID of the DAG to trigger
            conf: Optional configuration to pass to the DAG
            run_id: Optional run ID (auto-generated if not provided)
            correlation_id: Correlation ID for tracking

        Returns:
            Dict with dag_run_id, status, and execution details
        """
        logger.info("Triggering DAG", dag_id=dag_id, correlation_id=correlation_id)

        client = await self._get_http_client()

        # Prepare request body
        payload = {
            "conf": conf or {},
        }

        if run_id:
            payload["dag_run_id"] = run_id

        try:
            response = await client.post(
                f"/api/v1/dags/{dag_id}/dagRuns",
                json=payload
            )

            if response.status_code == 200:
                result = response.json()
                dag_run_id = result.get("dag_run_id")

                logger.info(
                    "DAG triggered successfully",
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    correlation_id=correlation_id
                )

                if _PROM_OK:
                    AIRFLOW_MCP_DAG_TRIGGERS.labels(dag_id=dag_id, status="success").inc()

                return {
                    "success": True,
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "state": result.get("state", "queued"),
                    "execution_date": result.get("execution_date"),
                    "correlation_id": correlation_id
                }
            else:
                error_msg = f"Failed to trigger DAG: {response.status_code} - {response.text}"
                logger.error(error_msg, dag_id=dag_id)
                if _PROM_OK:
                    AIRFLOW_MCP_DAG_TRIGGERS.labels(dag_id=dag_id, status="error").inc()
                return {
                    "success": False,
                    "dag_id": dag_id,
                    "error": error_msg,
                    "correlation_id": correlation_id
                }

        except Exception as e:
            error_msg = f"Exception triggering DAG: {str(e)}"
            logger.error(error_msg, dag_id=dag_id, error=str(e))
            if _PROM_OK:
                AIRFLOW_MCP_DAG_TRIGGERS.labels(dag_id=dag_id, status="exception").inc()
            return {
                "success": False,
                "dag_id": dag_id,
                "error": error_msg,
                "correlation_id": correlation_id
            }

    async def get_dag_run_status(
        self,
        dag_id: str,
        dag_run_id: str
    ) -> Dict[str, Any]:
        """
        Get the status of a DAG run.

        Args:
            dag_id: ID of the DAG
            dag_run_id: ID of the DAG run

        Returns:
            Dict with current status and details
        """
        client = await self._get_http_client()

        try:
            response = await client.get(
                f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
            )

            if response.status_code == 200:
                result = response.json()
                return {
                    "success": True,
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "state": result.get("state"),
                    "execution_date": result.get("execution_date"),
                    "start_date": result.get("start_date"),
                    "end_date": result.get("end_date"),
                }
            else:
                return {
                    "success": False,
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "error": f"Failed to get status: {response.status_code}"
                }

        except Exception as e:
            return {
                "success": False,
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "error": str(e)
            }

    async def wait_for_dag_completion(
        self,
        dag_id: str,
        dag_run_id: str,
        timeout_seconds: int = 600,
        poll_interval: int = 10
    ) -> Dict[str, Any]:
        """
        Wait for a DAG run to complete.

        Args:
            dag_id: ID of the DAG
            dag_run_id: ID of the DAG run
            timeout_seconds: Maximum time to wait
            poll_interval: Seconds between status checks

        Returns:
            Final status of the DAG run
        """
        start_time = datetime.utcnow()
        terminal_states = ["success", "failed", "upstream_failed"]

        while True:
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if elapsed > timeout_seconds:
                return {
                    "success": False,
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "state": "timeout",
                    "error": f"DAG run timed out after {timeout_seconds} seconds"
                }

            status = await self.get_dag_run_status(dag_id, dag_run_id)

            if not status.get("success"):
                return status

            state = status.get("state", "").lower()

            if state in terminal_states:
                logger.info(
                    "DAG run completed",
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    state=state,
                    elapsed_seconds=elapsed
                )
                return {
                    "success": state == "success",
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "state": state,
                    "start_date": status.get("start_date"),
                    "end_date": status.get("end_date"),
                    "elapsed_seconds": elapsed
                }

            await asyncio.sleep(poll_interval)

    async def retry_failed_task(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str
    ) -> Dict[str, Any]:
        """
        Clear and retry a failed task.

        Args:
            dag_id: ID of the DAG
            dag_run_id: ID of the DAG run
            task_id: ID of the task to retry

        Returns:
            Result of the retry operation
        """
        logger.info(
            "Retrying failed task",
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id
        )

        client = await self._get_http_client()

        try:
            # Clear the task instance to retry
            response = await client.post(
                f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/clear",
                json={"dry_run": False}
            )

            if response.status_code == 200:
                logger.info(
                    "Task cleared for retry",
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    task_id=task_id
                )
                return {
                    "success": True,
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "task_id": task_id,
                    "action": "cleared_for_retry"
                }
            else:
                return {
                    "success": False,
                    "dag_id": dag_id,
                    "error": f"Failed to clear task: {response.status_code}"
                }

        except Exception as e:
            return {
                "success": False,
                "dag_id": dag_id,
                "error": str(e)
            }

    async def handle_trigger_event(self, event: Dict[str, Any]) -> None:
        """
        Handle airflow.trigger_dag event from Kafka.

        Expected event format:
        {
            "dag_id": "my_dag",
            "conf": {"key": "value"},
            "correlation_id": "incident-123",
            "incident_id": "INC001",
            "timeout_seconds": 300,
            "wait_for_completion": true
        }
        """
        dag_id = event.get("dag_id")
        conf = event.get("conf", {})
        correlation_id = event.get("correlation_id", "")
        incident_id = event.get("incident_id", "")
        timeout = event.get("timeout_seconds", 600)
        wait_for_completion = event.get("wait_for_completion", True)

        logger.info(
            "Processing trigger_dag event",
            dag_id=dag_id,
            correlation_id=correlation_id,
            incident_id=incident_id
        )

        # Trigger the DAG
        trigger_result = await self.trigger_dag(
            dag_id=dag_id,
            conf=conf,
            correlation_id=correlation_id
        )

        if not trigger_result.get("success"):
            # Publish failure event
            await self._publish_completion_event(
                dag_id=dag_id,
                dag_run_id=None,
                success=False,
                error=trigger_result.get("error"),
                correlation_id=correlation_id,
                incident_id=incident_id
            )
            return

        dag_run_id = trigger_result.get("dag_run_id")

        if wait_for_completion:
            # Wait for DAG to complete
            completion_result = await self.wait_for_dag_completion(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                timeout_seconds=timeout
            )

            # Publish completion event
            await self._publish_completion_event(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                success=completion_result.get("success", False),
                state=completion_result.get("state"),
                error=completion_result.get("error"),
                elapsed_seconds=completion_result.get("elapsed_seconds"),
                correlation_id=correlation_id,
                incident_id=incident_id
            )
        else:
            # Just publish that it was triggered
            await self._publish_completion_event(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                success=True,
                state="triggered",
                correlation_id=correlation_id,
                incident_id=incident_id
            )

    async def handle_retry_event(self, event: Dict[str, Any]) -> None:
        """
        Handle airflow.retry_dag event from Kafka.

        This is used for remediation when ServiceNow reports a DAG failure.

        Expected event format:
        {
            "dag_id": "my_dag",
            "dag_run_id": "manual__2024-01-01",
            "task_id": "failed_task",  # optional
            "correlation_id": "incident-123",
            "incident_id": "INC001"
        }
        """
        dag_id = event.get("dag_id")
        dag_run_id = event.get("dag_run_id")
        task_id = event.get("task_id")
        correlation_id = event.get("correlation_id", "")
        incident_id = event.get("incident_id", "")

        logger.info(
            "Processing retry_dag event",
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            incident_id=incident_id
        )

        if task_id:
            # Retry specific task
            result = await self.retry_failed_task(dag_id, dag_run_id, task_id)
        else:
            # Clear entire DAG run and re-trigger
            result = await self.trigger_dag(
                dag_id=dag_id,
                conf={"retry_of": dag_run_id, "incident_id": incident_id},
                correlation_id=correlation_id
            )

        if result.get("success"):
            # Wait for completion
            new_dag_run_id = result.get("dag_run_id", dag_run_id)
            completion = await self.wait_for_dag_completion(
                dag_id=dag_id,
                dag_run_id=new_dag_run_id,
                timeout_seconds=600
            )

            await self._publish_completion_event(
                dag_id=dag_id,
                dag_run_id=new_dag_run_id,
                success=completion.get("success", False),
                state=completion.get("state"),
                error=completion.get("error"),
                elapsed_seconds=completion.get("elapsed_seconds"),
                correlation_id=correlation_id,
                incident_id=incident_id,
                is_retry=True
            )
        else:
            await self._publish_completion_event(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                success=False,
                error=result.get("error"),
                correlation_id=correlation_id,
                incident_id=incident_id,
                is_retry=True
            )

    async def _publish_completion_event(
        self,
        dag_id: str,
        dag_run_id: Optional[str],
        success: bool,
        state: Optional[str] = None,
        error: Optional[str] = None,
        elapsed_seconds: Optional[float] = None,
        correlation_id: Optional[str] = None,
        incident_id: Optional[str] = None,
        is_retry: bool = False
    ) -> None:
        """Publish DAG completion event to Kafka."""
        producer = await self._get_producer()

        event = {
            "event_type": "airflow.dag_completed",
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "success": success,
            "state": state,
            "error": error,
            "elapsed_seconds": elapsed_seconds,
            "correlation_id": correlation_id,
            "incident_id": incident_id,
            "is_retry": is_retry,
            "timestamp": datetime.utcnow().isoformat()
        }

        await producer.publish_event(
            topic=self.completed_topic,
            event=event,
            key=correlation_id or dag_id
        )

        logger.info(
            "Published completion event",
            dag_id=dag_id,
            success=success,
            correlation_id=correlation_id
        )

    async def _run_health_server(self, port: int = 8006) -> None:
        """Run a lightweight HTTP health + metrics endpoint alongside the Kafka consumer."""
        async def _handle_http(reader, writer):
            raw = b""
            try:
                raw = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            except (asyncio.TimeoutError, Exception):
                pass

            request_line = raw.decode(errors="replace").split("\r\n")[0] if raw else ""

            # Serve Prometheus metrics at /metrics
            if "/metrics" in request_line and _PROM_OK:
                metrics_body = generate_latest()
                resp = (
                    f"HTTP/1.1 200 OK\r\n"
                    f"Content-Type: {CONTENT_TYPE_LATEST}\r\n"
                    f"Content-Length: {len(metrics_body)}\r\n"
                    f"\r\n"
                ).encode() + metrics_body
                writer.write(resp)
                await writer.drain()
                writer.close()
                return

            # Default: health endpoint
            body = (
                f'{{"status":"ok","service":"airflow-mcp",'
                f'"airflow_url":"{self.airflow_url}",'
                f'"kafka_connected":{str(self._consumer_started).lower()}}}'
            )
            resp = (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n"
                f"\r\n{body}"
            )
            writer.write(resp.encode())
            await writer.drain()
            writer.close()

        srv = await asyncio.start_server(_handle_http, "0.0.0.0", port)
        logger.info("airflow_mcp_health_server", port=port)
        async with srv:
            await srv.serve_forever()

    async def run(self) -> None:
        """
        Main run loop - consume Kafka events and process them.
        Also starts a health HTTP endpoint so Docker healthcheck works.
        """
        logger.info("Starting Airflow MCP Server")
        self._consumer_started = False

        # Start health server in background
        health_port = int(os.getenv("AIRFLOW_MCP_HEALTH_PORT", "8006"))
        health_task = asyncio.create_task(self._run_health_server(health_port))

        try:
            from aiokafka import AIOKafkaConsumer
        except ImportError:
            logger.error("aiokafka not installed. Install with: pip install aiokafka")
            return

        consumer = AIOKafkaConsumer(
            self.trigger_topic,
            self.retry_topic,
            bootstrap_servers=self.kafka_servers,
            group_id="airflow-mcp-server",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest"
        )

        await consumer.start()
        self._consumer_started = True
        logger.info(
            "Kafka consumer started",
            topics=[self.trigger_topic, self.retry_topic]
        )

        try:
            async for msg in consumer:
                try:
                    event = msg.value
                    topic = msg.topic

                    logger.info(
                        "Received event",
                        topic=topic,
                        event_type=event.get("event_type", "unknown")
                    )

                    if topic == self.trigger_topic:
                        await self.handle_trigger_event(event)
                    elif topic == self.retry_topic:
                        await self.handle_retry_event(event)
                    else:
                        logger.warning("Unknown topic", topic=topic)

                    if _PROM_OK:
                        AIRFLOW_MCP_EVENTS.labels(topic=topic, status="success").inc()

                except Exception as e:
                    logger.error(
                        "Error processing event",
                        error=str(e),
                        topic=msg.topic
                    )
                    if _PROM_OK:
                        AIRFLOW_MCP_EVENTS.labels(topic=msg.topic, status="error").inc()

        finally:
            await consumer.stop()
            health_task.cancel()
            if self._http_client:
                await self._http_client.aclose()
            logger.info("Airflow MCP Server stopped")


async def main():
    """Entry point for Airflow MCP Server."""
    server = AirflowMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
