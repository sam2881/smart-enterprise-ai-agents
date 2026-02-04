"""
Event-Driven Pipeline Triggers

Enables pipelines to be triggered by external events instead of schedules.
Enterprise-grade real-time data processing feature.
"""

from typing import Dict, Any, List, Optional, Callable
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
import json


class TriggerType(str, Enum):
    """Types of event triggers."""
    FILE_ARRIVAL = "file_arrival"  # GCS file uploaded
    KAFKA_MESSAGE = "kafka_message"  # Kafka topic message
    PUBSUB_MESSAGE = "pubsub_message"  # Pub/Sub message
    WEBHOOK = "webhook"  # External HTTP webhook
    DATA_AVAILABILITY = "data_availability"  # Upstream table/view ready
    SCHEDULE = "schedule"  # Time-based (fallback)
    API_CALL = "api_call"  # Direct API invocation


@dataclass
class TriggerEvent:
    """Event that triggers a pipeline."""
    trigger_id: str
    trigger_type: TriggerType
    pipeline_id: str
    dag_id: str
    event_data: Dict[str, Any]
    triggered_at: datetime
    triggered_by: str  # System or user
    execution_date: Optional[str] = None
    conf: Optional[Dict[str, Any]] = None


class FileArrivalTrigger:
    """
    Triggers pipeline when file arrives in GCS.

    Uses Cloud Storage Notifications → Pub/Sub → Cloud Function → Airflow API
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        file_pattern: str = "*",
        dag_id: str = "",
        min_file_size_bytes: int = 0
    ):
        """
        Configure file arrival trigger.

        Args:
            bucket: GCS bucket name
            prefix: GCS prefix/folder
            file_pattern: Glob pattern (e.g., "*.csv")
            dag_id: Airflow DAG to trigger
            min_file_size_bytes: Minimum file size to trigger
        """
        self.bucket = bucket
        self.prefix = prefix
        self.file_pattern = file_pattern
        self.dag_id = dag_id
        self.min_file_size_bytes = min_file_size_bytes

    def should_trigger(self, event: Dict[str, Any]) -> bool:
        """
        Determine if event should trigger pipeline.

        Args:
            event: GCS notification event

        Returns:
            True if pipeline should be triggered
        """
        # Check bucket
        if event.get('bucket') != self.bucket:
            return False

        # Check prefix
        file_name = event.get('name', '')
        if not file_name.startswith(self.prefix):
            return False

        # Check pattern (simple glob)
        if self.file_pattern != "*":
            import fnmatch
            if not fnmatch.fnmatch(file_name, self.file_pattern):
                return False

        # Check file size
        file_size = event.get('size', 0)
        if file_size < self.min_file_size_bytes:
            return False

        return True

    def create_trigger_event(self, gcs_event: Dict[str, Any]) -> TriggerEvent:
        """
        Create trigger event from GCS notification.

        Args:
            gcs_event: GCS event payload

        Returns:
            TriggerEvent to send to Airflow
        """
        return TriggerEvent(
            trigger_id=gcs_event.get('id', ''),
            trigger_type=TriggerType.FILE_ARRIVAL,
            pipeline_id=self.dag_id,
            dag_id=self.dag_id,
            event_data={
                'bucket': gcs_event.get('bucket'),
                'file_path': gcs_event.get('name'),
                'file_size': gcs_event.get('size'),
                'content_type': gcs_event.get('contentType'),
                'created_at': gcs_event.get('timeCreated'),
                'md5_hash': gcs_event.get('md5Hash')
            },
            triggered_at=datetime.utcnow(),
            triggered_by='gcs_notification',
            execution_date=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
            conf={
                'gcs_file_path': f"gs://{gcs_event.get('bucket')}/{gcs_event.get('name')}",
                'trigger_type': 'file_arrival'
            }
        )


class KafkaTrigger:
    """
    Triggers pipeline based on Kafka messages.

    Requires Kafka consumer running that calls Airflow API on message.
    """

    def __init__(
        self,
        topic: str,
        consumer_group: str,
        dag_id: str,
        filter_key: Optional[str] = None,
        filter_value: Optional[str] = None
    ):
        """
        Configure Kafka trigger.

        Args:
            topic: Kafka topic to monitor
            consumer_group: Consumer group ID
            dag_id: Airflow DAG to trigger
            filter_key: Optional message key filter
            filter_value: Optional message value filter
        """
        self.topic = topic
        self.consumer_group = consumer_group
        self.dag_id = dag_id
        self.filter_key = filter_key
        self.filter_value = filter_value

    def should_trigger(self, message: Dict[str, Any]) -> bool:
        """Check if Kafka message should trigger pipeline."""
        if self.filter_key and self.filter_value:
            return message.get(self.filter_key) == self.filter_value
        return True

    def create_trigger_event(self, kafka_message: Dict[str, Any]) -> TriggerEvent:
        """Create trigger event from Kafka message."""
        return TriggerEvent(
            trigger_id=f"kafka_{kafka_message.get('offset', '')}",
            trigger_type=TriggerType.KAFKA_MESSAGE,
            pipeline_id=self.dag_id,
            dag_id=self.dag_id,
            event_data={
                'topic': self.topic,
                'partition': kafka_message.get('partition'),
                'offset': kafka_message.get('offset'),
                'key': kafka_message.get('key'),
                'value': kafka_message.get('value'),
                'timestamp': kafka_message.get('timestamp')
            },
            triggered_at=datetime.utcnow(),
            triggered_by='kafka_consumer',
            conf={
                'kafka_offset': kafka_message.get('offset'),
                'trigger_type': 'kafka_message'
            }
        )


class DataAvailabilityTrigger:
    """
    Triggers pipeline when upstream data becomes available.

    Checks if upstream table/view has new data before triggering downstream.
    """

    def __init__(
        self,
        upstream_dataset: str,
        upstream_table: str,
        dag_id: str,
        watermark_column: Optional[str] = None,
        check_interval_seconds: int = 300
    ):
        """
        Configure data availability trigger.

        Args:
            upstream_dataset: BigQuery dataset
            upstream_table: BigQuery table
            dag_id: Airflow DAG to trigger
            watermark_column: Column to check for new data
            check_interval_seconds: How often to check
        """
        self.upstream_dataset = upstream_dataset
        self.upstream_table = upstream_table
        self.dag_id = dag_id
        self.watermark_column = watermark_column
        self.check_interval_seconds = check_interval_seconds

    def check_data_availability(self, last_watermark: Any = None) -> Optional[Any]:
        """
        Check if new data is available.

        Args:
            last_watermark: Last processed watermark value

        Returns:
            New watermark if data available, None otherwise
        """
        # Pseudo-code: would execute BigQuery query
        query = f"""
        SELECT MAX({self.watermark_column}) as max_watermark
        FROM `{self.upstream_dataset}.{self.upstream_table}`
        """
        if last_watermark:
            query += f" WHERE {self.watermark_column} > '{last_watermark}'"

        # Execute query and return result
        # In production: use BigQuery client
        return None  # Placeholder

    def create_trigger_event(self, new_watermark: Any) -> TriggerEvent:
        """Create trigger event when data is available."""
        return TriggerEvent(
            trigger_id=f"data_avail_{datetime.utcnow().timestamp()}",
            trigger_type=TriggerType.DATA_AVAILABILITY,
            pipeline_id=self.dag_id,
            dag_id=self.dag_id,
            event_data={
                'upstream_dataset': self.upstream_dataset,
                'upstream_table': self.upstream_table,
                'watermark_column': self.watermark_column,
                'new_watermark': str(new_watermark)
            },
            triggered_at=datetime.utcnow(),
            triggered_by='data_availability_sensor',
            conf={
                'watermark_value': str(new_watermark),
                'trigger_type': 'data_availability'
            }
        )


class WebhookTrigger:
    """
    Triggers pipeline via external webhook.

    External systems POST to webhook endpoint, which triggers Airflow DAG.
    """

    def __init__(
        self,
        dag_id: str,
        auth_token: Optional[str] = None,
        allowed_sources: Optional[List[str]] = None
    ):
        """
        Configure webhook trigger.

        Args:
            dag_id: Airflow DAG to trigger
            auth_token: Optional authentication token
            allowed_sources: Optional list of allowed source IPs
        """
        self.dag_id = dag_id
        self.auth_token = auth_token
        self.allowed_sources = allowed_sources or []

    def validate_request(self, request_headers: Dict[str, str], source_ip: str) -> bool:
        """Validate webhook request."""
        # Check auth token
        if self.auth_token:
            auth_header = request_headers.get('Authorization', '')
            if auth_header != f"Bearer {self.auth_token}":
                return False

        # Check source IP
        if self.allowed_sources and source_ip not in self.allowed_sources:
            return False

        return True

    def create_trigger_event(self, webhook_payload: Dict[str, Any]) -> TriggerEvent:
        """Create trigger event from webhook."""
        return TriggerEvent(
            trigger_id=webhook_payload.get('id', f"webhook_{datetime.utcnow().timestamp()}"),
            trigger_type=TriggerType.WEBHOOK,
            pipeline_id=self.dag_id,
            dag_id=self.dag_id,
            event_data=webhook_payload,
            triggered_at=datetime.utcnow(),
            triggered_by=webhook_payload.get('source', 'webhook'),
            conf={
                'webhook_payload': json.dumps(webhook_payload),
                'trigger_type': 'webhook'
            }
        )


class TriggerRegistry:
    """
    Registry of all configured triggers.

    Manages trigger configurations and routing.
    """

    def __init__(self):
        self.triggers: Dict[str, Any] = {}

    def register_trigger(self, trigger_id: str, trigger: Any):
        """Register a new trigger."""
        self.triggers[trigger_id] = trigger

    def get_trigger(self, trigger_id: str) -> Optional[Any]:
        """Get trigger by ID."""
        return self.triggers.get(trigger_id)

    def list_triggers(self, trigger_type: Optional[TriggerType] = None) -> List[Any]:
        """List all triggers, optionally filtered by type."""
        if trigger_type:
            return [t for t in self.triggers.values() if isinstance(t, self._get_trigger_class(trigger_type))]
        return list(self.triggers.values())

    def _get_trigger_class(self, trigger_type: TriggerType):
        """Get trigger class by type."""
        mapping = {
            TriggerType.FILE_ARRIVAL: FileArrivalTrigger,
            TriggerType.KAFKA_MESSAGE: KafkaTrigger,
            TriggerType.DATA_AVAILABILITY: DataAvailabilityTrigger,
            TriggerType.WEBHOOK: WebhookTrigger
        }
        return mapping.get(trigger_type)


# Global registry
trigger_registry = TriggerRegistry()
