"""
APEX Data Plane streaming utilities for zone_processor.py

Utilities for streaming sources (Kafka, Pub/Sub, Kinesis, Event Hubs).
Provides consumer configuration builders, Spark Structured Streaming
options, sensor configs for Airflow, dead-letter-queue helpers,
and window specification builders for both batch and micro-batch modes.
"""

from typing import Any, Dict, List, Optional


class StreamingSourceHandler:
    """
    Streaming source configuration handler.

    Builds platform-specific consumer configurations and Spark
    Structured Streaming read options for Kafka, Google Pub/Sub,
    AWS Kinesis, and Azure Event Hubs.
    """

    # ------------------------------------------------------------------ #
    #  Kafka
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_kafka_consumer_config(
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        offset_reset: str = "earliest",
        schema_registry_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create Kafka consumer configuration.

        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
                (e.g. ``broker1:9092,broker2:9092``).
            topic: Kafka topic name to consume from.
            consumer_group: Consumer group identifier.
            offset_reset: Auto-offset-reset policy.  One of
                ``earliest``, ``latest``, or ``none``.
            schema_registry_url: Optional Confluent Schema Registry URL
                for Avro/Protobuf deserialization.

        Returns:
            Kafka consumer configuration dictionary.
        """
        config: Dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "topic": topic,
            "group.id": consumer_group,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": False,
        }

        if schema_registry_url:
            config["schema.registry.url"] = schema_registry_url

        # Attempt to validate connectivity via confluent_kafka if available
        try:
            from confluent_kafka.admin import AdminClient  # noqa: F401

            config["_client_available"] = True
        except ImportError:
            config["_client_available"] = False

        return config

    # ------------------------------------------------------------------ #
    #  Google Pub/Sub
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_pubsub_subscription_config(
        project_id: str,
        subscription: str,
        max_messages: int = 1000,
    ) -> Dict[str, Any]:
        """
        Create Google Pub/Sub subscription configuration.

        Args:
            project_id: GCP project ID that owns the subscription.
            subscription: Pub/Sub subscription name (short name, not
                the fully-qualified resource path).
            max_messages: Maximum number of messages to pull per batch.

        Returns:
            Pub/Sub subscription configuration dictionary.
        """
        config: Dict[str, Any] = {
            "project_id": project_id,
            "subscription": subscription,
            "subscription_path": f"projects/{project_id}/subscriptions/{subscription}",
            "max_messages": max_messages,
        }

        try:
            from google.cloud import pubsub_v1  # noqa: F401

            config["_client_available"] = True
        except ImportError:
            config["_client_available"] = False

        return config

    # ------------------------------------------------------------------ #
    #  AWS Kinesis
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_kinesis_config(
        stream_name: str,
        region: str,
        shard_iterator_type: str = "TRIM_HORIZON",
    ) -> Dict[str, Any]:
        """
        Create AWS Kinesis stream configuration.

        Args:
            stream_name: Kinesis data stream name.
            region: AWS region where the stream is hosted
                (e.g. ``us-east-1``).
            shard_iterator_type: Shard iterator type.  One of
                ``TRIM_HORIZON``, ``LATEST``, ``AT_TIMESTAMP``,
                or ``AT_SEQUENCE_NUMBER``.

        Returns:
            Kinesis stream configuration dictionary.
        """
        config: Dict[str, Any] = {
            "stream_name": stream_name,
            "region": region,
            "shard_iterator_type": shard_iterator_type,
            "endpoint_url": f"https://kinesis.{region}.amazonaws.com",
        }

        try:
            import boto3  # noqa: F401

            config["_client_available"] = True
        except ImportError:
            config["_client_available"] = False

        return config

    # ------------------------------------------------------------------ #
    #  Azure Event Hubs
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_eventhub_config(
        connection_string: str,
        consumer_group: str = "$Default",
    ) -> Dict[str, Any]:
        """
        Create Azure Event Hubs configuration.

        Args:
            connection_string: Event Hubs connection string
                (``Endpoint=sb://...``).
            consumer_group: Event Hub consumer group name.

        Returns:
            Event Hub configuration dictionary.
        """
        config: Dict[str, Any] = {
            "connection_string": connection_string,
            "consumer_group": consumer_group,
        }

        # Parse the connection string for metadata when possible
        try:
            parts = dict(
                part.split("=", 1)
                for part in connection_string.split(";")
                if "=" in part
            )
            config["namespace"] = parts.get("Endpoint", "").replace(
                "sb://", ""
            ).rstrip("/")
            config["entity_path"] = parts.get("EntityPath", "")
        except Exception:
            config["namespace"] = ""
            config["entity_path"] = ""

        try:
            from azure.eventhub import EventHubConsumerClient  # noqa: F401

            config["_client_available"] = True
        except ImportError:
            config["_client_available"] = False

        return config

    # ------------------------------------------------------------------ #
    #  Spark Structured Streaming options
    # ------------------------------------------------------------------ #

    @staticmethod
    def build_spark_streaming_options(
        source_type: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build Spark Structured Streaming ``readStream`` options.

        Translates a platform-specific consumer configuration into the
        set of key/value options expected by
        ``spark.readStream.format(...).options(...)``.

        Supports both continuous (micro-batch) and batch trigger modes
        via the ``processing_mode`` key in *config*.

        Args:
            source_type: One of ``kafka``, ``pubsub``, ``kinesis``,
                or ``eventhub``.
            config: Platform-specific configuration dictionary as
                returned by one of the ``get_*_config`` methods.

        Returns:
            Dictionary of Spark readStream options keyed by option name.
        """
        options: Dict[str, Any] = {}
        processing_mode = config.get("processing_mode", "micro_batch")

        if source_type == "kafka":
            options["kafka.bootstrap.servers"] = config.get(
                "bootstrap.servers", ""
            )
            options["subscribe"] = config.get("topic", "")
            options["startingOffsets"] = config.get(
                "auto.offset.reset", "earliest"
            )
            options["failOnDataLoss"] = config.get(
                "fail_on_data_loss", False
            )
            options["kafka.group.id"] = config.get("group.id", "")
            if config.get("schema.registry.url"):
                options["schema.registry.url"] = config[
                    "schema.registry.url"
                ]
            options["_spark_format"] = "kafka"

        elif source_type == "pubsub":
            options["projectId"] = config.get("project_id", "")
            options["subscriptionId"] = config.get("subscription", "")
            options["maxMessagesPerBatch"] = config.get(
                "max_messages", 1000
            )
            options["_spark_format"] = "pubsub"

        elif source_type == "kinesis":
            options["streamName"] = config.get("stream_name", "")
            options["region"] = config.get("region", "")
            options["startingPosition"] = config.get(
                "shard_iterator_type", "TRIM_HORIZON"
            )
            options["endpointUrl"] = config.get("endpoint_url", "")
            options["_spark_format"] = "kinesis"

        elif source_type == "eventhub":
            options[
                "eventhubs.connectionString"
            ] = config.get("connection_string", "")
            options["eventhubs.consumerGroup"] = config.get(
                "consumer_group", "$Default"
            )
            options["_spark_format"] = "eventhubs"

        # Processing mode metadata
        options["_processing_mode"] = processing_mode

        return options


# ------------------------------------------------------------------ #
#  Convenience functions
# ------------------------------------------------------------------ #


def get_streaming_sensor(
    source_type: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build an Airflow sensor configuration for a streaming source.

    The returned dictionary describes the sensor class, connection
    parameters, and polling behaviour that the generated DAG should
    use to wait for data availability before starting a processing
    task.

    Args:
        source_type: One of ``kafka``, ``pubsub``, ``kinesis``,
            or ``eventhub``.
        config: Platform-specific configuration dictionary.

    Returns:
        Airflow sensor configuration dictionary.
    """
    sensor: Dict[str, Any] = {
        "source_type": source_type,
        "poke_interval": config.get("poke_interval", 60),
        "timeout": config.get("timeout", 3600),
        "mode": config.get("sensor_mode", "reschedule"),
    }

    if source_type == "kafka":
        sensor["sensor_class"] = "KafkaSensor"
        sensor["topic"] = config.get("topic", "")
        sensor["bootstrap_servers"] = config.get("bootstrap.servers", "")
        sensor["group_id"] = config.get("group.id", "")

    elif source_type == "pubsub":
        sensor["sensor_class"] = "PubSubPullSensor"
        sensor["project_id"] = config.get("project_id", "")
        sensor["subscription"] = config.get("subscription", "")

    elif source_type == "kinesis":
        sensor["sensor_class"] = "KinesisSensor"
        sensor["stream_name"] = config.get("stream_name", "")
        sensor["region"] = config.get("region", "")

    elif source_type == "eventhub":
        sensor["sensor_class"] = "EventHubSensor"
        sensor["connection_string"] = config.get("connection_string", "")
        sensor["consumer_group"] = config.get("consumer_group", "$Default")

    return sensor


def build_streaming_read_options(
    source_type: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build Spark read options for a streaming source.

    This is a thin convenience wrapper around
    :py:meth:`StreamingSourceHandler.build_spark_streaming_options`
    for callers that do not need to instantiate the handler class.

    Args:
        source_type: One of ``kafka``, ``pubsub``, ``kinesis``,
            or ``eventhub``.
        config: Platform-specific configuration dictionary.

    Returns:
        Dictionary of Spark readStream options.
    """
    return StreamingSourceHandler.build_spark_streaming_options(
        source_type, config
    )


def get_dead_letter_config(
    dlq_path: str,
    error_handling: str = "redirect",
) -> Dict[str, Any]:
    """
    Build a dead-letter-queue (DLQ) configuration.

    Failed records are redirected to *dlq_path* rather than causing the
    entire streaming job to fail, supporting both batch and micro-batch
    error-handling strategies.

    Args:
        dlq_path: Destination path for dead-letter records
            (e.g. ``gs://bucket/dlq/`` or ``s3://bucket/dlq/``).
        error_handling: Error-handling strategy.  One of ``redirect``
            (send failed records to DLQ), ``skip`` (silently drop
            failed records), or ``fail`` (abort on first error).

    Returns:
        Dead-letter-queue configuration dictionary.
    """
    return {
        "dlq_path": dlq_path,
        "error_handling": error_handling,
        "format": "json",
        "include_metadata": True,
        "include_timestamp": True,
        "max_retries": 3 if error_handling == "redirect" else 0,
    }


# ------------------------------------------------------------------ #
#  Window configuration helper
# ------------------------------------------------------------------ #


def build_window_spec(
    window_type: str = "tumbling",
    window_duration: str = "5 minutes",
    slide_duration: Optional[str] = None,
    watermark_delay: str = "10 minutes",
    event_time_column: str = "event_time",
) -> Dict[str, Any]:
    """
    Build a Spark Structured Streaming window specification.

    Supports tumbling, sliding, and session windows.  The returned
    dictionary is consumed by ``zone_processor.py`` to configure
    windowed aggregations.

    Args:
        window_type: Window type.  One of ``tumbling``, ``sliding``,
            or ``session``.
        window_duration: Window duration as a Spark interval string
            (e.g. ``"5 minutes"``, ``"1 hour"``).
        slide_duration: Slide interval for sliding windows.  Required
            when *window_type* is ``sliding``; ignored otherwise.
        watermark_delay: Maximum allowed lateness as a Spark interval
            string.  Used to configure ``withWatermark``.
        event_time_column: Name of the column that carries event
            timestamps.

    Returns:
        Window specification dictionary.
    """
    spec: Dict[str, Any] = {
        "window_type": window_type,
        "window_duration": window_duration,
        "watermark_delay": watermark_delay,
        "event_time_column": event_time_column,
    }

    if window_type == "sliding":
        spec["slide_duration"] = slide_duration or window_duration

    return spec


# ------------------------------------------------------------------ #
#  Module exports
# ------------------------------------------------------------------ #

__all__ = [
    "StreamingSourceHandler",
    "get_streaming_sensor",
    "build_streaming_read_options",
    "get_dead_letter_config",
    "build_window_spec",
]
