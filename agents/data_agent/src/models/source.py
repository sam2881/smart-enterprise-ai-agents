"""
Source configuration models matching pipeline_sources table.

Supports ALL source types:
- File: CSV, JSON, XML, Parquet, Avro, Excel
- EBCDIC/Copybook: Mainframe files with COBOL copybook
- Fixed-Width: Legacy fixed-width files
- Database: PostgreSQL, MySQL, Oracle, SQL Server, BigQuery
- Streaming: Kafka, Pub/Sub
- API: REST endpoints
- DTSX: SSIS package migration
"""

from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class SourceType(str, Enum):
    """
    Data source types matching schema.

    Classification per system prompt:
    A. File-Based Sources
    B. Database (RDBMS)
    C. Streaming Sources
    D. API / SaaS Sources
    E. Legacy / Enterprise Systems
    F. Semi-Structured / Nested
    G. Unstructured / Observability
    H. Cloud Storage / External Lakes
    I. Special / Advanced
    """
    # A. File-Based Sources
    FILE_CSV = "file_csv"
    FILE_TSV = "file_tsv"
    FILE_TXT = "file_txt"
    FILE_JSON = "file_json"
    FILE_NDJSON = "file_ndjson"
    FILE_XML = "file_xml"
    FILE_PARQUET = "file_parquet"
    FILE_AVRO = "file_avro"
    FILE_ORC = "file_orc"
    FILE_EXCEL = "file_excel"
    FILE_FIXED_WIDTH = "file_fixed_width"
    FILE_EBCDIC = "file_ebcdic"
    FILE_ZIP = "file_zip"
    FILE_GZIP = "file_gzip"

    # B. Database (RDBMS)
    DATABASE_POSTGRES = "database_postgres"
    DATABASE_MYSQL = "database_mysql"
    DATABASE_ORACLE = "database_oracle"
    DATABASE_SQLSERVER = "database_sqlserver"
    DATABASE_DB2 = "database_db2"
    DATABASE_TERADATA = "database_teradata"
    DATABASE_SNOWFLAKE = "database_snowflake"
    DATABASE_BIGQUERY = "database_bigquery"
    DATABASE_SAP = "database_sap"

    # C. Streaming Sources
    STREAMING_KAFKA = "streaming_kafka"
    STREAMING_CONFLUENT = "streaming_confluent"
    STREAMING_KINESIS = "streaming_kinesis"
    STREAMING_PUBSUB = "streaming_pubsub"
    STREAMING_EVENTHUBS = "streaming_eventhubs"

    # D. API / SaaS Sources
    API_REST = "api_rest"
    API_GRAPHQL = "api_graphql"
    API_SOAP = "api_soap"
    SAAS_SALESFORCE = "saas_salesforce"
    SAAS_SERVICENOW = "saas_servicenow"
    SAAS_JIRA = "saas_jira"
    SAAS_WORKDAY = "saas_workday"
    SAAS_GA = "saas_google_analytics"

    # E. Legacy / Enterprise Systems
    LEGACY_DTSX = "legacy_dtsx"
    LEGACY_COBOL = "legacy_cobol"
    LEGACY_VSAM = "legacy_vsam"
    LEGACY_AS400 = "legacy_as400"
    LEGACY_MAINFRAME = "legacy_mainframe"

    # F. Semi-Structured / Nested
    NESTED_JSON = "nested_json"
    NESTED_XML = "nested_xml"
    NESTED_AVRO = "nested_avro"
    NESTED_PARQUET = "nested_parquet"

    # G. Unstructured / Observability
    LOGS_APPLICATION = "logs_application"
    LOGS_AIRFLOW = "logs_airflow"
    LOGS_SPARK = "logs_spark"
    LOGS_AUDIT = "logs_audit"
    LOGS_METRICS = "logs_metrics"

    # H. Cloud Storage / External Lakes
    STORAGE_S3 = "storage_s3"
    STORAGE_GCS = "storage_gcs"
    STORAGE_ADLS = "storage_adls"
    STORAGE_EXTERNAL = "storage_external"

    # I. Special / Advanced
    SPECIAL_IOT = "special_iot"
    SPECIAL_TIMESERIES = "special_timeseries"
    SPECIAL_GEOSPATIAL = "special_geospatial"
    SPECIAL_ML_FEATURES = "special_ml_features"
    SPECIAL_OPEN_DATA = "special_open_data"

    # Backward compatibility
    DTSX = "dtsx"


class SourceFormat(str, Enum):
    """File format types."""
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    XML = "xml"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    EXCEL = "excel"
    EBCDIC = "ebcdic"
    FIXED_WIDTH = "fixed_width"


class ExtractionMode(str, Enum):
    """Data extraction modes."""
    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"


# =============================================================================
# Source-Specific Configurations
# =============================================================================


class FileSourceConfig(BaseModel):
    """Configuration for file-based sources (CSV, JSON, Parquet, etc.)."""
    model_config = ConfigDict(extra="forbid")

    source_bucket: str = Field(..., description="GCS bucket name")
    source_prefix: str = Field(..., description="Path prefix within bucket")
    file_pattern: str = Field(default="*", description="Glob pattern for files")
    delimiter: str = Field(default=",", max_length=10)
    has_header: bool = Field(default=True)
    encoding: str = Field(default="utf-8", max_length=50)
    quote_char: str = Field(default='"')
    escape_char: str = Field(default="\\")
    null_value: str = Field(default="")
    compression: Optional[str] = Field(None, description="gzip, snappy, lz4")


class DatabaseSourceConfig(BaseModel):
    """Configuration for database sources."""
    model_config = ConfigDict(extra="forbid")

    connection_id: str = Field(..., description="Airflow connection ID")
    source_schema: str = Field(default="public")
    source_table: str = Field(...)
    source_query: Optional[str] = Field(None, description="Custom extraction query")
    extraction_mode: ExtractionMode = Field(default=ExtractionMode.FULL)
    watermark_column: Optional[str] = Field(None, description="Column for incremental loads")
    batch_size: int = Field(default=10000, ge=1000, le=1000000)


class StreamingSourceConfig(BaseModel):
    """Configuration for streaming sources (Kafka, Pub/Sub)."""
    model_config = ConfigDict(extra="forbid")

    # Kafka
    kafka_bootstrap_servers: Optional[str] = Field(None)
    kafka_topic: Optional[str] = Field(None)
    kafka_consumer_group: Optional[str] = Field(None)
    kafka_offset_reset: str = Field(default="earliest")

    # Pub/Sub
    pubsub_subscription: Optional[str] = Field(None)
    pubsub_project: Optional[str] = Field(None)

    # Common
    message_format: str = Field(default="json", description="json, avro, protobuf")
    schema_registry_url: Optional[str] = Field(None)

    # Windowing
    window_type: Optional[str] = Field(None, description="tumbling, sliding, session")
    window_duration: Optional[str] = Field(None, description="e.g. '5 minutes', '1 hour'")
    slide_duration: Optional[str] = Field(None, description="Slide interval for sliding windows")
    watermark_delay: Optional[str] = Field(None, description="e.g. '10 minutes' for late data tolerance")
    event_time_column: Optional[str] = Field(None, description="Column containing event timestamp")

    # Dead Letter Queue
    dlq_path: Optional[str] = Field(None, description="GCS path for failed/rejected records")


class APISourceConfig(BaseModel):
    """Configuration for REST API sources."""
    model_config = ConfigDict(extra="forbid")

    api_endpoint: str = Field(...)
    api_method: str = Field(default="GET")
    api_headers: Dict[str, str] = Field(default_factory=dict)
    api_auth_type: str = Field(default="bearer", description="bearer, basic, api_key, oauth2")
    api_auth_secret: str = Field(..., description="Secret Manager secret name")
    pagination_type: Optional[str] = Field(None, description="offset, cursor, page_number")
    pagination_key: Optional[str] = Field(None)
    rate_limit_rps: int = Field(default=10)
    response_path: str = Field(default="$", description="JSONPath to data array")


class EBCDICSourceConfig(BaseModel):
    """Configuration for EBCDIC/Copybook mainframe sources."""
    model_config = ConfigDict(extra="forbid")

    source_bucket: str = Field(...)
    source_prefix: str = Field(...)
    file_pattern: str = Field(default="*")

    # Copybook definition
    copybook_content: Optional[str] = Field(None, description="COBOL copybook content")
    copybook_path: Optional[str] = Field(None, description="GCS path to copybook file")

    # EBCDIC settings
    encoding: str = Field(default="cp037", description="EBCDIC code page")
    record_length: Optional[int] = Field(None, description="Fixed record length")
    is_variable_length: bool = Field(default=False)
    rdw_format: str = Field(default="IBM", description="Record Descriptor Word format")

    # Field definitions (parsed from copybook or provided)
    field_specs: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="[{name, start, length, type, decimal, comp}]"
    )


class DTSXSourceConfig(BaseModel):
    """Configuration for DTSX/SSIS package migration."""
    model_config = ConfigDict(extra="forbid")

    dtsx_location: str = Field(..., description="GCS path to .dtsx file")
    dtsx_package_name: str = Field(...)
    dataflow_name: Optional[str] = Field(None, description="Specific dataflow to extract")
    original_server: Optional[str] = Field(None)

    # Extracted configuration (populated by parser)
    extracted_sources: List[Dict[str, Any]] = Field(default_factory=list)
    extracted_transforms: List[Dict[str, Any]] = Field(default_factory=list)
    extracted_destinations: List[Dict[str, Any]] = Field(default_factory=list)
    column_mappings: List[Dict[str, Any]] = Field(default_factory=list)


# =============================================================================
# Unified Source Configuration
# =============================================================================


class SourceConfig(BaseModel):
    """
    Unified source configuration matching pipeline_sources table.

    All source types are normalized to this structure.
    """
    model_config = ConfigDict(from_attributes=True)

    source_id: Optional[UUID] = Field(default=None)
    pipeline_id: Optional[UUID] = Field(default=None)

    # Source classification
    source_type: SourceType
    source_format: Optional[SourceFormat] = Field(None)

    # Type-specific configuration (only one populated based on source_type)
    file_config: Optional[FileSourceConfig] = Field(None)
    database_config: Optional[DatabaseSourceConfig] = Field(None)
    streaming_config: Optional[StreamingSourceConfig] = Field(None)
    api_config: Optional[APISourceConfig] = Field(None)
    ebcdic_config: Optional[EBCDICSourceConfig] = Field(None)
    dtsx_config: Optional[DTSXSourceConfig] = Field(None)

    # JSONB field for additional config (matches schema)
    field_specs: List[Dict[str, Any]] = Field(default_factory=list)
    copybook: Dict[str, Any] = Field(default_factory=dict)
    record_length: Optional[int] = Field(None)

    def get_active_config(self) -> Optional[BaseModel]:
        """Return the active type-specific configuration."""
        config_map = {
            SourceType.FILE_CSV: self.file_config,
            SourceType.FILE_JSON: self.file_config,
            SourceType.FILE_XML: self.file_config,
            SourceType.FILE_PARQUET: self.file_config,
            SourceType.FILE_AVRO: self.file_config,
            SourceType.FILE_EXCEL: self.file_config,
            SourceType.FILE_FIXED_WIDTH: self.file_config,
            SourceType.FILE_EBCDIC: self.ebcdic_config,
            SourceType.DATABASE_POSTGRES: self.database_config,
            SourceType.DATABASE_MYSQL: self.database_config,
            SourceType.DATABASE_ORACLE: self.database_config,
            SourceType.DATABASE_SQLSERVER: self.database_config,
            SourceType.DATABASE_BIGQUERY: self.database_config,
            SourceType.STREAMING_KAFKA: self.streaming_config,
            SourceType.STREAMING_PUBSUB: self.streaming_config,
            SourceType.API_REST: self.api_config,
            SourceType.DTSX: self.dtsx_config,
        }
        return config_map.get(self.source_type)
