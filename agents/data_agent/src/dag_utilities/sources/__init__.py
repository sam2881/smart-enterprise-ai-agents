"""
DAG Utilities Sources Module

Source-specific utilities for APEX DAG patterns.
"""

from .file_source import (
    get_gcs_file_sensor,
    list_source_files,
    archive_files,
    create_gcs_file_sensor,
    count_source_files,
    download_to_pvc,
    upload_from_pvc,
)
from .database_source import (
    JDBC_DRIVER_MAP,
    DEFAULT_PORT_MAP,
    get_jdbc_connection,
    build_extraction_query,
    build_cdc_query,
    get_cdc_metadata,
    DatabaseSourceHandler,
    build_spark_read_config,
)
from .legacy_source import (
    DTSXParser,
    EBCDICDecoder,
    FixedWidthParser,
    COBOLCopybookParser,
    MainframeExtractHandler,
    EBCDIC_ENCODINGS,
)
from .streaming_source import (
    StreamingSourceHandler,
    get_streaming_sensor,
    build_streaming_read_options,
    get_dead_letter_config,
    build_window_spec,
)
from .api_source import APISourceHandler, SaaSSourceHandler, fetch_rest_api, fetch_graphql

__all__ = [
    # File sources
    "get_gcs_file_sensor",
    "list_source_files",
    "archive_files",
    "create_gcs_file_sensor",
    "count_source_files",
    "download_to_pvc",
    "upload_from_pvc",
    # Database sources
    "JDBC_DRIVER_MAP",
    "DEFAULT_PORT_MAP",
    "get_jdbc_connection",
    "build_extraction_query",
    "build_cdc_query",
    "get_cdc_metadata",
    "DatabaseSourceHandler",
    "build_spark_read_config",
    # Legacy sources
    "DTSXParser",
    "EBCDICDecoder",
    "FixedWidthParser",
    "COBOLCopybookParser",
    "MainframeExtractHandler",
    "EBCDIC_ENCODINGS",
    # Streaming sources
    "StreamingSourceHandler",
    "get_streaming_sensor",
    "build_streaming_read_options",
    "get_dead_letter_config",
    "build_window_spec",
    # API sources
    "APISourceHandler",
    "SaaSSourceHandler",
    "fetch_rest_api",
    "fetch_graphql",
]
