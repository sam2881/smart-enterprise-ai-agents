"""
APEX Data Plane - Database Source Handler

Complete DATA PLANE database source handler for the APEX pipeline framework.
Provides JDBC connection management, extraction query building, CDC support,
Spark read configuration, and partition-based parallel reads for all supported
RDBMS types (PostgreSQL, MySQL, Oracle, SQL Server, DB2, Teradata, Snowflake,
BigQuery).

This module is consumed by generated Spark jobs and Airflow DAG utilities.
All functions produce deterministic, auditable output suitable for data plane
execution -- no decision logic lives here.

Supported extraction modes:
- FULL: Complete table extraction
- INCREMENTAL: Watermark-based delta extraction
- CDC: Change Data Capture via system-versioned tables or CDC tables
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# =============================================================================
# JDBC Driver Registry
# =============================================================================

JDBC_DRIVER_MAP: Dict[str, str] = {
    "postgres": "org.postgresql.Driver",
    "mysql": "com.mysql.cj.jdbc.Driver",
    "oracle": "oracle.jdbc.OracleDriver",
    "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "db2": "com.ibm.db2.jcc.DB2Driver",
    "teradata": "com.teradata.jdbc.TeraDriver",
    "snowflake": "net.snowflake.client.jdbc.SnowflakeDriver",
    "bigquery": "com.simba.googlebigquery.jdbc42.Driver",
}

# Default JDBC ports per database type
DEFAULT_PORT_MAP: Dict[str, int] = {
    "postgres": 5432,
    "mysql": 3306,
    "oracle": 1521,
    "sqlserver": 1433,
    "db2": 50000,
    "teradata": 1025,
    "snowflake": 443,
    "bigquery": 443,
}


# =============================================================================
# Standalone Functions (original + new)
# =============================================================================


def get_jdbc_connection(
    connection_id: str,
    metadata_client: Any,
) -> Dict[str, Any]:
    """
    Get JDBC connection details from the connection registry.

    Resolution order:
    1. Query the metadata_client connection registry by connection_id.
    2. Retrieve credentials from Google Cloud Secret Manager.
    3. Fallback: return a shell dict with the connection_id so callers can
       detect missing configuration and fail fast.

    Args:
        connection_id: Registered connection identifier (e.g. 'prod_postgres_main').
        metadata_client: MetadataClient instance with ``get_connection(id)``
            and ``get_secret(name)`` methods.

    Returns:
        Dictionary with keys: connection_id, db_type, host, port, database,
        user, password, driver, url, extra_params.  Missing fields default
        to empty strings so downstream code can validate explicitly.
    """
    # Attempt registry lookup
    connection_record: Optional[Dict[str, Any]] = None
    try:
        if hasattr(metadata_client, "get_connection"):
            connection_record = metadata_client.get_connection(connection_id)
            logger.debug("connection_registry_hit", extra={"connection_id": connection_id})
    except Exception as exc:
        logger.warning(
            "connection_registry_lookup_failed",
            extra={"connection_id": connection_id, "error": str(exc)},
        )

    if connection_record:
        db_type = connection_record.get("db_type", "postgres")
        driver = connection_record.get(
            "driver", JDBC_DRIVER_MAP.get(db_type, "")
        )
        host = connection_record.get("host", "")
        port = connection_record.get("port", DEFAULT_PORT_MAP.get(db_type, 5432))
        database = connection_record.get("database", "")
        extra_params = connection_record.get("extra_params", {})

        # Retrieve credentials from Secret Manager with fallback to record
        user = connection_record.get("user", "")
        password = ""
        secret_name = connection_record.get("secret_name")
        if secret_name:
            try:
                if hasattr(metadata_client, "get_secret"):
                    secret_payload = metadata_client.get_secret(secret_name)
                    if isinstance(secret_payload, dict):
                        user = secret_payload.get("user", user)
                        password = secret_payload.get("password", "")
                    elif isinstance(secret_payload, str):
                        password = secret_payload
                    logger.debug("secret_resolved", extra={"secret_name": secret_name})
            except Exception as exc:
                logger.warning(
                    "secret_manager_lookup_failed",
                    extra={"secret_name": secret_name, "error": str(exc)},
                )

        url = _build_jdbc_url(db_type, host, port, database, extra_params)

        return {
            "connection_id": connection_id,
            "db_type": db_type,
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            "driver": driver,
            "url": url,
            "extra_params": extra_params,
        }

    # Fallback: return stub so callers fail fast with clear context
    logger.warning(
        "connection_not_found_returning_stub",
        extra={"connection_id": connection_id},
    )
    return {
        "connection_id": connection_id,
        "db_type": "",
        "host": "",
        "port": 0,
        "database": "",
        "user": "",
        "password": "",
        "driver": "",
        "url": "",
        "extra_params": {},
    }


def build_extraction_query(
    source_config: Dict[str, Any],
    extraction_mode: str,
    watermark_column: Optional[str] = None,
    last_watermark: Optional[str] = None,
    batch_size: int = 100000,
) -> str:
    """
    Build extraction query based on mode.

    Generates a deterministic SQL query string for one of three extraction
    strategies: full, incremental (watermark-based), or CDC.

    Args:
        source_config: Source configuration dictionary.  Expected keys:
            ``source_schema`` (default ``'public'``), ``source_table``,
            and optionally ``source_query`` for a custom override.
        extraction_mode: One of ``'full'``, ``'incremental'``, or ``'cdc'``.
        watermark_column: Column name used for incremental tracking
            (required when extraction_mode is ``'incremental'``).
        last_watermark: Last successfully processed watermark value.
            When ``None`` for incremental mode, returns the first batch
            ordered by the watermark column.
        batch_size: Maximum number of records per batch (default 100 000).

    Returns:
        SQL query string ready for JDBC execution.
    """
    # Allow a custom query override
    custom_query = source_config.get("source_query")
    if custom_query:
        return custom_query

    schema = source_config.get("source_schema", "public")
    table = source_config.get("source_table", "")

    if extraction_mode == "full":
        return f"SELECT * FROM {schema}.{table}"

    elif extraction_mode == "incremental":
        if last_watermark:
            return (
                f"SELECT * FROM {schema}.{table}"
                f" WHERE {watermark_column} > '{last_watermark}'"
                f" ORDER BY {watermark_column}"
                f" LIMIT {batch_size}"
            )
        else:
            return (
                f"SELECT * FROM {schema}.{table}"
                f" ORDER BY {watermark_column}"
                f" LIMIT {batch_size}"
            )

    elif extraction_mode == "cdc":
        return (
            f"SELECT * FROM {schema}.{table}_cdc"
            f" WHERE _commit_timestamp > '{last_watermark or '1970-01-01'}'"
            f" ORDER BY _commit_timestamp"
        )

    return f"SELECT * FROM {schema}.{table}"


# =============================================================================
# CDC Helper Functions
# =============================================================================


def build_cdc_query(
    schema: str,
    table: str,
    cdc_column: str,
    last_value: Optional[str] = None,
    operation_types: Optional[List[str]] = None,
) -> str:
    """
    Build a CDC (Change Data Capture) extraction SQL query.

    Targets database-native CDC tables (e.g. SQL Server CDC, PostgreSQL logical
    replication output tables) where each row carries an operation type column
    and a monotonically increasing change-tracking column.

    Args:
        schema: Database schema name (e.g. ``'cdc'``, ``'dbo'``).
        table: CDC table name (typically ``<base_table>_ct`` or
            ``<base_table>_cdc``).
        cdc_column: Column that tracks change ordering (e.g.
            ``'__$seqval'``, ``'_commit_timestamp'``, ``'lsn'``).
        last_value: Last processed value for the ``cdc_column``.  When
            ``None``, all available changes are extracted.
        operation_types: Optional list of CDC operation types to include
            (e.g. ``['INSERT', 'UPDATE']``).  When ``None``, all operation
            types are returned.

    Returns:
        SQL query string targeting the CDC table.
    """
    base = f"SELECT * FROM {schema}.{table}"
    predicates: List[str] = []

    if last_value is not None:
        predicates.append(f"{cdc_column} > '{last_value}'")

    if operation_types:
        formatted = ", ".join(f"'{op}'" for op in operation_types)
        predicates.append(f"__$operation IN ({formatted})")

    if predicates:
        base += " WHERE " + " AND ".join(predicates)

    base += f" ORDER BY {cdc_column}"
    return base


def get_cdc_metadata(
    connection_config: Dict[str, Any],
    schema: str,
    table: str,
) -> Dict[str, Any]:
    """
    Retrieve CDC availability metadata for a given table.

    In production this queries the database's system catalog to determine
    whether CDC is enabled and which tracking columns / tables are available.
    The current implementation returns a best-effort metadata dictionary based
    on the ``db_type`` in the connection configuration.

    Args:
        connection_config: Connection details dictionary (as returned by
            ``get_jdbc_connection``).  Must contain ``db_type``.
        schema: Database schema name.
        table: Base table name (not the CDC table).

    Returns:
        Dictionary with keys:
        - ``cdc_available`` (bool): Whether CDC can be used.
        - ``cdc_table``: Name of the corresponding CDC table.
        - ``cdc_column``: Tracking column name.
        - ``supported_operations``: List of tracked operation types.
        - ``db_type``: Database type string.
    """
    db_type = connection_config.get("db_type", "")

    # Database-specific CDC conventions
    cdc_conventions: Dict[str, Dict[str, Any]] = {
        "postgres": {
            "cdc_table": f"{schema}.{table}_cdc",
            "cdc_column": "_commit_timestamp",
            "supported_operations": ["INSERT", "UPDATE", "DELETE"],
        },
        "mysql": {
            "cdc_table": f"{schema}.{table}_binlog",
            "cdc_column": "_binlog_position",
            "supported_operations": ["INSERT", "UPDATE", "DELETE"],
        },
        "sqlserver": {
            "cdc_table": f"cdc.{schema}_{table}_CT",
            "cdc_column": "__$seqval",
            "supported_operations": ["INSERT", "UPDATE", "DELETE"],
        },
        "oracle": {
            "cdc_table": f"{schema}.{table}_LOG",
            "cdc_column": "ORA_ROWSCN",
            "supported_operations": ["INSERT", "UPDATE", "DELETE"],
        },
        "db2": {
            "cdc_table": f"ASN.{schema}_{table}_CD",
            "cdc_column": "IBMSNAP_COMMITSEQ",
            "supported_operations": ["INSERT", "UPDATE", "DELETE"],
        },
    }

    convention = cdc_conventions.get(db_type)
    if convention:
        return {
            "cdc_available": True,
            "cdc_table": convention["cdc_table"],
            "cdc_column": convention["cdc_column"],
            "supported_operations": convention["supported_operations"],
            "db_type": db_type,
        }

    return {
        "cdc_available": False,
        "cdc_table": "",
        "cdc_column": "",
        "supported_operations": [],
        "db_type": db_type,
    }


# =============================================================================
# DatabaseSourceHandler Class
# =============================================================================


class DatabaseSourceHandler:
    """
    Comprehensive JDBC / database source handler for APEX data plane execution.

    Encapsulates all configuration needed to read from an RDBMS via Spark JDBC,
    including connection URL construction, driver selection, partition-based
    parallel reads, and predicate pushdown.

    Usage::

        handler = DatabaseSourceHandler({
            "db_type": "postgres",
            "host": "10.0.0.1",
            "port": 5432,
            "database": "analytics",
            "user": "etl_user",
            "password": "***",
        })

        spark_opts = handler.get_spark_jdbc_options()
        spark.read.format("jdbc").options(**spark_opts).load()

    Attributes:
        connection_config: Raw connection configuration dictionary.
    """

    def __init__(self, connection_config: Dict[str, Any]) -> None:
        """
        Initialize the handler with a connection configuration dictionary.

        Args:
            connection_config: Dictionary with database connection details.
                Expected keys: ``db_type``, ``host``, ``port``, ``database``,
                ``user``, ``password``.  Optional: ``driver``, ``fetchsize``,
                ``extra_params``, ``url`` (pre-built JDBC URL override).
        """
        self.connection_config = connection_config

    # -----------------------------------------------------------------
    # JDBC URL & Driver helpers
    # -----------------------------------------------------------------

    @staticmethod
    def get_driver_class(db_type: str) -> str:
        """
        Return the fully-qualified JDBC driver class for a database type.

        Args:
            db_type: Database type key (e.g. ``'postgres'``, ``'mysql'``,
                ``'oracle'``, ``'sqlserver'``, ``'db2'``, ``'teradata'``,
                ``'snowflake'``, ``'bigquery'``).

        Returns:
            JDBC driver class string.

        Raises:
            ValueError: If ``db_type`` is not in the supported driver map.
        """
        driver = JDBC_DRIVER_MAP.get(db_type)
        if driver is None:
            raise ValueError(
                f"Unsupported database type '{db_type}'. "
                f"Supported types: {', '.join(sorted(JDBC_DRIVER_MAP))}"
            )
        return driver

    @staticmethod
    def get_connection_url(
        db_type: str,
        host: str,
        port: int,
        database: str,
        extra_params: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Build a JDBC connection URL for the given database type.

        Args:
            db_type: Database type key.
            host: Database host or IP address.
            port: TCP port number.
            database: Database / catalog / service name.
            extra_params: Additional JDBC URL parameters appended as
                query-string key=value pairs (or semicolon-separated for
                SQL Server).

        Returns:
            Fully-formed JDBC URL string.

        Raises:
            ValueError: If ``db_type`` is not supported.
        """
        return _build_jdbc_url(db_type, host, port, database, extra_params)

    # -----------------------------------------------------------------
    # Spark JDBC options
    # -----------------------------------------------------------------

    def get_spark_jdbc_options(self) -> Dict[str, Any]:
        """
        Build the complete Spark JDBC read-options dictionary.

        The returned dict can be unpacked directly into
        ``spark.read.format("jdbc").options(**opts).load()``.

        Keys produced:
        - ``url``: JDBC connection URL.
        - ``driver``: JDBC driver class.
        - ``user``: Database user.
        - ``password``: Database password.
        - ``fetchsize``: Row fetch size (default 10 000).
        - ``isolationLevel``: JDBC transaction isolation (default ``READ_COMMITTED``).

        Returns:
            Dictionary of Spark JDBC options.
        """
        cfg = self.connection_config
        db_type = cfg.get("db_type", "postgres")

        # Prefer pre-built URL, otherwise construct one
        url = cfg.get("url") or _build_jdbc_url(
            db_type,
            cfg.get("host", "localhost"),
            cfg.get("port", DEFAULT_PORT_MAP.get(db_type, 5432)),
            cfg.get("database", ""),
            cfg.get("extra_params"),
        )

        driver = cfg.get("driver") or JDBC_DRIVER_MAP.get(db_type, "")
        fetchsize = cfg.get("fetchsize", 10000)

        options: Dict[str, Any] = {
            "url": url,
            "driver": driver,
            "user": cfg.get("user", ""),
            "password": cfg.get("password", ""),
            "fetchsize": str(fetchsize),
            "isolationLevel": cfg.get("isolation_level", "READ_COMMITTED"),
        }

        # Forward any extra Spark-level JDBC options the caller supplied
        for key in ("queryTimeout", "sessionInitStatement", "customSchema"):
            if key in cfg:
                options[key] = cfg[key]

        return options

    # -----------------------------------------------------------------
    # Partition-based parallel reads
    # -----------------------------------------------------------------

    @staticmethod
    def build_partition_options(
        partition_column: str,
        num_partitions: int,
        lower_bound: int,
        upper_bound: int,
    ) -> Dict[str, Any]:
        """
        Build Spark JDBC partition options for parallel reads.

        These options cause Spark to issue ``num_partitions`` concurrent
        queries, each with a ``WHERE partition_column BETWEEN ...`` predicate,
        dividing the ``[lower_bound, upper_bound]`` range evenly.

        Args:
            partition_column: Integer column to partition on (e.g.
                a primary-key or surrogate-key column).
            num_partitions: Number of parallel readers / partitions.
            lower_bound: Minimum value of the partition column
                (inclusive, used for stride calculation only).
            upper_bound: Maximum value of the partition column
                (inclusive, used for stride calculation only).

        Returns:
            Dictionary with keys ``partitionColumn``, ``numPartitions``,
            ``lowerBound``, ``upperBound`` ready to merge into Spark
            JDBC options.
        """
        return {
            "partitionColumn": partition_column,
            "numPartitions": str(num_partitions),
            "lowerBound": str(lower_bound),
            "upperBound": str(upper_bound),
        }

    # -----------------------------------------------------------------
    # Predicate pushdown
    # -----------------------------------------------------------------

    @staticmethod
    def build_pushdown_query(
        base_query: str,
        predicates: List[str],
    ) -> str:
        """
        Wrap a base query with additional pushdown predicates.

        The predicates are combined with ``AND`` and the result is wrapped
        as a derived table so Spark's JDBC reader can accept it as the
        ``dbtable`` option.

        Args:
            base_query: Original SQL query (``SELECT ...``).
            predicates: List of SQL predicate strings to AND-combine
                (e.g. ``["region = 'US'", "amount > 0"]``).

        Returns:
            Query string wrapped as ``(SELECT * FROM (...) t WHERE ...) pushdown``
            suitable for use as a Spark JDBC ``dbtable``.
        """
        if not predicates:
            return f"({base_query}) pushdown"

        where_clause = " AND ".join(predicates)
        return f"(SELECT * FROM ({base_query}) t WHERE {where_clause}) pushdown"


# =============================================================================
# Convenience Function
# =============================================================================


def build_spark_read_config(
    source_config: Dict[str, Any],
    extraction_mode: str,
) -> Dict[str, Any]:
    """
    Build a complete Spark JDBC read configuration dictionary.

    Combines connection options, extraction query, and optional partition
    settings into a single dict ready for ``spark.read.format("jdbc").options(**config).load()``.

    This is the primary entry point for generated Spark jobs that need to
    read from a database source.

    Args:
        source_config: Source configuration dictionary with keys matching
            ``DatabaseSourceConfig`` fields: ``connection_id``, ``db_type``,
            ``host``, ``port``, ``database``, ``user``, ``password``,
            ``source_schema``, ``source_table``, ``extraction_mode``,
            ``watermark_column``, ``last_watermark``, ``batch_size``,
            and optionally ``partition_column``, ``num_partitions``,
            ``lower_bound``, ``upper_bound``, ``extra_params``.
        extraction_mode: One of ``'full'``, ``'incremental'``, or ``'cdc'``.

    Returns:
        Dictionary with all Spark JDBC options including ``dbtable``
        (the extraction query wrapped as a subquery), connection URL,
        driver, credentials, and optional partition settings.
    """
    db_type = source_config.get("db_type", "postgres")
    host = source_config.get("host", "localhost")
    port = source_config.get("port", DEFAULT_PORT_MAP.get(db_type, 5432))
    database = source_config.get("database", "")
    extra_params = source_config.get("extra_params")

    handler = DatabaseSourceHandler(source_config)
    options = handler.get_spark_jdbc_options()

    # Build extraction query
    query = build_extraction_query(
        source_config=source_config,
        extraction_mode=extraction_mode,
        watermark_column=source_config.get("watermark_column"),
        last_watermark=source_config.get("last_watermark"),
        batch_size=source_config.get("batch_size", 100000),
    )

    # Wrap query as subquery for Spark dbtable option
    options["dbtable"] = f"({query}) extraction"

    # Add partition options if configured
    partition_column = source_config.get("partition_column")
    if partition_column:
        num_partitions = source_config.get("num_partitions", 4)
        lower_bound = source_config.get("lower_bound", 0)
        upper_bound = source_config.get("upper_bound", 1000000)
        partition_opts = DatabaseSourceHandler.build_partition_options(
            partition_column=partition_column,
            num_partitions=num_partitions,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
        )
        options.update(partition_opts)

    return options


# =============================================================================
# Internal Helpers
# =============================================================================


def _build_jdbc_url(
    db_type: str,
    host: str,
    port: int,
    database: str,
    extra_params: Optional[Dict[str, str]] = None,
) -> str:
    """
    Internal helper to construct a JDBC URL.

    Args:
        db_type: Database type key.
        host: Hostname or IP.
        port: Port number.
        database: Database / service name.
        extra_params: Optional extra URL parameters.

    Returns:
        JDBC URL string.

    Raises:
        ValueError: If db_type is not supported.
    """
    param_str = ""
    if extra_params:
        if db_type == "sqlserver":
            # SQL Server uses semicolons
            param_str = ";" + ";".join(f"{k}={v}" for k, v in extra_params.items())
        elif db_type == "oracle":
            # Oracle TNS-style doesn't typically use query params;
            # append as properties if provided
            param_str = "?" + "&".join(f"{k}={v}" for k, v in extra_params.items())
        else:
            param_str = "?" + "&".join(f"{k}={v}" for k, v in extra_params.items())

    url_builders: Dict[str, str] = {
        "postgres": f"jdbc:postgresql://{host}:{port}/{database}{param_str}",
        "mysql": f"jdbc:mysql://{host}:{port}/{database}{param_str}",
        "oracle": f"jdbc:oracle:thin:@{host}:{port}:{database}{param_str}",
        "sqlserver": (
            f"jdbc:sqlserver://{host}:{port};databaseName={database}{param_str}"
        ),
        "db2": f"jdbc:db2://{host}:{port}/{database}{param_str}",
        "teradata": f"jdbc:teradata://{host}/DATABASE={database}{param_str}",
        "snowflake": f"jdbc:snowflake://{host}:{port}/?db={database}{param_str}",
        "bigquery": (
            f"jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443"
            f";ProjectId={database}{param_str}"
        ),
    }

    url = url_builders.get(db_type)
    if url is None:
        raise ValueError(
            f"Cannot build JDBC URL for unsupported db_type '{db_type}'. "
            f"Supported: {', '.join(sorted(url_builders))}"
        )
    return url


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Constants
    "JDBC_DRIVER_MAP",
    "DEFAULT_PORT_MAP",
    # Original standalone functions
    "get_jdbc_connection",
    "build_extraction_query",
    # CDC helpers
    "build_cdc_query",
    "get_cdc_metadata",
    # Handler class
    "DatabaseSourceHandler",
    # Convenience
    "build_spark_read_config",
]
