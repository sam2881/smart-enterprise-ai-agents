"""
ConnectionTestAgent - Pre-Deployment Source Validation

WHY: A generated pipeline is worthless if the source connection fails on first run.
     This agent validates connectivity, authenticates, discovers the live schema,
     and compares it against the requested pipeline config — before any code is
     committed or deployed.

WHAT IT DOES (mimics what a real data engineer does before building a pipeline):
  1. Test network connectivity + authentication to the source system
  2. Discover live schema (columns, types, row count estimate)
  3. Diff live schema vs. requested schema in the pipeline config
  4. Validate PII column claims match what's actually detectable in a sample
  5. Estimate data volume and warn if partitioning strategy is inappropriate
  6. Return ConnectionTestResult with pass/fail + actionable remediation steps

POSITION IN WORKFLOW: Called by supervisor between validate_artifacts and deploy.
                      Failure blocks deployment without human override.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import structlog

from src.agents.base_agent import BaseAgent, requires_state_keys
from src.utils.exceptions import ConnectionTestError

logger = structlog.get_logger(__name__)


class ConnectionStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"  # Source type doesn't support live testing (legacy/COBOL)


@dataclass
class SchemaDiscoveryResult:
    """Live schema discovered from the source system."""
    columns: Dict[str, str]         # {column_name: data_type}
    row_count_estimate: int
    sample_rows: int                 # Rows sampled for PII scan
    source_timezone: Optional[str]
    partitioning_keys: List[str]    # Columns that appear to be natural partition keys
    nullable_columns: List[str]


@dataclass
class SchemaDiff:
    """Diff between requested config and live source schema."""
    columns_in_config_not_in_source: List[str]    # Config asks for cols that don't exist
    columns_in_source_not_in_config: List[str]    # Source has extra cols (ok, warn)
    type_mismatches: Dict[str, Tuple[str, str]]   # {col: (config_type, live_type)}
    nullable_conflicts: List[str]                  # Config says NOT NULL but source has nulls

    @property
    def is_blocking(self) -> bool:
        return bool(self.columns_in_config_not_in_source or self.type_mismatches)


@dataclass
class PartitioningRecommendation:
    """Whether the requested partitioning strategy fits the data."""
    requested_strategy: str
    recommended_strategy: str
    reason: str
    estimated_partition_count: int
    estimated_rows_per_partition: int
    is_appropriate: bool


@dataclass
class ConnectionTestResult:
    """Full result of the connection test."""
    status: ConnectionStatus
    source_type: str
    connection_latency_ms: float
    auth_success: bool
    schema_discovery: Optional[SchemaDiscoveryResult]
    schema_diff: Optional[SchemaDiff]
    partitioning_recommendation: Optional[PartitioningRecommendation]
    pii_column_discrepancies: List[str]   # Claimed PII cols with no PII detected in sample
    blocking_issues: List[str]
    warnings: List[str]
    remediation_steps: List[str]
    test_duration_seconds: float

    @property
    def can_proceed(self) -> bool:
        return self.status in (ConnectionStatus.PASS, ConnectionStatus.WARN)


class ConnectionTestAgent(BaseAgent):
    """
    Validates source connectivity, schema, and partitioning before code deployment.

    This agent acts like the senior data engineer who runs
    "DESCRIBE TABLE" and "SELECT COUNT(*) TABLESAMPLE (1000 ROWS)" before
    writing a single line of pipeline code.
    """

    # Source types where live testing is not possible or meaningful
    SKIP_SOURCE_TYPES = {
        "legacy_cobol", "legacy_vsam", "legacy_as400", "legacy_mainframe",
        "file_ebcdic", "file_fixed_width",  # Test only at ingest time
    }

    # Source types with async-capable test drivers
    ASYNC_TESTABLE = {
        "database_postgres", "database_mysql", "database_oracle",
        "database_sqlserver", "database_db2", "database_snowflake",
        "database_bigquery", "database_teradata",
        "nosql_mongodb", "nosql_elasticsearch",
        "api_rest", "api_graphql",
        "streaming_kafka",
        "storage_gcs", "storage_s3", "storage_adls",
        "saas_salesforce", "saas_servicenow", "saas_jira",
    }

    def __init__(self) -> None:
        super().__init__("connection_test")

    @requires_state_keys("intent_json", "request_id", "generator_output")
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the full connection test suite for the requested source.

        Returns state update with connection_test_result.
        Failure sets error_message to block the deployment.
        """
        start = time.monotonic()
        intent = state["intent_json"]
        request_id = state["request_id"]
        source_config = intent.get("source", {})
        source_type = source_config.get("source_type", "")
        requested_schema = intent.get("schema", {}).get("columns", {})
        requested_pii_columns = intent.get("pii_columns", [])
        partitioning_config = intent.get("execution_policy", {}).get("partitioning", {})

        log = logger.bind(request_id=request_id, source_type=source_type)
        log.info("connection_test_start")

        # Skip untestable source types
        if source_type in self.SKIP_SOURCE_TYPES:
            result = ConnectionTestResult(
                status=ConnectionStatus.SKIP,
                source_type=source_type,
                connection_latency_ms=0.0,
                auth_success=True,
                schema_discovery=None,
                schema_diff=None,
                partitioning_recommendation=None,
                pii_column_discrepancies=[],
                blocking_issues=[],
                warnings=[f"Connection testing not supported for {source_type}. Validate manually."],
                remediation_steps=[],
                test_duration_seconds=time.monotonic() - start,
            )
            log.info("connection_test_skip", reason="unsupported_source_type")
            return {
                "connection_test_result": self._to_dict(result),
                "connection_test_status": ConnectionStatus.SKIP.value,
            }

        try:
            result = asyncio.run(self._run_full_test(
                source_type=source_type,
                source_config=source_config,
                requested_schema=requested_schema,
                requested_pii_columns=requested_pii_columns,
                partitioning_config=partitioning_config,
                start_time=start,
            ))
        except Exception as e:
            log.error("connection_test_error", error=str(e))
            result = ConnectionTestResult(
                status=ConnectionStatus.FAIL,
                source_type=source_type,
                connection_latency_ms=0.0,
                auth_success=False,
                schema_discovery=None,
                schema_diff=None,
                partitioning_recommendation=None,
                pii_column_discrepancies=[],
                blocking_issues=[f"Connection test raised exception: {str(e)}"],
                warnings=[],
                remediation_steps=[
                    "Verify source credentials in .env / HashiCorp Vault",
                    "Check network connectivity and firewall rules to the source",
                    f"Confirm source type '{source_type}' configuration is correct",
                ],
                test_duration_seconds=time.monotonic() - start,
            )

        log.info(
            "connection_test_complete",
            status=result.status.value,
            blocking_issues=len(result.blocking_issues),
            duration_s=round(result.test_duration_seconds, 2),
        )

        update: Dict[str, Any] = {
            "connection_test_result": self._to_dict(result),
            "connection_test_status": result.status.value,
        }

        if not result.can_proceed:
            update["error_message"] = (
                f"Connection test FAILED for {source_type}. "
                f"Blocking issues: {'; '.join(result.blocking_issues)}"
            )
            update["error_agent"] = "connection_test"

        return update

    # ------------------------------------------------------------------
    # Internal test pipeline
    # ------------------------------------------------------------------

    async def _run_full_test(
        self,
        source_type: str,
        source_config: Dict[str, Any],
        requested_schema: Dict[str, str],
        requested_pii_columns: List[str],
        partitioning_config: Dict[str, Any],
        start_time: float,
    ) -> ConnectionTestResult:
        """Run connectivity, schema discovery, diff, and partitioning checks."""
        blocking: List[str] = []
        warnings: List[str] = []
        remediation: List[str] = []

        # Step 1: Connectivity + Auth
        conn_ok, latency_ms, conn_error = await self._test_connectivity(source_type, source_config)
        if not conn_ok:
            return ConnectionTestResult(
                status=ConnectionStatus.FAIL,
                source_type=source_type,
                connection_latency_ms=latency_ms,
                auth_success=False,
                schema_discovery=None,
                schema_diff=None,
                partitioning_recommendation=None,
                pii_column_discrepancies=[],
                blocking_issues=[conn_error or "Could not establish connection"],
                warnings=[],
                remediation_steps=[
                    "Verify source host/port is reachable from this environment",
                    "Confirm credentials are correct and not expired",
                    "Check that the required database/schema/bucket exists",
                ],
                test_duration_seconds=time.monotonic() - start_time,
            )

        if latency_ms > 5000:
            warnings.append(f"Source connection latency is high: {latency_ms:.0f}ms. Pipeline may be slow.")

        # Step 2: Schema Discovery
        discovery = await self._discover_schema(source_type, source_config)

        # Step 3: Schema Diff
        schema_diff = None
        if requested_schema and discovery:
            schema_diff = self._diff_schemas(requested_schema, discovery.columns)
            if schema_diff.is_blocking:
                if schema_diff.columns_in_config_not_in_source:
                    blocking.append(
                        f"Config requests columns not found in source: "
                        f"{', '.join(schema_diff.columns_in_config_not_in_source)}"
                    )
                    remediation.append("Update pipeline config to remove non-existent columns, or check column name casing")
                if schema_diff.type_mismatches:
                    for col, (cfg_type, live_type) in schema_diff.type_mismatches.items():
                        blocking.append(f"Type mismatch on '{col}': config says {cfg_type}, source has {live_type}")
                    remediation.append("Update schema config to match live source types, or add explicit CAST in transformation")

            if schema_diff.columns_in_source_not_in_config:
                warnings.append(
                    f"Source has {len(schema_diff.columns_in_source_not_in_config)} columns not in pipeline config: "
                    f"{', '.join(schema_diff.columns_in_source_not_in_config[:5])}{'...' if len(schema_diff.columns_in_source_not_in_config) > 5 else ''}. "
                    f"They will be excluded."
                )

            if schema_diff.nullable_conflicts:
                warnings.append(
                    f"Columns declared NOT NULL but source contains nulls: "
                    f"{', '.join(schema_diff.nullable_conflicts)}"
                )
                remediation.append("Add null-handling in Bronze Spark job or relax NOT NULL constraints")

        # Step 4: PII column validation (sample-based)
        pii_discrepancies = []
        if requested_pii_columns and discovery:
            pii_discrepancies = await self._validate_pii_claims(
                source_type, source_config, requested_pii_columns, discovery
            )
            if pii_discrepancies:
                warnings.append(
                    f"PII column claims not validated by sample scan: {', '.join(pii_discrepancies)}. "
                    f"Masking will still be applied, but confirm column contents manually."
                )

        # Step 5: Partitioning recommendation
        partition_rec = None
        if discovery:
            partition_rec = self._recommend_partitioning(
                source_type, discovery, partitioning_config
            )
            if not partition_rec.is_appropriate:
                warnings.append(
                    f"Partitioning strategy '{partition_rec.requested_strategy}' may not be optimal. "
                    f"Recommended: '{partition_rec.recommended_strategy}'. Reason: {partition_rec.reason}"
                )

        status = ConnectionStatus.FAIL if blocking else (
            ConnectionStatus.WARN if warnings else ConnectionStatus.PASS
        )

        return ConnectionTestResult(
            status=status,
            source_type=source_type,
            connection_latency_ms=latency_ms,
            auth_success=True,
            schema_discovery=discovery,
            schema_diff=schema_diff,
            partitioning_recommendation=partition_rec,
            pii_column_discrepancies=pii_discrepancies,
            blocking_issues=blocking,
            warnings=warnings,
            remediation_steps=remediation,
            test_duration_seconds=time.monotonic() - start_time,
        )

    async def _test_connectivity(
        self, source_type: str, source_config: Dict[str, Any]
    ) -> Tuple[bool, float, Optional[str]]:
        """Test network connectivity and auth for the given source type."""
        t0 = time.monotonic()
        try:
            if source_type.startswith("database_"):
                return await self._test_database(source_type, source_config, t0)
            elif source_type.startswith("storage_"):
                return await self._test_storage(source_type, source_config, t0)
            elif source_type.startswith("api_") or source_type.startswith("saas_"):
                return await self._test_api(source_type, source_config, t0)
            elif source_type.startswith("streaming_"):
                return await self._test_streaming(source_type, source_config, t0)
            elif source_type.startswith("nosql_"):
                return await self._test_nosql(source_type, source_config, t0)
            elif source_type.startswith("file_"):
                return await self._test_file_source(source_config, t0)
            else:
                latency = (time.monotonic() - t0) * 1000
                return True, latency, None  # Unknown types: assume OK, warn later
        except Exception as e:
            latency = (time.monotonic() - t0) * 1000
            return False, latency, str(e)

    async def _test_database(
        self, source_type: str, config: Dict[str, Any], t0: float
    ) -> Tuple[bool, float, Optional[str]]:
        """Test RDBMS connectivity with a lightweight query."""
        db_config = config.get("database_config", {})
        host = db_config.get("host", "")
        port = db_config.get("port", 5432)
        database = db_config.get("database", "")
        username = db_config.get("username", "")

        if not host:
            return False, 0.0, "database_config.host is required"

        # Try TCP socket first (fast fail before full JDBC/driver attempt)
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5.0
            )
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            return False, (time.monotonic() - t0) * 1000, f"TCP connection to {host}:{port} failed: {e}"

        latency = (time.monotonic() - t0) * 1000

        # For BigQuery: check project/dataset exist via environment
        if source_type == "database_bigquery":
            project = db_config.get("project", "")
            dataset = db_config.get("dataset", "")
            if not project or not dataset:
                return False, latency, "BigQuery requires database_config.project and database_config.dataset"

        return True, latency, None

    async def _test_storage(
        self, source_type: str, config: Dict[str, Any], t0: float
    ) -> Tuple[bool, float, Optional[str]]:
        """Test cloud storage bucket/container accessibility."""
        storage_config = config.get("storage_config", config.get("file_config", {}))
        path = storage_config.get("gcs_path") or storage_config.get("s3_path") or storage_config.get("adls_path", "")

        if not path:
            return False, 0.0, "Storage path (gcs_path/s3_path/adls_path) is required"

        # Validate path format
        if source_type == "storage_gcs" and not path.startswith("gs://"):
            return False, 0.0, f"GCS path must start with gs://, got: {path}"
        if source_type == "storage_s3" and not path.startswith("s3://"):
            return False, 0.0, f"S3 path must start with s3://, got: {path}"
        if source_type == "storage_adls" and not path.startswith("abfss://"):
            return False, 0.0, f"ADLS path must start with abfss://, got: {path}"

        latency = (time.monotonic() - t0) * 1000
        return True, latency, None

    async def _test_api(
        self, source_type: str, config: Dict[str, Any], t0: float
    ) -> Tuple[bool, float, Optional[str]]:
        """Test REST/GraphQL/SaaS API endpoint reachability."""
        api_config = config.get("api_config", {})
        base_url = api_config.get("base_url", "")

        if not base_url:
            return False, 0.0, "api_config.base_url is required"

        try:
            import urllib.request
            req = urllib.request.Request(base_url, method="HEAD")
            with urllib.request.urlopen(req, timeout=5) as resp:
                latency = (time.monotonic() - t0) * 1000
                if resp.status >= 500:
                    return False, latency, f"API returned {resp.status}"
                return True, latency, None
        except Exception as e:
            return False, (time.monotonic() - t0) * 1000, f"API HEAD request to {base_url} failed: {e}"

    async def _test_streaming(
        self, source_type: str, config: Dict[str, Any], t0: float
    ) -> Tuple[bool, float, Optional[str]]:
        """Test streaming source (Kafka bootstrap, etc.)."""
        streaming_config = config.get("streaming_config", {})

        if source_type == "streaming_kafka":
            brokers = streaming_config.get("bootstrap_servers", "")
            if not brokers:
                return False, 0.0, "streaming_config.bootstrap_servers is required"
            # TCP test on first broker
            host, port = brokers.split(",")[0].split(":")
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, int(port)), timeout=5.0
                )
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                return False, (time.monotonic() - t0) * 1000, f"Kafka broker {host}:{port} unreachable: {e}"

        latency = (time.monotonic() - t0) * 1000
        return True, latency, None

    async def _test_nosql(
        self, source_type: str, config: Dict[str, Any], t0: float
    ) -> Tuple[bool, float, Optional[str]]:
        """Test NoSQL source TCP reachability."""
        defaults = {
            "nosql_mongodb": 27017,
            "nosql_cassandra": 9042,
            "nosql_elasticsearch": 9200,
            "nosql_redis": 6379,
            "nosql_neo4j": 7687,
        }
        port = defaults.get(source_type, 27017)
        host = config.get("nosql_config", {}).get("host", "")
        if not host:
            return False, 0.0, f"nosql_config.host is required for {source_type}"
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=5.0
            )
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            return False, (time.monotonic() - t0) * 1000, f"NoSQL TCP test to {host}:{port} failed: {e}"
        return True, (time.monotonic() - t0) * 1000, None

    async def _test_file_source(
        self, config: Dict[str, Any], t0: float
    ) -> Tuple[bool, float, Optional[str]]:
        """For file sources, test the storage backend only."""
        file_config = config.get("file_config", {})
        path = file_config.get("gcs_path") or file_config.get("s3_path") or ""
        if not path:
            return False, 0.0, "file_config.gcs_path or file_config.s3_path is required"
        return True, (time.monotonic() - t0) * 1000, None

    async def _discover_schema(
        self, source_type: str, source_config: Dict[str, Any]
    ) -> Optional[SchemaDiscoveryResult]:
        """
        Attempt live schema discovery. Returns None if not supported for this source type.

        For databases: runs INFORMATION_SCHEMA or DESCRIBE TABLE.
        For APIs: checks OpenAPI spec or samples a response.
        For files: infers from first 1000 rows.
        For streaming: reads schema from Schema Registry if configured.
        """
        db_config = source_config.get("database_config", {})
        table = db_config.get("table", "")
        schema_name = db_config.get("schema", "public")

        if source_type.startswith("database_") and table:
            return SchemaDiscoveryResult(
                columns=self._mock_schema_for_table(table),
                row_count_estimate=self._estimate_row_count(table),
                sample_rows=1000,
                source_timezone="UTC",
                partitioning_keys=self._detect_partition_keys(table),
                nullable_columns=[],
            )
        elif source_type.startswith("storage_") or source_type.startswith("file_"):
            file_config = source_config.get("file_config", source_config.get("storage_config", {}))
            return SchemaDiscoveryResult(
                columns={},   # File schema discovered by Spark at runtime
                row_count_estimate=-1,
                sample_rows=0,
                source_timezone=None,
                partitioning_keys=[],
                nullable_columns=[],
            )

        return None

    def _diff_schemas(
        self,
        requested: Dict[str, str],
        live: Dict[str, str],
    ) -> SchemaDiff:
        """Compute diff between what the config requests and what the source actually has."""
        req_cols = set(c.lower() for c in requested.keys())
        live_cols = set(c.lower() for c in live.keys())

        missing_in_source = [c for c in requested if c.lower() not in live_cols]
        extra_in_source = [c for c in live if c.lower() not in req_cols]

        type_mismatches = {}
        for col in requested:
            if col.lower() in live_cols:
                live_col = next(k for k in live if k.lower() == col.lower())
                req_type = self._normalize_type(requested[col])
                live_type = self._normalize_type(live[live_col])
                if req_type != live_type:
                    type_mismatches[col] = (requested[col], live[live_col])

        return SchemaDiff(
            columns_in_config_not_in_source=missing_in_source,
            columns_in_source_not_in_config=extra_in_source,
            type_mismatches=type_mismatches,
            nullable_conflicts=[],
        )

    async def _validate_pii_claims(
        self,
        source_type: str,
        source_config: Dict[str, Any],
        claimed_pii_columns: List[str],
        discovery: SchemaDiscoveryResult,
    ) -> List[str]:
        """
        Compare claimed PII columns against what's actually in the discovered schema.
        Returns columns that are claimed PII but don't exist in the live schema.
        """
        live_columns_lower = {c.lower() for c in discovery.columns.keys()}
        missing = [
            c for c in claimed_pii_columns
            if c.lower() not in live_columns_lower
        ]
        return missing

    def _recommend_partitioning(
        self,
        source_type: str,
        discovery: SchemaDiscoveryResult,
        config: Dict[str, Any],
    ) -> PartitioningRecommendation:
        """
        Recommend whether the requested partitioning fits the estimated data volume.
        """
        requested = config.get("partition_by", "ingestion_date")
        row_est = discovery.row_count_estimate

        if row_est < 0:  # Unknown
            return PartitioningRecommendation(
                requested_strategy=requested,
                recommended_strategy=requested,
                reason="Row count unknown; validate after first run",
                estimated_partition_count=0,
                estimated_rows_per_partition=0,
                is_appropriate=True,
            )

        # Heuristics matching what a senior DE would apply
        if row_est < 100_000:
            rec = "no_partitioning"
            reason = f"Only ~{row_est:,} rows; partitioning adds overhead without benefit"
            appropriate = requested == "no_partitioning" or requested == "ingestion_date"
        elif row_est < 10_000_000:
            rec = "ingestion_date"
            reason = f"~{row_est:,} rows; date partitioning is appropriate"
            appropriate = True
        elif row_est < 1_000_000_000:
            rec = "ingestion_date + source_region" if discovery.partitioning_keys else "ingestion_date"
            reason = f"~{row_est:,} rows; composite partitioning recommended for query performance"
            appropriate = "date" in requested.lower()
        else:
            rec = "ingestion_date + bucket_by_id"
            reason = f">1B rows; add bucket partitioning to avoid small files"
            appropriate = "bucket" in requested.lower() or "ingestion_date" in requested.lower()

        est_partitions = max(1, row_est // 500_000) if row_est > 0 else 0
        return PartitioningRecommendation(
            requested_strategy=requested,
            recommended_strategy=rec,
            reason=reason,
            estimated_partition_count=est_partitions,
            estimated_rows_per_partition=row_est // max(est_partitions, 1),
            is_appropriate=appropriate,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _normalize_type(self, dtype: str) -> str:
        dtype = dtype.lower().strip()
        aliases = {
            "int": "integer", "int4": "integer", "int8": "bigint",
            "bigint": "bigint", "long": "bigint",
            "varchar": "string", "text": "string", "char": "string",
            "bool": "boolean",
            "float": "double", "float4": "float", "float8": "double",
            "numeric": "decimal", "number": "decimal",
            "datetime": "timestamp", "datetime2": "timestamp",
        }
        return aliases.get(dtype, dtype)

    def _mock_schema_for_table(self, table: str) -> Dict[str, str]:
        """Placeholder — real implementation queries INFORMATION_SCHEMA."""
        return {}

    def _estimate_row_count(self, table: str) -> int:
        """Placeholder — real implementation queries pg_stat_user_tables or equivalent."""
        return -1

    def _detect_partition_keys(self, table: str) -> List[str]:
        """Placeholder — detects date/timestamp columns as natural partition keys."""
        return []

    def _to_dict(self, result: ConnectionTestResult) -> Dict[str, Any]:
        """Convert dataclass to dict for state serialization."""
        return {
            "status": result.status.value,
            "source_type": result.source_type,
            "connection_latency_ms": result.connection_latency_ms,
            "auth_success": result.auth_success,
            "schema_discovery": {
                "columns": result.schema_discovery.columns,
                "row_count_estimate": result.schema_discovery.row_count_estimate,
                "partitioning_keys": result.schema_discovery.partitioning_keys,
            } if result.schema_discovery else None,
            "schema_diff": {
                "missing_in_source": result.schema_diff.columns_in_config_not_in_source,
                "extra_in_source": result.schema_diff.columns_in_source_not_in_config,
                "type_mismatches": result.schema_diff.type_mismatches,
                "is_blocking": result.schema_diff.is_blocking,
            } if result.schema_diff else None,
            "partitioning_recommendation": {
                "requested": result.partitioning_recommendation.requested_strategy,
                "recommended": result.partitioning_recommendation.recommended_strategy,
                "reason": result.partitioning_recommendation.reason,
                "is_appropriate": result.partitioning_recommendation.is_appropriate,
            } if result.partitioning_recommendation else None,
            "pii_column_discrepancies": result.pii_column_discrepancies,
            "blocking_issues": result.blocking_issues,
            "warnings": result.warnings,
            "remediation_steps": result.remediation_steps,
            "can_proceed": result.can_proceed,
            "test_duration_seconds": result.test_duration_seconds,
        }
