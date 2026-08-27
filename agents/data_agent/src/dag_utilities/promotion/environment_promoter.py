"""
Environment Promoter - Dev/QA/Prod Pipeline Promotion

Part of the APEX Control Plane for pipeline lifecycle management.
Handles promoting data pipelines across environments (dev -> qa -> prod)
with validation gates, audit trails, and human approval enforcement.

Promotion Rules:
- Only sequential promotions are allowed: dev -> qa -> prod
- Direct dev -> prod promotion is NEVER permitted
- PROD promotion requires human approval (non-negotiable)
- All pre-promotion validations must pass before promotion proceeds
- Every promotion generates a compliance audit trail

Architecture:
    Control Plane (Airflow DAG) -> EnvironmentPromoter -> Metadata DB
    EnvironmentPromoter validates, copies metadata, and logs the audit trail.
    No business logic lives in code - all configuration is metadata-driven.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

# Valid promotion paths (source -> target)
VALID_PROMOTION_PATHS = {
    ("dev", "qa"),
    ("qa", "prod"),
}

# Valid environment names
VALID_ENVIRONMENTS = {"dev", "qa", "prod"}

# Metadata tables that are copied during promotion
PROMOTABLE_TABLES = [
    "feed_definitions",
    "contract_definitions",
    "schema_details",
    "zone_configurations",
    "quality_rules",
    "transform_definitions",
    "execution_policies",
]


@dataclass
class PromotionRequest:
    """
    Request to promote a pipeline from one environment to another.

    Encapsulates all information needed to initiate an environment promotion,
    including the feed to promote, source/target environments, the user
    requesting the promotion, and optional Jira ticket for traceability.

    Attributes:
        feed_id: Numeric feed identifier for the pipeline to promote.
        source_environment: Current environment of the pipeline (dev/qa/prod).
        target_environment: Desired target environment (dev/qa/prod).
        promoted_by: Username or service account initiating the promotion.
        jira_ticket: Optional Jira ticket ID for change management traceability.
        skip_validation: If True, bypass pre-promotion checks. Defaults to False.
            WARNING: Skipping validation is NOT permitted for prod promotions.
    """
    feed_id: int
    source_environment: str
    target_environment: str
    promoted_by: str
    jira_ticket: Optional[str] = None
    skip_validation: bool = False


@dataclass
class PromotionResult:
    """
    Result of a pipeline environment promotion attempt.

    Contains the outcome of the promotion including which artifacts were
    copied, validation results, and any error details if the promotion failed.

    Attributes:
        success: Whether the promotion completed successfully.
        feed_id: Numeric feed identifier that was promoted.
        source_environment: Environment the pipeline was promoted from.
        target_environment: Environment the pipeline was promoted to.
        artifacts_promoted: List of metadata table names that were copied.
        validation_results: Dictionary of pre-promotion check results.
        promoted_at: ISO 8601 timestamp of when the promotion occurred.
        error_message: Error details if the promotion failed, None on success.
    """
    success: bool
    feed_id: int
    source_environment: str
    target_environment: str
    artifacts_promoted: List[str] = field(default_factory=list)
    validation_results: Dict[str, Any] = field(default_factory=dict)
    promoted_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the promotion result to a serializable dictionary."""
        return {
            "success": self.success,
            "feed_id": self.feed_id,
            "source_environment": self.source_environment,
            "target_environment": self.target_environment,
            "artifacts_promoted": self.artifacts_promoted,
            "validation_results": self.validation_results,
            "promoted_at": self.promoted_at,
            "error_message": self.error_message,
        }


class EnvironmentPromoter:
    """
    Manages pipeline promotion across environments (dev -> qa -> prod).

    Part of the APEX Control Plane, this class enforces promotion policies:
    - Sequential promotion only (dev -> qa -> prod); no environment skipping
    - PROD promotions always require human approval
    - Pre-promotion validation must pass (metadata completeness, GE suites, etc.)
    - Full audit trail generated for every promotion attempt (pass or fail)

    The promoter copies pipeline metadata from source environment tables to
    target environment tables and updates all environment-specific references
    (zone configurations, dataset names, GCS paths, etc.).

    Usage:
        promoter = EnvironmentPromoter(metadata_client)
        request = PromotionRequest(
            feed_id=42,
            source_environment="dev",
            target_environment="qa",
            promoted_by="ci-pipeline@company.com",
            jira_ticket="DATA-1234",
        )
        result = promoter.promote(request)
        if result.success:
            print(f"Promoted {result.artifacts_promoted}")
    """

    # Environment-specific dataset prefixes for BigQuery
    ENVIRONMENT_DATASET_PREFIXES = {
        "dev": "dev_",
        "qa": "qa_",
        "prod": "",
    }

    # Environment-specific GCS bucket suffixes
    ENVIRONMENT_BUCKET_SUFFIXES = {
        "dev": "-dev",
        "qa": "-qa",
        "prod": "-prod",
    }

    def __init__(self, metadata_client: Any) -> None:
        """
        Initialize the EnvironmentPromoter.

        Args:
            metadata_client: MetadataClient instance for database access.
                Must support get_feed(), get_contract(), get_schema(),
                get_zone_configs(), get_quality_rules(), and execute_query()
                methods for reading and writing pipeline metadata.
        """
        self.metadata = metadata_client

    def promote(self, request: PromotionRequest) -> PromotionResult:
        """
        Promote a pipeline from one environment to another.

        Executes the full promotion workflow:
        1. Validate the promotion path (dev->qa or qa->prod only)
        2. Run pre-promotion checks (metadata, GE suites, health score)
        3. Copy metadata from source to target environment tables
        4. Update environment references in zone configurations
        5. Generate a compliance audit trail

        Args:
            request: PromotionRequest with feed_id, environments, and requester.

        Returns:
            PromotionResult indicating success or failure with details.
        """
        logger.info(
            "Promotion requested: feed_id=%d, %s -> %s, by=%s",
            request.feed_id,
            request.source_environment,
            request.target_environment,
            request.promoted_by,
        )

        # Step 1: Validate promotion path
        if not self.validate_promotion_path(
            request.source_environment, request.target_environment
        ):
            error_msg = (
                f"Invalid promotion path: {request.source_environment} -> "
                f"{request.target_environment}. Only dev->qa and qa->prod "
                f"are permitted. Direct dev->prod is never allowed."
            )
            logger.error(error_msg)
            result = PromotionResult(
                success=False,
                feed_id=request.feed_id,
                source_environment=request.source_environment,
                target_environment=request.target_environment,
                error_message=error_msg,
            )
            self.generate_audit_trail(request, result)
            return result

        # Step 1b: Enforce human approval for PROD promotions
        if request.target_environment == "prod":
            approval_check = self._check_human_approval(request)
            if not approval_check["approved"]:
                error_msg = (
                    "PROD promotion requires human approval. "
                    f"Reason: {approval_check.get('reason', 'No approval record found')}. "
                    "Submit an approval request before promoting to prod."
                )
                logger.error(error_msg)
                result = PromotionResult(
                    success=False,
                    feed_id=request.feed_id,
                    source_environment=request.source_environment,
                    target_environment=request.target_environment,
                    validation_results={"human_approval": approval_check},
                    error_message=error_msg,
                )
                self.generate_audit_trail(request, result)
                return result

        # Step 2: Run pre-promotion checks
        validation_results = {}
        if not request.skip_validation:
            validation_results = self.run_pre_promotion_checks(
                request.feed_id, request.source_environment
            )

            # Check if all validations passed
            all_passed = all(
                check.get("passed", False)
                for check in validation_results.values()
            )
            if not all_passed:
                failed_checks = [
                    name for name, check in validation_results.items()
                    if not check.get("passed", False)
                ]
                error_msg = (
                    f"Pre-promotion validation failed. "
                    f"Failed checks: {', '.join(failed_checks)}. "
                    f"All validations must pass before promotion."
                )
                logger.error(error_msg)
                result = PromotionResult(
                    success=False,
                    feed_id=request.feed_id,
                    source_environment=request.source_environment,
                    target_environment=request.target_environment,
                    validation_results=validation_results,
                    error_message=error_msg,
                )
                self.generate_audit_trail(request, result)
                return result
        elif request.target_environment == "prod":
            # Never allow skipping validation for prod
            error_msg = (
                "Cannot skip validation for PROD promotions. "
                "Validation is mandatory for production deployments."
            )
            logger.error(error_msg)
            result = PromotionResult(
                success=False,
                feed_id=request.feed_id,
                source_environment=request.source_environment,
                target_environment=request.target_environment,
                error_message=error_msg,
            )
            self.generate_audit_trail(request, result)
            return result

        # Step 3: Copy metadata from source to target
        try:
            artifacts_promoted = self.copy_metadata(
                request.feed_id,
                request.source_environment,
                request.target_environment,
            )
        except Exception as e:
            error_msg = f"Failed to copy metadata: {str(e)}"
            logger.error(error_msg, exc_info=True)
            result = PromotionResult(
                success=False,
                feed_id=request.feed_id,
                source_environment=request.source_environment,
                target_environment=request.target_environment,
                validation_results=validation_results,
                error_message=error_msg,
            )
            self.generate_audit_trail(request, result)
            return result

        # Step 4: Update environment references in zone configs
        try:
            self._update_environment_references(
                request.feed_id,
                request.target_environment,
            )
        except Exception as e:
            error_msg = (
                f"Metadata copied but failed to update environment "
                f"references: {str(e)}"
            )
            logger.error(error_msg, exc_info=True)
            result = PromotionResult(
                success=False,
                feed_id=request.feed_id,
                source_environment=request.source_environment,
                target_environment=request.target_environment,
                artifacts_promoted=artifacts_promoted,
                validation_results=validation_results,
                error_message=error_msg,
            )
            self.generate_audit_trail(request, result)
            return result

        # Step 5: Build success result and generate audit trail
        result = PromotionResult(
            success=True,
            feed_id=request.feed_id,
            source_environment=request.source_environment,
            target_environment=request.target_environment,
            artifacts_promoted=artifacts_promoted,
            validation_results=validation_results,
        )

        self.generate_audit_trail(request, result)

        logger.info(
            "Promotion successful: feed_id=%d, %s -> %s, artifacts=%s",
            request.feed_id,
            request.source_environment,
            request.target_environment,
            artifacts_promoted,
        )

        return result

    def validate_promotion_path(self, source: str, target: str) -> bool:
        """
        Validate that the promotion path is allowed.

        Only sequential promotions are permitted:
        - dev -> qa
        - qa -> prod

        Direct dev -> prod is never allowed. Promoting to the same environment
        or downgrading (e.g., prod -> dev) is also rejected.

        Args:
            source: Source environment name (dev, qa, or prod).
            target: Target environment name (dev, qa, or prod).

        Returns:
            True if the promotion path is valid, False otherwise.
        """
        source_lower = source.lower().strip()
        target_lower = target.lower().strip()

        # Validate environment names
        if source_lower not in VALID_ENVIRONMENTS:
            logger.warning(
                "Invalid source environment: '%s'. Must be one of: %s",
                source, VALID_ENVIRONMENTS,
            )
            return False

        if target_lower not in VALID_ENVIRONMENTS:
            logger.warning(
                "Invalid target environment: '%s'. Must be one of: %s",
                target, VALID_ENVIRONMENTS,
            )
            return False

        # Check if path is in allowed set
        is_valid = (source_lower, target_lower) in VALID_PROMOTION_PATHS
        if not is_valid:
            logger.warning(
                "Promotion path %s -> %s is not allowed. "
                "Valid paths: dev->qa, qa->prod.",
                source_lower, target_lower,
            )

        return is_valid

    def run_pre_promotion_checks(
        self, feed_id: int, source_env: str
    ) -> Dict[str, Any]:
        """
        Run all pre-promotion validation checks.

        Validates that the pipeline is ready for promotion by checking:
        - metadata_complete: Feed definition, contract, and schema exist and are valid
        - ge_suites_exist: Great Expectations validation suites are defined
        - quality_rules_defined: Quality rules exist for at least one zone
        - recent_execution_success: Most recent execution in source environment succeeded
        - health_score_threshold: Pipeline health score meets minimum threshold (0.7)

        Args:
            feed_id: Numeric feed identifier to validate.
            source_env: Source environment to check metadata in.

        Returns:
            Dictionary mapping check names to result dictionaries. Each result
            contains at minimum a 'passed' boolean and a 'message' string.
            Example::

                {
                    "metadata_complete": {"passed": True, "message": "..."},
                    "ge_suites_exist": {"passed": False, "message": "..."},
                    ...
                }
        """
        checks: Dict[str, Any] = {}

        # Check 1: Metadata completeness
        checks["metadata_complete"] = self._check_metadata_complete(
            feed_id, source_env
        )

        # Check 2: GE suites exist
        checks["ge_suites_exist"] = self._check_ge_suites_exist(
            feed_id, source_env
        )

        # Check 3: Quality rules defined
        checks["quality_rules_defined"] = self._check_quality_rules_defined(
            feed_id, source_env
        )

        # Check 4: Recent execution success
        checks["recent_execution_success"] = self._check_recent_execution(
            feed_id, source_env
        )

        # Check 5: Health score threshold
        checks["health_score_threshold"] = self._check_health_score(
            feed_id, source_env
        )

        logger.info(
            "Pre-promotion checks for feed_id=%d in %s: %s",
            feed_id,
            source_env,
            {k: v.get("passed") for k, v in checks.items()},
        )

        return checks

    def copy_metadata(
        self, feed_id: int, source_env: str, target_env: str
    ) -> List[str]:
        """
        Copy pipeline metadata from source to target environment tables.

        Iterates over all promotable metadata tables and copies rows for the
        given feed_id from source-environment-prefixed tables to
        target-environment-prefixed tables. Existing rows in the target are
        replaced (upsert semantics).

        The tables copied are:
        - feed_definitions
        - contract_definitions
        - schema_details
        - zone_configurations
        - quality_rules
        - transform_definitions
        - execution_policies

        Args:
            feed_id: Numeric feed identifier to copy metadata for.
            source_env: Source environment prefix (e.g., 'dev').
            target_env: Target environment prefix (e.g., 'qa').

        Returns:
            List of table names that were successfully copied.

        Raises:
            RuntimeError: If metadata copy fails for any table.
        """
        tables_copied: List[str] = []

        for table_name in PROMOTABLE_TABLES:
            source_table = f"{source_env}_{table_name}"
            target_table = f"{target_env}_{table_name}"

            try:
                # Read metadata from source environment table
                source_rows = self._read_table_rows(
                    source_table, feed_id
                )

                if not source_rows:
                    logger.info(
                        "No rows found in %s for feed_id=%d, skipping",
                        source_table, feed_id,
                    )
                    continue

                # Delete existing rows in target (upsert)
                self._delete_table_rows(target_table, feed_id)

                # Insert rows into target environment table
                self._insert_table_rows(
                    target_table, source_rows, target_env
                )

                tables_copied.append(table_name)
                logger.info(
                    "Copied %d rows from %s to %s for feed_id=%d",
                    len(source_rows), source_table, target_table, feed_id,
                )

            except Exception as e:
                raise RuntimeError(
                    f"Failed to copy table {table_name} from "
                    f"{source_env} to {target_env}: {str(e)}"
                ) from e

        return tables_copied

    def generate_audit_trail(
        self, request: PromotionRequest, result: PromotionResult
    ) -> Dict[str, Any]:
        """
        Generate a compliance audit trail for the promotion attempt.

        Creates a structured audit record capturing the full context of the
        promotion: who requested it, what was promoted, whether it succeeded,
        and all validation results. The audit record is persisted to the
        metadata database for compliance and traceability.

        Args:
            request: The original promotion request.
            result: The promotion result (success or failure).

        Returns:
            Dictionary containing the complete audit trail record with keys:
            event_type, timestamp, feed_id, promotion details, requester info,
            jira_ticket, validation_results, artifacts, and outcome.
        """
        audit_record = {
            "event_type": "ENVIRONMENT_PROMOTION",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "feed_id": request.feed_id,
            "source_environment": request.source_environment,
            "target_environment": request.target_environment,
            "promoted_by": request.promoted_by,
            "jira_ticket": request.jira_ticket,
            "skip_validation": request.skip_validation,
            "success": result.success,
            "artifacts_promoted": result.artifacts_promoted,
            "validation_results": result.validation_results,
            "error_message": result.error_message,
            "promoted_at": result.promoted_at,
            "requires_human_approval": (
                request.target_environment == "prod"
            ),
        }

        # Persist audit record to database
        self._persist_audit_record(audit_record)

        logger.info(
            "Audit trail generated for feed_id=%d promotion %s -> %s: success=%s",
            request.feed_id,
            request.source_environment,
            request.target_environment,
            result.success,
        )

        return audit_record

    # -------------------------------------------------------------------------
    # Private helper methods
    # -------------------------------------------------------------------------

    def _check_human_approval(
        self, request: PromotionRequest
    ) -> Dict[str, Any]:
        """
        Check whether human approval has been granted for a PROD promotion.

        Queries the metadata database for an approval record matching the
        feed_id, target environment, and requester.

        Args:
            request: The promotion request to check approval for.

        Returns:
            Dictionary with 'approved' boolean and 'reason' string.
        """
        try:
            if hasattr(self.metadata, "check_promotion_approval"):
                approval = self.metadata.check_promotion_approval(
                    feed_id=request.feed_id,
                    target_environment=request.target_environment,
                    requested_by=request.promoted_by,
                )
                return {
                    "approved": approval.get("approved", False),
                    "reason": approval.get("reason", "Approval status unknown"),
                    "approved_by": approval.get("approved_by"),
                    "approved_at": approval.get("approved_at"),
                }
        except Exception as e:
            logger.warning(
                "Failed to check human approval: %s", str(e)
            )

        return {
            "approved": False,
            "reason": "No approval record found or approval check unavailable",
        }

    def _check_metadata_complete(
        self, feed_id: int, source_env: str
    ) -> Dict[str, Any]:
        """
        Check that all required metadata exists for the feed.

        Verifies that feed definition, contract, and schema are present
        and valid in the source environment.

        Args:
            feed_id: Numeric feed identifier.
            source_env: Source environment to check.

        Returns:
            Dictionary with 'passed' boolean and 'message' string.
        """
        missing = []

        try:
            feed = self.metadata.get_feed(feed_id)
            if not feed:
                missing.append("feed_definition")

            contract = self.metadata.get_contract(feed_id)
            if not contract:
                missing.append("contract_definition")

            if hasattr(self.metadata, "get_schema"):
                schema = self.metadata.get_schema(feed_id)
                if not schema or not schema.get("columns"):
                    missing.append("schema_details")
        except Exception as e:
            return {
                "passed": False,
                "message": f"Metadata check failed with error: {str(e)}",
            }

        if missing:
            return {
                "passed": False,
                "message": (
                    f"Missing required metadata: {', '.join(missing)}"
                ),
                "missing": missing,
            }

        return {
            "passed": True,
            "message": "All required metadata present",
        }

    def _check_ge_suites_exist(
        self, feed_id: int, source_env: str
    ) -> Dict[str, Any]:
        """
        Check that Great Expectations validation suites are defined.

        Verifies that at least one GE expectation suite exists for the feed
        in the source environment.

        Args:
            feed_id: Numeric feed identifier.
            source_env: Source environment to check.

        Returns:
            Dictionary with 'passed' boolean, 'message' string, and
            'suite_count' integer.
        """
        try:
            if hasattr(self.metadata, "get_ge_suites"):
                suites = self.metadata.get_ge_suites(feed_id)
                suite_count = len(suites) if suites else 0
            elif hasattr(self.metadata, "get_quality_rules"):
                # Fall back to checking quality_rules as proxy for GE suites
                rules = self.metadata.get_quality_rules(feed_id)
                suite_count = 1 if rules else 0
            else:
                return {
                    "passed": True,
                    "message": "GE suite check skipped (method unavailable)",
                    "suite_count": 0,
                }

            if suite_count > 0:
                return {
                    "passed": True,
                    "message": f"{suite_count} GE suite(s) found",
                    "suite_count": suite_count,
                }
            else:
                return {
                    "passed": False,
                    "message": "No GE validation suites defined",
                    "suite_count": 0,
                }
        except Exception as e:
            return {
                "passed": False,
                "message": f"GE suite check failed: {str(e)}",
                "suite_count": 0,
            }

    def _check_quality_rules_defined(
        self, feed_id: int, source_env: str
    ) -> Dict[str, Any]:
        """
        Check that quality rules are defined for the feed.

        Verifies that at least one quality rule exists for data validation.

        Args:
            feed_id: Numeric feed identifier.
            source_env: Source environment to check.

        Returns:
            Dictionary with 'passed' boolean, 'message' string, and
            'rule_count' integer.
        """
        try:
            if hasattr(self.metadata, "get_quality_rules"):
                rules = self.metadata.get_quality_rules(feed_id)
                rule_count = len(rules) if rules else 0
            else:
                return {
                    "passed": True,
                    "message": "Quality rules check skipped (method unavailable)",
                    "rule_count": 0,
                }

            if rule_count > 0:
                return {
                    "passed": True,
                    "message": f"{rule_count} quality rule(s) defined",
                    "rule_count": rule_count,
                }
            else:
                return {
                    "passed": False,
                    "message": "No quality rules defined for this feed",
                    "rule_count": 0,
                }
        except Exception as e:
            return {
                "passed": False,
                "message": f"Quality rules check failed: {str(e)}",
                "rule_count": 0,
            }

    def _check_recent_execution(
        self, feed_id: int, source_env: str
    ) -> Dict[str, Any]:
        """
        Check that the most recent execution in the source environment succeeded.

        Ensures the pipeline has been successfully run at least once before
        promotion.

        Args:
            feed_id: Numeric feed identifier.
            source_env: Source environment to check.

        Returns:
            Dictionary with 'passed' boolean and 'message' string.
        """
        try:
            if hasattr(self.metadata, "get_latest_execution"):
                execution = self.metadata.get_latest_execution(
                    feed_id, environment=source_env
                )
                if not execution:
                    return {
                        "passed": False,
                        "message": (
                            f"No execution records found in {source_env}"
                        ),
                    }

                status = execution.get("status", "").lower()
                if status == "success":
                    return {
                        "passed": True,
                        "message": (
                            f"Latest execution in {source_env} succeeded"
                        ),
                        "execution_id": execution.get("execution_id"),
                    }
                else:
                    return {
                        "passed": False,
                        "message": (
                            f"Latest execution in {source_env} has status: "
                            f"{status}. Must be 'success' to promote."
                        ),
                        "execution_id": execution.get("execution_id"),
                        "status": status,
                    }
            else:
                return {
                    "passed": True,
                    "message": (
                        "Execution check skipped (method unavailable)"
                    ),
                }
        except Exception as e:
            return {
                "passed": False,
                "message": f"Execution check failed: {str(e)}",
            }

    def _check_health_score(
        self, feed_id: int, source_env: str
    ) -> Dict[str, Any]:
        """
        Check that the pipeline health score meets the minimum threshold.

        The minimum health score for promotion is 0.7 (on a 0.0-1.0 scale).
        Health score is a weighted combination of quality, freshness,
        stability, and cost efficiency metrics.

        Args:
            feed_id: Numeric feed identifier.
            source_env: Source environment to check.

        Returns:
            Dictionary with 'passed' boolean, 'message' string, and
            optionally 'score' float and 'threshold' float.
        """
        min_threshold = 0.7

        try:
            if hasattr(self.metadata, "get_health_score"):
                score_data = self.metadata.get_health_score(feed_id)
                if not score_data:
                    return {
                        "passed": False,
                        "message": "No health score available",
                        "threshold": min_threshold,
                    }

                score = score_data.get("overall_score", 0.0)
                if score >= min_threshold:
                    return {
                        "passed": True,
                        "message": (
                            f"Health score {score:.3f} meets threshold "
                            f"{min_threshold}"
                        ),
                        "score": score,
                        "threshold": min_threshold,
                    }
                else:
                    return {
                        "passed": False,
                        "message": (
                            f"Health score {score:.3f} below threshold "
                            f"{min_threshold}"
                        ),
                        "score": score,
                        "threshold": min_threshold,
                    }
            else:
                return {
                    "passed": True,
                    "message": (
                        "Health score check skipped (method unavailable)"
                    ),
                }
        except Exception as e:
            return {
                "passed": False,
                "message": f"Health score check failed: {str(e)}",
            }

    def _read_table_rows(
        self, table_name: str, feed_id: int
    ) -> List[Dict[str, Any]]:
        """
        Read all rows for a feed from a metadata table.

        Args:
            table_name: Environment-prefixed table name.
            feed_id: Numeric feed identifier.

        Returns:
            List of row dictionaries.
        """
        if hasattr(self.metadata, "execute_query"):
            return self.metadata.execute_query(
                f"SELECT * FROM {table_name} WHERE feed_id = %s",
                (feed_id,),
            )
        elif hasattr(self.metadata, "read_table"):
            return self.metadata.read_table(table_name, feed_id=feed_id)

        logger.warning(
            "No query method available on metadata client to read %s",
            table_name,
        )
        return []

    def _delete_table_rows(
        self, table_name: str, feed_id: int
    ) -> None:
        """
        Delete existing rows for a feed from a target metadata table.

        Args:
            table_name: Environment-prefixed table name.
            feed_id: Numeric feed identifier.
        """
        if hasattr(self.metadata, "execute_query"):
            self.metadata.execute_query(
                f"DELETE FROM {table_name} WHERE feed_id = %s",
                (feed_id,),
            )
        elif hasattr(self.metadata, "delete_rows"):
            self.metadata.delete_rows(table_name, feed_id=feed_id)

    def _insert_table_rows(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        target_env: str,
    ) -> None:
        """
        Insert rows into a target environment metadata table.

        Updates the 'environment' field in each row to the target environment
        before insertion.

        Args:
            table_name: Environment-prefixed table name.
            rows: List of row dictionaries to insert.
            target_env: Target environment name for updating references.
        """
        for row in rows:
            # Update environment field
            row["environment"] = target_env
            row["promoted_at"] = datetime.now(timezone.utc).isoformat()

        if hasattr(self.metadata, "insert_rows"):
            self.metadata.insert_rows(table_name, rows)
        elif hasattr(self.metadata, "execute_query"):
            for row in rows:
                columns = ", ".join(row.keys())
                placeholders = ", ".join(["%s"] * len(row))
                self.metadata.execute_query(
                    f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                    tuple(row.values()),
                )

    def _update_environment_references(
        self, feed_id: int, target_env: str
    ) -> None:
        """
        Update environment-specific references in zone configurations.

        After metadata is copied, this method updates all environment-specific
        paths and dataset names to match the target environment:
        - BigQuery dataset prefixes (dev_ / qa_ / no prefix for prod)
        - GCS bucket suffixes (-dev / -qa / -prod)
        - Connection string references

        Args:
            feed_id: Numeric feed identifier.
            target_env: Target environment to update references for.
        """
        try:
            zone_configs = self.metadata.get_zone_configs(feed_id)
            if not zone_configs:
                logger.info(
                    "No zone configurations to update for feed_id=%d",
                    feed_id,
                )
                return

            dataset_prefix = self.ENVIRONMENT_DATASET_PREFIXES.get(
                target_env, ""
            )
            bucket_suffix = self.ENVIRONMENT_BUCKET_SUFFIXES.get(
                target_env, ""
            )

            for zone_config in zone_configs:
                # Update BigQuery dataset references
                if "dataset" in zone_config:
                    base_dataset = zone_config["dataset"]
                    # Strip any existing environment prefix
                    for prefix in self.ENVIRONMENT_DATASET_PREFIXES.values():
                        if prefix and base_dataset.startswith(prefix):
                            base_dataset = base_dataset[len(prefix):]
                            break
                    zone_config["dataset"] = f"{dataset_prefix}{base_dataset}"

                # Update GCS path references
                if "gcs_path" in zone_config:
                    gcs_path = zone_config["gcs_path"]
                    # Replace bucket suffix for target environment
                    for suffix in self.ENVIRONMENT_BUCKET_SUFFIXES.values():
                        if suffix and suffix in gcs_path:
                            gcs_path = gcs_path.replace(
                                suffix, bucket_suffix
                            )
                            break
                    zone_config["gcs_path"] = gcs_path

                # Mark environment
                zone_config["environment"] = target_env

            # Persist updated zone configs
            if hasattr(self.metadata, "update_zone_configs"):
                self.metadata.update_zone_configs(feed_id, zone_configs)

        except Exception as e:
            raise RuntimeError(
                f"Failed to update environment references for "
                f"feed_id={feed_id} in {target_env}: {str(e)}"
            ) from e

    def _persist_audit_record(self, audit_record: Dict[str, Any]) -> None:
        """
        Persist the audit trail record to the metadata database.

        Args:
            audit_record: Complete audit trail dictionary to persist.
        """
        try:
            if hasattr(self.metadata, "log_event"):
                self.metadata.log_event(
                    event_type="ENVIRONMENT_PROMOTION",
                    feed_id=audit_record.get("feed_id"),
                    severity="INFO" if audit_record["success"] else "WARNING",
                    **{
                        k: v
                        for k, v in audit_record.items()
                        if k not in ("event_type", "feed_id")
                    },
                )
            elif hasattr(self.metadata, "insert_audit_record"):
                self.metadata.insert_audit_record(audit_record)
            else:
                logger.info(
                    "Audit record (no persistence method): %s",
                    audit_record,
                )
        except Exception as e:
            logger.warning(
                "Failed to persist audit record: %s", str(e)
            )


__all__ = [
    "EnvironmentPromoter",
    "PromotionRequest",
    "PromotionResult",
    "VALID_PROMOTION_PATHS",
    "VALID_ENVIRONMENTS",
    "PROMOTABLE_TABLES",
]
