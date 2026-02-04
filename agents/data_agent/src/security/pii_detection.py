"""
PII/PHI Detection and Masking Module

Automatically detects sensitive data (PII/PHI) in datasets and provides masking strategies.
Enterprise-grade security and compliance feature.
"""

import re
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (
    col, regexp_replace, sha2, md5, concat, lit, substring,
    when, length, rand, floor
)


class PIIType(str, Enum):
    """Types of PII data."""
    SSN = "ssn"  # Social Security Number
    CREDIT_CARD = "credit_card"
    EMAIL = "email"
    PHONE = "phone"
    IP_ADDRESS = "ip_address"
    DATE_OF_BIRTH = "date_of_birth"
    DRIVERS_LICENSE = "drivers_license"
    PASSPORT = "passport"

    # PHI (Protected Health Information)
    MEDICAL_RECORD_NUMBER = "medical_record_number"
    HEALTH_PLAN_NUMBER = "health_plan_number"

    # Financial
    BANK_ACCOUNT = "bank_account"
    ROUTING_NUMBER = "routing_number"

    # Personal Identifiers
    NAME = "name"
    ADDRESS = "address"
    ZIP_CODE = "zip_code"


class MaskingStrategy(str, Enum):
    """Data masking strategies."""
    REDACT = "redact"  # Replace with ***
    HASH = "hash"  # SHA-256 hash
    TOKENIZE = "tokenize"  # Format-preserving token
    PARTIAL_MASK = "partial_mask"  # Show last 4 digits
    ENCRYPT = "encrypt"  # Reversible encryption
    NULL = "null"  # Replace with NULL
    FAKE = "fake"  # Replace with fake data


@dataclass
class PIIDetectionResult:
    """Result of PII detection."""
    column_name: str
    pii_type: PIIType
    confidence: float  # 0.0 to 1.0
    sample_matches: List[str]
    match_count: int
    total_count: int
    match_percentage: float
    recommended_masking: MaskingStrategy


class PIIDetector:
    """Detects PII/PHI in DataFrames."""

    # Regex patterns for PII detection
    PATTERNS = {
        PIIType.SSN: r'\b\d{3}-\d{2}-\d{4}\b|\b\d{9}\b',
        PIIType.CREDIT_CARD: r'\b(?:\d{4}[-\s]?){3}\d{4}\b',
        PIIType.EMAIL: r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        PIIType.PHONE: r'\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',
        PIIType.IP_ADDRESS: r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
        PIIType.ZIP_CODE: r'\b\d{5}(?:-\d{4})?\b',
    }

    # Column name indicators
    COLUMN_NAME_INDICATORS = {
        PIIType.SSN: ['ssn', 'social_security', 'social_sec'],
        PIIType.EMAIL: ['email', 'e_mail', 'mail'],
        PIIType.PHONE: ['phone', 'tel', 'telephone', 'mobile', 'cell'],
        PIIType.CREDIT_CARD: ['credit_card', 'cc', 'card_number'],
        PIIType.NAME: ['name', 'first_name', 'last_name', 'full_name'],
        PIIType.ADDRESS: ['address', 'street', 'city', 'state'],
        PIIType.DATE_OF_BIRTH: ['dob', 'date_of_birth', 'birth_date', 'birthdate'],
        PIIType.MEDICAL_RECORD_NUMBER: ['mrn', 'medical_record', 'patient_id'],
        PIIType.BANK_ACCOUNT: ['account_number', 'account_no', 'bank_account'],
    }

    def __init__(self, confidence_threshold: float = 0.7):
        """
        Initialize PII detector.

        Args:
            confidence_threshold: Minimum confidence to flag as PII (0.0-1.0)
        """
        self.confidence_threshold = confidence_threshold

    def detect_pii_in_dataframe(
        self,
        df: DataFrame,
        sample_size: int = 1000
    ) -> List[PIIDetectionResult]:
        """
        Scan DataFrame for PII columns.

        Args:
            df: Spark DataFrame to scan
            sample_size: Number of rows to sample for detection

        Returns:
            List of PII detection results
        """
        results = []

        # Sample data for analysis
        sample_df = df.limit(sample_size)

        for column in df.columns:
            # Skip non-string columns
            if df.schema[column].dataType.simpleString() not in ['string', 'varchar']:
                continue

            # Check column name indicators
            column_lower = column.lower().replace('_', ' ')
            name_match = self._match_column_name(column_lower)

            # Check data patterns
            sample_data = [row[column] for row in sample_df.collect() if row[column]]
            if not sample_data:
                continue

            pattern_matches = self._match_patterns(sample_data)

            # Combine name and pattern confidence
            if name_match or pattern_matches:
                pii_type, confidence, matches, match_count = self._determine_pii_type(
                    column_lower, name_match, pattern_matches, sample_data
                )

                if confidence >= self.confidence_threshold:
                    result = PIIDetectionResult(
                        column_name=column,
                        pii_type=pii_type,
                        confidence=confidence,
                        sample_matches=matches[:5],  # Top 5 matches
                        match_count=match_count,
                        total_count=len(sample_data),
                        match_percentage=(match_count / len(sample_data)) * 100,
                        recommended_masking=self._recommend_masking(pii_type)
                    )
                    results.append(result)

        return results

    def _match_column_name(self, column_name: str) -> Optional[PIIType]:
        """Check if column name indicates PII."""
        for pii_type, indicators in self.COLUMN_NAME_INDICATORS.items():
            if any(indicator in column_name for indicator in indicators):
                return pii_type
        return None

    def _match_patterns(self, sample_data: List[str]) -> Dict[PIIType, int]:
        """Count pattern matches for each PII type."""
        matches = {}
        for pii_type, pattern in self.PATTERNS.items():
            regex = re.compile(pattern)
            match_count = sum(1 for value in sample_data if regex.search(str(value)))
            if match_count > 0:
                matches[pii_type] = match_count
        return matches

    def _determine_pii_type(
        self,
        column_name: str,
        name_match: Optional[PIIType],
        pattern_matches: Dict[PIIType, int],
        sample_data: List[str]
    ) -> Tuple[PIIType, float, List[str], int]:
        """Determine PII type and confidence."""
        # Name match has higher weight
        if name_match:
            confidence = 0.8
            if name_match in pattern_matches:
                confidence = 0.95
                match_count = pattern_matches[name_match]
                matches = [v for v in sample_data if re.search(self.PATTERNS.get(name_match, ''), str(v))]
            else:
                match_count = len(sample_data)
                matches = sample_data
            return name_match, confidence, matches, match_count

        # Pattern match only
        if pattern_matches:
            pii_type = max(pattern_matches, key=pattern_matches.get)
            match_count = pattern_matches[pii_type]
            match_percentage = match_count / len(sample_data)
            confidence = min(0.9, 0.5 + (match_percentage * 0.4))
            matches = [v for v in sample_data if re.search(self.PATTERNS[pii_type], str(v))]
            return pii_type, confidence, matches, match_count

        return PIIType.NAME, 0.5, [], 0

    def _recommend_masking(self, pii_type: PIIType) -> MaskingStrategy:
        """Recommend masking strategy based on PII type."""
        recommendations = {
            PIIType.SSN: MaskingStrategy.HASH,
            PIIType.CREDIT_CARD: MaskingStrategy.PARTIAL_MASK,
            PIIType.EMAIL: MaskingStrategy.TOKENIZE,
            PIIType.PHONE: MaskingStrategy.PARTIAL_MASK,
            PIIType.IP_ADDRESS: MaskingStrategy.HASH,
            PIIType.DATE_OF_BIRTH: MaskingStrategy.FAKE,
            PIIType.NAME: MaskingStrategy.TOKENIZE,
            PIIType.ADDRESS: MaskingStrategy.REDACT,
            PIIType.MEDICAL_RECORD_NUMBER: MaskingStrategy.HASH,
            PIIType.BANK_ACCOUNT: MaskingStrategy.PARTIAL_MASK,
        }
        return recommendations.get(pii_type, MaskingStrategy.HASH)


class PIIMasker:
    """Applies masking to PII columns."""

    @staticmethod
    def mask_column(
        df: DataFrame,
        column_name: str,
        strategy: MaskingStrategy,
        pii_type: PIIType
    ) -> DataFrame:
        """
        Apply masking strategy to a column.

        Args:
            df: Spark DataFrame
            column_name: Column to mask
            strategy: Masking strategy
            pii_type: Type of PII

        Returns:
            DataFrame with masked column
        """
        if strategy == MaskingStrategy.REDACT:
            return df.withColumn(
                column_name,
                when(col(column_name).isNotNull(), lit("***REDACTED***"))
                .otherwise(col(column_name))
            )

        elif strategy == MaskingStrategy.HASH:
            return df.withColumn(
                column_name,
                when(col(column_name).isNotNull(), sha2(col(column_name), 256))
                .otherwise(col(column_name))
            )

        elif strategy == MaskingStrategy.PARTIAL_MASK:
            return PIIMasker._partial_mask(df, column_name, pii_type)

        elif strategy == MaskingStrategy.TOKENIZE:
            return df.withColumn(
                column_name,
                when(col(column_name).isNotNull(),
                     concat(lit("TOK_"), md5(col(column_name))))
                .otherwise(col(column_name))
            )

        elif strategy == MaskingStrategy.NULL:
            return df.withColumn(column_name, lit(None))

        elif strategy == MaskingStrategy.FAKE:
            return PIIMasker._fake_data(df, column_name, pii_type)

        return df

    @staticmethod
    def _partial_mask(df: DataFrame, column_name: str, pii_type: PIIType) -> DataFrame:
        """Show last 4 characters, mask rest."""
        if pii_type == PIIType.CREDIT_CARD:
            # **** **** **** 1234
            return df.withColumn(
                column_name,
                when(
                    col(column_name).isNotNull(),
                    concat(
                        lit("**** **** **** "),
                        substring(col(column_name), -4, 4)
                    )
                ).otherwise(col(column_name))
            )
        else:
            # Generic: show last 4, mask rest
            return df.withColumn(
                column_name,
                when(
                    col(column_name).isNotNull(),
                    concat(
                        regexp_replace(
                            substring(col(column_name), 1, length(col(column_name)) - 4),
                            ".", "*"
                        ),
                        substring(col(column_name), -4, 4)
                    )
                ).otherwise(col(column_name))
            )

    @staticmethod
    def _fake_data(df: DataFrame, column_name: str, pii_type: PIIType) -> DataFrame:
        """Replace with fake data."""
        if pii_type == PIIType.DATE_OF_BIRTH:
            # Random date between 1950-2000
            return df.withColumn(
                column_name,
                when(
                    col(column_name).isNotNull(),
                    concat(
                        lit("19"),
                        floor(rand() * 50 + 50).cast("string"),
                        lit("-01-01")
                    )
                ).otherwise(col(column_name))
            )
        elif pii_type == PIIType.EMAIL:
            return df.withColumn(
                column_name,
                when(
                    col(column_name).isNotNull(),
                    concat(lit("user"), (rand() * 10000).cast("int").cast("string"), lit("@example.com"))
                ).otherwise(col(column_name))
            )
        else:
            return df.withColumn(column_name, lit("FAKE_DATA"))


def persist_classifications(
    results: List[PIIDetectionResult],
    asset_id: str,
    connection_string: Optional[str] = None,
) -> int:
    """
    Persist PII detection results to the data_classification table.

    Links detection output to governance enforcement pipeline:
    PII Detection → data_classification → GovernanceEnforcer → masking

    Args:
        results: PII detection results from PIIDetector
        asset_id: UUID of the data_asset being classified
        connection_string: PostgreSQL connection string

    Returns:
        Number of classifications persisted
    """
    import psycopg2
    import os
    import uuid as uuid_mod

    conn_str = connection_string or os.getenv("APEX_METADATA_DB_URL")
    conn = psycopg2.connect(conn_str)
    count = 0

    try:
        with conn.cursor() as cur:
            for result in results:
                cur.execute("""
                    INSERT INTO data_classification (
                        classification_id, asset_id, column_name,
                        classification_type, pii_type, confidence,
                        masking_strategy, detected_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (asset_id, column_name, classification_type)
                        DO UPDATE SET
                            confidence = EXCLUDED.confidence,
                            masking_strategy = EXCLUDED.masking_strategy,
                            detected_at = now()
                """, [
                    str(uuid_mod.uuid4()),
                    asset_id,
                    result.column_name,
                    "PHI" if result.pii_type in (PIIType.MEDICAL_RECORD_NUMBER, PIIType.HEALTH_PLAN_NUMBER) else "PII",
                    result.pii_type.value,
                    result.confidence,
                    result.recommended_masking.value.upper(),
                ])
                count += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return count


def detect_and_mask_pii(
    df: DataFrame,
    auto_mask: bool = False,
    confidence_threshold: float = 0.8
) -> Tuple[DataFrame, List[PIIDetectionResult]]:
    """
    Convenience function to detect and optionally mask PII.

    Args:
        df: Input DataFrame
        auto_mask: Automatically apply recommended masking
        confidence_threshold: Minimum confidence to flag as PII

    Returns:
        Tuple of (masked DataFrame, PII detection results)
    """
    detector = PIIDetector(confidence_threshold=confidence_threshold)
    results = detector.detect_pii_in_dataframe(df)

    masked_df = df
    if auto_mask:
        for result in results:
            masked_df = PIIMasker.mask_column(
                masked_df,
                result.column_name,
                result.recommended_masking,
                result.pii_type
            )

    return masked_df, results
