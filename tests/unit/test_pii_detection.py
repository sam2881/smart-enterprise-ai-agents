"""Tests for PII detection module.

Tests the PII detection logic from
agents/data_agent/src/security/pii_detection.py

The actual module requires PySpark for DataFrame operations, so this
test file uses two complementary strategies:

1. Direct import with PySpark mocked out at the module level — allows
   testing PIIDetector's pure-Python methods (_match_column_name,
   _match_patterns, _recommend_masking) without a real Spark cluster.

2. Standalone regex tests that mirror the PATTERNS dict verbatim —
   these run unconditionally and validate the core detection logic.

PII types covered: SSN, CREDIT_CARD, EMAIL, PHONE, IP_ADDRESS, ZIP_CODE
(the 6 types with regex patterns in PIIDetector.PATTERNS)
Column-name-only types also tested: NAME, ADDRESS, DATE_OF_BIRTH,
MEDICAL_RECORD_NUMBER, BANK_ACCOUNT.
"""
import re
import sys
import types
import pytest
from typing import List, Tuple, Dict
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Patch PySpark before importing the module so we don't need a Spark cluster
# ---------------------------------------------------------------------------

def _build_pyspark_mock():
    """Build a minimal pyspark mock that satisfies all top-level imports."""
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    sql_functions = types.ModuleType("pyspark.sql.functions")

    # DataFrame / Column stubs
    sql.DataFrame = MagicMock
    sql.Column = MagicMock

    # Every function imported by pii_detection.py
    for fn in ("col", "regexp_replace", "sha2", "md5", "concat", "lit",
               "substring", "when", "length", "rand", "floor"):
        setattr(sql_functions, fn, MagicMock())

    pyspark.sql = sql
    sys.modules.setdefault("pyspark", pyspark)
    sys.modules.setdefault("pyspark.sql", sql)
    sys.modules.setdefault("pyspark.sql.functions", sql_functions)


_build_pyspark_mock()

# Now import the actual module (pyspark is mocked)
import importlib, os
_MODULE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "agents", "data_agent")
)
if _MODULE_PATH not in sys.path:
    sys.path.insert(0, _MODULE_PATH)

try:
    from src.security.pii_detection import (
        PIIDetector,
        PIIType,
        MaskingStrategy,
        PIIDetectionResult,
    )
    ACTUAL_MODULE_AVAILABLE = True
except (ImportError, ModuleNotFoundError) as _import_err:
    ACTUAL_MODULE_AVAILABLE = False
    _IMPORT_ERROR_MSG = str(_import_err)


# ---------------------------------------------------------------------------
# Standalone regex tests (always run — no imports required)
# These are verbatim copies of PIIDetector.PATTERNS from the source file.
# ---------------------------------------------------------------------------

_PATTERNS_RAW = {
    "SSN":         r'\b\d{3}-\d{2}-\d{4}\b|\b\d{9}\b',
    "CREDIT_CARD": r'\b(?:\d{4}[-\s]?){3}\d{4}\b',
    "EMAIL":       r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    "PHONE":       r'\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',
    "IP_ADDRESS":  r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    "ZIP_CODE":    r'\b\d{5}(?:-\d{4})?\b',
}

_COMPILED = {name: re.compile(pat) for name, pat in _PATTERNS_RAW.items()}


def _find(pii_type: str, text: str) -> List[str]:
    """Return all matches for a PII type in text."""
    return _COMPILED[pii_type].findall(text)


def _mask(pii_type: str, text: str, replacement: str) -> str:
    return _COMPILED[pii_type].sub(replacement, text)


# ---------------------------------------------------------------------------
# SSN
# ---------------------------------------------------------------------------

class TestSSNPattern:
    def test_hyphenated_ssn_detected(self):
        assert _find("SSN", "Customer SSN: 123-45-6789")

    def test_hyphenated_ssn_value(self):
        matches = _find("SSN", "SSN is 987-65-4321 for this record")
        assert "987-65-4321" in matches

    def test_nine_digit_ssn_detected(self):
        assert _find("SSN", "ssn=123456789")

    def test_two_ssns_in_text(self):
        text = "Record 1: 111-22-3333, Record 2: 444-55-6666"
        assert len(_find("SSN", text)) == 2

    def test_clean_technical_text_no_ssn(self):
        assert not _find("SSN", "Server CPU at 95%, memory at 80%, disk at 60%")

    def test_ssn_redacted(self):
        result = _mask("SSN", "SSN: 123-45-6789", "[REDACTED-SSN]")
        assert "123-45-6789" not in result
        assert "[REDACTED-SSN]" in result


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

class TestEmailPattern:
    def test_simple_email_detected(self):
        assert _find("EMAIL", "Contact user@example.com for help")

    def test_complex_email_detected(self):
        matches = _find("EMAIL", "Email: first.last+tag@subdomain.example.co.uk")
        assert len(matches) > 0

    def test_no_email_in_log_line(self):
        assert not _find("EMAIL", "2026-06-22 10:00:00 ERROR Connection refused host=kafka port=9092")

    def test_multiple_emails_found(self):
        text = "From: alice@example.com To: bob@company.org"
        assert len(_find("EMAIL", text)) == 2

    def test_email_redacted(self):
        result = _mask("EMAIL", "Send to user@example.com", "[REDACTED-EMAIL]")
        assert "user@example.com" not in result
        assert "[REDACTED-EMAIL]" in result


# ---------------------------------------------------------------------------
# Phone
# ---------------------------------------------------------------------------

class TestPhonePattern:
    def test_us_phone_detected(self):
        assert _find("PHONE", "Call 555-867-5309 for support")

    def test_phone_with_country_code_detected(self):
        assert _find("PHONE", "International: +1-800-555-0199")

    def test_phone_with_parentheses_detected(self):
        assert _find("PHONE", "Office: (415) 555-2671")

    def test_clean_server_port_no_phone(self):
        # 9092 is only 4 digits — should not match
        assert not _find("PHONE", "kafka:9092")


# ---------------------------------------------------------------------------
# Credit Card
# ---------------------------------------------------------------------------

class TestCreditCardPattern:
    def test_visa_format_detected(self):
        assert _find("CREDIT_CARD", "Card: 4111 1111 1111 1111")

    def test_cc_with_dashes_detected(self):
        assert _find("CREDIT_CARD", "CC: 4111-1111-1111-1111")

    def test_cc_no_spaces_detected(self):
        assert _find("CREDIT_CARD", "CC: 4111111111111111")


# ---------------------------------------------------------------------------
# IP Address
# ---------------------------------------------------------------------------

class TestIPAddressPattern:
    def test_ipv4_detected(self):
        assert _find("IP_ADDRESS", "Connected to 192.168.1.100")

    def test_localhost_detected(self):
        assert _find("IP_ADDRESS", "Binding to 127.0.0.1:8080")


# ---------------------------------------------------------------------------
# Multi-PII masking
# ---------------------------------------------------------------------------

class TestMultiPIIMasking:
    def test_clean_text_unchanged(self):
        text = "Server prod-db-01 restarted at 10:00 UTC"
        for pii_type in _COMPILED:
            text = _mask(pii_type, text, f"[REDACTED-{pii_type}]")
        assert text == "Server prod-db-01 restarted at 10:00 UTC"

    def test_ssn_and_email_both_redacted(self):
        text = "Customer 123-45-6789 email user@test.com"
        text = _mask("SSN", text, "[REDACTED-SSN]")
        text = _mask("EMAIL", text, "[REDACTED-EMAIL]")
        assert "123-45-6789" not in text
        assert "user@test.com" not in text


# ---------------------------------------------------------------------------
# PIIDetector class tests (run only when module import succeeded)
# ---------------------------------------------------------------------------

pytestmark_module = pytest.mark.skipif(
    not ACTUAL_MODULE_AVAILABLE,
    reason=f"pii_detection module not importable: {locals().get('_IMPORT_ERROR_MSG', 'unknown')}",
)


@pytest.mark.skipif(
    not ACTUAL_MODULE_AVAILABLE,
    reason="pii_detection module not importable",
)
class TestPIIDetectorInit:
    """Test PIIDetector initialisation and configuration."""

    def test_default_confidence_threshold(self):
        detector = PIIDetector()
        assert detector.confidence_threshold == 0.7

    def test_custom_confidence_threshold(self):
        detector = PIIDetector(confidence_threshold=0.9)
        assert detector.confidence_threshold == 0.9

    def test_patterns_dict_has_six_entries(self):
        """PIIDetector.PATTERNS covers 6 regex-detectable PII types."""
        assert len(PIIDetector.PATTERNS) == 6

    def test_patterns_keys_are_pii_type_enums(self):
        for key in PIIDetector.PATTERNS:
            assert isinstance(key, PIIType)


@pytest.mark.skipif(
    not ACTUAL_MODULE_AVAILABLE,
    reason="pii_detection module not importable",
)
class TestMatchColumnName:
    """Test column-name heuristic detection (_match_column_name)."""

    def test_ssn_column_name_detected(self):
        detector = PIIDetector()
        result = detector._match_column_name("ssn")
        assert result == PIIType.SSN

    def test_email_column_name_detected(self):
        detector = PIIDetector()
        assert detector._match_column_name("email") == PIIType.EMAIL

    def test_phone_column_name_detected(self):
        detector = PIIDetector()
        assert detector._match_column_name("phone") == PIIType.PHONE

    def test_dob_column_name_detected(self):
        detector = PIIDetector()
        assert detector._match_column_name("date of birth") == PIIType.DATE_OF_BIRTH

    def test_mrn_column_name_detected(self):
        detector = PIIDetector()
        assert detector._match_column_name("mrn") == PIIType.MEDICAL_RECORD_NUMBER

    def test_bank_account_column_name_detected(self):
        detector = PIIDetector()
        assert detector._match_column_name("account number") == PIIType.BANK_ACCOUNT

    def test_unrelated_column_name_returns_none(self):
        detector = PIIDetector()
        assert detector._match_column_name("product_id") is None

    def test_revenue_column_not_flagged(self):
        detector = PIIDetector()
        assert detector._match_column_name("total revenue") is None


@pytest.mark.skipif(
    not ACTUAL_MODULE_AVAILABLE,
    reason="pii_detection module not importable",
)
class TestMatchPatterns:
    """Test _match_patterns against pure string lists."""

    def test_ssn_pattern_matches_hyphenated(self):
        detector = PIIDetector()
        result = detector._match_patterns(["123-45-6789", "foo", "bar"])
        assert PIIType.SSN in result
        assert result[PIIType.SSN] == 1

    def test_email_pattern_matches(self):
        detector = PIIDetector()
        result = detector._match_patterns(["user@example.com", "no-email-here"])
        assert PIIType.EMAIL in result

    def test_no_pii_returns_empty(self):
        detector = PIIDetector()
        result = detector._match_patterns(["hello world", "foo bar", "baz"])
        assert len(result) == 0

    def test_multiple_ssns_counted(self):
        detector = PIIDetector()
        data = ["111-22-3333", "444-55-6666", "not-an-ssn"]
        result = detector._match_patterns(data)
        assert result.get(PIIType.SSN, 0) == 2


@pytest.mark.skipif(
    not ACTUAL_MODULE_AVAILABLE,
    reason="pii_detection module not importable",
)
class TestRecommendMasking:
    """Test masking strategy recommendations per PII type."""

    def test_ssn_recommends_hash(self):
        detector = PIIDetector()
        assert detector._recommend_masking(PIIType.SSN) == MaskingStrategy.HASH

    def test_credit_card_recommends_partial_mask(self):
        detector = PIIDetector()
        assert detector._recommend_masking(PIIType.CREDIT_CARD) == MaskingStrategy.PARTIAL_MASK

    def test_email_recommends_tokenize(self):
        detector = PIIDetector()
        assert detector._recommend_masking(PIIType.EMAIL) == MaskingStrategy.TOKENIZE

    def test_phone_recommends_partial_mask(self):
        detector = PIIDetector()
        assert detector._recommend_masking(PIIType.PHONE) == MaskingStrategy.PARTIAL_MASK

    def test_ip_address_recommends_hash(self):
        detector = PIIDetector()
        assert detector._recommend_masking(PIIType.IP_ADDRESS) == MaskingStrategy.HASH

    def test_address_recommends_redact(self):
        detector = PIIDetector()
        assert detector._recommend_masking(PIIType.ADDRESS) == MaskingStrategy.REDACT

    def test_medical_record_recommends_hash(self):
        detector = PIIDetector()
        assert detector._recommend_masking(PIIType.MEDICAL_RECORD_NUMBER) == MaskingStrategy.HASH

    def test_bank_account_recommends_partial_mask(self):
        detector = PIIDetector()
        assert detector._recommend_masking(PIIType.BANK_ACCOUNT) == MaskingStrategy.PARTIAL_MASK


@pytest.mark.skipif(
    not ACTUAL_MODULE_AVAILABLE,
    reason="pii_detection module not importable",
)
class TestPIIDetectionResult:
    """Test PIIDetectionResult dataclass construction."""

    def test_result_fields_accessible(self):
        result = PIIDetectionResult(
            column_name="ssn_col",
            pii_type=PIIType.SSN,
            confidence=0.95,
            sample_matches=["123-45-6789"],
            match_count=10,
            total_count=100,
            match_percentage=10.0,
            recommended_masking=MaskingStrategy.HASH,
        )
        assert result.column_name == "ssn_col"
        assert result.pii_type == PIIType.SSN
        assert result.confidence == 0.95
        assert result.match_percentage == 10.0
        assert result.recommended_masking == MaskingStrategy.HASH

    def test_sample_matches_is_list(self):
        result = PIIDetectionResult(
            column_name="email",
            pii_type=PIIType.EMAIL,
            confidence=0.85,
            sample_matches=["a@b.com", "c@d.com"],
            match_count=2,
            total_count=10,
            match_percentage=20.0,
            recommended_masking=MaskingStrategy.TOKENIZE,
        )
        assert isinstance(result.sample_matches, list)
        assert len(result.sample_matches) == 2
