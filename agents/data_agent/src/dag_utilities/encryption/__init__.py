"""
APEX Encryption Module

Format-Preserving Encryption (FPE) and hashing for sensitive data columns.

Supported encryption types:
- fpe: Format-preserving encryption (AES-FF1)
- fpe_hk: FPE hash key (for Data Vault hub keys)
- fpe_col_concat_hk: FPE concatenated column hash key
- hash: SHA-256 one-way hash

All encryption configs come from metadata (encryption_config table).
"""

from .fpe_encryptor import FPEEncryptor, apply_encryption

__all__ = [
    "FPEEncryptor",
    "apply_encryption",
]
