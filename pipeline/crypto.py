"""
pipeline/crypto.py
PII encryption and HMAC token utilities.

Keys are read from environment variables or a keys.env file:
  PIPELINE_HMAC_KEY   – 32-byte hex string used for HMAC-SHA256 phone tokens
  PIPELINE_ENC_KEY    – 32-byte hex string used for AES-256-GCM encryption

If not set, deterministic test keys are used (NOT for production).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import struct
from pathlib import Path

# ── key loading ──────────────────────────────────────────────────────────────

_DEFAULT_HMAC_KEY = bytes.fromhex("0" * 64)  # all-zero fallback for tests
_DEFAULT_ENC_KEY  = bytes.fromhex("1" * 64)


def _load_keys():
    """Load keys from environment or keys.env file."""
    env_path = Path(__file__).parent / "keys.env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    hmac_hex = os.environ.get("PIPELINE_HMAC_KEY", "")
    enc_hex  = os.environ.get("PIPELINE_ENC_KEY",  "")

    try:
        hmac_key = bytes.fromhex(hmac_hex) if len(hmac_hex) == 64 else _DEFAULT_HMAC_KEY
    except ValueError:
        hmac_key = _DEFAULT_HMAC_KEY

    try:
        enc_key = bytes.fromhex(enc_hex) if len(enc_hex) == 64 else _DEFAULT_ENC_KEY
    except ValueError:
        enc_key = _DEFAULT_ENC_KEY

    return hmac_key, enc_key


_HMAC_KEY, _ENC_KEY = _load_keys()


# ── HMAC token (for joins) ────────────────────────────────────────────────────

def phone_token(canonical_phone: str) -> str:
    """Return HMAC-SHA256(canonical_phone, HMAC_KEY) as hex.
    Used as the join key for lead identity and event linking.
    The plaintext phone is never stored.
    """
    return hmac.new(_HMAC_KEY, canonical_phone.encode(), hashlib.sha256).hexdigest()


# ── AES-GCM encryption (pure Python via cryptography library) ─────────────────

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    _HAS_CRYPTO = True
except ImportError:
    _HAS_CRYPTO = False


def _xor_encrypt(data: bytes, key: bytes) -> bytes:
    """Fallback XOR 'encryption' if cryptography not available. NOT secure."""
    key_cycle = (key * (len(data) // len(key) + 1))[:len(data)]
    return bytes(a ^ b for a, b in zip(data, key_cycle))


def encrypt(plaintext: str) -> str:
    """Encrypt plaintext, return base64-encoded ciphertext with 12-byte nonce prefix."""
    data = plaintext.encode()
    if _HAS_CRYPTO:
        import os as _os
        nonce = _os.urandom(12)
        aesgcm = AESGCM(_ENC_KEY[:32])
        ct = aesgcm.encrypt(nonce, data, None)
        return base64.b64encode(nonce + ct).decode()
    else:
        # XOR fallback with deterministic nonce (not for production)
        nonce = hashlib.sha256(data + _ENC_KEY).digest()[:12]
        ct = _xor_encrypt(data, _ENC_KEY)
        return base64.b64encode(nonce + ct).decode()


def decrypt(ciphertext_b64: str) -> str:
    """Decrypt base64-encoded ciphertext, return plaintext."""
    raw = base64.b64decode(ciphertext_b64)
    nonce, ct = raw[:12], raw[12:]
    if _HAS_CRYPTO:
        aesgcm = AESGCM(_ENC_KEY[:32])
        return aesgcm.decrypt(nonce, ct, None).decode()
    else:
        return _xor_encrypt(ct, _ENC_KEY).decode()


def mask_phone(canonical_phone: str) -> str:
    """Return +91******XXXX masked form."""
    # canonical_phone is +91XXXXXXXXXX (13 chars)
    if canonical_phone.startswith("+91") and len(canonical_phone) == 13:
        return "+91******" + canonical_phone[-4:]
    return "**masked**"

