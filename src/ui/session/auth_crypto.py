from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets

from src.core.config import get_settings


_ENV_AUTH_STORAGE_SECRET = "AUTH_STORAGE_SECRET"


def _derive_key() -> bytes:
    secret = os.environ.get(_ENV_AUTH_STORAGE_SECRET, "").strip()
    if not secret:
        try:
            secret = get_settings().databricks.access_token.strip()
        except Exception:
            secret = ""
    if not secret:
        secret = "auth-storage-fallback-key"
    return hashlib.sha256(secret.encode("utf-8")).digest()


def _xor_stream(data: bytes, *, key: bytes, nonce: bytes) -> bytes:
    output = bytearray()
    counter = 0
    while len(output) < len(data):
        block = hashlib.sha256(key + nonce + counter.to_bytes(4, "big")).digest()
        output.extend(block)
        counter += 1
    return bytes(a ^ b for a, b in zip(data, output[: len(data)]))


def encrypt_auth_payload(payload: dict[str, object]) -> str:
    key = _derive_key()
    nonce = secrets.token_bytes(16)
    plaintext = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ciphertext = _xor_stream(plaintext, key=key, nonce=nonce)
    mac = hmac.new(key, nonce + ciphertext, hashlib.sha256).digest()
    token_bytes = nonce + mac + ciphertext
    return base64.urlsafe_b64encode(token_bytes).decode("ascii").rstrip("=")


def decrypt_auth_payload(token: str) -> dict[str, object] | None:
    if not token:
        return None

    padded_token = token + "=" * (-len(token) % 4)
    try:
        token_bytes = base64.urlsafe_b64decode(padded_token.encode("ascii"))
    except Exception:
        return None
    if len(token_bytes) < 48:
        return None

    nonce = token_bytes[:16]
    mac = token_bytes[16:48]
    ciphertext = token_bytes[48:]
    key = _derive_key()
    expected_mac = hmac.new(key, nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expected_mac):
        return None

    plaintext = _xor_stream(ciphertext, key=key, nonce=nonce)
    try:
        payload = json.loads(plaintext.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload
