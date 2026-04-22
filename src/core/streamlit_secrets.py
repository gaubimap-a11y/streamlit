from __future__ import annotations

from pathlib import Path
import tomllib
from typing import Any

_WEBAPP_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_SECRETS_TOML = _WEBAPP_ROOT / ".streamlit" / "secrets.toml"

try:
    import streamlit as st
except Exception:  # pragma: no cover - import fallback for non-streamlit environments
    st = None


def load_streamlit_secrets() -> dict[str, Any]:
    if st is not None:
        try:
            runtime_secrets = st.secrets.to_dict()
            if isinstance(runtime_secrets, dict):
                return runtime_secrets
        except Exception:
            pass

    if not _DEFAULT_SECRETS_TOML.exists():
        return {}

    try:
        with _DEFAULT_SECRETS_TOML.open("rb") as handle:
            parsed = tomllib.load(handle)
    except Exception:
        return {}

    if isinstance(parsed, dict):
        return parsed
    return {}


def get_secret_section(secrets: dict[str, Any], section: str) -> dict[str, Any]:
    raw = secrets.get(section, {})
    if isinstance(raw, dict):
        return raw
    return {}

