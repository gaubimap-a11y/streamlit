from __future__ import annotations

import logging
from typing import Any


_AUDIT_LOGGER = logging.getLogger("app.audit")


def log_action(action: str, **context: Any) -> None:
    """Lightweight audit hook for user-facing actions."""
    _AUDIT_LOGGER.info("action=%s context=%s", action, context or {})
