from __future__ import annotations

import os

import streamlit as st
import streamlit.components.v1 as components

from src.ui.session.auth_crypto import encrypt_auth_payload
from src.ui.session.auth_session import (
    KEY_AUTHENTICATED,
    KEY_AUTH_SOURCE,
    KEY_DISPLAY_NAME,
    KEY_EMAIL,
    KEY_LOGIN_TIME,
    KEY_PERMISSIONS,
    KEY_USER_ID,
    KEY_USERNAME,
)


_BROWSER_STORAGE_KEY = "tmn_auth_v1"
_BROWSER_RESTORE_ATTEMPT_KEY = "tmn_auth_restore_attempted_v1"


def _is_pytest_runtime() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))


def _normalize_permissions(value: object) -> list[str]:
    if value is None or value == "" or value == () or value == []:
        return []

    if isinstance(value, str):
        candidates = value.split(",")
    else:
        try:
            candidates = list(value)  # type: ignore[arg-type]
        except TypeError:
            candidates = [value]

    normalized: list[str] = []
    for candidate in candidates:
        permission = str(candidate).strip()
        if permission and permission not in normalized:
            normalized.append(permission)
    return normalized


def sync_auth_to_browser_storage(*, remember_me: bool) -> None:
    if _is_pytest_runtime():
        return

    if not st.session_state.get(KEY_AUTHENTICATED):
        return

    username = st.session_state.get(KEY_USERNAME, "")
    login_time = st.session_state.get(KEY_LOGIN_TIME)
    if not username or login_time is None:
        return

    payload = {
        "auth": "1",
        "user_id": str(st.session_state.get(KEY_USER_ID, username)).strip() or username,
        "user": username,
        "login_time": login_time.isoformat(),
        "auth_source": str(st.session_state.get(KEY_AUTH_SOURCE, "internal")).strip() or "internal",
        "permissions": _normalize_permissions(st.session_state.get(KEY_PERMISSIONS, ())),
        "display_name": str(st.session_state.get(KEY_DISPLAY_NAME, username)).strip() or username,
        "email": str(st.session_state.get(KEY_EMAIL, "")).strip(),
    }
    encrypted_payload = encrypt_auth_payload(payload)
    components.html(
        f"""
        <script>
        (function() {{
          const storageKey = { _BROWSER_STORAGE_KEY!r };
          const attemptKey = { _BROWSER_RESTORE_ATTEMPT_KEY!r };
          const cookieMaxAge = {30 * 24 * 60 * 60};
          const payload = {encrypted_payload!r};
          try {{
            sessionStorage.setItem(storageKey, payload);
            sessionStorage.removeItem(attemptKey);
          }} catch (e) {{}}
          if ({str(bool(remember_me)).lower()}) {{
            try {{ localStorage.setItem(storageKey, payload); }} catch (e) {{}}
          }} else {{
            try {{ localStorage.removeItem(storageKey); }} catch (e) {{}}
          }}
          try {{
            const encoded = encodeURIComponent(payload);
            const maxAge = {str(bool(remember_me)).lower()} ? `; Max-Age=${{cookieMaxAge}}` : "";
            document.cookie = `${{storageKey}}=${{encoded}}; Path=/; SameSite=Lax${{maxAge}}`;
          }} catch (e) {{}}
          try {{
            window.parent?.postMessage({{ type: "tmn_auth_storage_synced" }}, "*");
            window.postMessage({{ type: "tmn_auth_storage_synced" }}, "*");
          }} catch (e) {{}}
        }})();
        </script>
        """,
        height=0,
        width=0,
    )


def clear_browser_storage_auth(*, force_reload: bool = False) -> None:
    if _is_pytest_runtime():
        return

    components.html(
        f"""
        <script>
        (function() {{
          const storageKey = { _BROWSER_STORAGE_KEY!r };
          const attemptKey = { _BROWSER_RESTORE_ATTEMPT_KEY!r };
          try {{ sessionStorage.removeItem(storageKey); }} catch (e) {{}}
          try {{ localStorage.removeItem(storageKey); }} catch (e) {{}}
          try {{ sessionStorage.removeItem(attemptKey); }} catch (e) {{}}
          try {{ document.cookie = `${{storageKey}}=; Path=/; Max-Age=0; SameSite=Lax`; }} catch (e) {{}}
          if ({str(bool(force_reload)).lower()}) {{
            try {{
              const target = window.parent && window.parent.location ? window.parent : window;
              target.location.reload();
            }} catch (e) {{
              try {{ window.location.reload(); }} catch (e2) {{}}
            }}
          }}
        }})();
        </script>
        """,
        height=0,
        width=0,
    )


def render_auto_restore_auth_from_browser_storage() -> None:
    # Server-side restore from encrypted cookie/session token.
    if _is_pytest_runtime():
        return
    try:
        from src.ui.session.auth_session import restore_auth_from_query_params

        restore_auth_from_query_params()
    except Exception:
        return


def read_auth_token_from_cookie() -> str:
    if _is_pytest_runtime():
        return ""

    context = getattr(st, "context", None)
    cookies = getattr(context, "cookies", None)
    if cookies is None:
        return ""

    raw_payload = ""
    try:
        raw_payload = str(cookies.get(_BROWSER_STORAGE_KEY, "") or "")
    except Exception:
        return ""
    if not raw_payload:
        return ""

    try:
        import urllib.parse

        return urllib.parse.unquote(raw_payload)
    except Exception:
        return ""
