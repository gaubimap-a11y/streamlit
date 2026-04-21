from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

import streamlit as st
from streamlit.errors import StreamlitAPIException

from src.application.auth.authorization_service import AuthorizationService
from src.domain.auth_models import AUTH_SOURCE_INTERNAL, AuthenticatedSession
from src.core.config import get_settings
from src.ui.audit_events import record_ui_audit_event


KEY_AUTHENTICATED = "authenticated"
KEY_USER_ID = "user_id"
KEY_USERNAME = "username"
KEY_LOGIN_TIME = "login_time"
KEY_AUTH_SOURCE = "auth_source"
KEY_PERMISSIONS = "permissions"
KEY_DISPLAY_NAME = "display_name"
KEY_EMAIL = "email"
KEY_FAILED_ATTEMPTS = "failed_attempts"
KEY_REMEMBER_ME = "remember_me"
KEY_ROLES = "roles"
KEY_PERMISSIONS = "permissions"
KEY_AUTHZ_SUBJECT = "authz_subject"
KEY_LOGGED_OUT = "logged_out"

_LOGIN_PAGE = "pages/login.py"
_AUTH_PARAM = "auth"
_USER_ID_PARAM = "user_id"
_USER_PARAM = "user"
_LOGIN_TIME_PARAM = "login_time"
_authorization_service = AuthorizationService()
_AUTH_SOURCE_PARAM = "auth_source"
_PERMISSIONS_PARAM = "permissions"
_DISPLAY_NAME_PARAM = "display_name"
_EMAIL_PARAM = "email"
_OAUTH_CALLBACK_PARAMS = (
    "code",
    "state",
    "error",
    "error_description",
    "scope",
    "authuser",
    "prompt",
)
_AUTH_MODE_INTERNAL = "internal"
_AUTH_MODE_SSO = "sso"


def switch_page_safely(page_path: str) -> None:
    """
    `st.switch_page("pages/foo.py")` works when running from the main app script.
    Streamlit AppTest often runs the page script directly (base dir becomes `pages/`),
    so we fall back to `foo.py` in that context.
    """

    # Streamlit AppTest can run scripts without a full multipage registry.
    # In that context, `st.switch_page()` may raise even after fallback; for tests
    # we should never crash the run because of navigation.
    is_pytest = bool(os.environ.get("PYTEST_CURRENT_TEST"))

    last_exc: Exception | None = None
    for candidate in (page_path, Path(page_path).name):
        try:
            st.switch_page(candidate)
            return
        except StreamlitAPIException as exc:
            last_exc = exc

    if is_pytest:
        return
    if last_exc is not None:
        raise last_exc


def _get_query_params() -> dict[str, str]:
    if hasattr(st, "query_params"):
        params: dict[str, str] = {}
        for key, value in st.query_params.items():
            params[key] = value[-1] if isinstance(value, list) and value else str(value)
        return params

    raw_params = st.experimental_get_query_params()
    return {
        key: value[-1] if isinstance(value, list) and value else str(value)
        for key, value in raw_params.items()
    }


def _set_query_params(params: dict[str, str]) -> None:
    if hasattr(st, "query_params"):
        st.query_params.clear()
        for key, value in params.items():
            st.query_params[key] = value
        return
    st.experimental_set_query_params(**params)


def _clear_auth_query_params() -> None:
    params = _get_query_params()
    for key in (
        _AUTH_PARAM,
        _USER_ID_PARAM,
        _USER_PARAM,
        _LOGIN_TIME_PARAM,
        _AUTH_SOURCE_PARAM,
        _PERMISSIONS_PARAM,
        _DISPLAY_NAME_PARAM,
        _EMAIL_PARAM,
        *_OAUTH_CALLBACK_PARAMS,
    ):
        params.pop(key, None)
    _set_query_params(params)


def clear_oauth_callback_query_params() -> None:
    params = _get_query_params()
    for key in _OAUTH_CALLBACK_PARAMS:
        params.pop(key, None)
    _set_query_params(params)


def get_enabled_login_modes() -> tuple[str, ...]:
    settings = get_settings()
    auth_config = getattr(settings, "auth", None)
    modes = getattr(auth_config, "enabled_login_modes", None)
    if modes is None:
        return (_AUTH_MODE_INTERNAL, _AUTH_MODE_SSO)

    try:
        candidates = list(modes)  # type: ignore[arg-type]
    except TypeError:
        candidates = [modes]

    normalized: list[str] = []
    for candidate in candidates:
        mode = str(candidate).strip().lower()
        if mode in {_AUTH_MODE_INTERNAL, _AUTH_MODE_SSO} and mode not in normalized:
            normalized.append(mode)
    return tuple(normalized) or (_AUTH_MODE_INTERNAL, _AUTH_MODE_SSO)


def _session_from_state() -> AuthenticatedSession | None:
    if not st.session_state.get(KEY_AUTHENTICATED):
        return None

    user_id = str(st.session_state.get(KEY_USER_ID, "")).strip()
    username = str(st.session_state.get(KEY_USERNAME, "")).strip()
    login_time = st.session_state.get(KEY_LOGIN_TIME)
    if not user_id:
        user_id = username
    if not user_id or not username or login_time is None or not isinstance(login_time, datetime):
        return None

    if login_time.tzinfo is None:
        login_time = login_time.replace(tzinfo=timezone.utc)

    expires_at = login_time + timedelta(hours=get_settings().session_timeout_hours)
    return AuthenticatedSession(
        user_id=user_id,
        username=username,
        login_at=login_time,
        expires_at=expires_at,
        auth_source=str(st.session_state.get(KEY_AUTH_SOURCE, AUTH_SOURCE_INTERNAL)).strip() or AUTH_SOURCE_INTERNAL,
        display_name=str(st.session_state.get(KEY_DISPLAY_NAME, username)).strip() or username,
        email=str(st.session_state.get(KEY_EMAIL, "")).strip(),
        permissions=_normalize_permissions(st.session_state.get(KEY_PERMISSIONS, ())),
    )


def _normalize_permissions(value: object) -> tuple[str, ...]:
    if value is None or value == "" or value == () or value == []:
        return ()

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
    return tuple(normalized)


def _serialize_permissions(value: object) -> str:
    return ",".join(_normalize_permissions(value))


def _clear_browser_storage_auth() -> None:
    from src.ui.session.browser_storage import clear_browser_storage_auth

    clear_browser_storage_auth()


def _sync_auth_query_params_from_session() -> None:
    # Disabled: do not sync auth state into URL query params.
    return


def _is_session_expired(login_time: datetime, now: datetime | None = None) -> bool:
    current_time = now or datetime.now(tz=timezone.utc)
    return current_time - login_time >= timedelta(hours=get_settings().session_timeout_hours)


def set_auth_state(
    user_id: str | None,
    username: str,
    login_time: datetime,
    *,
    remember_me: bool = False,
    auth_source: str = AUTH_SOURCE_INTERNAL,
    permissions: tuple[str, ...] | list[str] = (),
    display_name: str = "",
    email: str = "",
) -> None:
    st.session_state[KEY_AUTHENTICATED] = True
    st.session_state.pop(KEY_LOGGED_OUT, None)
    normalized_user_id = str(user_id).strip() if user_id is not None else ""
    st.session_state[KEY_USER_ID] = normalized_user_id or username
    st.session_state[KEY_USERNAME] = username
    st.session_state[KEY_LOGIN_TIME] = login_time
    st.session_state[KEY_AUTH_SOURCE] = str(auth_source).strip() or AUTH_SOURCE_INTERNAL
    st.session_state[KEY_PERMISSIONS] = _normalize_permissions(permissions)
    st.session_state[KEY_DISPLAY_NAME] = display_name.strip() or username
    st.session_state[KEY_EMAIL] = email.strip()
    st.session_state[KEY_FAILED_ATTEMPTS] = 0
    # KEY_REMEMBER_ME is also used as a widget key on the login page.
    # Streamlit forbids mutating session_state for a widget key after the widget
    # is instantiated within the same run.
    if KEY_REMEMBER_ME not in st.session_state:
        st.session_state[KEY_REMEMBER_ME] = remember_me

    hydrate_authorization_context(username)


def clear_session() -> None:
    # Preserve UI preferences
    locale = st.session_state.get("locale")
    dark_mode = st.session_state.get("ui_dark_mode")

    # Clear everything from session state to avoid state leakage
    for key in list(st.session_state.keys()):
        st.session_state.pop(key, None)

    # Restore UI preferences
    if locale:
        st.session_state["locale"] = locale
    if dark_mode is not None:
        st.session_state["ui_dark_mode"] = dark_mode

    # Mark as intentional logout to prevent auto-restore on login page
    st.session_state[KEY_LOGGED_OUT] = True

    _clear_auth_query_params()
    _clear_browser_storage_auth()


def restore_auth_from_query_params() -> None:
    # Keep function name for backward compatibility, but restore from encrypted
    # browser token instead of URL query params.
    if st.session_state.get(KEY_AUTHENTICATED):
        return
    if st.session_state.get(KEY_LOGGED_OUT):
        return

    try:
        from src.ui.session.auth_crypto import decrypt_auth_payload
        from src.ui.session.browser_storage import read_auth_token_from_cookie
    except Exception:
        return

    token = read_auth_token_from_cookie()
    if not token:
        return

    payload = decrypt_auth_payload(token)
    if not isinstance(payload, dict):
        return

    username = str(payload.get("user", "")).strip()
    user_id = str(payload.get("user_id", username)).strip() or username
    login_time_raw = str(payload.get("login_time", "")).strip()
    if not username or not login_time_raw:
        return

    try:
        login_time = datetime.fromisoformat(login_time_raw)
    except ValueError:
        return
    if login_time.tzinfo is None:
        login_time = login_time.replace(tzinfo=timezone.utc)

    if _is_session_expired(login_time):
        return

    permissions = _normalize_permissions(payload.get("permissions", ()))
    set_auth_state(
        user_id,
        username,
        login_time,
        remember_me=bool(st.session_state.get(KEY_REMEMBER_ME, False)),
        auth_source=str(payload.get("auth_source", AUTH_SOURCE_INTERNAL)).strip() or AUTH_SOURCE_INTERNAL,
        permissions=permissions,
        display_name=str(payload.get("display_name", username)).strip() or username,
        email=str(payload.get("email", "")).strip(),
    )


def hydrate_authorization_context(username: str) -> None:
    roles, permissions = _authorization_service.resolve_authorization_context(username)
    st.session_state[KEY_ROLES] = roles
    st.session_state[KEY_PERMISSIONS] = permissions
    st.session_state[KEY_AUTHZ_SUBJECT] = (username or "").strip().lower()


def get_current_username() -> str:
    return str(st.session_state.get(KEY_USERNAME, ""))


def get_current_user_id() -> str:
    return str(st.session_state.get(KEY_USER_ID, "")).strip()


def get_current_permissions() -> tuple[str, ...]:
    return _normalize_permissions(st.session_state.get(KEY_PERMISSIONS, ()))


def get_current_display_name() -> str:
    return str(st.session_state.get(KEY_DISPLAY_NAME, "")).strip()


def get_current_email() -> str:
    return str(st.session_state.get(KEY_EMAIL, "")).strip()


def get_current_session() -> AuthenticatedSession | None:
    return _session_from_state()


def has_current_permission(permission: str) -> bool:
    return permission in get_current_permissions()


def require_auth() -> None:
    restore_auth_from_query_params()

    if not st.session_state.get(KEY_AUTHENTICATED):
        record_ui_audit_event(
            None,
            event_type="access_denied",
            resource="dashboard",
            action="app_access",
            result="denied",
            details={"reason": "not_authenticated"},
        )
        switch_page_safely(_LOGIN_PAGE)
        st.stop()

    username = str(st.session_state.get(KEY_USERNAME, "") or "").strip().lower()
    authz_subject = str(st.session_state.get(KEY_AUTHZ_SUBJECT, "") or "").strip().lower()
    if username and authz_subject != username:
        existing_permissions = _normalize_permissions(st.session_state.get(KEY_PERMISSIONS, ()))
        if existing_permissions:
            st.session_state[KEY_AUTHZ_SUBJECT] = username
        else:
            hydrate_authorization_context(username)

    login_time = st.session_state.get(KEY_LOGIN_TIME)
    _sync_auth_query_params_from_session()
    if login_time and _is_session_expired(login_time):
        record_ui_audit_event(
            get_current_session(),
            event_type="access_denied",
            resource="dashboard",
            action="app_access",
            result="denied",
            details={"reason": "session_expired"},
        )
        clear_session()
        st.error("Phiên làm việc đã hết hạn.")
        switch_page_safely(_LOGIN_PAGE)
        st.stop()
        return

    if not has_current_permission("app_access"):
        record_ui_audit_event(
            get_current_session(),
            event_type="access_denied",
            resource="dashboard",
            action="app_access",
            result="denied",
            details={"reason": "missing_app_access"},
        )
        clear_session()
        st.error("Tài khoản không có quyền truy cập ứng dụng.")
        switch_page_safely(_LOGIN_PAGE)
        st.stop()
        return
