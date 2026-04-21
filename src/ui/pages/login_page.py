from __future__ import annotations

from html import escape

import streamlit as st

from src.application.auth.auth_service import AuthService
from src.application.auth.google_oauth_service import build_google_oauth_authorization_url, handle_google_oauth_callback
from src.core.exceptions import AuthError
from src.core.config import get_google_oauth_config
from src.domain.auth_models import AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SSO
from src.domain.auth_validation import AuthenticationValidationError, InactiveUserError, InvalidCredentialsError
from src.ui.base.base_page import BasePage
from src.ui.audit_events import record_ui_audit_event
from src.ui.session.auth_session import (
    KEY_AUTHENTICATED,
    KEY_FAILED_ATTEMPTS,
    KEY_REMEMBER_ME,
    clear_oauth_callback_query_params,
    get_enabled_login_modes,
    set_auth_state,
    switch_page_safely,
)
from src.ui.session.browser_storage import (
    render_auto_restore_auth_from_browser_storage,
    sync_auth_to_browser_storage,
)
from src.ui.styles.loader import inject_css


_MENU_PAGE = "pages/menu.py"
_DASHBOARD_PAGE = "pages/dashboard.py"
_AUTH_MODE_INTERNAL = "internal"
_AUTH_MODE_SSO = "sso"

# Các thông báo hệ thống bằng tiếng Việt có dấu
_MSG_EMPTY = "Vui lòng nhập đầy đủ thông tin."
_MSG_WRONG = "Tên đăng nhập hoặc mật khẩu không đúng."
_MSG_INACTIVE = "Tài khoản đã bị khóa. Vui lòng liên hệ quản trị viên."
_MSG_LOCKOUT = "Tài khoản đã bị khóa do đăng nhập sai quá nhiều lần. Vui lòng thử lại sau."
_MSG_SYSTEM_ERROR = "Lỗi hệ thống. Vui lòng thử lại sau hoặc liên hệ quản trị viên."
_MAX_FAILED_ATTEMPTS = 5


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


class LoginPage(BasePage):
    def __init__(self, auth_service: AuthService | None = None) -> None:
        super().__init__()
        self._auth_service = auth_service or AuthService()

    @property
    def page_title(self) -> str:
        return "Hệ thống COOP-Kobe - Đăng nhập"

    @property
    def page_icon(self) -> str:
        return "🔑"

    @property
    def layout(self) -> str:
        return "centered"

    @property
    def sidebar_state(self) -> str:
        return "collapsed"

    def render(self) -> None:
        inject_css("login.css")
        render_auto_restore_auth_from_browser_storage()
        if st.session_state.get(KEY_AUTHENTICATED):
            switch_page_safely(_MENU_PAGE)
            st.stop()

        callback_handled = self._handle_google_oauth_callback()
        if st.session_state.get(KEY_AUTHENTICATED):
            switch_page_safely(_DASHBOARD_PAGE)
            st.stop()

        self._init_session_state()
        enabled_modes = get_enabled_login_modes()
        if _AUTH_MODE_INTERNAL in enabled_modes:
            self._render_login_form(enabled_modes)
        else:
            st.info("Môi trường này chỉ bật đăng nhập qua Google.")
            self._render_google_entrypoint(enabled_modes)

    def _init_session_state(self) -> None:
        st.session_state.setdefault(KEY_FAILED_ATTEMPTS, 0)
        st.session_state.setdefault(KEY_REMEMBER_ME, False)

    def _handle_login(self, username: str, password: str, failed_attempts: int) -> None:
        if not username or not password:
            st.error(_MSG_EMPTY)
            record_ui_audit_event(
                None,
                event_type="login_failure",
                resource="auth",
                action="internal_login",
                result="denied",
                details={"reason": "missing_username_or_password", "auth_source": AUTH_SOURCE_INTERNAL},
            )
            return

        try:
            session = self._auth_service.authenticate_session(username, password)
        except InactiveUserError:
            st.error(_MSG_INACTIVE)
            record_ui_audit_event(
                None,
                event_type="login_failure",
                resource="auth",
                action="internal_login",
                result="denied",
                details={"reason": "inactive_user", "auth_source": AUTH_SOURCE_INTERNAL},
            )
            return
        except InvalidCredentialsError:
            st.session_state[KEY_FAILED_ATTEMPTS] = failed_attempts + 1
            record_ui_audit_event(
                None,
                event_type="login_failure",
                resource="auth",
                action="internal_login",
                result="denied",
                details={"reason": "invalid_credentials", "auth_source": AUTH_SOURCE_INTERNAL},
            )
            if st.session_state[KEY_FAILED_ATTEMPTS] >= _MAX_FAILED_ATTEMPTS:
                st.error(_MSG_LOCKOUT)
                st.rerun()
            st.error(_MSG_WRONG)
            return
        except AuthError:
            st.error(_MSG_SYSTEM_ERROR)
            record_ui_audit_event(
                None,
                event_type="login_failure",
                resource="auth",
                action="internal_login",
                result="error",
                details={"reason": "auth_system_error", "auth_source": AUTH_SOURCE_INTERNAL},
            )
            return

        set_auth_state(
            session.user_id,
            session.username,
            session.login_at,
            remember_me=bool(st.session_state.get(KEY_REMEMBER_ME, False)),
            auth_source=session.auth_source,
            permissions=session.permissions,
            display_name=session.display_name,
            email=session.email,
        )
        sync_auth_to_browser_storage(remember_me=bool(st.session_state.get(KEY_REMEMBER_ME, False)))
        record_ui_audit_event(
            session,
            event_type="login_success",
            resource="auth",
            action="internal_login",
            result="success",
            details={
                "remember_me": str(bool(st.session_state.get(KEY_REMEMBER_ME, False))).lower(),
                "login_method": AUTH_SOURCE_INTERNAL,
            },
        )
        st.success("Đăng nhập thành công.")
        switch_page_safely(_DASHBOARD_PAGE)
        st.stop()

    def _handle_google_oauth_callback(self) -> bool:
        params = _get_query_params()
        code = params.get("code", "").strip()
        error = params.get("error", "").strip()
        if not code and not error:
            return False

        state = params.get("state", "").strip()
        error_description = params.get("error_description", "").strip()
        try:
            session = handle_google_oauth_callback(
                code=code,
                state=state,
                error=error,
                error_description=error_description,
            )
        except AuthenticationValidationError as exc:
            clear_oauth_callback_query_params()
            st.error("Đăng nhập bằng Google không thành công. Vui lòng thử lại.")
            record_ui_audit_event(
                None,
                event_type="login_failure",
                resource="auth",
                action="google_login",
                result="denied",
                details={
                    "reason": str(exc),
                    "auth_source": AUTH_SOURCE_SSO,
                },
            )
            return True
        except AuthError:
            clear_oauth_callback_query_params()
            st.error("Lỗi hệ thống khi xác thực Google. Vui lòng thử lại sau.")
            record_ui_audit_event(
                None,
                event_type="login_failure",
                resource="auth",
                action="google_login",
                result="error",
                details={
                    "reason": "google_oauth_system_error",
                    "auth_source": AUTH_SOURCE_SSO,
                },
            )
            return True
        except Exception:
            clear_oauth_callback_query_params()
            st.error("Đã xảy ra lỗi không xác định khi đăng nhập Google.")
            record_ui_audit_event(
                None,
                event_type="login_failure",
                resource="auth",
                action="google_login",
                result="error",
                details={
                    "reason": "unexpected_google_oauth_error",
                    "auth_source": AUTH_SOURCE_SSO,
                },
            )
            return True

        set_auth_state(
            session.user_id,
            session.username,
            session.login_at,
            remember_me=bool(st.session_state.get(KEY_REMEMBER_ME, False)),
            auth_source=session.auth_source,
            permissions=session.permissions,
            display_name=session.display_name,
            email=session.email,
        )
        sync_auth_to_browser_storage(remember_me=bool(st.session_state.get(KEY_REMEMBER_ME, False)))
        record_ui_audit_event(
            session,
            event_type="login_success",
            resource="auth",
            action="google_login",
            result="success",
            details={
                "login_method": "google_oauth2",
                "remember_me": str(bool(st.session_state.get(KEY_REMEMBER_ME, False))).lower(),
            },
        )
        st.success("Đăng nhập bằng Google thành công.")
        switch_page_safely(_DASHBOARD_PAGE)
        st.stop()

    def _render_google_entrypoint(self, enabled_modes: tuple[str, ...]) -> None:
        if _AUTH_MODE_SSO not in enabled_modes:
            return

        google_oauth = get_google_oauth_config()
        if google_oauth.is_configured():
            try:
                auth_url = build_google_oauth_authorization_url(google_oauth)
            except AuthError:
                self._render_google_button(disabled=True)
                return
            self._render_google_button(auth_url=auth_url)
        else:
            self._render_google_button(disabled=True)

    def _render_login_form(self, enabled_modes: tuple[str, ...]) -> None:
        failed_attempts = int(st.session_state[KEY_FAILED_ATTEMPTS])
        is_locked = failed_attempts >= _MAX_FAILED_ATTEMPTS

        st.markdown('<div class="login-layout">', unsafe_allow_html=True)
        _, center_col, _ = st.columns([0.2, 0.6, 0.2], gap="large")

        with center_col:
            st.markdown("### Đăng nhập hệ thống")
            st.caption("Hệ thống quản lý bán hàng.")
            form_container = st.container(border=True)
            with form_container:
                st.markdown('<div class="login-form-wrap">', unsafe_allow_html=True)
                if is_locked:
                    st.error(_MSG_LOCKOUT)
                elif failed_attempts > 0:
                    remaining_attempts = _MAX_FAILED_ATTEMPTS - failed_attempts
                    st.caption(
                        f"Bạn còn {remaining_attempts} lần thử trước khi tài khoản bị khóa tạm thời."
                    )

                username = st.text_input(
                    "Tên đăng nhập",
                    value="admin",
                    placeholder="admin", 
                    disabled=is_locked
                )
                password = st.text_input(
                    "Mật khẩu",
                    type="password",
                    value="admin",
                    placeholder="Nhập mật khẩu của bạn",
                    disabled=is_locked,
                )
                st.checkbox(
                    "Ghi nhớ đăng nhập trên thiết bị này",
                    key=KEY_REMEMBER_ME,
                    disabled=is_locked,
                )
                button_placeholder = st.empty()
                if button_placeholder.button(
                    "Đăng nhập",
                    type="primary",
                    disabled=is_locked,
                    use_container_width=True,
                    key="btn_login",
                ):
                    button_placeholder.button(
                        "Đang xác thực...",
                        disabled=True,
                        use_container_width=True,
                        key="btn_login_loading",
                    )
                    self._handle_login(username, password, failed_attempts)
                    
                self._render_google_entrypoint(enabled_modes)
                st.markdown("</div>", unsafe_allow_html=True)

            st.markdown(
                """
                <div class="login-footer">
                    Quên mật khẩu hoặc cần cấp lại quyền truy cập? Liên hệ quản trị viên hệ thống để được hỗ trợ.
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)

    def _render_google_button(self, *, auth_url: str | None = None, disabled: bool = False) -> None:
        if disabled or not auth_url:
            st.markdown(
                """
                <div class="google-auth-button google-auth-button--disabled" aria-disabled="true">
                    <span class="google-auth-button__icon" aria-hidden="true">
                        <svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">
                            <path fill="#4285F4" d="M21.35 11.1H12v2.97h5.34c-.23 1.27-.95 2.35-2.02 3.07v2.55h3.26c1.91-1.76 3.01-4.36 3.01-7.45 0-.72-.07-1.41-.24-2.14z"/>
                            <path fill="#34A853" d="M12 22c2.7 0 4.96-.89 6.61-2.41l-3.26-2.55c-.9.6-2.06.96-3.35.96-2.57 0-4.75-1.73-5.53-4.06H3.11v2.61A10 10 0 0 0 12 22z"/>
                            <path fill="#FBBC05" d="M6.47 13.94A5.99 5.99 0 0 1 6.16 12c0-.68.12-1.34.31-1.94V7.45H3.11A10 10 0 0 0 2 12c0 1.61.38 3.13 1.11 4.47l3.36-2.53z"/>
                            <path fill="#EA4335" d="M12 5.38c1.47 0 2.78.51 3.82 1.5l2.86-2.86A9.65 9.65 0 0 0 12 2C8.08 2 4.7 4.25 3.11 7.45l3.36 2.61C7.25 7.1 9.43 5.38 12 5.38z"/>
                        </svg>
                    </span>
                    <span class="google-auth-button__label">Tiếp tục với Google</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            return

        st.markdown(
            f"""
            <a class="google-auth-button" href="{escape(auth_url, quote=True)}" target="_self" rel="noopener noreferrer">
                <span class="google-auth-button__icon" aria-hidden="true">
                    <svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">
                        <path fill="#4285F4" d="M21.35 11.1H12v2.97h5.34c-.23 1.27-.95 2.35-2.02 3.07v2.55h3.26c1.91-1.76 3.01-4.36 3.01-7.45 0-.72-.07-1.41-.24-2.14z"/>
                        <path fill="#34A853" d="M12 22c2.7 0 4.96-.89 6.61-2.41l-3.26-2.55c-.9.6-2.06.96-3.35.96-2.57 0-4.75-1.73-5.53-4.06H3.11v2.61A10 10 0 0 0 12 22z"/>
                        <path fill="#FBBC05" d="M6.47 13.94A5.99 5.99 0 0 1 6.16 12c0-.68.12-1.34.31-1.94V7.45H3.11A10 10 0 0 0 2 12c0 1.61.38 3.13 1.11 4.47l3.36-2.53z"/>
                        <path fill="#EA4335" d="M12 5.38c1.47 0 2.78.51 3.82 1.5l2.86-2.86A9.65 9.65 0 0 0 12 2C8.08 2 4.7 4.25 3.11 7.45l3.36 2.61C7.25 7.1 9.43 5.38 12 5.38z"/>
                    </svg>
                </span>
                <span class="google-auth-button__label">Tiếp tục với Google</span>
            </a>
            """,
            unsafe_allow_html=True,
        )
