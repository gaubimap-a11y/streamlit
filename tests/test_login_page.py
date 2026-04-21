"""
UT for pages/login.py — UI layer.

Tool: streamlit.testing.v1.AppTest (runs the actual script; no st.* mocking).
AC coverage: AC-1, AC-2, AC-3, AC-4, B-1, B-2.
"""

import pathlib
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from streamlit.testing.v1 import AppTest

from src.core.exceptions import AuthError
from src.domain.auth_models import AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SSO, AuthenticatedSession
from src.domain.auth_validation import InactiveUserError, InvalidCredentialsError

_LOGIN_SCRIPT = str(pathlib.Path(__file__).parent.parent / "pages" / "login.py")

_MSG_EMPTY = "Vui lòng nhập đầy đủ thông tin."
_MSG_WRONG = "Tên đăng nhập hoặc mật khẩu không đúng."
_MSG_INACTIVE = "Tài khoản đã bị khóa"
_MSG_SYSTEM_ERROR = "Lỗi hệ thống. Vui lòng thử lại sau hoặc liên hệ quản trị viên."

_LOGIN_TIME = datetime(2026, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
_SESSION_PERMISSIONS = ("app_access", "view_dashboard", "view_data")


def _authenticated_session(
    *,
    username: str = "admin",
    display_name: str = "Admin",
    email: str = "admin@example.com",
    auth_source: str = AUTH_SOURCE_INTERNAL,
    permissions: tuple[str, ...] = _SESSION_PERMISSIONS,
) -> AuthenticatedSession:
    return AuthenticatedSession(
        user_id="user-001",
        username=username,
        login_at=_LOGIN_TIME,
        expires_at=_LOGIN_TIME + timedelta(hours=8),
        auth_source=auth_source,
        display_name=display_name,
        email=email,
        permissions=permissions,
    )


# ---------------------------------------------------------------------------
# AC-5 — Empty field validation (counter must NOT increment)
# ---------------------------------------------------------------------------


def test_empty_username_shows_validation_error_and_no_counter_increment():
    """AC-5: username trống → error "đầy đủ thông tin", failed_attempts không tăng."""
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.run()
    at.text_input[1].set_value("password123")
    at.button[0].click().run()

    assert len(at.error) == 1
    assert _MSG_EMPTY in at.error[0].value
    assert at.session_state["failed_attempts"] == 0


def test_empty_password_shows_validation_error_and_no_counter_increment():
    """AC-5: password trống → error "đầy đủ thông tin", failed_attempts không tăng."""
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.run()
    at.text_input[0].set_value("admin")
    at.button[0].click().run()

    assert len(at.error) == 1
    assert _MSG_EMPTY in at.error[0].value
    assert at.session_state["failed_attempts"] == 0


# ---------------------------------------------------------------------------
# AC-2 — Wrong credentials (counter increments)
# ---------------------------------------------------------------------------


def test_wrong_credentials_shows_error_and_increments_counter():
    """AC-2: sai credentials → error thân thiện, failed_attempts tăng 1."""
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.run()
    at.text_input[0].set_value("admin")
    at.text_input[1].set_value("wrongpassword")
    with patch(
        "src.application.auth.auth_service.AuthService.authenticate_session",
        side_effect=InvalidCredentialsError("invalid"),
    ):
        at.button[0].click().run()

    assert len(at.error) == 1
    assert _MSG_WRONG in at.error[0].value
    assert at.session_state["failed_attempts"] == 1


# ---------------------------------------------------------------------------
# AC-1 — Valid credentials (session set, redirect triggered)
# ---------------------------------------------------------------------------


def test_valid_credentials_set_session_authenticated():
    """AC-1: đúng credentials → authenticated=True, username set, counter reset."""
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.run()
    at.text_input[0].set_value("admin")
    at.text_input[1].set_value("password123")
    with patch(
        "src.application.auth.auth_service.AuthService.authenticate_session",
        return_value=_authenticated_session(),
    ):
        at.button[0].click().run()

    assert at.session_state["authenticated"] is True
    assert at.session_state["user_id"] == "user-001"
    assert at.session_state["username"] == "admin"
    assert at.session_state["display_name"] == "Admin"
    assert at.session_state["email"] == "admin@example.com"
    assert at.session_state["failed_attempts"] == 0
    assert at.session_state["permissions"] == _SESSION_PERMISSIONS
    assert at.session_state["auth_source"] == AUTH_SOURCE_INTERNAL
    assert "login_time" in at.session_state


def test_google_oauth_callback_sets_session_and_redirects():
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.query_params["code"] = "google-auth-code"
    at.query_params["state"] = "google-oauth-state"
    with patch(
        "src.ui.pages.login_page.handle_google_oauth_callback",
        return_value=_authenticated_session(
            username="user@gmail.com",
            display_name="Google User",
            email="user@gmail.com",
            auth_source=AUTH_SOURCE_SSO,
        ),
    ) as mock_callback, patch("src.ui.pages.login_page.switch_page_safely") as mock_switch:
        at.run()

    mock_callback.assert_called_once_with(
        code="google-auth-code",
        state="google-oauth-state",
        error="",
        error_description="",
    )
    assert at.session_state["authenticated"] is True
    assert at.session_state["auth_source"] == AUTH_SOURCE_SSO
    assert at.session_state["username"] == "user@gmail.com"
    assert at.session_state["display_name"] == "Google User"
    assert at.session_state["email"] == "user@gmail.com"
    mock_switch.assert_called_with("pages/dashboard.py")


# ---------------------------------------------------------------------------
# AUTH-04 — Inactive user + correct password shows account locked message
# ---------------------------------------------------------------------------


def test_inactive_user_correct_password_shows_account_locked_message():
    """AUTH-04: inactive user + correct password → 'Tài khoản đã bị khóa', failed_attempts không tăng."""
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.run()
    at.text_input[0].set_value("admin")
    at.text_input[1].set_value("password123")
    with patch(
        "src.application.auth.auth_service.AuthService.authenticate_session",
        side_effect=InactiveUserError("inactive"),
    ):
        at.button[0].click().run()

    assert len(at.error) == 1
    assert _MSG_INACTIVE in at.error[0].value
    assert at.session_state["failed_attempts"] == 0


# ---------------------------------------------------------------------------
# DB error — show generic system error message
# ---------------------------------------------------------------------------


def test_db_error_shows_system_error_message_and_does_not_increment_counter():
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.run()
    at.text_input[0].set_value("admin")
    at.text_input[1].set_value("password123")
    with patch(
        "src.application.auth.auth_service.AuthService.authenticate_session",
        side_effect=AuthError("timeout"),
    ):
        at.button[0].click().run()

    assert len(at.error) == 1
    assert _MSG_SYSTEM_ERROR in at.error[0].value
    assert at.session_state["failed_attempts"] == 0


# ---------------------------------------------------------------------------
# AC-3 / B-1 — 5th failure triggers lockout immediately
# ---------------------------------------------------------------------------


def test_fifth_wrong_attempt_triggers_lockout():
    """AC-3 / B-1: lần sai thứ 5 → failed_attempts=5, form bị vô hiệu hoá ngay."""
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.session_state["failed_attempts"] = 4
    at.run()
    at.text_input[0].set_value("admin")
    at.text_input[1].set_value("wrong")
    with patch(
        "src.application.auth.auth_service.AuthService.authenticate_session",
        side_effect=InvalidCredentialsError("invalid"),
    ):
        at.button[0].click().run()

    assert at.session_state["failed_attempts"] == 5
    assert at.button[0].disabled is True


# ---------------------------------------------------------------------------
# AC-4 — Already locked form
# ---------------------------------------------------------------------------


def test_locked_form_shows_lockout_message_and_disables_button():
    """AC-4: form đang lockout → hiển thị lockout message, button disabled."""
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.session_state["failed_attempts"] = 5
    at.run()

    assert len(at.error) == 1
    assert "bị khoá" in at.error[0].value
    assert at.button[0].disabled is True


# ---------------------------------------------------------------------------
# B-2 — 4th failure does NOT trigger lockout
# ---------------------------------------------------------------------------


def test_fourth_wrong_attempt_does_not_trigger_lockout():
    """B-2: lần sai thứ 4 → failed_attempts=4, form vẫn hoạt động bình thường."""
    at = AppTest.from_file(_LOGIN_SCRIPT, default_timeout=5)
    at.session_state["failed_attempts"] = 3
    at.run()
    at.text_input[0].set_value("admin")
    at.text_input[1].set_value("wrong")
    with patch(
        "src.application.auth.auth_service.AuthService.authenticate_session",
        side_effect=InvalidCredentialsError("invalid"),
    ):
        at.button[0].click().run()

    assert at.session_state["failed_attempts"] == 4
    assert at.button[0].disabled is False
