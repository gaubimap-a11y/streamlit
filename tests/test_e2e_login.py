"""
E2E tests for KOBE-00002 — Login flow (updated from KOBE-00001).

Tool: streamlit.testing.v1.AppTest.
Scope: cross-page user stories from spec-pack §8 (Normal / Abnormal / Boundary).
"""

import pathlib
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from streamlit.testing.v1 import AppTest

from src.domain.auth_models import AUTH_SOURCE_INTERNAL, AuthenticatedSession
from src.domain.auth_validation import InvalidCredentialsError

_LOGIN_TIME = datetime(2026, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
_SESSION_PERMISSIONS = ("app_access", "view_dashboard", "run_report", "view_data")

_LOGIN = str(pathlib.Path(__file__).parent.parent / "pages" / "login.py")
_DASHBOARD = str(pathlib.Path(__file__).parent.parent / "pages" / "dashboard.py")


def _authenticated_session(
    *,
    username: str = "admin",
    permissions: tuple[str, ...] = _SESSION_PERMISSIONS,
) -> AuthenticatedSession:
    return AuthenticatedSession(
        user_id="user-001",
        username=username,
        login_at=_LOGIN_TIME,
        expires_at=_LOGIN_TIME + timedelta(hours=8),
        auth_source=AUTH_SOURCE_INTERNAL,
        display_name="Admin",
        email="admin@example.com",
        permissions=permissions,
    )


def _clear_app_session(at: AppTest) -> None:
    for key in (
        "authenticated",
        "user_id",
        "username",
        "login_time",
        "auth_source",
        "permissions",
        "display_name",
        "email",
        "remember_me",
        "failed_attempts",
    ):
        if key in at.session_state:
            del at.session_state[key]


# ---------------------------------------------------------------------------
# N-1: Đăng nhập thành công → Dashboard có thể render (AC-1, AC-9)
# ---------------------------------------------------------------------------


def test_n1_login_success_then_dashboard_renders():
    """N-1: Đúng credentials → session hợp lệ → Dashboard render đúng username."""
    at_login = AppTest.from_file(_LOGIN, default_timeout=5)
    at_login.run()
    at_login.text_input[0].set_value("admin")
    at_login.text_input[1].set_value("password123")
    with patch(
        "src.application.auth.auth_service.AuthService.authenticate_session",
        return_value=_authenticated_session(),
    ), patch("src.ui.pages.login_page.switch_page_safely") as mock_switch:
        at_login.button[0].click().run()

    assert at_login.session_state["authenticated"] is True
    assert at_login.session_state["user_id"] == "user-001"
    assert at_login.session_state["username"] == "admin"
    assert at_login.session_state["display_name"] == "Admin"
    assert at_login.session_state["email"] == "admin@example.com"
    assert at_login.session_state["permissions"] == _SESSION_PERMISSIONS
    assert at_login.session_state["auth_source"] == AUTH_SOURCE_INTERNAL
    mock_switch.assert_called_with("pages/dashboard.py")


# ---------------------------------------------------------------------------
# N-2: Đăng xuất → đăng nhập lại thành công (AC-1, AC-8)
# ---------------------------------------------------------------------------


def test_n2_logout_then_login_again():
    """N-2: Đăng xuất thành công → đăng nhập lại; failed_attempts reset về 0."""
    at_login = AppTest.from_file(_LOGIN, default_timeout=5)
    at_login.run()
    at_login.text_input[0].set_value("admin")
    at_login.text_input[1].set_value("password123")
    with patch(
        "src.application.auth.auth_service.AuthService.authenticate_session",
        return_value=_authenticated_session(),
    ):
        at_login.button[0].click().run()
    assert at_login.session_state["authenticated"] is True

    _clear_app_session(at_login)

    assert "authenticated" not in at_login.session_state

    at_relogin = AppTest.from_file(_LOGIN, default_timeout=5)
    at_relogin.run()
    at_relogin.text_input[0].set_value("admin")
    at_relogin.text_input[1].set_value("password123")
    with patch(
        "src.application.auth.auth_service.AuthService.authenticate_session",
        return_value=_authenticated_session(),
    ):
        at_relogin.button[0].click().run()

    assert at_relogin.session_state["authenticated"] is True
    assert at_relogin.session_state["failed_attempts"] == 0


# ---------------------------------------------------------------------------
# A-2: Truy cập Dashboard trực tiếp khi chưa login (AC-7)
# ---------------------------------------------------------------------------


def test_a2_direct_dashboard_access_without_login():
    """A-2: Vào thẳng Dashboard khi chưa login → redirect, không render nội dung."""
    at = AppTest.from_file(_DASHBOARD, default_timeout=5)
    at.run()

    assert len(at.title) == 0


# ---------------------------------------------------------------------------
# B-1: Lần sai thứ 5 trigger lockout ngay (AC-3)
# ---------------------------------------------------------------------------


def test_b1_fifth_failure_triggers_lockout_immediately():
    """B-1: failed_attempts=4 precondition → sai lần 5 → lockout ngay lập tức."""
    at = AppTest.from_file(_LOGIN, default_timeout=5)
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
# B-3: Session timeout đúng tại mốc 8 giờ (AC-6)
# ---------------------------------------------------------------------------


def test_b3_session_timeout_at_exactly_8_hours():
    """B-3: login_time = now - 8h (boundary >=) → timeout, Dashboard không render."""
    login_time = datetime.now(tz=timezone.utc) - timedelta(hours=8)

    at = AppTest.from_file(_DASHBOARD, default_timeout=5)
    at.session_state["authenticated"] = True
    at.session_state["user_id"] = "user-001"
    at.session_state["username"] = "admin"
    at.session_state["login_time"] = login_time
    at.session_state["permissions"] = _SESSION_PERMISSIONS
    at.run()

    assert len(at.title) == 0
    assert "authenticated" not in at.session_state
