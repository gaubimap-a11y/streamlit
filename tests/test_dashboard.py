"""UT for pages/dashboard.py - UI layer."""

import pathlib
from datetime import datetime, timedelta, timezone

from streamlit.testing.v1 import AppTest

from src.core.reporting import ReportData
from src.domain.auth_models import AUTH_SOURCE_INTERNAL
from src.ui.pages.dashboard_page import DashboardPage

_DASHBOARD_SCRIPT = str(pathlib.Path(__file__).parent.parent / "pages" / "dashboard.py")


def _set_valid_session(at: AppTest, username: str = "admin") -> None:
    at.session_state["authenticated"] = True
    at.session_state["user_id"] = "user-001"
    at.session_state["username"] = username
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    at.session_state["auth_source"] = AUTH_SOURCE_INTERNAL
    at.session_state["display_name"] = "Admin"
    at.session_state["email"] = "admin@example.com"
    at.session_state["permissions"] = ("app_access", "view_dashboard", "run_report", "view_data")


def test_dashboard_shows_username_when_authenticated():
    at = AppTest.from_file(_DASHBOARD_SCRIPT, default_timeout=5)
    _set_valid_session(at, username="admin")
    at.run()

    assert len(at.error) == 0
    assert len(at.dataframe) > 0
    assert at.session_state["username"] == "admin"
    assert at.session_state["display_name"] == "Admin"


def test_dashboard_logout_clears_all_auth_keys():
    at = AppTest.from_file(_DASHBOARD_SCRIPT, default_timeout=5)
    _set_valid_session(at)
    at.run()
    at.button[0].click().run()

    assert "authenticated" not in at.session_state
    assert "user_id" not in at.session_state
    assert "username" not in at.session_state
    assert "login_time" not in at.session_state
    assert "auth_source" not in at.session_state
    assert "display_name" not in at.session_state
    assert "email" not in at.session_state
    assert "permissions" not in at.session_state


def test_dashboard_redirects_when_not_authenticated():
    at = AppTest.from_file(_DASHBOARD_SCRIPT, default_timeout=5)
    at.run()

    assert len(at.title) == 0


def test_dashboard_redirects_when_session_expired():
    at = AppTest.from_file(_DASHBOARD_SCRIPT, default_timeout=5)
    at.session_state["authenticated"] = True
    at.session_state["username"] = "admin"
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=9)
    at.run()

    assert len(at.title) == 0
    assert "authenticated" not in at.session_state


def test_dashboard_denies_when_missing_view_dashboard_permission():
    at = AppTest.from_file(_DASHBOARD_SCRIPT, default_timeout=5)
    at.session_state["authenticated"] = True
    at.session_state["user_id"] = "user-001"
    at.session_state["username"] = "admin"
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    at.session_state["auth_source"] = AUTH_SOURCE_INTERNAL
    at.session_state["permissions"] = ("app_access", "run_report", "view_data")
    at.run()

    assert len(at.error) == 1
    assert "không có quyền truy cập trang Dashboard" in at.error[0].value


def test_dashboard_restores_session_from_query_params_after_refresh():
    at = AppTest.from_file(_DASHBOARD_SCRIPT, default_timeout=5)
    at.query_params["auth"] = "1"
    at.query_params["user_id"] = "user-001"
    at.query_params["user"] = "admin"
    at.query_params["login_time"] = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).isoformat()
    at.query_params["auth_source"] = AUTH_SOURCE_INTERNAL
    at.query_params["display_name"] = "Admin"
    at.query_params["email"] = "admin@example.com"
    at.query_params["permissions"] = "app_access,view_dashboard,run_report,view_data"
    at.run()

    assert at.session_state["authenticated"] is True
    assert at.session_state["user_id"] == "user-001"
    assert at.session_state["username"] == "admin"
    assert at.session_state["display_name"] == "Admin"
    assert at.session_state["email"] == "admin@example.com"
    assert at.session_state["permissions"] == ("app_access", "view_dashboard", "run_report", "view_data")
    assert at.title[0].value == "Dashboard"


def test_dashboard_exporter_generates_csv_bytes(mocker):
    session = {
        "authenticated": True,
        "user_id": "user-001",
        "username": "admin",
        "login_time": datetime.now(tz=timezone.utc) - timedelta(hours=1),
        "auth_source": AUTH_SOURCE_INTERNAL,
        "display_name": "Admin",
        "email": "admin@example.com",
        "permissions": ("app_access", "view_dashboard", "run_report", "view_data", "export_output"),
    }
    mocker.patch("src.ui.session.auth_session.st.session_state", session)
    mocker.patch("src.ui.pages.dashboard_page.st.session_state", session)

    exporter = DashboardPage().exporter
    csv_bytes = exporter.export(
        ReportData(total=1, rows=[{"product_name": "x", "price": 1000}], chart_data=[]),
    )

    assert b"product_name" in csv_bytes
    assert b"x" in csv_bytes
