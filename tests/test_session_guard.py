from datetime import datetime, timedelta, timezone

from src.ui.session import auth_session


def _patch_session_helpers(mocker):
    mocker.patch("src.ui.session.auth_session.restore_auth_from_query_params")
    mocker.patch("src.ui.session.auth_session._sync_auth_query_params_from_session")
    mocker.patch("src.ui.session.auth_session._clear_auth_query_params")


def test_require_auth_redirects_when_not_authenticated(mocker):
    _patch_session_helpers(mocker)
    mocker.patch("src.ui.session.auth_session.st.session_state", {})
    mock_switch = mocker.patch("src.ui.session.auth_session.st.switch_page")
    mocker.patch("src.ui.session.auth_session.st.stop")

    auth_session.require_auth()

    mock_switch.assert_called_once()


def test_require_auth_redirects_when_session_expired(mocker):
    _patch_session_helpers(mocker)
    session = {
        auth_session.KEY_AUTHENTICATED: True,
        auth_session.KEY_USER_ID: "user-001",
        auth_session.KEY_LOGIN_TIME: datetime.now(tz=timezone.utc) - timedelta(hours=9),
        auth_session.KEY_USERNAME: "admin",
        auth_session.KEY_PERMISSIONS: ("app_access", "view_dashboard"),
    }
    mocker.patch("src.ui.session.auth_session.st.session_state", session)
    mock_switch = mocker.patch("src.ui.session.auth_session.st.switch_page")
    mocker.patch("src.ui.session.auth_session.st.stop")
    mock_error = mocker.patch("src.ui.session.auth_session.st.error")

    class _S:
        session_timeout_hours = 8

    mocker.patch("src.ui.session.auth_session.get_settings", return_value=_S())

    auth_session.require_auth()

    mock_switch.assert_called_once()
    mock_error.assert_called_once_with("Phiên làm việc đã hết hạn.")
    assert auth_session.KEY_AUTHENTICATED not in session


def test_require_auth_passes_when_authenticated_and_not_expired(mocker):
    _patch_session_helpers(mocker)
    session = {
        auth_session.KEY_AUTHENTICATED: True,
        auth_session.KEY_USER_ID: "user-001",
        auth_session.KEY_LOGIN_TIME: datetime.now(tz=timezone.utc) - timedelta(hours=1),
        auth_session.KEY_USERNAME: "admin",
        auth_session.KEY_PERMISSIONS: ("app_access", "view_dashboard"),
    }
    mocker.patch("src.ui.session.auth_session.st.session_state", session)
    mock_switch = mocker.patch("src.ui.session.auth_session.st.switch_page")
    mocker.patch("src.ui.session.auth_session.st.stop")

    class _S:
        session_timeout_hours = 8

    mocker.patch("src.ui.session.auth_session.get_settings", return_value=_S())

    auth_session.require_auth()

    mock_switch.assert_not_called()


def test_clear_session_removes_all_auth_keys(mocker):
    mocker.patch("src.ui.session.auth_session._clear_auth_query_params")
    session = {
        auth_session.KEY_AUTHENTICATED: True,
        auth_session.KEY_USER_ID: "user-001",
        auth_session.KEY_USERNAME: "admin",
        auth_session.KEY_LOGIN_TIME: datetime.now(tz=timezone.utc),
        auth_session.KEY_AUTH_SOURCE: "internal",
        auth_session.KEY_PERMISSIONS: ("app_access", "view_dashboard"),
        auth_session.KEY_DISPLAY_NAME: "Admin",
        auth_session.KEY_EMAIL: "admin@example.com",
        auth_session.KEY_FAILED_ATTEMPTS: 2,
        auth_session.KEY_REMEMBER_ME: True,
    }
    mocker.patch("src.ui.session.auth_session.st.session_state", session)

    auth_session.clear_session()

    assert auth_session.KEY_AUTHENTICATED not in session
    assert auth_session.KEY_USER_ID not in session
    assert auth_session.KEY_USERNAME not in session
    assert auth_session.KEY_LOGIN_TIME not in session
    assert auth_session.KEY_AUTH_SOURCE not in session
    assert auth_session.KEY_PERMISSIONS not in session
    assert auth_session.KEY_DISPLAY_NAME not in session
    assert auth_session.KEY_EMAIL not in session
    assert auth_session.KEY_FAILED_ATTEMPTS not in session
    assert auth_session.KEY_REMEMBER_ME not in session


def test_require_auth_redirects_when_missing_app_access(mocker):
    _patch_session_helpers(mocker)
    session = {
        auth_session.KEY_AUTHENTICATED: True,
        auth_session.KEY_USER_ID: "user-001",
        auth_session.KEY_LOGIN_TIME: datetime.now(tz=timezone.utc) - timedelta(hours=1),
        auth_session.KEY_USERNAME: "admin",
        auth_session.KEY_PERMISSIONS: ("view_dashboard",),
    }
    mocker.patch("src.ui.session.auth_session.st.session_state", session)
    mock_switch = mocker.patch("src.ui.session.auth_session.st.switch_page")
    mocker.patch("src.ui.session.auth_session.st.stop")
    mock_error = mocker.patch("src.ui.session.auth_session.st.error")

    class _S:
        session_timeout_hours = 8

    mocker.patch("src.ui.session.auth_session.get_settings", return_value=_S())

    auth_session.require_auth()

    mock_switch.assert_called_once()
    mock_error.assert_called_once_with("Tài khoản không có quyền truy cập ứng dụng.")
    assert auth_session.KEY_AUTHENTICATED not in session
