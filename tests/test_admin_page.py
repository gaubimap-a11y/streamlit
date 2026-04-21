from __future__ import annotations

import pathlib
from datetime import datetime, timedelta, timezone

from streamlit.testing.v1 import AppTest

from src.domain.auth_models import AUTH_SOURCE_INTERNAL
from src.domain.security_admin_models import SecurityPermission, SecurityRole, SecurityRoleDetail
from src.ui.pages.admin_page import SecurityAdminPage

_ADMIN_SCRIPT = str(pathlib.Path(__file__).parent.parent / "pages" / "admin.py")


def test_admin_page_renders_for_authorized_user(mocker):
    mock_service = mocker.MagicMock()
    mock_service.can_access_admin.return_value = True
    mock_service.list_users.return_value = ()
    mock_service.list_roles.return_value = ()
    mock_service.list_permissions.return_value = ()
    mock_service.list_audit.return_value = ()
    mocker.patch(
        "src.ui.pages.admin_page.SecurityAdminService.from_current_config",
        return_value=mock_service,
    )

    at = AppTest.from_file(_ADMIN_SCRIPT, default_timeout=5)
    at.session_state["authenticated"] = True
    at.session_state["user_id"] = "user-001"
    at.session_state["username"] = "admin"
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    at.session_state["auth_source"] = AUTH_SOURCE_INTERNAL
    at.session_state["permissions"] = (
        "app_access",
        "view_dashboard",
        "security_admin",
        "manage_users",
        "manage_roles",
        "manage_permissions",
        "view_security_audit",
    )
    at.run()

    assert len(at.error) == 0
    page_text = " ".join(str(widget.value) for widget in at.markdown)
    assert ("Quản trị bảo mật" in page_text) or ("Security workbench" in page_text)


def test_admin_page_filters_multiselect_defaults_outside_options(mocker):
    mock_service = mocker.MagicMock()
    mock_service.can_access_admin.return_value = True
    mock_service.list_users.return_value = ()
    mock_service.list_roles.return_value = (
        SecurityRole(
            role_id="role-1",
            role_name="Role 1",
            description="Role 1",
            is_active=True,
            users_count=1,
            permissions_count=2,
            updated_at=None,
        ),
    )
    mock_service.list_permissions.return_value = (
        SecurityPermission(
            permission_id="perm-view",
            permission_name="View",
            description="View",
            is_active=True,
            role_count=1,
        ),
    )
    mock_service.list_audit.return_value = ()
    mock_service.get_role_detail.return_value = SecurityRoleDetail(
        role=SecurityRole(
            role_id="role-1",
            role_name="Role 1",
            description="Role 1",
            is_active=True,
            users_count=1,
            permissions_count=2,
            updated_at=None,
        ),
        assigned_permissions=("perm-view", "perm-edit"),
        assigned_principals=(),
    )
    mocker.patch(
        "src.ui.pages.admin_page.SecurityAdminService.from_current_config",
        return_value=mock_service,
    )

    at = AppTest.from_file(_ADMIN_SCRIPT, default_timeout=5)
    at.session_state["authenticated"] = True
    at.session_state["user_id"] = "user-001"
    at.session_state["username"] = "admin"
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    at.session_state["auth_source"] = AUTH_SOURCE_INTERNAL
    at.session_state["permissions"] = (
        "app_access",
        "view_dashboard",
        "security_admin",
        "manage_users",
        "manage_roles",
        "manage_permissions",
        "view_security_audit",
    )
    at.run()

    assert len(at.error) == 0


def test_admin_page_keeps_single_dialog_when_multiple_modal_states_exist(mocker):
    mock_service = mocker.MagicMock()
    mock_service.can_access_admin.return_value = True
    mock_service.list_users.return_value = ()
    mock_service.list_roles.return_value = ()
    mock_service.list_permissions.return_value = ()
    mock_service.list_audit.return_value = ()
    mocker.patch(
        "src.ui.pages.admin_page.SecurityAdminService.from_current_config",
        return_value=mock_service,
    )

    at = AppTest.from_file(_ADMIN_SCRIPT, default_timeout=5)
    at.session_state["authenticated"] = True
    at.session_state["user_id"] = "user-001"
    at.session_state["username"] = "admin"
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    at.session_state["auth_source"] = AUTH_SOURCE_INTERNAL
    at.session_state["permissions"] = (
        "app_access",
        "view_dashboard",
        "security_admin",
        "manage_users",
        "manage_roles",
        "manage_permissions",
        "view_security_audit",
    )
    at.session_state["admin_role_modal_state"] = {"action": "add", "target_id": ""}
    at.session_state["admin_permission_modal_state"] = {"action": "add", "target_id": ""}
    at.run()

    assert len(at.error) == 0


def test_permission_ids_helper_maps_assigned_permission_names_to_catalog_ids(mocker):
    page = SecurityAdminPage(admin_service=mocker.MagicMock())
    permission_catalog = (
        SecurityPermission(
            permission_id="P006",
            permission_name="manage_users",
            description="Manage users",
            is_active=True,
            role_count=1,
        ),
        SecurityPermission(
            permission_id="P007",
            permission_name="manage_roles",
            description="Manage roles",
            is_active=True,
            role_count=1,
        ),
    )

    assert page._permission_ids_for_assigned_permissions(
        permission_catalog,
        ("manage_users", "P007", "unknown", "manage_users"),
    ) == ["P006", "P007"]


def test_invalidate_admin_cache_clears_scoped_and_unscoped_keys(mocker):
    page = SecurityAdminPage(admin_service=mocker.MagicMock())
    fake_session = mocker.MagicMock()
    fake_session.principal_id = "user-001"
    fake_session.permissions = ("manage_users",)
    scope = page._cache_scope(fake_session)
    target_key = ("target-id",)

    users_bucket = {
        (scope, target_key): {"ts": 1.0, "value": "scoped"},
        target_key: {"ts": 1.0, "value": "unscoped"},
        ("keep-me",): {"ts": 1.0, "value": "kept"},
    }
    cache_state = {"users": users_bucket}

    mocker.patch("src.ui.pages.admin_page.get_current_session", return_value=fake_session)
    mocker.patch("src.ui.pages.admin_page.st.session_state", {"admin_data_cache": cache_state})

    page._invalidate_admin_cache(bucket="users", key=target_key)

    assert (scope, target_key) not in users_bucket
    assert target_key not in users_bucket
    assert ("keep-me",) in users_bucket


def test_user_edit_modal_dismiss_invalidates_users_bucket(mocker):
    page = SecurityAdminPage(admin_service=mocker.MagicMock())
    fake_session = mocker.MagicMock()
    fake_session.principal_id = "user-001"
    fake_session.permissions = ("manage_users",)
    scope = page._cache_scope(fake_session)
    query_key = ("", "all", "all")

    users_bucket = {
        (scope, query_key): {"ts": 1.0, "value": ("cached",)},
    }
    cache_state = {"users": users_bucket}

    mocker.patch("src.ui.pages.admin_page.get_current_session", return_value=fake_session)
    mocker.patch("src.ui.pages.admin_page.st.session_state", {"admin_data_cache": cache_state})

    page._on_user_edit_modal_dismiss()

    assert cache_state["users"] == {}


def test_close_user_modal_with_users_refresh_invalidates_users_and_reruns(mocker):
    page = SecurityAdminPage(admin_service=mocker.MagicMock())
    fake_session = mocker.MagicMock()
    fake_session.principal_id = "user-001"
    fake_session.permissions = ("manage_users",)
    scope = page._cache_scope(fake_session)
    query_key = ("", "all", "all")

    users_bucket = {
        (scope, query_key): {"ts": 1.0, "value": ("cached",)},
    }
    cache_state = {"users": users_bucket}

    mocker.patch("src.ui.pages.admin_page.get_current_session", return_value=fake_session)
    mocker.patch("src.ui.pages.admin_page.st.session_state", {"admin_data_cache": cache_state})
    rerun_mock = mocker.patch("src.ui.pages.admin_page.st.rerun")

    page._close_user_modal_with_users_refresh()

    assert cache_state["users"] == {}
    rerun_mock.assert_called_once_with()


def test_role_edit_modal_dismiss_invalidates_roles_bucket(mocker):
    page = SecurityAdminPage(admin_service=mocker.MagicMock())
    fake_session = mocker.MagicMock()
    fake_session.principal_id = "user-001"
    fake_session.permissions = ("manage_roles",)
    scope = page._cache_scope(fake_session)
    query_key = ("", "all")

    roles_bucket = {
        (scope, query_key): {"ts": 1.0, "value": ("cached",)},
    }
    cache_state = {"roles": roles_bucket}

    mocker.patch("src.ui.pages.admin_page.get_current_session", return_value=fake_session)
    mocker.patch("src.ui.pages.admin_page.st.session_state", {"admin_data_cache": cache_state})

    page._on_role_edit_modal_dismiss()

    assert cache_state["roles"] == {}


def test_close_role_modal_with_roles_refresh_invalidates_roles_and_reruns(mocker):
    page = SecurityAdminPage(admin_service=mocker.MagicMock())
    fake_session = mocker.MagicMock()
    fake_session.principal_id = "user-001"
    fake_session.permissions = ("manage_roles",)
    scope = page._cache_scope(fake_session)
    query_key = ("", "all")

    roles_bucket = {
        (scope, query_key): {"ts": 1.0, "value": ("cached",)},
    }
    cache_state = {"roles": roles_bucket}

    mocker.patch("src.ui.pages.admin_page.get_current_session", return_value=fake_session)
    mocker.patch("src.ui.pages.admin_page.st.session_state", {"admin_data_cache": cache_state})
    rerun_mock = mocker.patch("src.ui.pages.admin_page.st.rerun")

    page._close_role_modal_with_roles_refresh()

    assert cache_state["roles"] == {}
    rerun_mock.assert_called_once_with()


def test_permission_edit_modal_dismiss_invalidates_permissions_bucket(mocker):
    page = SecurityAdminPage(admin_service=mocker.MagicMock())
    fake_session = mocker.MagicMock()
    fake_session.principal_id = "user-001"
    fake_session.permissions = ("manage_permissions",)
    scope = page._cache_scope(fake_session)
    query_key = ("",)

    permissions_bucket = {
        (scope, query_key): {"ts": 1.0, "value": ("cached",)},
    }
    cache_state = {"permissions": permissions_bucket}

    mocker.patch("src.ui.pages.admin_page.get_current_session", return_value=fake_session)
    mocker.patch("src.ui.pages.admin_page.st.session_state", {"admin_data_cache": cache_state})

    page._on_permission_edit_modal_dismiss()

    assert cache_state["permissions"] == {}


def test_close_permission_modal_with_permissions_refresh_invalidates_and_reruns(mocker):
    page = SecurityAdminPage(admin_service=mocker.MagicMock())
    fake_session = mocker.MagicMock()
    fake_session.principal_id = "user-001"
    fake_session.permissions = ("manage_permissions",)
    scope = page._cache_scope(fake_session)
    query_key = ("",)

    permissions_bucket = {
        (scope, query_key): {"ts": 1.0, "value": ("cached",)},
    }
    cache_state = {"permissions": permissions_bucket}

    mocker.patch("src.ui.pages.admin_page.get_current_session", return_value=fake_session)
    mocker.patch("src.ui.pages.admin_page.st.session_state", {"admin_data_cache": cache_state})
    rerun_mock = mocker.patch("src.ui.pages.admin_page.st.rerun")

    page._close_permission_modal_with_permissions_refresh()

    assert cache_state["permissions"] == {}
    rerun_mock.assert_called_once_with()


def test_admin_page_with_open_modal_skips_background_tab_queries(mocker):
    mock_service = mocker.MagicMock()
    mock_service.can_access_admin.return_value = True
    mock_service.list_users.return_value = ()
    mock_service.list_roles.return_value = ()
    mock_service.list_permissions.return_value = ()
    mock_service.list_audit.return_value = ()
    mocker.patch(
        "src.ui.pages.admin_page.SecurityAdminService.from_current_config",
        return_value=mock_service,
    )

    at = AppTest.from_file(_ADMIN_SCRIPT, default_timeout=5)
    at.session_state["authenticated"] = True
    at.session_state["user_id"] = "user-001"
    at.session_state["username"] = "admin"
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    at.session_state["auth_source"] = AUTH_SOURCE_INTERNAL
    at.session_state["permissions"] = (
        "app_access",
        "view_dashboard",
        "manage_roles",
    )
    at.session_state["admin_role_modal_state"] = {"action": "add", "target_id": ""}
    at.run()

    assert len(at.error) == 0
    assert mock_service.list_users.call_count == 0
    assert mock_service.list_roles.call_count == 0
    assert mock_service.list_permissions.call_count == 0
