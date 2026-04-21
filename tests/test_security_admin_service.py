from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.application.auth.security_admin_service import SecurityAdminService
from src.domain.auth_models import AUTH_SOURCE_INTERNAL, AuthenticatedSession
from src.domain.auth_validation import AuthenticationValidationError
from src.domain.security_admin_models import (
    SecurityPermission,
    SecurityPrincipal,
    SecurityRole,
    SecurityRoleDetail,
)


_BASE_TIME = datetime(2026, 1, 1, 8, 0, 0, tzinfo=timezone.utc)


def _session(*, user_id: str, username: str, permissions: tuple[str, ...]) -> AuthenticatedSession:
    return AuthenticatedSession(
        user_id=user_id,
        username=username,
        login_at=_BASE_TIME,
        expires_at=_BASE_TIME + timedelta(hours=8),
        auth_source=AUTH_SOURCE_INTERNAL,
        display_name=username.title(),
        email=f"{username}@example.com",
        permissions=permissions,
    )


def _principal(
    *,
    principal_id: str,
    username: str,
    roles_count: int = 1,
    is_active: bool = True,
) -> SecurityPrincipal:
    return SecurityPrincipal(
        principal_id=principal_id,
        username=username,
        email=f"{username}@example.com",
        display_name=username.title(),
        auth_source=AUTH_SOURCE_INTERNAL,
        is_active=is_active,
        roles_count=roles_count,
        last_login_at=None,
        updated_at=None,
    )


def _role(*, role_id: str, role_name: str, is_active: bool = True) -> SecurityRole:
    return SecurityRole(
        role_id=role_id,
        role_name=role_name,
        description=f"{role_name} description",
        is_active=is_active,
        users_count=1,
        permissions_count=1,
        updated_at=None,
    )


def _permission(*, permission_id: str, permission_name: str, is_active: bool = True) -> SecurityPermission:
    return SecurityPermission(
        permission_id=permission_id,
        permission_name=permission_name,
        description=f"{permission_name} description",
        is_active=is_active,
        role_count=1,
    )


def _build_service(mocker):
    store = mocker.MagicMock()
    store.username_exists.return_value = False
    store.email_exists.return_value = False
    store.role_name_exists.return_value = False
    store.list_permissions.return_value = ()
    store.list_all_role_permissions.return_value = {}
    store.list_all_principal_active_roles.return_value = {}
    service = SecurityAdminService(store=store, audit_writer=None)
    return service, store


def _install_security_snapshot(
    store,
    *,
    principals: tuple[SecurityPrincipal, ...],
    roles: tuple[SecurityRole, ...],
    role_permissions: dict[str, tuple[str, ...]],
    principal_roles: dict[str, tuple[str, ...]],
) -> None:
    store.list_principals.return_value = principals
    store.list_roles.return_value = roles
    store.list_all_role_permissions.return_value = dict(role_permissions)
    store.list_all_principal_active_roles.return_value = dict(principal_roles)
    all_permission_names = sorted({permission for permissions in role_permissions.values() for permission in permissions})
    store.list_permissions.return_value = tuple(
        _permission(permission_id=f"P{index:03d}", permission_name=permission_name)
        for index, permission_name in enumerate(all_permission_names, start=1)
    )

    def _assigned_roles(*, principal_id: str):
        return principal_roles.get(principal_id, ())

    def _assigned_permissions(*, role_id: str):
        return role_permissions.get(role_id, ())

    def _assigned_principals(*, role_id: str):
        return tuple(
            principal_id
            for principal_id, assigned_roles in principal_roles.items()
            if role_id in assigned_roles
        )

    store.list_assigned_roles_for_principal.side_effect = _assigned_roles
    store.list_assigned_permissions_for_role.side_effect = _assigned_permissions
    store.list_assigned_principals_for_role.side_effect = _assigned_principals


def test_save_user_updates_existing_user_with_normalized_fields(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_users",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    user_role = _role(role_id="role-user", role_name="User")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    target_principal = _principal(principal_id="user-2", username="user2")
    store.get_principal.side_effect = lambda *, principal_id: {
        "admin-1": admin_principal,
        "user-2": target_principal,
    }.get(principal_id)
    _install_security_snapshot(
        store,
        principals=(admin_principal, target_principal),
        roles=(admin_role, user_role),
        role_permissions={
            "role-admin": ("manage_users",),
            "role-user": ("view_dashboard",),
        },
        principal_roles={
            "admin-1": ("role-admin",),
            "user-2": ("role-user",),
        },
    )

    service.save_user(
        session,
        principal_id=" user-2 ",
        username=" user2 ",
        email=" USER2@example.com ",
        display_name=" User Two ",
        auth_source=" internal ",
        is_active=True,
    )

    store.upsert_principal.assert_called_once_with(
        principal_id="user-2",
        username="user2",
        email="user2@example.com",
        display_name="User Two",
        auth_source="internal",
        is_active=True,
    )


def test_save_user_rejects_self_lockout_when_deactivating_own_admin_account(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_users",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    store.get_principal.return_value = admin_principal
    _install_security_snapshot(
        store,
        principals=(admin_principal,),
        roles=(admin_role,),
        role_permissions={"role-admin": ("manage_users",)},
        principal_roles={"admin-1": ("role-admin",)},
    )

    with pytest.raises(AuthenticationValidationError, match="remove your admin access"):
        service.save_user(
            session,
            principal_id="admin-1",
            username="admin",
            email="admin@example.com",
            display_name="Admin",
            auth_source="internal",
            is_active=False,
        )

    store.upsert_principal.assert_not_called()


def test_unassign_roles_rejects_self_lockout_when_removing_own_admin_role(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_users",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    store.get_principal.return_value = admin_principal
    _install_security_snapshot(
        store,
        principals=(admin_principal,),
        roles=(admin_role,),
        role_permissions={"role-admin": ("manage_users",)},
        principal_roles={"admin-1": ("role-admin",)},
    )

    with pytest.raises(AuthenticationValidationError, match="remove your admin access"):
        service.unassign_roles(session, principal_id="admin-1", role_ids=["role-admin"])

    store.unassign_roles_from_principal.assert_not_called()


def test_save_role_rejects_last_admin_when_deactivating_only_admin_role(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="editor-1", username="editor", permissions=("manage_roles",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    editor_role = _role(role_id="role-editor", role_name="Editor")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    editor_principal = _principal(principal_id="editor-1", username="editor", roles_count=0)
    store.get_role.return_value = admin_role
    store.get_principal.side_effect = lambda *, principal_id: {
        "admin-1": admin_principal,
        "editor-1": editor_principal,
    }.get(principal_id)
    _install_security_snapshot(
        store,
        principals=(admin_principal, editor_principal),
        roles=(admin_role, editor_role),
        role_permissions={
            "role-admin": ("manage_users",),
            "role-editor": ("view_dashboard",),
        },
        principal_roles={
            "admin-1": ("role-admin",),
            "editor-1": (),
        },
    )

    with pytest.raises(AuthenticationValidationError, match="remove the last admin"):
        service.save_role(
            session,
            role_id="role-admin",
            role_name="Admin",
            description="Admin description",
            is_active=False,
        )

    store.upsert_role.assert_not_called()


def test_unassign_permissions_rejects_last_admin_when_removing_only_admin_permission(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="editor-1", username="editor", permissions=("manage_permissions",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    store.get_role.return_value = admin_role
    _install_security_snapshot(
        store,
        principals=(admin_principal,),
        roles=(admin_role,),
        role_permissions={"role-admin": ("manage_users",)},
        principal_roles={"admin-1": ("role-admin",)},
    )

    with pytest.raises(AuthenticationValidationError, match="remove the last admin"):
        service.unassign_permissions(session, role_id="role-admin", permission_ids=["manage_users"])

    store.unassign_permissions_from_role.assert_not_called()


def test_delete_user_soft_deletes_target_principal_when_safe(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_users",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    user_role = _role(role_id="role-user", role_name="User")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    target_principal = _principal(principal_id="user-2", username="user2")
    store.get_principal.side_effect = lambda *, principal_id: {
        "admin-1": admin_principal,
        "user-2": target_principal,
    }.get(principal_id)
    _install_security_snapshot(
        store,
        principals=(admin_principal, target_principal),
        roles=(admin_role, user_role),
        role_permissions={
            "role-admin": ("manage_users",),
            "role-user": ("view_dashboard",),
        },
        principal_roles={
            "admin-1": ("role-admin",),
            "user-2": ("role-user",),
        },
    )

    service.delete_user(session, principal_id="user-2")

    store.soft_delete_principal.assert_called_once_with(principal_id="user-2", actor="admin")


def test_delete_permission_rejects_last_admin_when_it_removes_final_admin_gate(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="editor-1", username="editor", permissions=("manage_permissions",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    store.get_permission.return_value = _permission(permission_id="P006", permission_name="manage_users")
    store.list_assigned_roles_for_permission.return_value = ("role-admin",)
    _install_security_snapshot(
        store,
        principals=(admin_principal,),
        roles=(admin_role,),
        role_permissions={"role-admin": ("manage_users",)},
        principal_roles={"admin-1": ("role-admin",)},
    )

    with pytest.raises(AuthenticationValidationError, match="remove the last admin"):
        service.delete_permission(session, permission_id="P006")

    store.soft_delete_permission.assert_not_called()


def test_save_permission_rejects_rename_that_removes_last_admin_gate(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="editor-1", username="editor", permissions=("manage_permissions",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    store.get_permission.return_value = _permission(permission_id="P006", permission_name="manage_users")
    store.permission_name_exists.return_value = False
    store.list_assigned_roles_for_permission.return_value = ("role-admin",)
    _install_security_snapshot(
        store,
        principals=(admin_principal,),
        roles=(admin_role,),
        role_permissions={"role-admin": ("manage_users",)},
        principal_roles={"admin-1": ("role-admin",)},
    )

    with pytest.raises(AuthenticationValidationError, match="remove the last admin"):
        service.save_permission(
            session,
            permission_id="P006",
            permission_name="manage_users_v2",
            description="updated",
            is_active=True,
        )

    store.upsert_permission.assert_not_called()


def test_create_role_generates_id_and_persists(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_roles",))

    store.generate_next_role_id.return_value = "R011"
    store.role_name_exists.return_value = False
    store.get_role.return_value = _role(role_id="R011", role_name="auditor")

    created_role_id = service.create_role(
        session,
        role_name=" auditor ",
        description=" Audit role ",
        is_active=True,
    )

    assert created_role_id == "R011"
    store.generate_next_role_id.assert_called_once_with()
    store.upsert_role.assert_called_once_with(
        role_id="R011",
        role_name="auditor",
        description="Audit role",
        is_active=True,
    )


def test_create_permission_generates_id_and_persists(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_permissions",))

    store.generate_next_permission_id.return_value = "P011"
    store.permission_name_exists.return_value = False
    store.get_permission.return_value = _permission(permission_id="P011", permission_name="approve_budget")

    created_permission_id = service.create_permission(
        session,
        permission_name=" approve_budget ",
        description=" Allow budget approval ",
        is_active=True,
    )

    assert created_permission_id == "P011"
    store.generate_next_permission_id.assert_called_once_with()
    store.upsert_permission.assert_called_once_with(
        permission_id="P011",
        permission_name="approve_budget",
        description="Allow budget approval",
        is_active=True,
    )


def test_sync_role_permissions_accepts_permission_ids_without_false_self_lockout(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_roles",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    store.get_role_detail.return_value = SecurityRoleDetail(
        role=admin_role,
        assigned_permissions=("manage_users", "view_dashboard"),
        assigned_principals=("admin-1",),
    )
    store.get_role.return_value = admin_role
    _install_security_snapshot(
        store,
        principals=(admin_principal,),
        roles=(admin_role,),
        role_permissions={"role-admin": ("manage_users", "view_dashboard")},
        principal_roles={"admin-1": ("role-admin",)},
    )
    store.list_permissions.return_value = (
        _permission(permission_id="P006", permission_name="manage_users"),
        _permission(permission_id="P002", permission_name="view_dashboard"),
    )

    service.sync_role_permissions(
        session,
        role_id="role-admin",
        role_name="Admin",
        description="Admin role",
        is_active=True,
        target_permission_ids=("P006", "P002"),
    )

    store.assign_permissions_to_role.assert_not_called()
    store.unassign_permissions_from_role.assert_not_called()


def test_sync_user_roles_without_admin_rights_change_skips_global_admin_snapshot(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_users",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    user_role = _role(role_id="role-user", role_name="User")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    target_principal = _principal(principal_id="user-2", username="user2")
    store.get_principal.side_effect = lambda *, principal_id: {
        "admin-1": admin_principal,
        "user-2": target_principal,
    }.get(principal_id)
    _install_security_snapshot(
        store,
        principals=(admin_principal, target_principal),
        roles=(admin_role, user_role),
        role_permissions={
            "role-admin": ("manage_users",),
            "role-user": ("view_dashboard",),
        },
        principal_roles={
            "admin-1": ("role-admin",),
            "user-2": ("role-user",),
        },
    )

    service.sync_user_roles(
        session,
        principal_id="user-2",
        username="user2",
        email="user2@example.com",
        display_name="User Two",
        auth_source="internal",
        is_active=True,
        target_role_ids=("role-user",),
    )

    store.list_all_principal_active_roles.assert_not_called()
    store.upsert_principal.assert_called_once()


def test_sync_user_roles_rejects_self_lockout_and_keeps_admin_safety_check(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_users",))

    admin_role = _role(role_id="role-admin", role_name="Admin")
    admin_principal = _principal(principal_id="admin-1", username="admin")
    store.get_principal.return_value = admin_principal
    _install_security_snapshot(
        store,
        principals=(admin_principal,),
        roles=(admin_role,),
        role_permissions={"role-admin": ("manage_users",)},
        principal_roles={"admin-1": ("role-admin",)},
    )

    with pytest.raises(AuthenticationValidationError, match="remove your admin access"):
        service.sync_user_roles(
            session,
            principal_id="admin-1",
            username="admin",
            email="admin@example.com",
            display_name="Admin",
            auth_source="internal",
            is_active=True,
            target_role_ids=(),
        )

    store.list_all_principal_active_roles.assert_called_once()
    store.upsert_principal.assert_not_called()


def test_save_user_without_admin_rights_change_skips_safety_snapshot(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_users",))
    target = _principal(principal_id="user-2", username="user2", is_active=True)
    store.get_principal.return_value = target

    service.save_user(
        session,
        principal_id="user-2",
        username="user2",
        email="user2@example.com",
        display_name="User Two Updated",
        auth_source="internal",
        is_active=True,
    )

    store.list_roles.assert_not_called()
    store.list_principals.assert_not_called()
    store.list_assigned_roles_for_principal.assert_not_called()
    store.upsert_principal.assert_called_once()


def test_save_role_active_update_skips_safety_snapshot(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_roles",))
    store.get_role.return_value = _role(role_id="role-user", role_name="User")
    store.role_name_exists.return_value = False

    service.save_role(
        session,
        role_id="role-user",
        role_name="User",
        description="Updated description",
        is_active=True,
    )

    store.list_roles.assert_not_called()
    store.list_principals.assert_not_called()
    store.upsert_role.assert_called_once()


def test_save_permission_non_admin_change_skips_safety_snapshot(mocker):
    service, store = _build_service(mocker)
    session = _session(user_id="admin-1", username="admin", permissions=("manage_permissions",))
    store.get_permission.return_value = _permission(permission_id="P002", permission_name="view_dashboard")
    store.permission_name_exists.return_value = False

    service.save_permission(
        session,
        permission_id="P002",
        permission_name="view_dashboard",
        description="Updated description",
        is_active=True,
    )

    store.list_roles.assert_not_called()
    store.list_assigned_roles_for_permission.assert_not_called()
    store.list_principals.assert_not_called()
    store.upsert_permission.assert_called_once()
