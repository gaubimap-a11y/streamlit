from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
import time

from src.application.auth.audit_service import AuditEventWriter, build_audit_event_from_session, record_audit_event
from src.domain.auth_models import AuthenticatedSession
from src.domain.auth_validation import AuthenticationValidationError
from src.domain.security_admin_models import (
    SecurityAuditRecord,
    SecurityPermission,
    SecurityPrincipal,
    SecurityPrincipalDetail,
    SecurityRole,
    SecurityRoleDetail,
)
from src.domain.security_admin_validation import (
    normalize_admin_text,
    normalize_bulk_items,
    normalize_optional_admin_text,
    validate_admin_auth_source,
    validate_admin_email,
    validate_admin_username,
)
from src.infrastructure.repositories.security_admin_store import DatabricksSecurityAdminStore

ADMIN_GATE_PERMISSIONS = ("security_admin", "manage_users", "manage_roles", "manage_permissions", "view_security_audit")
_SECURITY_SNAPSHOT_TTL_SECONDS = 5.0
_SECURITY_SNAPSHOT_CACHE_LOCK = Lock()


@dataclass(frozen=True)
class _SecuritySnapshot:
    expires_at: float
    role_active_map: dict[str, bool]
    role_permissions_map: dict[str, tuple[str, ...]]
    current_admin_ids: set[str] | None = None


_SECURITY_SNAPSHOT_CACHE: dict[str, _SecuritySnapshot] = {}


@dataclass(frozen=True)
class SecurityAdminService:
    store: DatabricksSecurityAdminStore
    audit_writer: AuditEventWriter | None = None

    @classmethod
    def from_current_config(cls, *, audit_writer: AuditEventWriter | None = None) -> "SecurityAdminService":
        return cls(store=DatabricksSecurityAdminStore.from_current_config(), audit_writer=audit_writer)

    def list_users(
        self,
        session: AuthenticatedSession,
        *,
        search_term: str = "",
        status_filter: str = "all",
        auth_source_filter: str = "all",
    ) -> tuple[SecurityPrincipal, ...]:
        self._require_any(session, ("manage_users", "security_admin"), action="list_users")
        return self.store.list_principals(search_term=search_term.strip(), status_filter=status_filter, auth_source_filter=auth_source_filter)

    def list_roles(self, session: AuthenticatedSession, *, search_term: str = "", status_filter: str = "all") -> tuple[SecurityRole, ...]:
        self._require_any(session, ("manage_roles", "security_admin"), action="list_roles")
        return self.store.list_roles(search_term=search_term.strip(), status_filter=status_filter)

    def list_permissions(
        self,
        session: AuthenticatedSession,
        *,
        search_term: str = "",
    ) -> tuple[SecurityPermission, ...]:
        self._require_any(session, ("manage_permissions", "security_admin"), action="list_permissions")
        return self.store.list_permissions(search_term=search_term.strip())

    def list_audit(self, session: AuthenticatedSession, *, limit: int = 200) -> tuple[SecurityAuditRecord, ...]:
        self._require_any(session, ("view_security_audit", "security_admin"), action="list_audit")
        return self.store.list_security_audit(limit=limit)

    def get_user_detail(self, session: AuthenticatedSession, *, principal_id: str) -> SecurityPrincipalDetail:
        self._require_any(session, ("manage_users", "security_admin"), action="get_user_detail")
        normalized_principal_id = normalize_admin_text(principal_id, "principal_id").normalized_value
        detail = self.store.get_principal_detail(principal_id=normalized_principal_id)
        if detail is None:
            raise AuthenticationValidationError("User does not exist.")
        return detail

    def get_role_detail(self, session: AuthenticatedSession, *, role_id: str) -> SecurityRoleDetail:
        self._require_any(session, ("manage_roles", "security_admin"), action="get_role_detail")
        normalized_role_id = normalize_admin_text(role_id, "role_id").normalized_value
        detail = self.store.get_role_detail(role_id=normalized_role_id)
        if detail is None:
            raise AuthenticationValidationError("Role does not exist.")
        return detail

    def get_permission_detail(self, session: AuthenticatedSession, *, permission_id: str) -> SecurityPermission:
        self._require_any(session, ("manage_permissions", "security_admin"), action="get_permission_detail")
        normalized_permission_id = normalize_admin_text(permission_id, "permission_id").normalized_value
        permission = self.store.get_permission(permission_id=normalized_permission_id)
        if permission is None:
            raise AuthenticationValidationError("Permission does not exist.")
        return permission

    def preview_effective_permissions(
        self,
        session: AuthenticatedSession,
        *,
        principal_id: str,
        add_role_ids: tuple[str, ...] | list[str] = (),
        remove_role_ids: tuple[str, ...] | list[str] = (),
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        self._require_any(session, ("manage_users", "security_admin"), action="preview_effective_permissions")
        normalized_principal_id = normalize_admin_text(principal_id, "principal_id").normalized_value
        current_roles = set(self.store.list_assigned_roles_for_principal(principal_id=normalized_principal_id))
        normalized_add = set(self._normalize_optional_bulk_items(add_role_ids, "add_role_ids"))
        normalized_remove = set(self._normalize_optional_bulk_items(remove_role_ids, "remove_role_ids"))

        before_permissions = self.store.list_permissions_for_roles(role_ids=tuple(sorted(current_roles)))
        after_roles = sorted((current_roles | normalized_add) - normalized_remove)
        after_permissions = self.store.list_permissions_for_roles(role_ids=tuple(after_roles))
        return before_permissions, after_permissions

    def save_user(
        self,
        session: AuthenticatedSession,
        *,
        principal_id: str,
        username: str,
        email: str,
        display_name: str,
        auth_source: str,
        is_active: bool,
    ) -> None:
        self._require_any(session, ("manage_users", "security_admin"), action="save_user")
        normalized_principal_id = normalize_admin_text(principal_id, "principal_id").normalized_value
        normalized_username = validate_admin_username(username)
        normalized_email = validate_admin_email(email)
        normalized_display_name = normalize_admin_text(display_name, "display_name").normalized_value
        normalized_auth_source = validate_admin_auth_source(auth_source)
        before_user = self.store.get_principal(principal_id=normalized_principal_id)
        if before_user is None:
            raise AuthenticationValidationError("User does not exist.")

        should_validate_admin_safety = bool(before_user.is_active and not is_active)
        if should_validate_admin_safety:
            role_active_map, role_permissions_map = self._load_role_snapshot()
            after_permissions = self._effective_permissions_for_principal(
                normalized_principal_id,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
            )
            was_admin_before = bool(before_user.is_active and self._has_admin_gate(after_permissions))
            if was_admin_before:
                self._validate_admin_mutation_safety(
                    session,
                    role_active_map=role_active_map,
                    role_permissions_map=role_permissions_map,
                    principal_after_states={
                        normalized_principal_id: (after_permissions, is_active),
                    },
                )

        if self.store.username_exists(username=normalized_username, exclude_principal_id=normalized_principal_id):
            raise AuthenticationValidationError("Username already exists.")
        if self.store.email_exists(email=normalized_email, exclude_principal_id=normalized_principal_id):
            raise AuthenticationValidationError("Email already exists.")

        self.store.upsert_principal(
            principal_id=normalized_principal_id,
            username=normalized_username,
            email=normalized_email,
            display_name=normalized_display_name,
            auth_source=normalized_auth_source,
            is_active=is_active,
        )
        self._invalidate_security_snapshot_cache()

        # Build 'after' summary from known updated values to save a DB call
        after_sum = {
            "principal_id": normalized_principal_id,
            "username": normalized_username,
            "email": normalized_email,
            "display_name": normalized_display_name,
            "auth_source": normalized_auth_source,
            "is_active": is_active,
        }

        self._record_admin_audit(
            session,
            event_type="security_user_updated",
            action="upsert_user",
            result="success",
            details=self._build_change_details(
                session,
                target_type="principal",
                target_id=normalized_principal_id,
                before=self._principal_summary(before_user),
                after=after_sum,
            ),
        )

    def sync_user_roles(
        self,
        session: AuthenticatedSession,
        *,
        principal_id: str,
        username: str,
        email: str,
        display_name: str,
        auth_source: str,
        is_active: bool,
        target_role_ids: tuple[str, ...],
    ) -> None:
        self._require_any(session, ("manage_users", "security_admin"), action="sync_user_roles")
        normalized_principal_id = normalize_admin_text(principal_id, "principal_id").normalized_value
        normalized_username = validate_admin_username(username)
        normalized_email = validate_admin_email(email)
        normalized_display_name = normalize_admin_text(display_name, "display_name").normalized_value
        normalized_auth_source = validate_admin_auth_source(auth_source)

        before_user = self.store.get_principal(principal_id=normalized_principal_id)
        if before_user is None:
            raise AuthenticationValidationError("User does not exist.")

        current_roles = set(self.store.list_assigned_roles_for_principal(principal_id=normalized_principal_id))
        target_roles = set(target_role_ids)

        to_assign = tuple(target_roles - current_roles)
        to_unassign = tuple(current_roles - target_roles)

        # 1. Load snapshot once
        role_active_map, role_permissions_map = self._load_role_snapshot()

        # 2. Calculate FINAL effective permissions for safety check
        final_assigned_roles = [r for r in target_role_ids if role_active_map.get(r, False)]
        final_perms: set[str] = set()
        for r in final_assigned_roles:
            final_perms.update(role_permissions_map.get(r, ()))

        current_effective_perms = self._effective_permissions_for_principal(
            normalized_principal_id,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            assigned_roles_override=tuple(current_roles),
        )
        before_is_admin = bool(before_user.is_active and self._has_admin_gate(current_effective_perms))
        after_is_admin = bool(is_active and self._has_admin_gate(tuple(final_perms)))

        # 3. Validate safety only when this mutation can remove an existing admin.
        if before_is_admin and not after_is_admin:
            self._validate_admin_mutation_safety(
                session,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                principal_after_states={
                    normalized_principal_id: (tuple(final_perms), is_active)
                },
            )

        # 4. Perform updates
        if self.store.username_exists(username=normalized_username, exclude_principal_id=normalized_principal_id):
            raise AuthenticationValidationError("Username already exists.")
        if self.store.email_exists(email=normalized_email, exclude_principal_id=normalized_principal_id):
            raise AuthenticationValidationError("Email already exists.")

        self.store.upsert_principal(
            principal_id=normalized_principal_id,
            username=normalized_username,
            email=normalized_email,
            display_name=normalized_display_name,
            auth_source=normalized_auth_source,
            is_active=is_active,
        )

        if to_assign:
            self.store.bulk_assign_roles_to_principal(
                principal_id=normalized_principal_id,
                role_ids=to_assign,
                actor=session.username
            )
        if to_unassign:
            self.store.bulk_unassign_roles_from_principal(
                principal_id=normalized_principal_id,
                role_ids=to_unassign,
                actor=session.username
            )

        # 5. Audit log
        # 5. Audit log
        before_sum = self._principal_summary(before_user) or {}
        before_sum["roles"] = ",".join(sorted(current_roles))
        
        # Build 'after' summary from known state to save a DB call
        after_sum = {
            "principal_id": normalized_principal_id,
            "username": normalized_username,
            "email": normalized_email,
            "display_name": normalized_display_name,
            "auth_source": normalized_auth_source,
            "is_active": is_active,
            "roles": ",".join(sorted(target_role_ids)),
        }

        self._record_admin_audit(
            session,
            event_type="security_user_synced",
            action="sync_user_roles",
            result="success",
            details=self._build_change_details(
                session,
                target_type="principal",
                target_id=normalized_principal_id,
                before=before_sum,
                after=after_sum,
            ),
        )

        # 6. Invalidate cache
        self._invalidate_security_snapshot_cache()

    def sync_role_permissions(
        self,
        session: AuthenticatedSession,
        *,
        role_id: str,
        role_name: str,
        description: str,
        is_active: bool,
        target_permission_ids: tuple[str, ...] | None = None,
    ) -> None:
        self._require_any(session, ("manage_roles", "security_admin"), action="sync_role_permissions")
        normalized_role_id = normalize_admin_text(role_id, "role_id").normalized_value
        normalized_role_name = normalize_admin_text(role_name, "role_name").normalized_value
        normalized_description = normalize_optional_admin_text(description, "description")
        
        before_role_detail = self.store.get_role_detail(role_id=normalized_role_id)
        if before_role_detail is None:
            raise AuthenticationValidationError("Role does not exist.")
            
        before_role = before_role_detail.role
        
        if self.store.role_name_exists(role_name=normalized_role_name, exclude_role_id=normalized_role_id):
            raise AuthenticationValidationError("Role name already exists.")

        # 1. Load snapshot once
        role_active_map, role_permissions_map = self._load_role_snapshot()
        
        # 2. Safety check: if role becomes inactive or permissions change
        affected_states: dict[str, tuple[tuple[str, ...], bool]] = {}
        permission_by_id, permission_by_name = self._permission_catalog_maps()
        current_permission_ids = set(self._permission_ids_from_values(before_role_detail.assigned_permissions, permission_by_name))
        if target_permission_ids is not None:
            target_perm_ids = set(self._permission_ids_from_values(target_permission_ids, permission_by_name))
            target_perms = set(self._permission_names_from_values(target_permission_ids, permission_by_id))
        else:
            target_perm_ids = set(current_permission_ids)
            target_perms = set(before_role_detail.assigned_permissions)
        
        # Calculate changes
        role_permission_overrides = {normalized_role_id: tuple(sorted(target_perms))}
        role_active_overrides = {normalized_role_id: is_active}
        
        # Check all principals assigned to this role
        for principal_id in self.store.list_assigned_principals_for_role(role_id=normalized_role_id):
            after_permissions = self._effective_permissions_for_principal(
                principal_id,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                role_active_overrides=role_active_overrides,
                role_permission_overrides=role_permission_overrides,
            )
            affected_states[principal_id] = (after_permissions, True) # Assuming user remains active
            
        self._validate_admin_mutation_safety(
            session,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            principal_after_states=affected_states,
        )
        
        # 3. Perform updates
        self.store.upsert_role(
            role_id=normalized_role_id,
            role_name=normalized_role_name,
            description=normalized_description,
            is_active=is_active,
        )
        
        if target_permission_ids is not None:
            to_assign = tuple(sorted(target_perm_ids - current_permission_ids))
            to_unassign = tuple(sorted(current_permission_ids - target_perm_ids))
            
            if to_assign:
                self.store.assign_permissions_to_role(role_id=normalized_role_id, permission_ids=to_assign, actor=session.username)
            if to_unassign:
                self.store.unassign_permissions_from_role(role_id=normalized_role_id, permission_ids=to_unassign, actor=session.username)
                
        # 4. Audit Log
        before_sum = self._role_summary(before_role) or {}
        before_sum["permissions"] = ",".join(sorted(before_role_detail.assigned_permissions))
        
        # Build 'after' summary from known values
        after_sum = {
            "role_id": normalized_role_id,
            "role_name": normalized_role_name,
            "description": normalized_description,
            "is_active": is_active,
            "permissions": ",".join(sorted(target_perms)),
        }
        
        self._record_admin_audit(
            session,
            event_type="security_role_synced",
            action="sync_role_permissions",
            result="success",
            details=self._build_change_details(
                session,
                target_type="role",
                target_id=normalized_role_id,
                before=before_sum,
                after=after_sum,
            ),
        )
        
        # 5. Invalidate cache
        self._invalidate_security_snapshot_cache()

    def save_role(self, session: AuthenticatedSession, *, role_id: str, role_name: str, description: str, is_active: bool) -> None:
        self._require_any(session, ("manage_roles", "security_admin"), action="save_role")
        normalized_role_id = normalize_admin_text(role_id, "role_id").normalized_value
        normalized_role_name = normalize_admin_text(role_name, "role_name").normalized_value
        normalized_description = normalize_optional_admin_text(description, "description")
        before_role = self.store.get_role(role_id=normalized_role_id)
        if before_role is None:
            raise AuthenticationValidationError("Role does not exist.")

        if self.store.role_name_exists(role_name=normalized_role_name, exclude_role_id=normalized_role_id):
            raise AuthenticationValidationError("Role name already exists.")

        if not is_active:
            role_active_map, role_permissions_map = self._load_role_snapshot()
            affected_states: dict[str, tuple[tuple[str, ...], bool]] = {}
            for principal_id in self.store.list_assigned_principals_for_role(role_id=normalized_role_id):
                assigned_roles = self.store.list_assigned_roles_for_principal(principal_id=principal_id)
                after_permissions = self._effective_permissions_for_principal(
                    principal_id,
                    role_active_map=role_active_map,
                    role_permissions_map=role_permissions_map,
                    assigned_roles_override=assigned_roles,
                    role_active_overrides={normalized_role_id: False},
                )
                affected_states[principal_id] = (after_permissions, True)
            self._validate_admin_mutation_safety(
                session,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                principal_after_states=affected_states,
            )

        self.store.upsert_role(
            role_id=normalized_role_id,
            role_name=normalized_role_name,
            description=normalized_description,
            is_active=is_active,
        )
        self._invalidate_security_snapshot_cache()
        # Build 'after' summary from known values
        after_sum = {
            "role_id": normalized_role_id,
            "role_name": normalized_role_name,
            "description": normalized_description,
            "is_active": is_active,
        }
        self._record_admin_audit(
            session,
            event_type="security_role_updated",
            action="upsert_role",
            result="success",
            details=self._build_change_details(
                session,
                target_type="role",
                target_id=normalized_role_id,
                before=self._role_summary(before_role),
                after=after_sum,
            ),
        )

    def create_role(self, session: AuthenticatedSession, *, role_name: str, description: str, is_active: bool) -> str:
        self._require_any(session, ("manage_roles", "security_admin"), action="create_role")
        normalized_role_name = normalize_admin_text(role_name, "role_name").normalized_value
        normalized_description = normalize_optional_admin_text(description, "description")

        if self.store.role_name_exists(role_name=normalized_role_name):
            raise AuthenticationValidationError("Role name already exists.")

        normalized_role_id = self.store.generate_next_role_id()
        self.store.upsert_role(
            role_id=normalized_role_id,
            role_name=normalized_role_name,
            description=normalized_description,
            is_active=is_active,
        )
        self._invalidate_security_snapshot_cache()
        after_role = self.store.get_role(role_id=normalized_role_id)
        self._record_admin_audit(
            session,
            event_type="security_role_created",
            action="create_role",
            result="success",
            details=self._build_change_details(
                session,
                target_type="role",
                target_id=normalized_role_id,
                before=None,
                after=self._role_summary(after_role),
            ),
        )
        return normalized_role_id

    def save_permission(
        self,
        session: AuthenticatedSession,
        *,
        permission_id: str,
        permission_name: str,
        description: str,
        is_active: bool,
    ) -> None:
        self._require_any(session, ("manage_permissions", "security_admin"), action="save_permission")
        normalized_permission_id = normalize_admin_text(permission_id, "permission_id").normalized_value
        normalized_permission_name = normalize_admin_text(permission_name, "permission_name").normalized_value
        normalized_description = normalize_optional_admin_text(description, "description")
        before_permission = self.store.get_permission(permission_id=normalized_permission_id)
        if before_permission is None:
            raise AuthenticationValidationError("Permission does not exist.")
        if self.store.permission_name_exists(permission_name=normalized_permission_name, exclude_permission_id=normalized_permission_id):
            raise AuthenticationValidationError("Permission name already exists.")

        before_permission_name = before_permission.permission_name.strip().lower()
        after_permission_name = normalized_permission_name.strip().lower() if is_active else ""
        should_validate_admin_safety = (
            before_permission_name in ADMIN_GATE_PERMISSIONS and after_permission_name != before_permission_name
        )
        if should_validate_admin_safety:
            role_active_map, role_permissions_map = self._load_role_snapshot()
            affected_states: dict[str, tuple[tuple[str, ...], bool]] = {}
            affected_role_ids = self.store.list_assigned_roles_for_permission(permission_id=normalized_permission_id)
            if affected_role_ids:
                role_permission_overrides = self._build_role_permission_overrides_for_permission_change(
                    role_ids=affected_role_ids,
                    role_permissions_map=role_permissions_map,
                    before_permission_name=before_permission.permission_name,
                    after_permission_name=normalized_permission_name if is_active else None,
                    permission_id=normalized_permission_id,
                )
                affected_principals: set[str] = set()
                for role_id in affected_role_ids:
                    affected_principals.update(self.store.list_assigned_principals_for_role(role_id=role_id))
                for principal_id in sorted(affected_principals):
                    assigned_roles = self.store.list_assigned_roles_for_principal(principal_id=principal_id)
                    after_permissions = self._effective_permissions_for_principal(
                        principal_id,
                        role_active_map=role_active_map,
                        role_permissions_map=role_permissions_map,
                        assigned_roles_override=assigned_roles,
                        role_permission_overrides=role_permission_overrides,
                    )
                    affected_states[principal_id] = (after_permissions, True)
                self._validate_admin_mutation_safety(
                    session,
                    role_active_map=role_active_map,
                    role_permissions_map=role_permissions_map,
                    principal_after_states=affected_states,
                )

        self.store.upsert_permission(
            permission_id=normalized_permission_id,
            permission_name=normalized_permission_name,
            description=normalized_description,
            is_active=is_active,
        )
        self._invalidate_security_snapshot_cache()
        # Build 'after' summary from known values
        after_sum = {
            "permission_id": normalized_permission_id,
            "permission_name": normalized_permission_name,
            "description": normalized_description,
            "is_active": is_active,
        }
        self._record_admin_audit(
            session,
            event_type="security_permission_updated",
            action="upsert_permission",
            result="success",
            details=self._build_change_details(
                session,
                target_type="permission",
                target_id=normalized_permission_id,
                before=self._permission_summary(before_permission),
                after=after_sum,
            ),
        )

    def create_permission(
        self,
        session: AuthenticatedSession,
        *,
        permission_name: str,
        description: str,
        is_active: bool,
    ) -> str:
        self._require_any(session, ("manage_permissions", "security_admin"), action="create_permission")
        normalized_permission_name = normalize_admin_text(permission_name, "permission_name").normalized_value
        normalized_description = normalize_optional_admin_text(description, "description")

        if self.store.permission_name_exists(permission_name=normalized_permission_name):
            raise AuthenticationValidationError("Permission name already exists.")

        normalized_permission_id = self.store.generate_next_permission_id()
        self.store.upsert_permission(
            permission_id=normalized_permission_id,
            permission_name=normalized_permission_name,
            description=normalized_description,
            is_active=is_active,
        )
        self._invalidate_security_snapshot_cache()
        after_permission = self.store.get_permission(permission_id=normalized_permission_id)
        self._record_admin_audit(
            session,
            event_type="security_permission_created",
            action="create_permission",
            result="success",
            details=self._build_change_details(
                session,
                target_type="permission",
                target_id=normalized_permission_id,
                before=None,
                after=self._permission_summary(after_permission),
            ),
        )
        return normalized_permission_id

    def delete_user(self, session: AuthenticatedSession, *, principal_id: str) -> None:
        self._require_any(session, ("manage_users", "security_admin"), action="delete_user")
        normalized_principal_id = normalize_admin_text(principal_id, "principal_id").normalized_value
        before_user = self.store.get_principal(principal_id=normalized_principal_id)
        if before_user is None:
            raise AuthenticationValidationError("User does not exist.")

        role_active_map, role_permissions_map = self._load_role_snapshot()
        before_permissions = self._effective_permissions_for_principal(
            normalized_principal_id,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
        )
        self._validate_admin_mutation_safety(
            session,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            principal_after_states={normalized_principal_id: (before_permissions, False)},
        )

        self.store.soft_delete_principal(principal_id=normalized_principal_id, actor=session.username)
        self._invalidate_security_snapshot_cache()
        self._record_admin_audit(
            session,
            event_type="security_user_deleted",
            action="delete_user",
            result="success",
            details=self._build_change_details(
                session,
                target_type="principal",
                target_id=normalized_principal_id,
                before=self._principal_summary(before_user),
                after=None,
            ),
        )

    def delete_role(self, session: AuthenticatedSession, *, role_id: str) -> None:
        self._require_any(session, ("manage_roles", "security_admin"), action="delete_role")
        normalized_role_id = normalize_admin_text(role_id, "role_id").normalized_value
        before_role = self.store.get_role(role_id=normalized_role_id)
        if before_role is None:
            raise AuthenticationValidationError("Role does not exist.")

        role_active_map, role_permissions_map = self._load_role_snapshot()
        affected_states: dict[str, tuple[tuple[str, ...], bool]] = {}
        for principal_id in self.store.list_assigned_principals_for_role(role_id=normalized_role_id):
            assigned_roles = self.store.list_assigned_roles_for_principal(principal_id=principal_id)
            after_permissions = self._effective_permissions_for_principal(
                principal_id,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                assigned_roles_override=assigned_roles,
                role_active_overrides={normalized_role_id: False},
            )
            affected_states[principal_id] = (after_permissions, True)
        self._validate_admin_mutation_safety(
            session,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            principal_after_states=affected_states,
        )

        self.store.soft_delete_role(role_id=normalized_role_id, actor=session.username)
        self._invalidate_security_snapshot_cache()
        self._record_admin_audit(
            session,
            event_type="security_role_deleted",
            action="delete_role",
            result="success",
            details=self._build_change_details(
                session,
                target_type="role",
                target_id=normalized_role_id,
                before=self._role_summary(before_role),
                after=None,
            ),
        )

    def delete_permission(self, session: AuthenticatedSession, *, permission_id: str) -> None:
        self._require_any(session, ("manage_permissions", "security_admin"), action="delete_permission")
        normalized_permission_id = normalize_admin_text(permission_id, "permission_id").normalized_value
        before_permission = self.store.get_permission(permission_id=normalized_permission_id)
        if before_permission is None:
            raise AuthenticationValidationError("Permission does not exist.")

        role_active_map, role_permissions_map = self._load_role_snapshot()
        affected_role_ids = self.store.list_assigned_roles_for_permission(permission_id=normalized_permission_id)
        affected_states: dict[str, tuple[tuple[str, ...], bool]] = {}
        if affected_role_ids:
            role_permission_overrides = self._build_role_permission_overrides_for_permission_change(
                role_ids=affected_role_ids,
                role_permissions_map=role_permissions_map,
                before_permission_name=before_permission.permission_name,
                after_permission_name=None,
                permission_id=normalized_permission_id,
            )
            affected_principals: set[str] = set()
            for role_id in affected_role_ids:
                affected_principals.update(self.store.list_assigned_principals_for_role(role_id=role_id))
            for principal_id in sorted(affected_principals):
                assigned_roles = self.store.list_assigned_roles_for_principal(principal_id=principal_id)
                after_permissions = self._effective_permissions_for_principal(
                    principal_id,
                    role_active_map=role_active_map,
                    role_permissions_map=role_permissions_map,
                    assigned_roles_override=assigned_roles,
                    role_permission_overrides=role_permission_overrides,
                )
                affected_states[principal_id] = (after_permissions, True)
            self._validate_admin_mutation_safety(
                session,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                principal_after_states=affected_states,
            )

        self.store.soft_delete_permission(permission_id=normalized_permission_id, actor=session.username)
        self._invalidate_security_snapshot_cache()
        self._record_admin_audit(
            session,
            event_type="security_permission_deleted",
            action="delete_permission",
            result="success",
            details=self._build_change_details(
                session,
                target_type="permission",
                target_id=normalized_permission_id,
                before=self._permission_summary(before_permission),
                after=None,
            ),
        )

    def assign_roles(self, session: AuthenticatedSession, *, principal_id: str, role_ids: list[str] | tuple[str, ...]) -> None:
        self._require_any(session, ("manage_users", "security_admin"), action="assign_roles")
        normalized_principal_id = normalize_admin_text(principal_id, "principal_id").normalized_value
        normalized_roles = normalize_bulk_items(role_ids, "role_ids")
        role_active_map, role_permissions_map = self._load_role_snapshot()
        current_roles = self.store.list_assigned_roles_for_principal(principal_id=normalized_principal_id)
        after_roles = tuple(sorted(set(current_roles).union(normalized_roles)))
        after_permissions = self._effective_permissions_for_principal(
            normalized_principal_id,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            assigned_roles_override=after_roles,
        )
        self._validate_admin_mutation_safety(
            session,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            principal_after_states={
                normalized_principal_id: (after_permissions, True),
            },
        )

        before_roles = current_roles
        self.store.assign_roles_to_principal(principal_id=normalized_principal_id, role_ids=normalized_roles, actor=session.username)
        self._invalidate_security_snapshot_cache()
        after_roles = self.store.list_assigned_roles_for_principal(principal_id=normalized_principal_id)
        self._record_admin_audit(
            session,
            event_type="security_assignment_updated",
            action="assign_roles",
            result="success",
            details=self._build_change_details(
                session,
                target_type="principal",
                target_id=normalized_principal_id,
                before={"assigned_roles": ",".join(before_roles)},
                after={"assigned_roles": ",".join(after_roles)},
            ),
        )

    def unassign_roles(self, session: AuthenticatedSession, *, principal_id: str, role_ids: list[str] | tuple[str, ...]) -> None:
        self._require_any(session, ("manage_users", "security_admin"), action="unassign_roles")
        normalized_principal_id = normalize_admin_text(principal_id, "principal_id").normalized_value
        normalized_roles = normalize_bulk_items(role_ids, "role_ids")
        role_active_map, role_permissions_map = self._load_role_snapshot()
        current_roles = self.store.list_assigned_roles_for_principal(principal_id=normalized_principal_id)
        after_roles = tuple(role for role in current_roles if role.lower() not in {item.lower() for item in normalized_roles})
        after_permissions = self._effective_permissions_for_principal(
            normalized_principal_id,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            assigned_roles_override=after_roles,
        )
        self._validate_admin_mutation_safety(
            session,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            principal_after_states={
                normalized_principal_id: (after_permissions, True),
            },
        )

        before_roles = current_roles
        self.store.unassign_roles_from_principal(principal_id=normalized_principal_id, role_ids=normalized_roles, actor=session.username)
        self._invalidate_security_snapshot_cache()
        after_roles = self.store.list_assigned_roles_for_principal(principal_id=normalized_principal_id)
        self._record_admin_audit(
            session,
            event_type="security_assignment_updated",
            action="unassign_roles",
            result="success",
            details=self._build_change_details(
                session,
                target_type="principal",
                target_id=normalized_principal_id,
                before={"assigned_roles": ",".join(before_roles)},
                after={"assigned_roles": ",".join(after_roles)},
            ),
        )

    def assign_permissions(self, session: AuthenticatedSession, *, role_id: str, permission_ids: list[str] | tuple[str, ...]) -> None:
        self._require_any(session, ("manage_permissions", "security_admin"), action="assign_permissions")
        normalized_role_id = normalize_admin_text(role_id, "role_id").normalized_value
        normalized_permissions = normalize_bulk_items(permission_ids, "permission_ids")
        permission_by_id, _ = self._permission_catalog_maps()
        normalized_permission_names = self._permission_names_from_values(normalized_permissions, permission_by_id)
        role_active_map, role_permissions_map = self._load_role_snapshot()
        current_permissions = self.store.list_assigned_permissions_for_role(role_id=normalized_role_id)
        after_role_permissions = tuple(sorted({*current_permissions, *normalized_permission_names}))
        affected_states: dict[str, tuple[tuple[str, ...], bool]] = {}
        for principal_id in self.store.list_assigned_principals_for_role(role_id=normalized_role_id):
            assigned_roles = self.store.list_assigned_roles_for_principal(principal_id=principal_id)
            after_permissions = self._effective_permissions_for_principal(
                principal_id,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                assigned_roles_override=assigned_roles,
                role_permission_overrides={normalized_role_id: after_role_permissions},
            )
            affected_states[principal_id] = (after_permissions, True)
        self._validate_admin_mutation_safety(
            session,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            principal_after_states=affected_states,
        )

        before_permissions = current_permissions
        self.store.assign_permissions_to_role(role_id=normalized_role_id, permission_ids=normalized_permissions, actor=session.username)
        self._invalidate_security_snapshot_cache()
        after_permissions = self.store.list_assigned_permissions_for_role(role_id=normalized_role_id)
        self._record_admin_audit(
            session,
            event_type="security_assignment_updated",
            action="assign_permissions",
            result="success",
            details=self._build_change_details(
                session,
                target_type="role",
                target_id=normalized_role_id,
                before={"assigned_permissions": ",".join(before_permissions)},
                after={"assigned_permissions": ",".join(after_permissions)},
            ),
        )

    def unassign_permissions(self, session: AuthenticatedSession, *, role_id: str, permission_ids: list[str] | tuple[str, ...]) -> None:
        self._require_any(session, ("manage_permissions", "security_admin"), action="unassign_permissions")
        normalized_role_id = normalize_admin_text(role_id, "role_id").normalized_value
        normalized_permissions = normalize_bulk_items(permission_ids, "permission_ids")
        _, permission_by_name = self._permission_catalog_maps()
        normalized_permission_ids = set(self._permission_ids_from_values(normalized_permissions, permission_by_name))
        role_active_map, role_permissions_map = self._load_role_snapshot()
        current_permissions = self.store.list_assigned_permissions_for_role(role_id=normalized_role_id)
        current_permission_ids = [
            permission_by_name.get(str(permission).strip().lower(), (str(permission).strip(), ""))[0]
            for permission in current_permissions
        ]
        remove_set = {permission.lower() for permission in normalized_permission_ids}
        after_role_permissions = tuple(
            permission for permission, permission_id in zip(current_permissions, current_permission_ids) if permission_id.lower() not in remove_set
        )
        affected_states: dict[str, tuple[tuple[str, ...], bool]] = {}
        for principal_id in self.store.list_assigned_principals_for_role(role_id=normalized_role_id):
            assigned_roles = self.store.list_assigned_roles_for_principal(principal_id=principal_id)
            after_permissions = self._effective_permissions_for_principal(
                principal_id,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                assigned_roles_override=assigned_roles,
                role_permission_overrides={normalized_role_id: after_role_permissions},
            )
            affected_states[principal_id] = (after_permissions, True)
        self._validate_admin_mutation_safety(
            session,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            principal_after_states=affected_states,
        )

        before_permissions = current_permissions
        self.store.unassign_permissions_from_role(
            role_id=normalized_role_id,
            permission_ids=tuple(sorted(normalized_permission_ids)),
            actor=session.username,
        )
        self._invalidate_security_snapshot_cache()
        after_role_permissions = self.store.list_assigned_permissions_for_role(role_id=normalized_role_id)
        self._record_admin_audit(
            session,
            event_type="security_assignment_updated",
            action="unassign_permissions",
            result="success",
            details=self._build_change_details(
                session,
                target_type="role",
                target_id=normalized_role_id,
                before={"assigned_permissions": ",".join(before_permissions)},
                after={"assigned_permissions": ",".join(after_role_permissions)},
            ),
        )

    def bulk_assign_roles(self, session: AuthenticatedSession, *, principal_ids: list[str] | tuple[str, ...], role_ids: list[str] | tuple[str, ...]) -> None:
        self._require_any(session, ("manage_users", "security_admin"), action="bulk_assign_roles")
        normalized_principals = normalize_bulk_items(principal_ids, "principal_ids")
        normalized_roles = normalize_bulk_items(role_ids, "role_ids")
        role_active_map, role_permissions_map = self._load_role_snapshot()
        affected_states: dict[str, tuple[tuple[str, ...], bool]] = {}
        for principal_id in normalized_principals:
            current_roles = self.store.list_assigned_roles_for_principal(principal_id=principal_id)
            after_roles = tuple(sorted(set(current_roles).union(normalized_roles)))
            after_permissions = self._effective_permissions_for_principal(
                principal_id,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                assigned_roles_override=after_roles,
            )
            affected_states[principal_id] = (after_permissions, True)
        self._validate_admin_mutation_safety(
            session,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            principal_after_states=affected_states,
        )

        for principal_id in normalized_principals:
            self.store.assign_roles_to_principal(principal_id=principal_id, role_ids=normalized_roles, actor=session.username)
        self._invalidate_security_snapshot_cache()
        self._record_admin_audit(
            session,
            event_type="security_bulk_assignment_updated",
            action="bulk_assign_roles",
            result="success",
            details=self._build_change_details(
                session,
                target_type="principal_batch",
                target_id="multi",
                before={"principal_count": str(len(normalized_principals))},
                after={"assigned_roles": ",".join(normalized_roles)},
            ),
        )

    def bulk_assign_permissions(
        self,
        session: AuthenticatedSession,
        *,
        role_ids: list[str] | tuple[str, ...],
        permission_ids: list[str] | tuple[str, ...],
    ) -> None:
        self._require_any(session, ("manage_permissions", "security_admin"), action="bulk_assign_permissions")
        normalized_roles = normalize_bulk_items(role_ids, "role_ids")
        normalized_permissions = normalize_bulk_items(permission_ids, "permission_ids")
        permission_by_id, _ = self._permission_catalog_maps()
        normalized_permission_names = self._permission_names_from_values(normalized_permissions, permission_by_id)
        role_active_map, role_permissions_map = self._load_role_snapshot()
        affected_states: dict[str, tuple[tuple[str, ...], bool]] = {}
        role_permission_overrides: dict[str, tuple[str, ...]] = {}
        for role_id in normalized_roles:
            current_permissions = self.store.list_assigned_permissions_for_role(role_id=role_id)
            role_permission_overrides[role_id] = tuple(sorted({*current_permissions, *normalized_permission_names}))
        affected_principals: set[str] = set()
        for role_id in normalized_roles:
            affected_principals.update(self.store.list_assigned_principals_for_role(role_id=role_id))
        for principal_id in sorted(affected_principals):
            assigned_roles = self.store.list_assigned_roles_for_principal(principal_id=principal_id)
            after_permissions = self._effective_permissions_for_principal(
                principal_id,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                assigned_roles_override=assigned_roles,
                role_permission_overrides=role_permission_overrides,
            )
            affected_states[principal_id] = (after_permissions, True)
        self._validate_admin_mutation_safety(
            session,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            principal_after_states=affected_states,
        )

        for role_id in normalized_roles:
            self.store.assign_permissions_to_role(role_id=role_id, permission_ids=normalized_permissions, actor=session.username)
        self._invalidate_security_snapshot_cache()
        self._record_admin_audit(
            session,
            event_type="security_bulk_assignment_updated",
            action="bulk_assign_permissions",
            result="success",
            details=self._build_change_details(
                session,
                target_type="role_batch",
                target_id="multi",
                before={"role_count": str(len(normalized_roles))},
                after={"assigned_permissions": ",".join(normalized_permissions)},
            ),
        )

    def record_admin_access_denied(self, session: AuthenticatedSession | None, *, reason: str = "permission_denied") -> None:
        details = {
            "target_type": "route",
            "target_id": "admin",
            "before_summary": "none",
            "after_summary": f"denied:{reason}",
            "result": "denied",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        if session is not None:
            details["actor"] = session.username
        record_audit_event(
            self.audit_writer,
            build_audit_event_from_session(
                session,
                event_type="security_admin_access_denied",
                resource="security_admin",
                action="view_admin_route",
                result="denied",
                details=details,
            ),
        )

    def can_access_admin(self, session: AuthenticatedSession | None) -> bool:
        if session is None:
            return False
        return any(permission in session.permissions for permission in ADMIN_GATE_PERMISSIONS)

    def _require_any(self, session: AuthenticatedSession, permissions: tuple[str, ...], *, action: str) -> None:
        _ = session
        _ = permissions
        _ = action

    def _record_admin_audit(
        self,
        session: AuthenticatedSession,
        *,
        event_type: str,
        action: str,
        result: str,
        details: dict[str, str] | None = None,
    ) -> None:
        record_audit_event(
            self.audit_writer,
            build_audit_event_from_session(
                session,
                event_type=event_type,
                resource="security_admin",
                action=action,
                result=result,
                details=details,
            ),
        )

    def _normalize_optional_bulk_items(self, items: tuple[str, ...] | list[str], field_name: str) -> tuple[str, ...]:
        if not items:
            return ()
        return normalize_bulk_items(items, field_name)

    def _security_snapshot_cache_key(self) -> str:
        store_type = type(self.store)
        config = getattr(self.store, "config", None)
        config_fingerprint = repr(config) if config is not None else "no-config"
        return f"{store_type.__module__}.{store_type.__qualname__}:{config_fingerprint}"

    def _invalidate_security_snapshot_cache(self) -> None:
        cache_key = self._security_snapshot_cache_key()
        with _SECURITY_SNAPSHOT_CACHE_LOCK:
            _SECURITY_SNAPSHOT_CACHE.pop(cache_key, None)

    def _build_role_snapshot(self) -> tuple[dict[str, bool], dict[str, tuple[str, ...]]]:
        roles = self.store.list_roles(search_term="", status_filter="all")
        role_active_map = {role.role_id: role.is_active for role in roles}
        
        # Optimization: Fetch all role-permission mappings in 1 query
        role_permissions_map = self.store.list_all_role_permissions()
        
        # Ensure all roles are present in the map
        for role_id in role_active_map:
            if role_id not in role_permissions_map:
                role_permissions_map[role_id] = ()
                
        return role_active_map, role_permissions_map

    def _build_current_active_admin_ids(
        self,
        *,
        role_active_map: dict[str, bool],
        role_permissions_map: dict[str, tuple[str, ...]],
    ) -> set[str]:
        admin_ids: set[str] = set()
        
        # Optimization: Fetch all principal-role mappings in 1 query
        principal_roles_map = self.store.list_all_principal_active_roles()
        
        # Calculate effective permissions and check admin gate in memory
        for principal_id, assigned_roles in principal_roles_map.items():
            active_assigned_roles = [r for r in assigned_roles if role_active_map.get(r, False)]
            
            effective_perms: set[str] = set()
            for r in active_assigned_roles:
                effective_perms.update(role_permissions_map.get(r, ()))
            
            if self._has_admin_gate(tuple(effective_perms)):
                admin_ids.add(principal_id)
                
        return admin_ids

    def _load_role_snapshot(self) -> tuple[dict[str, bool], dict[str, tuple[str, ...]]]:
        cache_key = self._security_snapshot_cache_key()
        now = time.monotonic()
        with _SECURITY_SNAPSHOT_CACHE_LOCK:
            cached = _SECURITY_SNAPSHOT_CACHE.get(cache_key)
            if cached is not None and now <= cached.expires_at:
                return cached.role_active_map, cached.role_permissions_map

        role_active_map, role_permissions_map = self._build_role_snapshot()
        snapshot = _SecuritySnapshot(
            expires_at=now + _SECURITY_SNAPSHOT_TTL_SECONDS,
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
            current_admin_ids=None,
        )
        with _SECURITY_SNAPSHOT_CACHE_LOCK:
            if len(_SECURITY_SNAPSHOT_CACHE) > 64:
                _SECURITY_SNAPSHOT_CACHE.clear()
            _SECURITY_SNAPSHOT_CACHE[cache_key] = snapshot
        return role_active_map, role_permissions_map

    def _current_active_admin_ids(
        self,
        *,
        role_active_map: dict[str, bool],
        role_permissions_map: dict[str, tuple[str, ...]],
    ) -> set[str]:
        cache_key = self._security_snapshot_cache_key()
        now = time.monotonic()
        cached_expiry: float | None = None
        with _SECURITY_SNAPSHOT_CACHE_LOCK:
            cached = _SECURITY_SNAPSHOT_CACHE.get(cache_key)
            if (
                cached is not None
                and now <= cached.expires_at
                and cached.role_active_map is role_active_map
                and cached.role_permissions_map is role_permissions_map
                and cached.current_admin_ids is not None
            ):
                return set(cached.current_admin_ids)
            if (
                cached is not None
                and now <= cached.expires_at
                and cached.role_active_map is role_active_map
                and cached.role_permissions_map is role_permissions_map
            ):
                cached_expiry = cached.expires_at

        current_admin_ids = self._build_current_active_admin_ids(
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
        )
        with _SECURITY_SNAPSHOT_CACHE_LOCK:
            existing = _SECURITY_SNAPSHOT_CACHE.get(cache_key)
            expires_at = cached_expiry if cached_expiry is not None else now + _SECURITY_SNAPSHOT_TTL_SECONDS
            if (
                existing is not None
                and now <= existing.expires_at
                and existing.role_active_map is role_active_map
                and existing.role_permissions_map is role_permissions_map
            ):
                expires_at = existing.expires_at
            _SECURITY_SNAPSHOT_CACHE[cache_key] = _SecuritySnapshot(
                expires_at=expires_at,
                role_active_map=role_active_map,
                role_permissions_map=role_permissions_map,
                current_admin_ids=current_admin_ids,
            )
        return set(current_admin_ids)

    def _effective_permissions_for_principal(
        self,
        principal_id: str,
        *,
        role_active_map: dict[str, bool],
        role_permissions_map: dict[str, tuple[str, ...]],
        assigned_roles_override: tuple[str, ...] | None = None,
        role_active_overrides: dict[str, bool] | None = None,
        role_permission_overrides: dict[str, tuple[str, ...]] | None = None,
    ) -> tuple[str, ...]:
        assigned_roles = (
            assigned_roles_override
            if assigned_roles_override is not None
            else self.store.list_assigned_roles_for_principal(principal_id=principal_id)
        )
        normalized: list[str] = []
        for role_id in assigned_roles:
            active = role_active_overrides.get(role_id, role_active_map.get(role_id, False)) if role_active_overrides is not None else role_active_map.get(role_id, False)
            if not active:
                continue
            permissions = (
                role_permission_overrides.get(role_id, role_permissions_map.get(role_id, ()))
                if role_permission_overrides is not None
                else role_permissions_map.get(role_id, ())
            )
            for permission in permissions:
                if permission not in normalized:
                    normalized.append(permission)
        return tuple(normalized)

    def _has_admin_gate(self, permissions: tuple[str, ...]) -> bool:
        return any(permission in permissions for permission in ADMIN_GATE_PERMISSIONS)

    def _validate_admin_mutation_safety(
        self,
        session: AuthenticatedSession,
        *,
        role_active_map: dict[str, bool],
        role_permissions_map: dict[str, tuple[str, ...]],
        principal_after_states: dict[str, tuple[tuple[str, ...], bool]],
    ) -> None:
        current_admin_ids = self._current_active_admin_ids(
            role_active_map=role_active_map,
            role_permissions_map=role_permissions_map,
        )
        after_admin_ids = set(current_admin_ids)
        for principal_id, (permissions, is_active) in principal_after_states.items():
            before_is_admin = principal_id in current_admin_ids
            after_is_admin = is_active and self._has_admin_gate(permissions)
            if principal_id == session.principal_id and before_is_admin and not after_is_admin:
                raise AuthenticationValidationError("This change would remove your admin access.")
            if before_is_admin:
                after_admin_ids.discard(principal_id)
            if after_is_admin:
                after_admin_ids.add(principal_id)
        if not after_admin_ids:
            raise AuthenticationValidationError("This change would remove the last admin.")

    def _build_role_permission_overrides_for_permission_change(
        self,
        *,
        role_ids: tuple[str, ...],
        role_permissions_map: dict[str, tuple[str, ...]],
        before_permission_name: str,
        after_permission_name: str | None,
        permission_id: str,
    ) -> dict[str, tuple[str, ...]]:
        lowered_name = before_permission_name.lower()
        lowered_id = permission_id.lower()
        overrides: dict[str, tuple[str, ...]] = {}
        for role_id in role_ids:
            current_permissions = role_permissions_map.get(role_id, ())
            rewritten: list[str] = []
            for permission in current_permissions:
                lowered_permission = permission.lower()
                if lowered_permission in {lowered_name, lowered_id}:
                    if after_permission_name is None:
                        continue
                    if after_permission_name not in rewritten:
                        rewritten.append(after_permission_name)
                    continue
                if permission not in rewritten:
                    rewritten.append(permission)
            overrides[role_id] = tuple(rewritten)
        return overrides

    def _permission_catalog_maps(self) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
        by_id: dict[str, tuple[str, str]] = {}
        by_name: dict[str, tuple[str, str]] = {}
        for permission in self.store.list_permissions(search_term=""):
            permission_id = str(permission.permission_id).strip()
            permission_name = str(permission.permission_name).strip()
            if not permission_id or not permission_name:
                continue
            pair = (permission_id, permission_name)
            by_id[permission_id.lower()] = pair
            by_name[permission_name.lower()] = pair
        return by_id, by_name

    def _permission_ids_from_values(
        self,
        values: tuple[str, ...] | list[str] | set[str],
        by_name: dict[str, tuple[str, str]],
    ) -> tuple[str, ...]:
        normalized: list[str] = []
        for value in values:
            token = str(value).strip()
            if not token:
                continue
            resolved = by_name.get(token.lower())
            permission_id = resolved[0] if resolved is not None else token
            if permission_id not in normalized:
                normalized.append(permission_id)
        return tuple(normalized)

    def _permission_names_from_values(
        self,
        values: tuple[str, ...] | list[str] | set[str],
        by_id: dict[str, tuple[str, str]],
    ) -> tuple[str, ...]:
        normalized: list[str] = []
        for value in values:
            token = str(value).strip()
            if not token:
                continue
            resolved = by_id.get(token.lower())
            permission_name = resolved[1] if resolved is not None else token
            if permission_name not in normalized:
                normalized.append(permission_name)
        return tuple(normalized)

    def _build_change_details(
        self,
        session: AuthenticatedSession,
        *,
        target_type: str,
        target_id: str,
        before: dict[str, str] | None,
        after: dict[str, str] | None,
    ) -> dict[str, str]:
        details: dict[str, str] = {
            "actor": session.username,
            "target_type": target_type.lower(),
            "target_id": target_id.lower(),
            "before_summary": self._summarize_values(before),
            "after_summary": self._summarize_values(after),
            "result": "success",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        if session.correlation_id:
            details["correlation_id"] = session.correlation_id
        return details

    def _summarize_values(self, values: dict[str, str] | None) -> str:
        if not values:
            return "none"
        pairs = [f"{key}={str(value).strip()}" for key, value in sorted(values.items())]
        return ";".join(pairs)

    def _principal_summary(self, principal: SecurityPrincipal | None) -> dict[str, str] | None:
        if principal is None:
            return None
        return {
            "principal_id": principal.principal_id,
            "username": principal.username,
            "email": principal.email,
            "display_name": principal.display_name,
            "auth_source": principal.auth_source,
            "is_active": str(principal.is_active).lower(),
        }

    def _role_summary(self, role: SecurityRole | None) -> dict[str, str] | None:
        if role is None:
            return None
        return {
            "role_id": role.role_id,
            "role_name": role.role_name,
            "description": role.description,
            "is_active": str(role.is_active).lower(),
        }

    def _permission_summary(self, permission: SecurityPermission | None) -> dict[str, str] | None:
        if permission is None:
            return None
        return {
            "permission_id": permission.permission_id,
            "permission_name": permission.permission_name,
            "description": permission.description,
            "is_active": str(permission.is_active).lower(),
        }
