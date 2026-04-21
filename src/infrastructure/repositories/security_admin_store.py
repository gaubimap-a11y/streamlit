from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.domain.auth_validation import AuthSystemUnavailableError
from src.domain.security_admin_models import (
    SecurityAuditRecord,
    SecurityPermission,
    SecurityPrincipal,
    SecurityPrincipalDetail,
    SecurityRole,
    SecurityRoleDetail,
)
from src.infrastructure.repositories.sql_warehouse_source import (
    DatabricksConfig,
    DatabricksUnavailableError,
    _databricks_api_request,
    _normalize_base_url,
    _poll_statement_completion,
    load_databricks_config,
)

AUTH_SCHEMA_NAME = "tmn_kobe.auth"


@dataclass(frozen=True)
class DatabricksSecurityAdminStore:
    config: DatabricksConfig

    @classmethod
    def from_current_config(cls) -> "DatabricksSecurityAdminStore":
        return cls(config=load_databricks_config())

    def list_principals(self, *, search_term: str = "", status_filter: str = "all", auth_source_filter: str = "all") -> tuple[SecurityPrincipal, ...]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT u.user_id AS principal_id, u.username, u.email,",
                    "       COALESCE(u.display_name, u.username) AS display_name,",
                    "       u.auth_source, u.is_active, u.last_login_at, u.updated_at,",
                    "       COUNT(DISTINCT CASE WHEN COALESCE(user_role.is_deleted, false) = false THEN user_role.role_id END) AS roles_count",
                    f"FROM {AUTH_SCHEMA_NAME}.users u",
                    f"LEFT JOIN {AUTH_SCHEMA_NAME}.user_roles user_role",
                    "  ON u.user_id = user_role.user_id",
                    "WHERE (",
                    "        :search_term = ''",
                    "     OR LOWER(u.user_id) LIKE CONCAT('%', LOWER(:search_term), '%')",
                    "     OR LOWER(u.username) LIKE CONCAT('%', LOWER(:search_term), '%')",
                    "     OR LOWER(u.email) LIKE CONCAT('%', LOWER(:search_term), '%')",
                    "      )",
                    "  AND (",
                    "        :status_filter = 'all'",
                    "     OR (:status_filter = 'active' AND u.is_active = TRUE)",
                    "     OR (:status_filter = 'inactive' AND u.is_active = FALSE)",
                    "      )",
                    "  AND (:auth_source_filter = 'all' OR LOWER(u.auth_source) = LOWER(:auth_source_filter))",
                    "  AND COALESCE(u.is_deleted, false) = false",
                    "GROUP BY u.user_id, u.username, u.email, u.display_name, u.auth_source, u.is_active, u.last_login_at, u.updated_at",
                    "ORDER BY u.updated_at DESC, u.user_id",
                ]
            ),
            [
                {"name": "search_term", "value": search_term, "type": "STRING"},
                {"name": "status_filter", "value": status_filter, "type": "STRING"},
                {"name": "auth_source_filter", "value": auth_source_filter, "type": "STRING"},
            ],
        )
        return tuple(
            SecurityPrincipal(
                principal_id=str(row.get("principal_id", "")).strip(),
                username=str(row.get("username", "")).strip(),
                email=str(row.get("email", "")).strip(),
                display_name=str(row.get("display_name", "")).strip(),
                auth_source=str(row.get("auth_source", "")).strip(),
                is_active=bool(row.get("is_active", False)),
                roles_count=int(row.get("roles_count") or 0),
                last_login_at=_parse_datetime(row.get("last_login_at")),
                updated_at=_parse_datetime(row.get("updated_at")),
            )
            for row in rows
        )

    def list_roles(self, *, search_term: str = "", status_filter: str = "all") -> tuple[SecurityRole, ...]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT r.role_id, r.role_name, r.role_description AS description, r.is_active, r.updated_at,",
                    "       COUNT(DISTINCT CASE WHEN COALESCE(user_role.is_deleted, false) = false THEN user_role.user_id END) AS users_count,",
                    "       COUNT(DISTINCT CASE WHEN COALESCE(role_permission.is_deleted, false) = false THEN role_permission.permission_id END) AS permissions_count",
                    f"FROM {AUTH_SCHEMA_NAME}.roles r",
                    f"LEFT JOIN {AUTH_SCHEMA_NAME}.user_roles user_role",
                    "  ON r.role_id = user_role.role_id",
                    f"LEFT JOIN {AUTH_SCHEMA_NAME}.role_permissions role_permission",
                    "  ON r.role_id = role_permission.role_id",
                    "WHERE (",
                    "        :search_term = ''",
                    "     OR LOWER(r.role_id) LIKE CONCAT('%', LOWER(:search_term), '%')",
                    "     OR LOWER(r.role_name) LIKE CONCAT('%', LOWER(:search_term), '%')",
                    "     OR LOWER(r.role_description) LIKE CONCAT('%', LOWER(:search_term), '%')",
                    "      )",
                    "  AND (",
                    "        :status_filter = 'all'",
                    "     OR (:status_filter = 'active' AND r.is_active = TRUE)",
                    "     OR (:status_filter = 'inactive' AND r.is_active = FALSE)",
                    "      )",
                    "  AND COALESCE(r.is_deleted, false) = false",
                    "GROUP BY r.role_id, r.role_name, r.role_description, r.is_active, r.updated_at",
                    "ORDER BY r.updated_at DESC, r.role_id",
                ]
            ),
            [
                {"name": "search_term", "value": search_term, "type": "STRING"},
                {"name": "status_filter", "value": status_filter, "type": "STRING"},
            ],
        )
        return tuple(
            SecurityRole(
                role_id=str(row.get("role_id", "")).strip(),
                role_name=str(row.get("role_name", "")).strip(),
                description=str(row.get("description", "")).strip(),
                is_active=bool(row.get("is_active", False)),
                users_count=int(row.get("users_count") or 0),
                permissions_count=int(row.get("permissions_count") or 0),
                updated_at=_parse_datetime(row.get("updated_at")),
            )
            for row in rows
        )

    def list_permissions(self, *, search_term: str = "") -> tuple[SecurityPermission, ...]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT perm.permission_id, perm.permission_name, perm.permission_description AS description,",
                    "       perm.is_active,",
                    "       COUNT(DISTINCT CASE WHEN COALESCE(role_permission.is_deleted, false) = false THEN role_permission.role_id END) AS role_count",
                    f"FROM {AUTH_SCHEMA_NAME}.permissions perm",
                    f"LEFT JOIN {AUTH_SCHEMA_NAME}.role_permissions role_permission",
                    "  ON perm.permission_id = role_permission.permission_id",
                    "WHERE (",
                    "        :search_term = ''",
                    "     OR LOWER(perm.permission_id) LIKE CONCAT('%', LOWER(:search_term), '%')",
                    "     OR LOWER(perm.permission_name) LIKE CONCAT('%', LOWER(:search_term), '%')",
                    "     OR LOWER(perm.permission_description) LIKE CONCAT('%', LOWER(:search_term), '%')",
                    "      )",
                    "  AND COALESCE(perm.is_deleted, false) = false",
                    "GROUP BY perm.permission_id, perm.permission_name, perm.permission_description, perm.is_active",
                    "ORDER BY perm.permission_name",
                ]
            ),
            [
                {"name": "search_term", "value": search_term, "type": "STRING"},
            ],
        )
        return tuple(
            SecurityPermission(
                permission_id=str(row.get("permission_id", "")).strip(),
                permission_name=str(row.get("permission_name", "")).strip(),
                description=str(row.get("description", "")).strip(),
                is_active=bool(row.get("is_active", False)),
                role_count=int(row.get("role_count") or 0),
            )
            for row in rows
        )

    def list_security_audit(self, *, limit: int = 200) -> tuple[SecurityAuditRecord, ...]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT event_id, event_type, principal_id, resource, action, result, occurred_at, details_json, correlation_id",
                    f"FROM {AUTH_SCHEMA_NAME}.app_audit_events",
                    "WHERE resource = 'security_admin' OR resource = 'security'",
                    "ORDER BY occurred_at DESC",
                    "LIMIT :limit_value",
                ]
            ),
            [{"name": "limit_value", "value": str(limit), "type": "INT"}],
        )
        return tuple(
            SecurityAuditRecord(
                event_id=str(row.get("event_id", "")).strip(),
                event_type=str(row.get("event_type", "")).strip(),
                principal_id=str(row.get("principal_id", "")).strip(),
                resource=str(row.get("resource", "")).strip(),
                action=str(row.get("action", "")).strip(),
                result=str(row.get("result", "")).strip(),
                occurred_at=_parse_datetime(row.get("occurred_at")),
                details_summary=str(row.get("details_json", "")).strip()[:120],
                correlation_id=str(row.get("correlation_id", "")).strip(),
            )
            for row in rows
        )

    def get_principal(self, *, principal_id: str) -> SecurityPrincipal | None:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT u.user_id AS principal_id, u.username, u.email, COALESCE(u.display_name, u.username) AS display_name,",
                    "       u.auth_source, u.is_active, u.last_login_at, u.updated_at,",
                    "       COUNT(DISTINCT CASE WHEN COALESCE(user_role.is_deleted, false) = false THEN user_role.role_id END) AS roles_count",
                    f"FROM {AUTH_SCHEMA_NAME}.users u",
                    f"LEFT JOIN {AUTH_SCHEMA_NAME}.user_roles user_role",
                    "  ON u.user_id = user_role.user_id",
                    "WHERE u.user_id = :principal_id",
                    "  AND COALESCE(u.is_deleted, false) = false",
                    "GROUP BY u.user_id, u.username, u.email, u.display_name, u.auth_source, u.is_active, u.last_login_at, u.updated_at",
                ]
            ),
            [{"name": "principal_id", "value": principal_id, "type": "STRING"}],
        )
        if not rows:
            return None
        row = rows[0]
        return SecurityPrincipal(
            principal_id=str(row.get("principal_id", "")).strip(),
            username=str(row.get("username", "")).strip(),
            email=str(row.get("email", "")).strip(),
            display_name=str(row.get("display_name", "")).strip(),
            auth_source=str(row.get("auth_source", "")).strip(),
            is_active=bool(row.get("is_active", False)),
            roles_count=int(row.get("roles_count") or 0),
            last_login_at=_parse_datetime(row.get("last_login_at")),
            updated_at=_parse_datetime(row.get("updated_at")),
        )

    def get_role(self, *, role_id: str) -> SecurityRole | None:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT r.role_id, r.role_name, r.role_description AS description, r.is_active, r.updated_at,",
                    "       COUNT(DISTINCT CASE WHEN COALESCE(user_role.is_deleted, false) = false THEN user_role.user_id END) AS users_count,",
                    "       COUNT(DISTINCT CASE WHEN COALESCE(role_permission.is_deleted, false) = false THEN role_permission.permission_id END) AS permissions_count",
                    f"FROM {AUTH_SCHEMA_NAME}.roles r",
                    f"LEFT JOIN {AUTH_SCHEMA_NAME}.user_roles user_role",
                    "  ON r.role_id = user_role.role_id",
                    f"LEFT JOIN {AUTH_SCHEMA_NAME}.role_permissions role_permission",
                    "  ON r.role_id = role_permission.role_id",
                    "WHERE r.role_id = :role_id",
                    "  AND COALESCE(r.is_deleted, false) = false",
                    "GROUP BY r.role_id, r.role_name, r.role_description, r.is_active, r.updated_at",
                ]
            ),
            [{"name": "role_id", "value": role_id, "type": "STRING"}],
        )
        if not rows:
            return None
        row = rows[0]
        return SecurityRole(
            role_id=str(row.get("role_id", "")).strip(),
            role_name=str(row.get("role_name", "")).strip(),
            description=str(row.get("description", "")).strip(),
            is_active=bool(row.get("is_active", False)),
            users_count=int(row.get("users_count") or 0),
            permissions_count=int(row.get("permissions_count") or 0),
            updated_at=_parse_datetime(row.get("updated_at")),
        )

    def get_permission(self, *, permission_id: str) -> SecurityPermission | None:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT perm.permission_id, perm.permission_name, perm.permission_description AS description,",
                    "       perm.is_active,",
                    "       COUNT(DISTINCT CASE WHEN COALESCE(role_permission.is_deleted, false) = false THEN role_permission.role_id END) AS role_count",
                    f"FROM {AUTH_SCHEMA_NAME}.permissions perm",
                    f"LEFT JOIN {AUTH_SCHEMA_NAME}.role_permissions role_permission",
                    "  ON perm.permission_id = role_permission.permission_id",
                    "WHERE perm.permission_id = :permission_id",
                    "  AND COALESCE(perm.is_deleted, false) = false",
                    "GROUP BY perm.permission_id, perm.permission_name, perm.permission_description, perm.is_active",
                ]
            ),
            [{"name": "permission_id", "value": permission_id, "type": "STRING"}],
        )
        if not rows:
            return None
        row = rows[0]
        return SecurityPermission(
            permission_id=str(row.get("permission_id", "")).strip(),
            permission_name=str(row.get("permission_name", "")).strip(),
            description=str(row.get("description", "")).strip(),
            is_active=bool(row.get("is_active", False)),
            role_count=int(row.get("role_count") or 0),
        )

    def username_exists(self, *, username: str, exclude_principal_id: str = "") -> bool:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT COUNT(*) AS item_count",
                    f"FROM {AUTH_SCHEMA_NAME}.users",
                    "WHERE LOWER(username) = LOWER(:username)",
                    "  AND COALESCE(is_deleted, false) = false",
                    "  AND (:exclude_principal_id = '' OR user_id <> :exclude_principal_id)",
                ]
            ),
            [
                {"name": "username", "value": username, "type": "STRING"},
                {"name": "exclude_principal_id", "value": exclude_principal_id, "type": "STRING"},
            ],
        )
        return int((rows[0].get("item_count") if rows else 0) or 0) > 0

    def email_exists(self, *, email: str, exclude_principal_id: str = "") -> bool:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT COUNT(*) AS item_count",
                    f"FROM {AUTH_SCHEMA_NAME}.users",
                    "WHERE LOWER(email) = LOWER(:email)",
                    "  AND COALESCE(is_deleted, false) = false",
                    "  AND (:exclude_principal_id = '' OR user_id <> :exclude_principal_id)",
                ]
            ),
            [
                {"name": "email", "value": email, "type": "STRING"},
                {"name": "exclude_principal_id", "value": exclude_principal_id, "type": "STRING"},
            ],
        )
        return int((rows[0].get("item_count") if rows else 0) or 0) > 0

    def role_name_exists(self, *, role_name: str, exclude_role_id: str = "") -> bool:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT COUNT(*) AS item_count",
                    f"FROM {AUTH_SCHEMA_NAME}.roles",
                    "WHERE LOWER(role_name) = LOWER(:role_name)",
                    "  AND COALESCE(is_deleted, false) = false",
                    "  AND (:exclude_role_id = '' OR role_id <> :exclude_role_id)",
                ]
            ),
            [
                {"name": "role_name", "value": role_name, "type": "STRING"},
                {"name": "exclude_role_id", "value": exclude_role_id, "type": "STRING"},
            ],
        )
        return int((rows[0].get("item_count") if rows else 0) or 0) > 0

    def permission_name_exists(self, *, permission_name: str, exclude_permission_id: str = "") -> bool:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT COUNT(*) AS item_count",
                    f"FROM {AUTH_SCHEMA_NAME}.permissions",
                    "WHERE LOWER(permission_name) = LOWER(:permission_name)",
                    "  AND COALESCE(is_deleted, false) = false",
                    "  AND (:exclude_permission_id = '' OR permission_id <> :exclude_permission_id)",
                ]
            ),
            [
                {"name": "permission_name", "value": permission_name, "type": "STRING"},
                {"name": "exclude_permission_id", "value": exclude_permission_id, "type": "STRING"},
            ],
        )
        return int((rows[0].get("item_count") if rows else 0) or 0) > 0

    def generate_next_role_id(self) -> str:
        return self._generate_next_prefixed_id(table_name="roles", id_column="role_id", prefix="R")

    def generate_next_permission_id(self) -> str:
        return self._generate_next_prefixed_id(table_name="permissions", id_column="permission_id", prefix="P")

    def list_assigned_roles_for_principal(self, *, principal_id: str) -> tuple[str, ...]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT role_id",
                    f"FROM {AUTH_SCHEMA_NAME}.user_roles",
                    "WHERE user_id = :principal_id AND COALESCE(is_deleted, false) = false",
                    "ORDER BY role_id",
                ]
            ),
            [{"name": "principal_id", "value": principal_id, "type": "STRING"}],
        )
        return tuple(str(row.get("role_id", "")).strip() for row in rows if str(row.get("role_id", "")).strip())

    def list_assigned_permissions_for_role(self, *, role_id: str) -> tuple[str, ...]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT perm.permission_name",
                    f"FROM {AUTH_SCHEMA_NAME}.role_permissions role_permission",
                    f"INNER JOIN {AUTH_SCHEMA_NAME}.permissions perm",
                    "  ON role_permission.permission_id = perm.permission_id",
                    "WHERE role_permission.role_id = :role_id",
                    "  AND COALESCE(role_permission.is_deleted, false) = false",
                    "  AND perm.is_active = TRUE",
                    "  AND COALESCE(perm.is_deleted, false) = false",
                    "ORDER BY perm.permission_name",
                ]
            ),
            [{"name": "role_id", "value": role_id, "type": "STRING"}],
        )
        return tuple(str(row.get("permission_name", "")).strip() for row in rows if str(row.get("permission_name", "")).strip())

    def list_assigned_principals_for_role(self, *, role_id: str) -> tuple[str, ...]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT user_id",
                    f"FROM {AUTH_SCHEMA_NAME}.user_roles",
                    "WHERE role_id = :role_id AND COALESCE(is_deleted, false) = false",
                    "ORDER BY user_id",
                ]
            ),
            [{"name": "role_id", "value": role_id, "type": "STRING"}],
        )
        return tuple(str(row.get("user_id", "")).strip() for row in rows if str(row.get("user_id", "")).strip())

    def list_assigned_roles_for_permission(self, *, permission_id: str) -> tuple[str, ...]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT role_id",
                    f"FROM {AUTH_SCHEMA_NAME}.role_permissions",
                    "WHERE permission_id = :permission_id AND COALESCE(is_deleted, false) = false",
                    "ORDER BY role_id",
                ]
            ),
            [{"name": "permission_id", "value": permission_id, "type": "STRING"}],
        )
        return tuple(str(row.get("role_id", "")).strip() for row in rows if str(row.get("role_id", "")).strip())

    def list_permissions_for_roles(self, *, role_ids: tuple[str, ...]) -> tuple[str, ...]:
        if not role_ids:
            return ()
        role_filters = [f"LOWER(role_permission.role_id) = LOWER(:role_id_{index})" for index, _ in enumerate(role_ids)]
        parameters = [{"name": f"role_id_{index}", "value": role_id, "type": "STRING"} for index, role_id in enumerate(role_ids)]
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT DISTINCT perm.permission_name",
                    f"FROM {AUTH_SCHEMA_NAME}.role_permissions role_permission",
                    f"INNER JOIN {AUTH_SCHEMA_NAME}.permissions perm",
                    "  ON role_permission.permission_id = perm.permission_id",
                    "WHERE COALESCE(role_permission.is_deleted, false) = false",
                    "  AND perm.is_active = TRUE",
                    "  AND COALESCE(perm.is_deleted, false) = false",
                    f"  AND ({' OR '.join(role_filters)})",
                    "ORDER BY perm.permission_name",
                ]
            ),
            parameters,
        )
        return tuple(
            str(row.get("permission_name", "")).strip() for row in rows if str(row.get("permission_name", "")).strip()
        )

    def list_effective_permissions_for_principal(self, *, principal_id: str) -> tuple[str, ...]:
        role_ids = self.list_assigned_roles_for_principal(principal_id=principal_id)
        return self.list_permissions_for_roles(role_ids=role_ids)

    def list_related_security_audit(self, *, target_type: str, target_id: str, limit: int = 50) -> tuple[SecurityAuditRecord, ...]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT event_id, event_type, principal_id, resource, action, result, occurred_at, details_json, correlation_id",
                    f"FROM {AUTH_SCHEMA_NAME}.app_audit_events",
                    "WHERE (resource = 'security_admin' OR resource = 'security')",
                    "  AND (",
                    "        LOWER(details_json) LIKE CONCAT('%\"target_type\":\"', LOWER(:target_type), '\"%')",
                    "    AND LOWER(details_json) LIKE CONCAT('%\"target_id\":\"', LOWER(:target_id), '\"%')",
                    "      )",
                    "ORDER BY occurred_at DESC",
                    "LIMIT :limit_value",
                ]
            ),
            [
                {"name": "target_type", "value": target_type.lower(), "type": "STRING"},
                {"name": "target_id", "value": target_id.lower(), "type": "STRING"},
                {"name": "limit_value", "value": str(limit), "type": "INT"},
            ],
        )
        return tuple(
            SecurityAuditRecord(
                event_id=str(row.get("event_id", "")).strip(),
                event_type=str(row.get("event_type", "")).strip(),
                principal_id=str(row.get("principal_id", "")).strip(),
                resource=str(row.get("resource", "")).strip(),
                action=str(row.get("action", "")).strip(),
                result=str(row.get("result", "")).strip(),
                occurred_at=_parse_datetime(row.get("occurred_at")),
                details_summary=str(row.get("details_json", "")).strip()[:120],
                correlation_id=str(row.get("correlation_id", "")).strip(),
            )
            for row in rows
        )

    def get_principal_detail(self, *, principal_id: str) -> SecurityPrincipalDetail | None:
        principal = self.get_principal(principal_id=principal_id)
        if principal is None:
            return None
        assigned_roles = self.list_assigned_roles_for_principal(principal_id=principal_id)
        effective_permissions = self.list_effective_permissions_for_principal(principal_id=principal_id)
        related_audit = self.list_related_security_audit(target_type="principal", target_id=principal_id, limit=50)
        return SecurityPrincipalDetail(
            principal=principal,
            assigned_roles=assigned_roles,
            effective_permissions=effective_permissions,
            related_audit=related_audit,
        )

    def get_role_detail(self, *, role_id: str) -> SecurityRoleDetail | None:
        role = self.get_role(role_id=role_id)
        if role is None:
            return None
        assigned_permissions = self.list_assigned_permissions_for_role(role_id=role_id)
        assigned_principals = self.list_assigned_principals_for_role(role_id=role_id)
        return SecurityRoleDetail(
            role=role,
            assigned_permissions=assigned_permissions,
            assigned_principals=assigned_principals,
        )

    def upsert_principal(self, *, principal_id: str, username: str, email: str, display_name: str, auth_source: str, is_active: bool) -> None:
        self._execute_statement(
            "\n".join(
                [
                    f"MERGE INTO {AUTH_SCHEMA_NAME}.users AS target",
                    "USING (SELECT :principal_id user_id, :username username, :email email, :display_name display_name, :auth_source auth_source) source",
                    "ON target.user_id = source.user_id",
                    "WHEN MATCHED THEN UPDATE SET",
                    "  target.username = source.username, target.email = source.email, target.display_name = source.display_name,",
                    "  target.auth_source = source.auth_source, target.is_active = :is_active, target.updated_at = current_timestamp(),",
                    "  target.updated_by = 'security-admin', target.is_deleted = false, target.deleted_at = NULL, target.deleted_by = NULL",
                    "WHEN NOT MATCHED THEN INSERT (user_id, username, email, display_name, password_hash, auth_source, last_login_at, is_active, created_at, updated_at, deleted_at, created_by, updated_by, deleted_by, is_deleted)",
                    "VALUES (source.user_id, source.username, source.email, source.display_name, NULL, source.auth_source, NULL, :is_active, current_timestamp(), current_timestamp(), NULL, 'security-admin', 'security-admin', NULL, false)",
                ]
            ),
            [
                {"name": "principal_id", "value": principal_id, "type": "STRING"},
                {"name": "username", "value": username, "type": "STRING"},
                {"name": "email", "value": email, "type": "STRING"},
                {"name": "display_name", "value": display_name, "type": "STRING"},
                {"name": "auth_source", "value": auth_source, "type": "STRING"},
                {"name": "is_active", "value": "true" if is_active else "false", "type": "BOOLEAN"},
            ],
        )

    def upsert_role(self, *, role_id: str, role_name: str, description: str, is_active: bool) -> None:
        self._execute_statement(
            "\n".join(
                [
                    f"MERGE INTO {AUTH_SCHEMA_NAME}.roles AS target",
                    "USING (SELECT :role_id role_id, :role_name role_name, :description role_description) source",
                    "ON target.role_id = source.role_id",
                    "WHEN MATCHED THEN UPDATE SET target.role_name = source.role_name, target.role_description = source.role_description, target.is_active = :is_active, target.updated_at = current_timestamp(), target.updated_by = 'security-admin', target.is_deleted = false, target.deleted_at = NULL, target.deleted_by = NULL",
                    "WHEN NOT MATCHED THEN INSERT (role_id, role_name, role_description, is_active, created_at, updated_at, deleted_at, created_by, updated_by, deleted_by, is_deleted)",
                    "VALUES (source.role_id, source.role_name, source.role_description, :is_active, current_timestamp(), current_timestamp(), NULL, 'security-admin', 'security-admin', NULL, false)",
                ]
            ),
            [
                {"name": "role_id", "value": role_id, "type": "STRING"},
                {"name": "role_name", "value": role_name, "type": "STRING"},
                {"name": "description", "value": description, "type": "STRING"},
                {"name": "is_active", "value": "true" if is_active else "false", "type": "BOOLEAN"},
            ],
        )

    def upsert_permission(self, *, permission_id: str, permission_name: str, description: str, is_active: bool) -> None:
        self._execute_statement(
            "\n".join(
                [
                    f"MERGE INTO {AUTH_SCHEMA_NAME}.permissions AS target",
                    "USING (SELECT :permission_id permission_id, :permission_name permission_name, :description permission_description) source",
                    "ON target.permission_id = source.permission_id",
                    "WHEN MATCHED THEN UPDATE SET target.permission_name = source.permission_name, target.permission_description = source.permission_description, target.is_active = :is_active, target.updated_at = current_timestamp(), target.updated_by = 'security-admin', target.is_deleted = false, target.deleted_at = NULL, target.deleted_by = NULL",
                    "WHEN NOT MATCHED THEN INSERT (permission_id, permission_name, permission_description, is_active, created_at, updated_at, deleted_at, created_by, updated_by, deleted_by, is_deleted)",
                    "VALUES (source.permission_id, source.permission_name, source.permission_description, :is_active, current_timestamp(), current_timestamp(), NULL, 'security-admin', 'security-admin', NULL, false)",
                ]
            ),
            [
                {"name": "permission_id", "value": permission_id, "type": "STRING"},
                {"name": "permission_name", "value": permission_name, "type": "STRING"},
                {"name": "description", "value": description, "type": "STRING"},
                {"name": "is_active", "value": "true" if is_active else "false", "type": "BOOLEAN"},
            ],
        )

    def soft_delete_principal(self, *, principal_id: str, actor: str) -> None:
        self._execute_statement(
            "\n".join(
                [
                    f"UPDATE {AUTH_SCHEMA_NAME}.users",
                    "SET is_deleted = TRUE, is_active = FALSE, deleted_at = current_timestamp(), deleted_by = :actor, updated_at = current_timestamp(), updated_by = :actor",
                    "WHERE user_id = :principal_id AND COALESCE(is_deleted, false) = false",
                ]
            ),
            [
                {"name": "principal_id", "value": principal_id, "type": "STRING"},
                {"name": "actor", "value": actor, "type": "STRING"},
            ],
        )
        self._execute_statement(
            "\n".join(
                [
                    f"UPDATE {AUTH_SCHEMA_NAME}.user_roles",
                    "SET is_deleted = TRUE, deleted_at = current_timestamp(), deleted_by = :actor, updated_at = current_timestamp(), updated_by = :actor",
                    "WHERE user_id = :principal_id AND COALESCE(is_deleted, false) = false",
                ]
            ),
            [
                {"name": "principal_id", "value": principal_id, "type": "STRING"},
                {"name": "actor", "value": actor, "type": "STRING"},
            ],
        )

    def soft_delete_role(self, *, role_id: str, actor: str) -> None:
        self._execute_statement(
            "\n".join(
                [
                    f"UPDATE {AUTH_SCHEMA_NAME}.roles",
                    "SET is_deleted = TRUE, is_active = FALSE, deleted_at = current_timestamp(), deleted_by = :actor, updated_at = current_timestamp(), updated_by = :actor",
                    "WHERE role_id = :role_id AND COALESCE(is_deleted, false) = false",
                ]
            ),
            [
                {"name": "role_id", "value": role_id, "type": "STRING"},
                {"name": "actor", "value": actor, "type": "STRING"},
            ],
        )
        self._execute_statement(
            "\n".join(
                [
                    f"UPDATE {AUTH_SCHEMA_NAME}.user_roles",
                    "SET is_deleted = TRUE, deleted_at = current_timestamp(), deleted_by = :actor, updated_at = current_timestamp(), updated_by = :actor",
                    "WHERE role_id = :role_id AND COALESCE(is_deleted, false) = false",
                ]
            ),
            [
                {"name": "role_id", "value": role_id, "type": "STRING"},
                {"name": "actor", "value": actor, "type": "STRING"},
            ],
        )
        self._execute_statement(
            "\n".join(
                [
                    f"UPDATE {AUTH_SCHEMA_NAME}.role_permissions",
                    "SET is_deleted = TRUE, deleted_at = current_timestamp(), deleted_by = :actor, updated_at = current_timestamp(), updated_by = :actor",
                    "WHERE role_id = :role_id AND COALESCE(is_deleted, false) = false",
                ]
            ),
            [
                {"name": "role_id", "value": role_id, "type": "STRING"},
                {"name": "actor", "value": actor, "type": "STRING"},
            ],
        )

    def soft_delete_permission(self, *, permission_id: str, actor: str) -> None:
        self._execute_statement(
            "\n".join(
                [
                    f"UPDATE {AUTH_SCHEMA_NAME}.permissions",
                    "SET is_deleted = TRUE, is_active = FALSE, deleted_at = current_timestamp(), deleted_by = :actor, updated_at = current_timestamp(), updated_by = :actor",
                    "WHERE permission_id = :permission_id AND COALESCE(is_deleted, false) = false",
                ]
            ),
            [
                {"name": "permission_id", "value": permission_id, "type": "STRING"},
                {"name": "actor", "value": actor, "type": "STRING"},
            ],
        )
        self._execute_statement(
            "\n".join(
                [
                    f"UPDATE {AUTH_SCHEMA_NAME}.role_permissions",
                    "SET is_deleted = TRUE, deleted_at = current_timestamp(), deleted_by = :actor, updated_at = current_timestamp(), updated_by = :actor",
                    "WHERE permission_id = :permission_id AND COALESCE(is_deleted, false) = false",
                ]
            ),
            [
                {"name": "permission_id", "value": permission_id, "type": "STRING"},
                {"name": "actor", "value": actor, "type": "STRING"},
            ],
        )

    def assign_roles_to_principal(self, *, principal_id: str, role_ids: tuple[str, ...], actor: str) -> None:
        for role_id in role_ids:
            self._execute_statement(
                "\n".join(
                    [
                        f"MERGE INTO {AUTH_SCHEMA_NAME}.user_roles AS target",
                        "USING (SELECT :principal_id user_id, :role_id role_id) source",
                        "ON target.user_id = source.user_id AND target.role_id = source.role_id",
                        "WHEN MATCHED THEN UPDATE SET target.updated_at = current_timestamp(), target.updated_by = :assigned_by, target.deleted_at = NULL, target.deleted_by = NULL, target.is_deleted = false",
                        "WHEN NOT MATCHED THEN INSERT (user_id, role_id, created_at, updated_at, deleted_at, created_by, updated_by, deleted_by, is_deleted)",
                        "VALUES (source.user_id, source.role_id, current_timestamp(), current_timestamp(), NULL, :assigned_by, :assigned_by, NULL, false)",
                    ]
                ),
                [
                    {"name": "principal_id", "value": principal_id, "type": "STRING"},
                    {"name": "role_id", "value": role_id, "type": "STRING"},
                    {"name": "assigned_by", "value": actor, "type": "STRING"},
                ],
            )

    def unassign_roles_from_principal(self, *, principal_id: str, role_ids: tuple[str, ...], actor: str) -> None:
        for role_id in role_ids:
            self._execute_statement(
                "\n".join(
                    [
                        f"UPDATE {AUTH_SCHEMA_NAME}.user_roles",
                        "SET is_deleted = TRUE, deleted_at = current_timestamp(), deleted_by = :assigned_by, updated_at = current_timestamp(), updated_by = :assigned_by",
                        "WHERE user_id = :principal_id AND role_id = :role_id AND COALESCE(is_deleted, false) = false",
                    ]
                ),
                [
                    {"name": "principal_id", "value": principal_id, "type": "STRING"},
                    {"name": "role_id", "value": role_id, "type": "STRING"},
                    {"name": "assigned_by", "value": actor, "type": "STRING"},
                ],
            )

    def assign_permissions_to_role(self, *, role_id: str, permission_ids: tuple[str, ...], actor: str) -> None:
        if not permission_ids:
            return
        values_clause, permission_params = self._build_values_parameters(permission_ids, parameter_prefix="permission_id")
        self._execute_statement(
            "\n".join(
                [
                    f"MERGE INTO {AUTH_SCHEMA_NAME}.role_permissions AS target",
                    "USING (",
                    "  SELECT :role_id AS role_id, src.permission_id",
                    f"  FROM (VALUES {values_clause}) AS src(permission_id)",
                    ") AS source",
                    "ON target.role_id = source.role_id AND target.permission_id = source.permission_id",
                    "WHEN MATCHED THEN UPDATE SET target.updated_at = current_timestamp(), target.updated_by = :assigned_by, target.deleted_at = NULL, target.deleted_by = NULL, target.is_deleted = false",
                    "WHEN NOT MATCHED THEN INSERT (role_id, permission_id, created_at, updated_at, deleted_at, created_by, updated_by, deleted_by, is_deleted)",
                    "VALUES (source.role_id, source.permission_id, current_timestamp(), current_timestamp(), NULL, :assigned_by, :assigned_by, NULL, false)",
                ]
            ),
            [
                {"name": "role_id", "value": role_id, "type": "STRING"},
                {"name": "assigned_by", "value": actor, "type": "STRING"},
                *permission_params,
            ],
        )

    def unassign_permissions_from_role(self, *, role_id: str, permission_ids: tuple[str, ...], actor: str) -> None:
        if not permission_ids:
            return
        values_clause, permission_params = self._build_values_parameters(permission_ids, parameter_prefix="permission_id")
        self._execute_statement(
            "\n".join(
                [
                    f"MERGE INTO {AUTH_SCHEMA_NAME}.role_permissions AS target",
                    "USING (",
                    "  SELECT :role_id AS role_id, src.permission_id",
                    f"  FROM (VALUES {values_clause}) AS src(permission_id)",
                    ") AS source",
                    "ON target.role_id = source.role_id AND target.permission_id = source.permission_id",
                    "WHEN MATCHED THEN UPDATE SET target.is_deleted = TRUE, target.deleted_at = current_timestamp(), target.deleted_by = :assigned_by, target.updated_at = current_timestamp(), target.updated_by = :assigned_by",
                ]
            ),
            [
                {"name": "role_id", "value": role_id, "type": "STRING"},
                {"name": "assigned_by", "value": actor, "type": "STRING"},
                *permission_params,
            ],
        )

    def list_all_role_permissions(self) -> dict[str, tuple[str, ...]]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT rp.role_id, p.permission_name",
                    f"FROM {AUTH_SCHEMA_NAME}.role_permissions rp",
                    f"JOIN {AUTH_SCHEMA_NAME}.permissions p ON rp.permission_id = p.permission_id",
                    "WHERE COALESCE(rp.is_deleted, false) = false",
                    "  AND p.is_active = TRUE",
                    "  AND COALESCE(p.is_deleted, false) = false",
                    "ORDER BY rp.role_id, p.permission_name",
                ]
            ),
            [],
        )
        mapping: dict[str, list[str]] = {}
        for row in rows:
            role_id = str(row.get("role_id", "")).strip()
            perm_name = str(row.get("permission_name", "")).strip()
            if role_id not in mapping:
                mapping[role_id] = []
            mapping[role_id].append(perm_name)
        return {k: tuple(v) for k, v in mapping.items()}

    def list_all_principal_active_roles(self) -> dict[str, tuple[str, ...]]:
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT ur.user_id, ur.role_id",
                    f"FROM {AUTH_SCHEMA_NAME}.user_roles ur",
                    f"JOIN {AUTH_SCHEMA_NAME}.users u ON ur.user_id = u.user_id",
                    "WHERE COALESCE(ur.is_deleted, false) = false",
                    "  AND u.is_active = TRUE",
                    "  AND COALESCE(u.is_deleted, false) = false",
                    "ORDER BY ur.user_id, ur.role_id",
                ]
            ),
            [],
        )
        mapping: dict[str, list[str]] = {}
        for row in rows:
            user_id = str(row.get("user_id", "")).strip()
            role_id = str(row.get("role_id", "")).strip()
            if user_id not in mapping:
                mapping[user_id] = []
            mapping[user_id].append(role_id)
        return {k: tuple(v) for k, v in mapping.items()}

    def bulk_assign_roles_to_principal(self, *, principal_id: str, role_ids: tuple[str, ...], actor: str) -> None:
        if not role_ids:
            return
        values_clause = ", ".join([f"('{role_id}')" for role_id in role_ids])
        self._execute_statement(
            "\n".join(
                [
                    f"MERGE INTO {AUTH_SCHEMA_NAME}.user_roles AS target",
                    f"USING (SELECT :principal_id user_id, col1 role_id FROM (VALUES {values_clause})) source",
                    "ON target.user_id = source.user_id AND target.role_id = source.role_id",
                    "WHEN MATCHED THEN UPDATE SET target.updated_at = current_timestamp(), target.updated_by = :assigned_by, target.deleted_at = NULL, target.deleted_by = NULL, target.is_deleted = false",
                    "WHEN NOT MATCHED THEN INSERT (user_id, role_id, created_at, updated_at, deleted_at, created_by, updated_by, deleted_by, is_deleted)",
                    "VALUES (source.user_id, source.role_id, current_timestamp(), current_timestamp(), NULL, :assigned_by, :assigned_by, NULL, false)",
                ]
            ),
            [
                {"name": "principal_id", "value": principal_id, "type": "STRING"},
                {"name": "assigned_by", "value": actor, "type": "STRING"},
            ],
        )

    def bulk_unassign_roles_from_principal(self, *, principal_id: str, role_ids: tuple[str, ...], actor: str) -> None:
        if not role_ids:
            return
        role_list = ", ".join([f"'{role_id}'" for role_id in role_ids])
        self._execute_statement(
            "\n".join(
                [
                    f"UPDATE {AUTH_SCHEMA_NAME}.user_roles",
                    "SET is_deleted = TRUE, deleted_at = current_timestamp(), deleted_by = :assigned_by, updated_at = current_timestamp(), updated_by = :assigned_by",
                    f"WHERE user_id = :principal_id AND role_id IN ({role_list}) AND COALESCE(is_deleted, false) = false",
                ]
            ),
            [
                {"name": "principal_id", "value": principal_id, "type": "STRING"},
                {"name": "assigned_by", "value": actor, "type": "STRING"},
            ],
        )

    def _execute_select(self, statement: str, parameters: list[dict[str, str]]) -> list[dict[str, Any]]:
        return _parse_select_rows(self._execute_statement(statement, parameters))

    def _build_values_parameters(
        self,
        values: tuple[str, ...],
        *,
        parameter_prefix: str,
    ) -> tuple[str, list[dict[str, str]]]:
        placeholders: list[str] = []
        parameters: list[dict[str, str]] = []
        for index, value in enumerate(values):
            parameter_name = f"{parameter_prefix}_{index}"
            placeholders.append(f"(:{parameter_name})")
            parameters.append({"name": parameter_name, "value": value, "type": "STRING"})
        return ", ".join(placeholders), parameters

    def _execute_statement(self, statement: str, parameters: list[dict[str, str]] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "statement": statement,
            "warehouse_id": self.config.warehouse_id,
            "wait_timeout": self.config.wait_timeout,
            "on_wait_timeout": "CONTINUE",
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
        }
        if parameters:
            payload["parameters"] = parameters
        base_url = _normalize_base_url(self.config.host)
        try:
            response = _databricks_api_request(base_url=base_url, token=self.config.token, method="POST", path="/api/2.0/sql/statements/", payload=payload)
            statement_id = str(response.get("statement_id", "")).strip()
            if not statement_id:
                raise AuthSystemUnavailableError("Databricks security admin response is missing statement_id.")
            status = response.get("status", {}) if isinstance(response, dict) else {}
            state = str(status.get("state", "")).upper() if isinstance(status, dict) else ""
            if state != "SUCCEEDED":
                response = _poll_statement_completion(
                    base_url=base_url,
                    token=self.config.token,
                    statement_id=statement_id,
                    poll_seconds=self.config.poll_seconds,
                    timeout_seconds=self.config.timeout_seconds,
                )
            _raise_for_unsuccessful_statement(response, context="Databricks security admin query")
            return response
        except DatabricksUnavailableError as exc:
            raise AuthSystemUnavailableError(str(exc)) from exc

    def _generate_next_prefixed_id(self, *, table_name: str, id_column: str, prefix: str, width: int = 3) -> str:
        normalized_prefix = prefix.upper()
        prefix_length = len(normalized_prefix)
        pattern = f"^{normalized_prefix}[0-9]+$"
        rows = self._execute_select(
            "\n".join(
                [
                    "SELECT COALESCE(",
                    "  MAX(",
                    "    CASE",
                    f"      WHEN UPPER({id_column}) RLIKE '{pattern}'",
                    f"      THEN CAST(SUBSTRING(UPPER({id_column}), {prefix_length + 1}) AS INT)",
                    "      ELSE 0",
                    "    END",
                    "  ),",
                    "  0",
                    ") AS max_value",
                    f"FROM {AUTH_SCHEMA_NAME}.{table_name}",
                ]
            ),
            [],
        )
        raw_value = rows[0].get("max_value") if rows else 0
        try:
            max_value = int(raw_value or 0)
        except (TypeError, ValueError):
            max_value = 0
        return f"{prefix}{max_value + 1:0{width}d}"


def _parse_datetime(raw_value: object) -> datetime | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_select_rows(statement_response: dict[str, Any]) -> list[dict[str, Any]]:
    manifest = statement_response.get("manifest", {}) if isinstance(statement_response, dict) else {}
    result = statement_response.get("result", {}) if isinstance(statement_response, dict) else {}
    columns = (manifest.get("schema", {}) if isinstance(manifest, dict) else {}).get("columns", [])
    raw_rows = result.get("data_array", []) if isinstance(result, dict) else []
    if not isinstance(columns, list) or not columns:
        raise AuthSystemUnavailableError("Databricks security admin response is missing schema columns.")
    if raw_rows is None:
        raw_rows = []
    if not isinstance(raw_rows, list):
        raise AuthSystemUnavailableError("Databricks security admin rows must be a list.")
    column_names = [str(column.get("name", "")).strip() for column in columns if isinstance(column, dict)]
    parsed_rows: list[dict[str, Any]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, list):
            raise AuthSystemUnavailableError("Databricks security admin row must be a list.")
        if len(raw_row) != len(column_names):
            raise AuthSystemUnavailableError("Databricks security admin row length does not match schema columns.")
        parsed_rows.append(dict(zip(column_names, raw_row)))
    return parsed_rows


def _raise_for_unsuccessful_statement(response: dict[str, Any], *, context: str) -> None:
    status = response.get("status", {}) if isinstance(response, dict) else {}
    state = str(status.get("state", "")).upper() if isinstance(status, dict) else ""
    if state in {"", "SUCCEEDED"}:
        return
    error = status.get("error", {}) if isinstance(status, dict) else {}
    error_message = ""
    if isinstance(error, dict):
        error_message = str(error.get("message", "")).strip()
    if not error_message:
        error_message = str(status.get("state_message", "")).strip() if isinstance(status, dict) else ""
    details = f": {error_message}" if error_message else ""
    raise AuthSystemUnavailableError(f"{context} failed with state {state}{details}")
