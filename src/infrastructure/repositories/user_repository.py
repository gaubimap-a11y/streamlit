from __future__ import annotations

from src.core.config import get_settings
from src.core.exceptions import DataAccessError
from src.domain.user import UserRow


class UserRepository:
    def _get_find_by_username_sql(self) -> str:
        catalog = get_settings().databricks.catalog
        return (
            f"SELECT user_id, username, email, password_hash, is_active "
            f"FROM {catalog}.auth.users "
            "WHERE username = ?"
        )

    def find_by_username(self, username: str, conn) -> UserRow | None:
        try:
            with conn.cursor() as cursor:
                cursor.execute(self._get_find_by_username_sql(), [username])
                row = cursor.fetchone()
        except Exception as exc:
            raise DataAccessError(f"Failed to fetch user by username: {username}") from exc

        if row is None:
            return None

        return UserRow(
            user_id=row[0],
            username=row[1],
            email=row[2],
            password_hash=row[3],
            is_active=bool(row[4]),
        )

    def find_roles_and_permissions_by_username(
        self,
        username: str,
        conn,
    ) -> tuple[list[str], list[str]]:
        catalog = get_settings().databricks.catalog
        normalized_username = (username or "").strip().lower()
        if not normalized_username:
            return [], []

        roles_sql = (
            "SELECT DISTINCT LOWER(r.role_name) AS role_name "
            f"FROM {catalog}.auth.users u "
            f"JOIN {catalog}.auth.user_roles ur ON u.user_id = ur.user_id "
            f"JOIN {catalog}.auth.roles r ON ur.role_id = r.role_id "
            "WHERE LOWER(u.username) = ? "
            "AND COALESCE(u.is_deleted, false) = false "
            "AND COALESCE(ur.is_deleted, false) = false "
            "AND COALESCE(r.is_deleted, false) = false "
            "AND COALESCE(r.is_active, true) = true"
        )

        permissions_sql = (
            "SELECT DISTINCT LOWER(p.permission_name) AS permission_name "
            f"FROM {catalog}.auth.users u "
            f"JOIN {catalog}.auth.user_roles ur ON u.user_id = ur.user_id "
            f"JOIN {catalog}.auth.roles r ON ur.role_id = r.role_id "
            f"JOIN {catalog}.auth.role_permissions rp ON r.role_id = rp.role_id "
            f"JOIN {catalog}.auth.permissions p ON rp.permission_id = p.permission_id "
            "WHERE LOWER(u.username) = ? "
            "AND COALESCE(u.is_deleted, false) = false "
            "AND COALESCE(ur.is_deleted, false) = false "
            "AND COALESCE(r.is_deleted, false) = false "
            "AND COALESCE(rp.is_deleted, false) = false "
            "AND COALESCE(p.is_deleted, false) = false "
            "AND COALESCE(r.is_active, true) = true "
            "AND COALESCE(p.is_active, true) = true"
        )

        try:
            with conn.cursor() as cursor:
                cursor.execute(roles_sql, [normalized_username])
                role_rows = cursor.fetchall() or []
            with conn.cursor() as cursor:
                cursor.execute(permissions_sql, [normalized_username])
                permission_rows = cursor.fetchall() or []
        except Exception as exc:
            raise DataAccessError(
                f"Failed to fetch roles/permissions by username: {normalized_username}",
            ) from exc

        roles = sorted({str(row[0]).strip().lower() for row in role_rows if row and row[0]})
        permissions = sorted(
            {str(row[0]).strip().lower() for row in permission_rows if row and row[0]},
        )
        return roles, permissions
    def _get_list_users_sql(self) -> str:
        catalog = get_settings().databricks.catalog
        return (
            f"SELECT user_id, username, email, password_hash, is_active "
            f"FROM {catalog}.auth.users "
            "ORDER BY username"
        )

    def _get_find_by_user_id_sql(self) -> str:
        catalog = get_settings().databricks.catalog
        return (
            f"SELECT user_id, username, email, password_hash, is_active "
            f"FROM {catalog}.auth.users "
            "WHERE user_id = ?"
        )

    def list_users(self, conn) -> list[UserRow]:
        try:
            with conn.cursor() as cursor:
                cursor.execute(self._get_list_users_sql())
                rows = cursor.fetchall()
        except Exception as exc:
            raise DataAccessError("Failed to fetch users.") from exc

        return [
            UserRow(
                user_id=row[0],
                username=row[1],
                email=row[2],
                password_hash=row[3],
                is_active=bool(row[4]),
            )
            for row in rows
        ]

    def find_by_user_id(self, user_id: str, conn) -> UserRow | None:
        try:
            with conn.cursor() as cursor:
                cursor.execute(self._get_find_by_user_id_sql(), [user_id])
                row = cursor.fetchone()
        except Exception as exc:
            raise DataAccessError(f"Failed to fetch user by user_id: {user_id}") from exc

        if row is None:
            return None

        return UserRow(
            user_id=row[0],
            username=row[1],
            email=row[2],
            password_hash=row[3],
            is_active=bool(row[4]),
        )
