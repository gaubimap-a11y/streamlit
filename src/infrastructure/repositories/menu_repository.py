from __future__ import annotations

from datetime import datetime, timezone

from src.core.config import get_settings


def _menu_table() -> str:
    catalog = get_settings().databricks.catalog
    # Table is managed by migration scripts, not runtime code.
    return f"{catalog}.menu.menus"


def _menu_table_has_column(connection, *, column_name: str) -> bool:
    menus_table = _menu_table()
    target = (column_name or "").strip().lower()
    if not target:
        return False
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {menus_table} LIMIT 0")
        description = getattr(cursor, "description", None) or []
    columns = {str(item[0]).strip().lower() for item in description if item and item[0]}
    return target in columns


def _map_menu_row(row: object, *, include_hide_menu: bool) -> dict[str, object]:
    values = list(row or [])
    sort_order_value = values[5] if len(values) > 5 else 0
    is_active_value = values[7] if len(values) > 7 else True
    try:
        sort_order = int(sort_order_value) if sort_order_value is not None else 0
    except (TypeError, ValueError):
        sort_order = 0
    return {
        "menu_key": str(values[0]).strip().lower(),
        "title": str(values[1]),
        "parent_key": str(values[2]).strip().lower() if values[2] else None,
        "route": str(values[3]).strip() if values[3] else None,
        "icon": str(values[4]).strip() if values[4] else None,
        "sort_order": sort_order,
        "permission_code": str(values[6]).strip().lower(),
        "is_active": True if is_active_value is None else bool(is_active_value),
        "hide_menu": (
            bool(values[8])
            if include_hide_menu and len(values) > 8 and values[8] is not None
            else None
        ),
    }


def list_menus(connection, *, active_only: bool = False) -> list[dict[str, object]]:
    menus_table = _menu_table()
    has_hide_menu = _menu_table_has_column(connection, column_name="hide_menu")
    has_created_at = _menu_table_has_column(connection, column_name="created_at")
    select_cols = "menu_key, title, parent_key, route, icon, sort_order, permission_code, is_active"
    if has_hide_menu:
        select_cols += ", hide_menu"
    if has_created_at:
        select_cols += ", created_at"
    sql = (
        f"SELECT {select_cols} "
        f"FROM {menus_table} "
    )
    params: list[object] = []
    if active_only:
        sql += "WHERE is_active = TRUE "
    if has_created_at:
        # Keep older records on top and newly created records at the bottom.
        sql += "ORDER BY created_at ASC, updated_at ASC, title ASC"
    else:
        sql += "ORDER BY sort_order ASC, title ASC"
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall() or []
    return [
        _map_menu_row(row, include_hide_menu=has_hide_menu)
        for row in rows
        if row and row[0]
    ]


def get_menu_by_key(connection, menu_key: str) -> dict[str, object] | None:
    menus_table = _menu_table()
    normalized = (menu_key or "").strip().lower()
    if not normalized:
        return None
    has_hide_menu = _menu_table_has_column(connection, column_name="hide_menu")
    select_cols = "menu_key, title, parent_key, route, icon, sort_order, permission_code, is_active"
    if has_hide_menu:
        select_cols += ", hide_menu"
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT {select_cols}
            FROM {menus_table}
            WHERE LOWER(menu_key) = LOWER(?)
            LIMIT 1
            """,
            [normalized],
        )
        row = cursor.fetchone()
    if not row:
        return None
    return _map_menu_row(row, include_hide_menu=has_hide_menu)


def create_menu(connection, *, menu: dict[str, object]) -> bool:
    menus_table = _menu_table()
    has_hide_menu = _menu_table_has_column(connection, column_name="hide_menu")
    has_created_at = _menu_table_has_column(connection, column_name="created_at")
    now = datetime.now(tz=timezone.utc)
    with connection.cursor() as cursor:
        if has_hide_menu and has_created_at:
            cursor.execute(
                f"""
                INSERT INTO {menus_table}
                (menu_key, title, parent_key, route, icon, sort_order, permission_code, is_active, hide_menu, created_at, updated_at)
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM {menus_table} WHERE LOWER(menu_key) = LOWER(?)
                )
                """,
                [
                    menu["menu_key"],
                    menu["title"],
                    menu.get("parent_key"),
                    menu.get("route"),
                    menu.get("icon"),
                    menu["sort_order"],
                    menu["permission_code"],
                    bool(menu.get("is_active", True)),
                    menu.get("hide_menu"),
                    now,
                    now,
                    menu["menu_key"],
                ],
            )
        elif has_hide_menu and not has_created_at:
            cursor.execute(
                f"""
                INSERT INTO {menus_table}
                (menu_key, title, parent_key, route, icon, sort_order, permission_code, is_active, hide_menu, updated_at)
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM {menus_table} WHERE LOWER(menu_key) = LOWER(?)
                )
                """,
                [
                    menu["menu_key"],
                    menu["title"],
                    menu.get("parent_key"),
                    menu.get("route"),
                    menu.get("icon"),
                    menu["sort_order"],
                    menu["permission_code"],
                    bool(menu.get("is_active", True)),
                    menu.get("hide_menu"),
                    now,
                    menu["menu_key"],
                ],
            )
        elif (not has_hide_menu) and has_created_at:
            cursor.execute(
                f"""
                INSERT INTO {menus_table}
                (menu_key, title, parent_key, route, icon, sort_order, permission_code, is_active, created_at, updated_at)
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM {menus_table} WHERE LOWER(menu_key) = LOWER(?)
                )
                """,
                [
                    menu["menu_key"],
                    menu["title"],
                    menu.get("parent_key"),
                    menu.get("route"),
                    menu.get("icon"),
                    menu["sort_order"],
                    menu["permission_code"],
                    bool(menu.get("is_active", True)),
                    now,
                    now,
                    menu["menu_key"],
                ],
            )
        else:
            cursor.execute(
                f"""
                INSERT INTO {menus_table}
                (menu_key, title, parent_key, route, icon, sort_order, permission_code, is_active, updated_at)
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM {menus_table} WHERE LOWER(menu_key) = LOWER(?)
                )
                """,
                [
                    menu["menu_key"],
                    menu["title"],
                    menu.get("parent_key"),
                    menu.get("route"),
                    menu.get("icon"),
                    menu["sort_order"],
                    menu["permission_code"],
                    bool(menu.get("is_active", True)),
                    now,
                    menu["menu_key"],
                ],
            )
        cursor.execute(
            f"SELECT 1 FROM {menus_table} WHERE LOWER(menu_key) = LOWER(?) LIMIT 1",
            [menu["menu_key"]],
        )
        return cursor.fetchone() is not None


def list_available_roles(connection) -> list[str]:
    catalog = get_settings().databricks.catalog
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT DISTINCT LOWER(role_name) AS role_name
            FROM {catalog}.auth.roles
            WHERE COALESCE(is_deleted, false) = false
              AND COALESCE(is_active, true) = true
            ORDER BY role_name
            """,
        )
        rows = cursor.fetchall() or []
    return [str(row[0]).strip().lower() for row in rows if row and row[0]]


def list_available_permissions(connection) -> list[str]:
    catalog = get_settings().databricks.catalog
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT DISTINCT LOWER(permission_name) AS permission_name
            FROM {catalog}.auth.permissions
            WHERE COALESCE(is_deleted, false) = false
              AND COALESCE(is_active, true) = true
            ORDER BY permission_name
            """,
        )
        rows = cursor.fetchall() or []
    return [str(row[0]).strip().lower() for row in rows if row and row[0]]


def update_menu(connection, *, menu_key: str, updates: dict[str, object]) -> bool:
    menus_table = _menu_table()
    normalized = (menu_key or "").strip().lower()
    if not normalized:
        return False

    has_hide_menu = _menu_table_has_column(connection, column_name="hide_menu")
    allowed = ("menu_key", "title", "parent_key", "route", "icon", "sort_order", "permission_code", "is_active")
    if has_hide_menu:
        allowed = (*allowed, "hide_menu")
    fields: list[str] = []
    params: list[object] = []
    for key in allowed:
        if key in updates:
            fields.append(f"{key} = ?")
            params.append(updates[key])
    if not fields:
        return False

    fields.append("updated_at = ?")
    params.append(datetime.now(tz=timezone.utc))
    params.append(normalized)

    sql = f"UPDATE {menus_table} SET {', '.join(fields)} WHERE LOWER(menu_key) = LOWER(?)"
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
    return True


def deactivate_menu(connection, *, menu_key: str) -> bool:
    return update_menu(connection, menu_key=menu_key, updates={"is_active": False})


def update_menu_orders(connection, *, updates: list[tuple[str, int]]) -> bool:
    """Cập nhật thứ tự sắp xếp cho nhiều menu cùng lúc."""
    if not updates:
        return True
    menus_table = _menu_table()
    sql = f"UPDATE {menus_table} SET sort_order = ?, updated_at = ? WHERE LOWER(menu_key) = LOWER(?)"
    now = datetime.now(timezone.utc)
    # updates contains (key, sort_order)
    # SQL params need (sort_order, updated_at, menu_key)
    batch = [(sort_order, now, key.strip().lower()) for key, sort_order in updates]
    with connection.cursor() as cursor:
        cursor.executemany(sql, batch)
    return True

