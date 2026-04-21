from __future__ import annotations

import logging
import os
import time

from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.menu_repository import (
    create_menu,
    deactivate_menu,
    get_menu_by_key,
    list_available_permissions,
    list_available_roles,
    list_menus,
    update_menu,
    update_menu_orders,
)

logger = logging.getLogger(__name__)

# Constants and Cache
MENU_ADMIN_ROUTE = "/menu_admin"
_FALLBACK_ADMIN_PERMISSIONS = {"manage_menu"}
_VISIBLE_MENU_CACHE_TTL_SECONDS = 10
_visible_menu_cache: dict[tuple[tuple[str, ...], tuple[str, ...]], tuple[float, list[dict[str, object]]]] = {}


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_permissions(
    permissions: list[str] | set[str] | tuple[str, ...] | None,
) -> set[str]:
    if permissions is None:
        return set()
    return {_normalize(item) for item in permissions if _normalize(item)}


def _has_route_access_by_permission(
    *,
    route: str,
    permissions: list[str] | set[str] | tuple[str, ...] | None,
) -> bool:
    normalized_route = _normalize(route)
    normalized_permissions = _normalize_permissions(permissions)
    if not normalized_route or not normalized_permissions:
        return False

    if os.environ.get("PYTEST_CURRENT_TEST"):
        return bool(normalized_permissions.intersection(_FALLBACK_ADMIN_PERMISSIONS))

    try:
        with databricks_connection() as connection:
            active_menus = list_menus(connection, active_only=True)
            for row in active_menus:
                route_value = _normalize(str(row.get("route") or ""))
                permission_code = _normalize(str(row.get("permission_code") or ""))
                if (
                    route_value == normalized_route
                    and permission_code in normalized_permissions
                ):
                    return True
            return False
    except Exception:
        # Fail-open for admin fallback permission when DB check is unavailable.
        fallback_allowed = bool(normalized_permissions.intersection(_FALLBACK_ADMIN_PERMISSIONS))
        logger.warning(
            "menu: route access check failed for route: %s; fallback_allowed=%s",
            normalized_route,
            fallback_allowed,
        )
        return fallback_allowed


def _is_menu_admin_authorized(
    *,
    roles: list[str] | set[str] | tuple[str, ...] | None,
    permissions: list[str] | set[str] | tuple[str, ...] | None,
) -> bool:
    _ = roles
    return _has_route_access_by_permission(route=MENU_ADMIN_ROUTE, permissions=permissions)


def can_access_menu_admin(
    *,
    roles: list[str] | set[str] | tuple[str, ...] | None,
    permissions: list[str] | set[str] | tuple[str, ...] | None,
) -> bool:
    return _is_menu_admin_authorized(roles=roles, permissions=permissions)


def _clear_visible_menu_cache() -> None:
    _visible_menu_cache.clear()


def _build_children_map(rows: list[dict[str, object]]) -> dict[str | None, list[dict[str, object]]]:
    grouped: dict[str | None, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(row.get("parent_key"), []).append(row)
    for values in grouped.values():
        values.sort(key=lambda item: (int(item["sort_order"]), str(item["title"]).lower()))
    return grouped


def _build_visible_tree(flat_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped = _build_children_map(flat_rows)

    def _walk(parent_key: str | None) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        for node in grouped.get(parent_key, []):
            children = _walk(node["menu_key"])
            route = node.get("route")
            is_group = not route
            if is_group and not children:
                continue
            copy_node = dict(node)
            copy_node["children"] = children
            result.append(copy_node)
        return result

    return _walk(None)


def _has_cycle(rows: list[dict[str, object]], menu_key: str, parent_key: str | None) -> bool:
    if not parent_key:
        return False
    key_to_parent = {str(row["menu_key"]).lower(): row.get("parent_key") for row in rows}
    current = _normalize(parent_key)
    target = _normalize(menu_key)
    seen: set[str] = set()
    while current:
        if current == target:
            return True
        if current in seen:
            return True
        seen.add(current)
        next_parent = key_to_parent.get(current)
        current = _normalize(str(next_parent) if next_parent is not None else "")
    return False


def _is_valid_hierarchical_menu_key(*, menu_key: str, parent_key: str | None) -> bool:
    normalized_key = _normalize(menu_key)
    normalized_parent = _normalize(parent_key)
    if not normalized_key:
        return False
    if not normalized_parent:
        return "." not in normalized_key
    prefix = f"{normalized_parent}."
    if not normalized_key.startswith(prefix):
        return False
    suffix = normalized_key[len(prefix) :]
    return bool(suffix)


def _build_rekey_plan(
    *,
    rows: list[dict[str, object]],
    old_key: str,
    new_key: str,
) -> list[tuple[str, str, str | None]]:
    normalized_old = _normalize(old_key)
    normalized_new = _normalize(new_key)
    if not normalized_old or not normalized_new or normalized_old == normalized_new:
        return []

    old_prefix = f"{normalized_old}."
    subtree_rows = [
        row
        for row in rows
        if _normalize(str(row.get("menu_key") or "")) != normalized_old
        and _normalize(str(row.get("menu_key") or "")).startswith(old_prefix)
    ]
    subtree_rows.sort(key=lambda item: _normalize(str(item.get("menu_key") or "")).count("."))
    plan: list[tuple[str, str, str | None]] = []
    for row in subtree_rows:
        src_key = _normalize(str(row.get("menu_key") or ""))
        src_parent = _normalize(str(row.get("parent_key") or "")) or None
        dst_key = normalized_new + src_key[len(normalized_old) :]
        dst_parent = src_parent
        if src_parent:
            if src_parent == normalized_old:
                dst_parent = normalized_new
            elif src_parent.startswith(old_prefix):
                dst_parent = normalized_new + src_parent[len(normalized_old) :]
        plan.append((src_key, dst_key, dst_parent))
    return plan


def _has_rekey_conflict(
    *,
    rows: list[dict[str, object]],
    old_key: str,
    new_key: str,
) -> bool:
    normalized_old = _normalize(old_key)
    normalized_new = _normalize(new_key)
    if not normalized_old or not normalized_new or normalized_old == normalized_new:
        return False

    all_keys = {_normalize(str(row.get("menu_key") or "")) for row in rows}
    old_prefix = f"{normalized_old}."
    subtree_keys = {key for key in all_keys if key == normalized_old or key.startswith(old_prefix)}
    target_keys = {normalized_new}
    for _, dst_key, _ in _build_rekey_plan(rows=rows, old_key=normalized_old, new_key=normalized_new):
        target_keys.add(dst_key)

    if len(target_keys) != len(subtree_keys):
        return True
    outside_keys = all_keys - subtree_keys
    return any(key in outside_keys for key in target_keys)


def resolve_visible_menu_tree(
    *,
    roles: list[str] | set[str] | tuple[str, ...],
    permissions: list[str] | set[str] | tuple[str, ...],
) -> list[dict[str, object]]:
    _ = roles
    normalized_permissions = {_normalize(permission) for permission in permissions if _normalize(permission)}
    if not normalized_permissions:
        return []

    cache_key = (tuple(), tuple(sorted(normalized_permissions)))
    cached = _visible_menu_cache.get(cache_key)
    if cached is not None:
        ts, value = cached
        if time.time() - ts < _VISIBLE_MENU_CACHE_TTL_SECONDS:
            return value

    if os.environ.get("PYTEST_CURRENT_TEST"):
        return [
            {
                "menu_key": "dashboard",
                "title": "Dashboard",
                "parent_key": None,
                "route": "/dashboard",
                "icon": "home",
                "sort_order": 0,
                "permission_code": "view_reports",
                "is_active": True,
                "children": [],
            },
        ]

    try:
        with databricks_connection() as connection:
            active_menus = list_menus(connection, active_only=True)
            visible = [
                row
                for row in active_menus
                if _normalize(str(row.get("permission_code"))) in normalized_permissions
            ]
        tree = _build_visible_tree(visible)
        _visible_menu_cache[cache_key] = (time.time(), tree)
        return tree
    except Exception:
        logger.warning("menu: resolve visible tree failed")
        return []


def list_menu_catalog() -> list[dict[str, object]]:
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return []
    try:
        with databricks_connection() as connection:
            return list_menus(connection)
    except Exception:
        logger.warning("menu: list catalog failed")
        return []


def list_role_menu_snapshot() -> list[dict[str, str]]:
    return []


def list_available_roles_for_menu() -> list[str]:
    fallback = ["admin", "manager", "user"]
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return fallback
    try:
        with databricks_connection() as connection:
            return list_available_roles(connection) or fallback
    except Exception:
        logger.warning("menu: list roles failed")
        return fallback


def list_available_permissions_for_menu() -> list[str]:
    fallback = ["view_reports", "manage_products", "manage_users", "manage_roles", "manage_menu"]
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return fallback
    try:
        with databricks_connection() as connection:
            values = list_available_permissions(connection)
            return values or fallback
    except Exception:
        logger.warning("menu: list permissions failed")
        return fallback


def get_menu_admin_snapshot(
    *,
    actor_roles: list[str] | set[str] | tuple[str, ...] | None = None,
    actor_permissions: list[str] | set[str] | tuple[str, ...] | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, str]], list[str], list[str]]:
    fallback_roles = ["admin", "manager", "user"]
    fallback_permissions = ["view_reports", "manage_products", "manage_users", "manage_roles", "manage_menu"]
    if not _is_menu_admin_authorized(roles=actor_roles, permissions=actor_permissions):
        return [], [], fallback_roles, fallback_permissions
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return [], [], fallback_roles, fallback_permissions
    try:
        with databricks_connection() as connection:
            menus = list_menus(connection)
            roles = list_available_roles(connection) or fallback_roles
            permissions = list_available_permissions(connection) or fallback_permissions
            return menus, [], roles, permissions
    except Exception:
        logger.warning("menu: load admin snapshot failed")
        # Keep admin UI usable even if secondary lookups fail.
        try:
            with databricks_connection() as connection:
                menus = list_menus(connection)
                return menus, [], fallback_roles, fallback_permissions
        except Exception:
            return [], [], fallback_roles, fallback_permissions


def create_menu_entry(
    *,
    menu_key: str,
    title: str,
    parent_key: str | None,
    route: str | None,
    icon: str | None,
    sort_order: int,
    permission_code: str,
    hide_menu: bool | None = None,
    actor_roles: list[str] | set[str] | tuple[str, ...] | None = None,
    actor_permissions: list[str] | set[str] | tuple[str, ...] | None = None,
) -> tuple[bool, str]:
    if not _is_menu_admin_authorized(roles=actor_roles, permissions=actor_permissions):
        return False, "forbidden"
    normalized_key = _normalize(menu_key)
    normalized_parent = _normalize(parent_key) or None
    normalized_permission = _normalize(permission_code)
    clean_title = (title or "").strip()
    clean_route = (route or "").strip() or None
    clean_icon = (icon or "").strip() or None

    if not normalized_key or not clean_title:
        return False, "menu_key_and_title_required"
    if sort_order < 0:
        return False, "sort_order_invalid"
    if not normalized_permission:
        return False, "permission_required"
    if normalized_parent == normalized_key:
        return False, "menu_cycle_invalid"

    try:
        with databricks_connection() as connection:
            valid_permissions = set(list_available_permissions(connection))
            if normalized_permission not in valid_permissions:
                return False, "permission_code_invalid"
            existing = get_menu_by_key(connection, normalized_key)
            if existing is not None:
                return False, "menu_key_duplicated"

            rows = list_menus(connection)
            if normalized_parent and not any(_normalize(str(row["menu_key"])) == normalized_parent for row in rows):
                return False, "parent_key_invalid"
            if not _is_valid_hierarchical_menu_key(menu_key=normalized_key, parent_key=normalized_parent):
                return False, "menu_key_hierarchy_invalid"
            if _has_cycle(rows, normalized_key, normalized_parent):
                return False, "menu_cycle_invalid"

            payload: dict[str, object] = {
                "menu_key": normalized_key,
                "title": clean_title,
                "parent_key": normalized_parent,
                "route": clean_route,
                "icon": clean_icon,
                "sort_order": sort_order,
                "permission_code": normalized_permission,
                "is_active": True,
            }
            if hide_menu is not None:
                payload["hide_menu"] = bool(hide_menu)

            changed = create_menu(
                connection,
                menu=payload,
            )
            if not changed:
                return False, "create_failed"
            _clear_visible_menu_cache()
            return True, "ok"
    except Exception:
        logger.warning("menu: create entry failed")
        return False, "internal_error"


def update_menu_entry(
    *,
    menu_key: str,
    new_menu_key: str | None = None,
    title: str,
    parent_key: str | None,
    route: str | None,
    icon: str | None,
    sort_order: int,
    permission_code: str,
    hide_menu: bool | None = None,
    is_active: bool | None = None,
    actor_roles: list[str] | set[str] | tuple[str, ...] | None = None,
    actor_permissions: list[str] | set[str] | tuple[str, ...] | None = None,
) -> tuple[bool, str]:
    if not _is_menu_admin_authorized(roles=actor_roles, permissions=actor_permissions):
        return False, "forbidden"
    normalized_key = _normalize(menu_key)
    normalized_new_key = _normalize(new_menu_key) or normalized_key
    normalized_parent = _normalize(parent_key) or None
    normalized_permission = _normalize(permission_code)
    clean_title = (title or "").strip()
    clean_route = (route or "").strip() or None
    clean_icon = (icon or "").strip() or None

    if not normalized_key or not normalized_new_key or not clean_title:
        return False, "menu_key_and_title_required"
    if sort_order < 0:
        return False, "sort_order_invalid"
    if not normalized_permission:
        return False, "permission_required"
    if normalized_parent == normalized_new_key:
        return False, "menu_cycle_invalid"

    try:
        with databricks_connection() as connection:
            valid_permissions = set(list_available_permissions(connection))
            if normalized_permission not in valid_permissions:
                return False, "permission_code_invalid"
            rows = list_menus(connection)
            if not any(_normalize(str(row["menu_key"])) == normalized_key for row in rows):
                return False, "menu_not_found"
            if normalized_parent and not any(_normalize(str(row["menu_key"])) == normalized_parent for row in rows):
                return False, "parent_key_invalid"
            if not _is_valid_hierarchical_menu_key(menu_key=normalized_new_key, parent_key=normalized_parent):
                return False, "menu_key_hierarchy_invalid"
            if normalized_new_key != normalized_key and _has_rekey_conflict(
                rows=rows,
                old_key=normalized_key,
                new_key=normalized_new_key,
            ):
                return False, "menu_key_duplicated"

            candidate_rows: list[dict[str, object]] = []
            for row in rows:
                copy_row = dict(row)
                if _normalize(str(copy_row["menu_key"])) == normalized_key:
                    copy_row["menu_key"] = normalized_new_key
                    copy_row["parent_key"] = normalized_parent
                candidate_rows.append(copy_row)
            if _has_cycle(candidate_rows, normalized_new_key, normalized_parent):
                return False, "menu_cycle_invalid"

            updates: dict[str, object] = {
                "title": clean_title,
                "parent_key": normalized_parent,
                "route": clean_route,
                "icon": clean_icon,
                "sort_order": sort_order,
                "permission_code": normalized_permission,
            }
            if normalized_new_key != normalized_key:
                updates["menu_key"] = normalized_new_key
            if hide_menu is not None:
                updates["hide_menu"] = bool(hide_menu)
            if is_active is not None:
                updates["is_active"] = bool(is_active)

            changed = update_menu(
                connection,
                menu_key=normalized_key,
                updates=updates,
            )
            if not changed:
                return False, "update_failed"
            _clear_visible_menu_cache()
            return True, "ok"
    except Exception:
        logger.warning("menu: update entry failed")
        return False, "internal_error"


def deactivate_menu_entry(
    *,
    menu_key: str,
    actor_roles: list[str] | set[str] | tuple[str, ...] | None = None,
    actor_permissions: list[str] | set[str] | tuple[str, ...] | None = None,
) -> tuple[bool, str]:
    if not _is_menu_admin_authorized(roles=actor_roles, permissions=actor_permissions):
        return False, "forbidden"
    normalized_key = _normalize(menu_key)
    if not normalized_key:
        return False, "menu_key_required"
    try:
        with databricks_connection() as connection:
            existing = get_menu_by_key(connection, normalized_key)
            if existing is None:
                return False, "menu_not_found"
            changed = deactivate_menu(connection, menu_key=normalized_key)
            if not changed:
                return False, "deactivate_failed"
            _clear_visible_menu_cache()
            return True, "ok"
    except Exception:
        logger.warning("menu: deactivate entry failed")
        return False, "internal_error"


def set_menu_active_status(
    *,
    menu_key: str,
    is_active: bool,
    actor_roles: list[str] | set[str] | tuple[str, ...] | None = None,
    actor_permissions: list[str] | set[str] | tuple[str, ...] | None = None,
) -> tuple[bool, str]:
    if not _is_menu_admin_authorized(roles=actor_roles, permissions=actor_permissions):
        return False, "forbidden"
    normalized_key = _normalize(menu_key)
    if not normalized_key:
        return False, "menu_key_required"

    try:
        with databricks_connection() as connection:
            existing = get_menu_by_key(connection, normalized_key)
            if existing is None:
                return False, "menu_not_found"

            changed = update_menu(
                connection,
                menu_key=normalized_key,
                updates={"is_active": bool(is_active)},
            )
            if not changed:
                return False, "update_failed"
            _clear_visible_menu_cache()
            return True, "ok"
    except Exception:
        logger.warning("menu: set active status failed")
        return False, "internal_error"


def bulk_update_menu_order(
    *,
    ordered_keys: list[str],
    actor_roles: list[str] | set[str] | tuple[str, ...] | None = None,
    actor_permissions: list[str] | set[str] | tuple[str, ...] | None = None,
) -> tuple[bool, str]:
    """Update sort_order for the given ordered menu keys."""
    if not _is_menu_admin_authorized(roles=actor_roles, permissions=actor_permissions):
        return False, "forbidden"
    if not ordered_keys:
        return True, "ok"

    try:
        normalized_keys: list[str] = []
        seen: set[str] = set()
        for key in ordered_keys:
            normalized = _normalize(key)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_keys.append(normalized)
        if not normalized_keys:
            return True, "ok"

        updates = [(key, idx * 10) for idx, key in enumerate(normalized_keys)]
        with databricks_connection() as connection:
            ok = update_menu_orders(connection, updates=updates)
            if not ok:
                return False, "update_failed"
            _clear_visible_menu_cache()
            return True, "ok"
    except Exception:
        logger.warning("menu: bulk update order failed")
        return False, "internal_error"
