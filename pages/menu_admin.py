from __future__ import annotations

import html
import inspect

import streamlit as st

from src.ui.components.header import render_dashboard_header
from src.ui.components.navbar import render_top_navbar
from src.ui.components.sidebar import render_app_sidebar
from src.application.menu.menu_service import (
    bulk_update_menu_order,
    can_access_menu_admin,
    create_menu_entry,
    get_menu_admin_snapshot,
    list_available_permissions_for_menu,
    list_menu_catalog,
    resolve_visible_menu_tree,
    set_menu_active_status,
    update_menu_entry,
)
from src.ui.session.auth_session import (
    KEY_PERMISSIONS,
    KEY_REMEMBER_ME,
    get_current_display_name,
    get_current_username,
    require_auth,
)
from src.ui.session.browser_storage import sync_auth_to_browser_storage
from src.ui.styles.loader import inject_css, sync_theme_mode

st.set_page_config(
    page_title="Quản lý menu",
    page_icon=":card_file_box:",
    layout="wide",
    initial_sidebar_state="expanded",
)

_FLASH_MESSAGE_KEY = "menu_admin_flash_message"
_FLASH_LEVEL_KEY = "menu_admin_flash_level"
_DIALOG_OPEN_KEY = "menu_admin_dialog_open"
_DIALOG_MODE_KEY = "menu_admin_dialog_mode"
_DIALOG_TARGET_KEY = "menu_admin_dialog_target"
_DIALOG_CONTEXT_KEY = "menu_admin_dialog_context"
_CATALOG_PAGE_KEY = "menu_catalog_page"
_CATALOG_SEARCH_KEY = "menu_catalog_search_title"
_CATALOG_SEARCH_PREVIOUS_KEY = "menu_catalog_search_title_previous"
_CATALOG_PAGE_SIZE = 10
_TREE_SELECTED_KEY = "menu_admin_tree_selected_key"


def _set_flash(message: str, level: str = "info") -> None:
    st.session_state[_FLASH_MESSAGE_KEY] = message
    st.session_state[_FLASH_LEVEL_KEY] = level


def _render_flash() -> None:
    message = st.session_state.pop(_FLASH_MESSAGE_KEY, None)
    level = st.session_state.pop(_FLASH_LEVEL_KEY, "info")
    if not message:
        return
    if level == "success":
        st.success(message)
        return
    if level == "error":
        st.error(message)
        return
    st.info(message)


def _open_dialog(mode: str, menu_key: str | None = None) -> None:
    st.session_state[_DIALOG_OPEN_KEY] = True
    st.session_state[_DIALOG_MODE_KEY] = mode
    st.session_state[_DIALOG_TARGET_KEY] = (menu_key or "").strip().lower()
    st.session_state[_DIALOG_CONTEXT_KEY] = ""


def _close_dialog() -> None:
    st.session_state[_DIALOG_OPEN_KEY] = False
    st.session_state[_DIALOG_MODE_KEY] = ""
    st.session_state[_DIALOG_TARGET_KEY] = ""
    st.session_state[_DIALOG_CONTEXT_KEY] = ""


def _reset_menu_form_defaults(permissions: list[str], *, parent_key: str | None = None) -> None:
    st.session_state["menu_form_key"] = ""
    st.session_state["menu_form_title"] = ""
    st.session_state["menu_form_parent"] = _normalize_menu_key(parent_key)
    st.session_state["menu_form_route"] = ""
    st.session_state["menu_form_icon"] = ""
    st.session_state["menu_form_display_position"] = 1
    st.session_state["menu_form_permission_name"] = (permissions or ["view_reports"])[0]
    st.session_state["menu_form_hide_menu"] = False
    st.session_state["menu_form_deactivate"] = False


def _sync_menu_form_from_selected_menu(menu: dict[str, object]) -> None:
    st.session_state["menu_form_key"] = str(menu.get("menu_key") or "")
    st.session_state["menu_form_title"] = str(menu.get("title") or "")
    st.session_state["menu_form_parent"] = str(menu.get("parent_key") or "")
    st.session_state["menu_form_route"] = str(menu.get("route") or "")
    st.session_state["menu_form_icon"] = str(menu.get("icon") or "")
    # Chuyển đổi sort_order sang vị trí
    raw_sort = int(menu.get("sort_order") or 0)
    st.session_state["menu_form_display_position"] = (raw_sort // 10) + 1
    st.session_state["menu_form_permission_name"] = str(menu.get("permission_code") or "")
    st.session_state["menu_form_hide_menu"] = bool(menu.get("hide_menu"))
    st.session_state["menu_form_deactivate"] = not bool(menu.get("is_active", True))


def _ensure_dialog_form_state(
    *,
    mode: str,
    target_key: str,
    menu_map: dict[str, dict[str, object]],
    permissions: list[str],
) -> None:
    context = f"{mode}:{target_key}"
    if str(st.session_state.get(_DIALOG_CONTEXT_KEY, "")) == context:
        return

    if mode == "create":
        parent_key = _normalize_menu_key(str(st.session_state.get("menu_form_parent", "") or ""))
        _reset_menu_form_defaults(permissions, parent_key=parent_key or None)
    elif mode == "edit":
        selected = menu_map.get(target_key)
        if selected:
            _sync_menu_form_from_selected_menu(selected)
    st.session_state[_DIALOG_CONTEXT_KEY] = context
    st.session_state["menu_admin_upsert_loading"] = False


def _normalize_menu_key(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_search_text(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())


def _menu_key_sort_tuple(value: str) -> tuple[object, ...]:
    parts = [segment.strip() for segment in str(value).split(".")]
    result: list[object] = []
    for part in parts:
        if part.isdigit():
            result.append((0, int(part)))
        else:
            result.append((1, part.lower()))
    return tuple(result)


def _next_menu_key_for_parent(
    *,
    menus: list[dict[str, object]],
    parent_key: str | None,
    excluded_prefixes: tuple[str, ...] = (),
) -> str:
    normalized_parent = _normalize_menu_key(parent_key) or None
    excluded = tuple(_normalize_menu_key(item) for item in excluded_prefixes if _normalize_menu_key(item))

    def _is_excluded(key: str) -> bool:
        for prefix in excluded:
            if key == prefix or key.startswith(f"{prefix}."):
                return True
        return False

    keys = [
        _normalize_menu_key(str(item.get("menu_key") or ""))
        for item in menus
        if _normalize_menu_key(str(item.get("menu_key") or ""))
    ]
    keys = [key for key in keys if not _is_excluded(key)]

    if not normalized_parent:
        root_numbers = [int(key) for key in keys if "." not in key and key.isdigit()]
        next_number = (max(root_numbers) + 1) if root_numbers else 1
        return str(next_number)

    prefix = f"{normalized_parent}."
    child_numbers: list[int] = []
    for key in keys:
        if not key.startswith(prefix):
            continue
        suffix = key[len(prefix) :]
        if suffix.isdigit():
            child_numbers.append(int(suffix))
    next_child = (max(child_numbers) + 1) if child_numbers else 1
    return f"{normalized_parent}.{next_child}"


def _sync_auto_menu_key(
    *,
    mode: str,
    target_key: str,
    menus: list[dict[str, object]],
) -> str:
    selected_parent = _normalize_menu_key(str(st.session_state.get("menu_form_parent", "") or "")) or None
    if mode == "create":
        auto_key = _next_menu_key_for_parent(menus=menus, parent_key=selected_parent)
        st.session_state["menu_form_key"] = auto_key
        return auto_key

    selected = _normalize_menu_key(target_key)
    original_parent = ""
    for item in menus:
        if _normalize_menu_key(str(item.get("menu_key") or "")) == selected:
            original_parent = _normalize_menu_key(str(item.get("parent_key") or ""))
            break

    if selected_parent == (original_parent or None):
        st.session_state["menu_form_key"] = selected
        return selected

    auto_key = _next_menu_key_for_parent(
        menus=menus,
        parent_key=selected_parent,
        excluded_prefixes=(selected,),
    )
    st.session_state["menu_form_key"] = auto_key
    return auto_key


@st.cache_data(ttl=10, show_spinner=False)
def _load_admin_snapshot_cached(
    actor_permissions: tuple[str, ...],
) -> tuple[list[dict[str, object]], list[dict[str, str]], list[str], list[str]]:
    if "menu_admin_tree_version" not in st.session_state:
        st.session_state["menu_admin_tree_version"] = 0
    return get_menu_admin_snapshot(actor_permissions=actor_permissions)


@st.dialog("Chi tiết Menu", width="large")
def _render_menu_upsert_dialog(
    *,
    mode: str,
    target_key: str | None,
    actor_permissions: tuple[str, ...],
    permissions: list[str],
    menus: list[dict[str, object]],
) -> None:
    menu_title_map = {
        _normalize_menu_key(str(item.get("menu_key") or "")): str(item.get("title") or "")
        for item in menus
        if item.get("menu_key")
    }
    all_keys = sorted(
        {_normalize_menu_key(str(item.get("menu_key") or "")) for item in menus if item.get("menu_key")},
        key=_menu_key_sort_tuple,
    )
    parent_options = [""] + [
        key for key in all_keys
        if not (mode == "edit" and key == _normalize_menu_key(target_key))
    ]
    if str(st.session_state.get("menu_form_parent", "")) not in parent_options:
        st.session_state["menu_form_parent"] = ""

    menu_key = _sync_auto_menu_key(mode=mode, target_key=target_key, menus=menus)

    is_loading = bool(st.session_state.get("menu_admin_upsert_loading", False))
    
    title_value = st.text_input("Tên menu", key="menu_form_title", disabled=is_loading)

    col_c, col_d = st.columns(2)
    with col_c:
        selected_parent = st.selectbox(
            "Menu cha (Key)",
            options=parent_options,
            key="menu_form_parent",
            disabled=is_loading,
            format_func=lambda item: (
                "Không có menu cha"
                if item == ""
                else (menu_title_map.get(item) or item)
            ),
        )
    with col_d:
        route = st.text_input("Đường dẫn", key="menu_form_route", placeholder="/example_page", disabled=is_loading)

    col_e, col_f, col_g = st.columns([1.1, 0.9, 1.4])
    with col_e:
        icon = st.text_input("Biểu tượng", key="menu_form_icon", disabled=is_loading)
    with col_f:
        st.number_input(
            "Vị trí hiển thị (1, 2, 3...)",
            min_value=1,
            step=1,
            key="menu_form_display_position",
            help="Vị trí 1 là trên cùng. Hệ thống sẽ tự quy đổi sang sort_order (vị trí - 1) * 10",
            disabled=is_loading
        )
    with col_g:
        permission_name = st.selectbox(
            "Permission",
            options=permissions or ["view_reports"],
            key="menu_form_permission_name",
            disabled=is_loading,
        )
        st.caption("Nếu chưa có permission, click vào đây để tạo:")
        st.page_link("pages/admin.py", label="Đi tới trang tạo permission", icon="🔗", disabled=is_loading)

    col_h, col_i = st.columns(2)
    with col_h:
        hide_menu = st.checkbox(
            "Ẩn menu trên trang này",
            key="menu_form_hide_menu",
            disabled=is_loading,
        )

    is_active = True
    with col_i:
        if mode == "edit":
            deactivate_menu = st.checkbox(
                "Ngừng sử dụng menu",
                key="menu_form_deactivate",
                disabled=is_loading,
            )
            is_active = not deactivate_menu

    action_placeholder = st.empty()
    with action_placeholder.container():
        action_col, close_col = st.columns([1.3, 1.0])
        with action_col:
            submit_label = "Tạo menu" if mode == "create" else "Cập nhật menu"
            if st.button(submit_label, use_container_width=True, type="primary", key="menu_upsert_real_btn"):
                # Thay thế nội dung col bằng loading ngay lập tức
                action_placeholder.empty()
                with action_placeholder.container():
                    st.button("⌛ Đang xử lý...", disabled=True, use_container_width=True)
                    with st.spinner("Đang cập nhật hệ thống..."):
                        display_pos = int(st.session_state.get("menu_form_display_position", 1))
                        calculated_sort_order = (display_pos - 1) * 10
                        if mode == "create":
                            ok, reason = create_menu_entry(
                                menu_key=menu_key, title=title_value, parent_key=selected_parent or None,
                                route=route or None, icon=icon or None, sort_order=calculated_sort_order,
                                permission_code=permission_name, hide_menu=hide_menu, actor_permissions=actor_permissions,
                            )
                        else:
                            ok, reason = update_menu_entry(
                                menu_key=target_key, new_menu_key=menu_key, title=title_value, parent_key=selected_parent or None,
                                route=route or None, icon=icon or None, sort_order=calculated_sort_order,
                                permission_code=permission_name, hide_menu=hide_menu, is_active=is_active, actor_permissions=actor_permissions,
                            )
                        
                        if ok:
                            st.session_state.pop("menu_admin_sac_tree_fixed", None)
                            st.session_state["menu_admin_tree_version"] += 1
                            _load_admin_snapshot_cached.clear()
                            _set_flash(f"{'Tạo' if mode=='create' else 'Cập nhật'} menu thành công.", "success")
                            _close_dialog()
                            st.rerun()
                        else:
                            st.error(f"Lỗi: {reason}")
                            # Quay lại trạng thái bình thường nếu lỗi
                            st.button("Thử lại", use_container_width=True)

        with close_col:
            if st.button("Đóng", use_container_width=True, key="menu_upsert_close_btn"):
                _close_dialog()
                st.rerun()
@st.dialog("Xác nhận trạng thái")
def _render_toggle_active_dialog(
    *,
    target_key: str,
    target_title: str,
    current_active: bool,
    route: str | None,
    actor_permissions: tuple[str, ...],
) -> None:
    next_active = not bool(current_active)
    if next_active:
        st.info(f"Bạn có chắc muốn tiếp tục sử dụng menu '{target_title}' không?")
    else:
        st.warning(f"Bạn có chắc muốn ngừng sử dụng menu '{target_title}' không?")
    action_placeholder = st.empty()
    with action_placeholder.container():
        confirm_col, cancel_col = st.columns(2)
        with confirm_col:
            submit_text = "Tiếp tục sử dụng" if next_active else "Ngừng sử dụng"
            if st.button(submit_text, key="menu_toggle_active_confirm", use_container_width=True, type="primary"):
                action_placeholder.empty()
                with action_placeholder.container():
                    st.button("⌛ Đang xử lý...", disabled=True, use_container_width=True)
                    with st.spinner("Đang cập nhật trạng thái..."):
                        ok, reason = set_menu_active_status(
                            menu_key=target_key,
                            is_active=next_active,
                            actor_permissions=actor_permissions,
                        )
                        if ok:
                            st.session_state.pop("menu_admin_sac_tree_fixed", None)
                            st.session_state["menu_admin_tree_version"] += 1
                            _load_admin_snapshot_cached.clear()
                            success_text = "Tiếp tục sử dụng menu thành công." if next_active else "Ngừng sử dụng menu thành công."
                            _set_flash(success_text, "success")
                            _close_dialog()
                            st.rerun()
                        else:
                            st.error(f"Lỗi: {reason}")
                            st.button("Quay lại", use_container_width=True)
                            
        with cancel_col:
            if st.button("Hủy", key="menu_toggle_active_cancel", use_container_width=True):
                _close_dialog()
                st.rerun()


def _build_menu_tree_structure(menus: list[dict[str, object]]) -> list[dict[str, object]]:
    node_map = {
        _normalize_menu_key(str(item.get("menu_key") or "")): {**item, "children": []}
        for item in menus
        if _normalize_menu_key(str(item.get("menu_key") or ""))
    }
    roots: list[dict[str, object]] = []

    def _sort_tuple(item: dict[str, object]) -> tuple[object, ...]:
        try:
            sort_order = int(item.get("sort_order") or 0)
        except (TypeError, ValueError):
            sort_order = 0
        return (
            sort_order,
            str(item.get("title") or "").strip().lower(),
            _normalize_menu_key(str(item.get("menu_key") or "")),
        )

    for key in sorted(node_map.keys(), key=lambda k: _sort_tuple(node_map[k])):
        node = node_map[key]
        parent = _normalize_menu_key(str(node.get("parent_key") or ""))
        if parent and parent in node_map and parent != key:
            node_map[parent]["children"].append(node)
        else:
            roots.append(node)

    def _sort_children(items: list[dict[str, object]]) -> None:
        items.sort(key=_sort_tuple)
        for item in items:
            children = item.get("children")
            if isinstance(children, list) and children:
                _sort_children(children)

    _sort_children(roots)
    return roots


def _build_tree_lookup(nodes: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    lookup: dict[str, dict[str, object]] = {}

    def _walk(items: list[dict[str, object]]) -> None:
        for item in items:
            key = _normalize_menu_key(str(item.get("menu_key") or ""))
            if key:
                lookup[key] = item
            children = item.get("children")
            if isinstance(children, list) and children:
                _walk(children)

    _walk(nodes)
    return lookup


def _build_title_lookup(nodes: list[dict[str, object]]) -> dict[str, str]:
    lookup: dict[str, str] = {}

    def _walk(items: list[dict[str, object]]) -> None:
        for item in items:
            key = _normalize_menu_key(str(item.get("menu_key") or ""))
            title = str(item.get("title") or "").strip()
            is_active = bool(item.get("is_active", True))
            route = str(item.get("route") or "").strip()
            normalized_title = _normalize_menu_key(title)
            if key and normalized_title and normalized_title not in lookup:
                lookup[normalized_title] = key
            display_title = _format_tree_label(title, is_active, route)
            normalized_display_title = _normalize_menu_key(display_title)
            if key and normalized_display_title and normalized_display_title not in lookup:
                lookup[normalized_display_title] = key
            plain_display_title = f"{title} ({route})" if route else title
            normalized_plain_display_title = _normalize_menu_key(plain_display_title)
            if key and normalized_plain_display_title and normalized_plain_display_title not in lookup:
                lookup[normalized_plain_display_title] = key
            children = item.get("children")
            if isinstance(children, list) and children:
                _walk(children)

    _walk(nodes)
    return lookup


def _build_sac_tree_items(sac_module, nodes: list[dict[str, object]]) -> list[object]:
    items: list[object] = []
    for node in nodes:
        menu_key = _normalize_menu_key(str(node.get("menu_key") or ""))
        title = str(node.get("title") or "").strip() or menu_key
        is_active = bool(node.get("is_active", True))
        route = str(node.get("route") or "").strip()
        
        # Label cố định (Title + Route) để sac.tree không bị mất dấu highlight/chọn
        label = _format_tree_label(title, is_active, route)
        
        children = node.get("children")
        child_items = _build_sac_tree_items(sac_module, children if isinstance(children, list) else [])
        
        children_payload = child_items if child_items else None
        try:
            item = sac_module.TreeItem(label=label, children=children_payload)
        except Exception:
            try:
                item = sac_module.TreeItem(title=label, children=children_payload)
            except Exception:
                try:
                    item = sac_module.TreeItem(label=label, children=children_payload)
                except Exception:
                    item = sac_module.TreeItem(title=label, children=children_payload)
            
        items.append(item)
    return items


def _format_tree_label(title: str, is_active: bool = True, route: str | None = None) -> str:
    status_icon = "✅" if is_active else "❌"
    clean_title = (title or "").strip()
    route_info = f" ({route})" if route else ""
    return f"{status_icon} {clean_title}{route_info}"


def _parse_tree_selection(value: object) -> str | None:
    if value is None:
        return None
    raw = value[0] if isinstance(value, list) and value else value
    if isinstance(raw, dict):
        candidate = raw.get("value") or raw.get("key") or raw.get("label") or raw.get("title") or ""
        text = str(candidate).strip()
    else:
        text = str(raw).strip()
    if not text:
        return None
    # Normalize common rendered prefixes used by tree labels.
    for prefix in ("✅ ", "❌ ", "[INACTIVE] "):
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
            break
    if "|" in text:
        return _normalize_menu_key(text.split("|", 1)[0])
    return _normalize_menu_key(text)


def _render_catalog_tree_with_sac(
    *,
    menus: list[dict[str, object]],
    actor_permissions: tuple[str, ...],
    permissions: list[str],
) -> dict[str, str] | None:
    try:
        import streamlit_antd_components as sac  # type: ignore
    except Exception:
        st.error("Lỗi: Không tìm thấy thư viện streamlit-antd-components. Vui lòng cài đặt để sử dụng cây menu.")
        return None

    tree_nodes = _build_menu_tree_structure(menus)
    lookup = _build_tree_lookup(tree_nodes)
    title_lookup = _build_title_lookup(tree_nodes)
    items = _build_sac_tree_items(sac, tree_nodes)

    tree_fn = getattr(sac, "tree", None)
    if tree_fn is None:
        st.error("Lỗi: Không thể khởi tạo thành phần Cây menu.")
        return None

    params = inspect.signature(tree_fn).parameters
    kwargs: dict[str, object] = {}
    if "key" in params:
        kwargs["key"] = "menu_admin_sac_tree_fixed"
    if "open_all" in params:
        kwargs["open_all"] = True
    if "show_line" in params:
        kwargs["show_line"] = True

    tree_state_key = "menu_admin_sac_tree_fixed"
    raw_tree_state = st.session_state.get(tree_state_key)

    # IMPORTANT: sac.tree internal state is label/index based.
    # Never write menu_key into widget state, only clear obviously stale values.
    if raw_tree_state is not None:
        if isinstance(raw_tree_state, list):
            if not raw_tree_state:
                st.session_state.pop(tree_state_key, None)
            else:
                first = str(raw_tree_state[0]).strip()
                if first and _normalize_menu_key(first) in lookup:
                    # stale menu_key-like state from old logic -> clear to recover
                    st.session_state.pop(tree_state_key, None)
        else:
            token = str(raw_tree_state).strip()
            if token and _normalize_menu_key(token) in lookup:
                st.session_state.pop(tree_state_key, None)

    try:
        selected = tree_fn(items=items, **kwargs) if "items" in params else tree_fn(items, **kwargs)
    except Exception as e:
        message = str(e)
        if "invalid in tree component" in message.lower():
            st.session_state.pop(tree_state_key, None)
            st.session_state.pop(_TREE_SELECTED_KEY, None)
            st.rerun()
        st.error(f"Lỗi hiển thị cây menu: {message}")
        return None

    def _iter_candidate_tokens(value: object):
        if value is None:
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                yield from _iter_candidate_tokens(item)
            return
        if isinstance(value, dict):
            for key_name in ("value", "key", "label", "title", "id", "name"):
                if key_name in value:
                    yield from _iter_candidate_tokens(value.get(key_name))
            for item in value.values():
                yield from _iter_candidate_tokens(item)
            return
        text = str(value).strip()
        if text:
            yield text

    def _resolve_selected_key(candidate: object) -> str | None:
        for token in _iter_candidate_tokens(candidate):
            normalized = _normalize_menu_key(token)
            if not normalized:
                continue
            for prefix in ("✅ ", "❌ ", "[inactive] "):
                if normalized.startswith(prefix):
                    normalized = normalized[len(prefix):].strip()
                    break
            if "|" in normalized:
                normalized = _normalize_menu_key(normalized.split("|", 1)[0])
            if normalized in lookup:
                return normalized
            mapped = title_lookup.get(normalized)
            if mapped:
                return mapped
        return None

    selected_key = _resolve_selected_key(selected)
    if not selected_key:
        selected_key = _resolve_selected_key(raw_tree_state)
    if not selected_key:
        selected_key = _resolve_selected_key(st.session_state.get(_TREE_SELECTED_KEY))
    if selected_key:
        st.session_state[_TREE_SELECTED_KEY] = selected_key
    else:
        selected_key = _normalize_menu_key(str(st.session_state.get(_TREE_SELECTED_KEY, "") or "")) or None
        if selected_key and selected_key not in lookup:
            selected_key = title_lookup.get(selected_key)
    if not selected_key or selected_key not in lookup:
        return None

    selected_node = lookup[selected_key]

    try:
        from streamlit_sortables import sort_items  # type: ignore
    except Exception:
        sort_items = None

    if sort_items is not None:
        parent_key = _normalize_menu_key(str(selected_node.get("parent_key") or "")) or None

        scope_parent_key: str | None = None
        scope_nodes: list[dict[str, object]] = []
        
        # Luôn ưu tiên lấy các menu cùng cấp (siblings) của menu đang được chọn
        if parent_key and parent_key in lookup:
            scope_parent_key = parent_key
            scope_nodes = [item for item in lookup[parent_key].get("children", []) if isinstance(item, dict)]
        else:
            # Nếu không có parent_key, tức là menu đang chọn thuộc cấp cao nhất (root)
            scope_parent_key = None
            scope_nodes = [item for item in tree_nodes if isinstance(item, dict)]

        scope_keys = [
            _normalize_menu_key(str(item.get("menu_key") or ""))
            for item in scope_nodes
            if _normalize_menu_key(str(item.get("menu_key") or ""))
        ]
        if len(scope_keys) > 1:
            label_to_key: dict[str, str] = {}
            sortable_labels: list[str] = []
            seen_titles: dict[str, int] = {}
            for key in scope_keys:
                title = str(lookup.get(key, {}).get("title") or key).strip() or key
                title_count = seen_titles.get(title, 0) + 1
                seen_titles[title] = title_count
                label = title if title_count == 1 else f"{title} ({title_count})"
                label_to_key[label] = key
                sortable_labels.append(label)

            st.write("Kéo thả để đổi thứ tự menu:")
            sorted_labels = sort_items(
                sortable_labels,
                direction="vertical",
                key=f"menu_admin_sortables_{scope_parent_key or 'root'}",
            )

            if isinstance(sorted_labels, list) and sorted_labels != sortable_labels:
                ordered_keys = [
                    label_to_key[item]
                    for item in sorted_labels
                    if isinstance(item, str) and item in label_to_key
                ]
                if len(ordered_keys) == len(scope_keys):
                    ok, reason = bulk_update_menu_order(
                        ordered_keys=ordered_keys,
                        actor_permissions=actor_permissions,
                    )
                    if ok:
                        st.session_state.pop("menu_admin_sac_tree_fixed", None)
                        _load_admin_snapshot_cached.clear()
                        _set_flash("Cập nhật thứ tự menu thành công.", "success")
                    else:
                        _set_flash(f"Không thể cập nhật thứ tự menu: {reason}", "error")
                    st.rerun()

    is_active = bool(selected_node.get("is_active", True))
    add_col, edit_col, stop_col, _ = st.columns([0.55, 0.55, 0.55, 8.35], vertical_alignment="center")
    add_icon = "➕"
    edit_icon = "✏️"
    stop_icon_active = "🛑"
    stop_icon_inactive = "↩️"
    action_to_open: tuple[str, str | None] | None = None
    with add_col:
        if st.button(add_icon, key=f"sac_add_{selected_key}", help="Thêm con", use_container_width=True):
            action_to_open = ("create", None)
    with edit_col:
        if st.button(edit_icon, key=f"sac_edit_{selected_key}", help="Sửa", use_container_width=True):
            action_to_open = ("edit", selected_key)
    with stop_col:
        stop_icon = stop_icon_active if is_active else stop_icon_inactive
        stop_help = "Ngừng sử dụng" if is_active else "Bật lại"
        if st.button(stop_icon, key=f"sac_del_{selected_key}", help=stop_help, use_container_width=True):
            action_to_open = ("toggle_active", selected_key)

    if action_to_open is not None:
        mode, target = action_to_open
        if mode == "create":
            st.session_state["menu_form_parent"] = selected_key
        
        # Gọi trực tiếp dialog tương ứng (vì đã chuyển sang @st.dialog)
        if mode in {"create", "edit"}:
            _ensure_dialog_form_state(
                mode=mode,
                target_key=str(target or ""),
                menu_map=lookup,
                permissions=permissions,
            )
            _render_menu_upsert_dialog(
                mode=mode,
                target_key=target,
                actor_permissions=actor_permissions,
                permissions=permissions,
                menus=menus
            )
        elif mode == "toggle_active":
            target_node = lookup.get(target or "")
            if target_node:
                _render_toggle_active_dialog(
                    target_key=target or "",
                    target_title=str(target_node.get("title", target)),
                    current_active=bool(target_node.get("is_active", True)),
                    route=str(target_node.get("route" or "")).strip(),
                    actor_permissions=actor_permissions,
                )
        return None
    return None



# Giao diện Bảng (Table View) đã bị xóa hoàn toàn để chỉ sử dụng giao diện Cây (Tree View).



def _render_page() -> None:
    require_auth()
    inject_css("base.css", "dashboard.css")
    with st.sidebar:
        render_app_sidebar("Quản lý menu", current_route="/menu_admin")
    sync_theme_mode(bool(st.session_state.get("ui_dark_mode", False)))
    st.markdown(
        """
        <style>
        div[data-testid="stButton"] > button[kind="primary"] {
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
            font-weight: 600;
        }
        div[data-testid="stButton"] > button[kind="primary"]:hover {
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
            filter: brightness(0.95);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    remember_me = bool(st.session_state.get(KEY_REMEMBER_ME, False))
    sync_auth_to_browser_storage(remember_me=remember_me)

    render_dashboard_header(get_current_display_name() or get_current_username() or "Team")

    actor_permissions = tuple(str(item).strip().lower() for item in st.session_state.get(KEY_PERMISSIONS, []))
    if not can_access_menu_admin(roles=None, permissions=actor_permissions):
        st.error("Bạn không có quyền quản trị menu.")
        return

    render_top_navbar(current_route="/menu_admin", render_sidebar_in_left_sidebar=False)

    st.caption("Quản trị menu catalog và cập nhật cấu hình menu.")
    _render_flash()

    menus, mappings, roles, permissions = _load_admin_snapshot_cached(actor_permissions)
    if not permissions:
        permissions = list_available_permissions_for_menu()

    _ = mappings
    _ = roles

    menu_map = {str(item.get("menu_key") or "").strip().lower(): item for item in menus}
    catalog_tab = st.tabs(["Danh mục"])[0]
    pending_mode = ""
    pending_target_key = ""
    pending_parent_key = ""
    with catalog_tab:
        action_col, _ = st.columns([1.8, 5.2])
        with action_col:
            if st.button("Tạo menu", key="menu_admin_create_button", type="primary", use_container_width=True):
                pending_mode = "create"

        tree_event = _render_catalog_tree_with_sac(
            menus=menus, 
            actor_permissions=actor_permissions,
            permissions=permissions
        )
        if tree_event:
            if tree_event["action"] == "add_child":
                pending_mode = "create"
                pending_parent_key = tree_event["parent_key"]
            elif tree_event["action"] == "edit":
                pending_mode = "edit"
                pending_target_key = tree_event["menu_key"]
            elif tree_event["action"] == "toggle_active":
                pending_mode = "toggle_active"
                pending_target_key = tree_event["menu_key"]

    if pending_mode:
        if pending_mode == "create":
            st.session_state["menu_form_parent"] = pending_parent_key or ""
        
        target_key = pending_target_key or None
        if pending_mode in {"create", "edit"}:
            _ensure_dialog_form_state(
                mode=pending_mode,
                target_key=target_key,
                menu_map=menu_map,
                permissions=permissions,
            )
            _render_menu_upsert_dialog(
                mode=pending_mode,
                target_key=target_key,
                actor_permissions=actor_permissions,
                permissions=permissions,
                menus=menus,
            )
        elif pending_mode == "toggle_active":
            target_node = menu_map.get(target_key, {})
            _render_toggle_active_dialog(
                target_key=target_key,
                target_title=str(target_node.get("title", target_key)),
                current_active=bool(target_node.get("is_active", True)),
                route=str(target_node.get("route" or "")).strip(),
                actor_permissions=actor_permissions,
            )


_render_page()




