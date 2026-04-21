from __future__ import annotations

import html
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from src.application.menu.menu_service import resolve_visible_menu_tree
from src.ui.session.auth_session import KEY_PERMISSIONS, KEY_ROLES, switch_page_safely

_NAV_QUERY_PARAM = "menu_nav"
_NAV_CONTEXT_PARAM = "menu_nav_ctx"
_NAV_CONTEXT_STATE_KEY = "menu_nav_context_key"


def _route_to_page_path(route: str) -> str | None:
    normalized = (route or "").strip().lower()
    mapping = {
        "/menu": "pages/menu.py",
        "/dashboard": "pages/dashboard.py",
        "/supply_report": "pages/supply_report.py",
        "/admin": "pages/admin.py",
        "/dashboard1": "pages/dashboard1.py",
        "/dashboard2": "pages/dashboard2.py",
        "/menu_admin": "pages/menu_admin.py",
    }
    mapped = mapping.get(normalized)
    if mapped:
        return mapped

    # Fallback: auto-resolve /my_route -> pages/my_route.py (or my-route -> my_route.py)
    slug = normalized.lstrip("/")
    if not slug or "/" in slug or ".." in slug:
        return None

    pages_dir = Path(__file__).resolve().parents[3] / "pages"
    candidate_names = (slug, slug.replace("-", "_"))
    for candidate_name in candidate_names:
        if not candidate_name:
            continue
        page_file = pages_dir / f"{candidate_name}.py"
        if page_file.exists():
            return f"pages/{candidate_name}.py"
    return None


def _consume_menu_nav_query_param() -> str | None:
    if hasattr(st, "query_params"):
        raw = st.query_params.get(_NAV_QUERY_PARAM, "")
        if isinstance(raw, list):
            value = str(raw[-1] if raw else "").strip()
        else:
            value = str(raw or "").strip()
        if value:
            try:
                del st.query_params[_NAV_QUERY_PARAM]
            except Exception:
                pass
            return value
        return None

    params = st.experimental_get_query_params()
    raw = params.get(_NAV_QUERY_PARAM, "")
    if isinstance(raw, list):
        value = str(raw[-1] if raw else "").strip()
    else:
        value = str(raw or "").strip()
    if value:
        params.pop(_NAV_QUERY_PARAM, None)
        st.experimental_set_query_params(**params)
        return value
    return None


def _consume_menu_nav_context_query_param() -> str | None:
    if hasattr(st, "query_params"):
        raw = st.query_params.get(_NAV_CONTEXT_PARAM, "")
        if isinstance(raw, list):
            value = str(raw[-1] if raw else "").strip().lower()
        else:
            value = str(raw or "").strip().lower()
        if value:
            try:
                del st.query_params[_NAV_CONTEXT_PARAM]
            except Exception:
                pass
            return value
        return None

    params = st.experimental_get_query_params()
    raw = params.get(_NAV_CONTEXT_PARAM, "")
    if isinstance(raw, list):
        value = str(raw[-1] if raw else "").strip().lower()
    else:
        value = str(raw or "").strip().lower()
    if value:
        params.pop(_NAV_CONTEXT_PARAM, None)
        st.experimental_set_query_params(**params)
        return value
    return None


def _build_menu_nav_href(route: str, *, nav_context: str | None = None) -> str:
    _ = nav_context
    normalized_route = str(route or "").strip().lower()
    if not normalized_route:
        return "#"
    return normalized_route


def _handle_pending_menu_navigation(
    *,
    visible_menu_tree: list[dict[str, object]],
    current_route: str,
) -> None:
    pending_context = _consume_menu_nav_context_query_param()
    if pending_context:
        st.session_state[_NAV_CONTEXT_STATE_KEY] = pending_context

    pending_route = _consume_menu_nav_query_param()
    if not pending_route:
        return

    normalized_target = pending_route.strip().lower()
    if not normalized_target or normalized_target == current_route.strip().lower():
        return
    if not _tree_contains_route(visible_menu_tree, route=normalized_target):
        return

    page_path = _route_to_page_path(normalized_target)
    if not page_path:
        return

    switch_page_safely(page_path)
    st.stop()


def _sync_browser_path_with_route(current_route: str) -> None:
    _ = current_route
    # Keep URL untouched in page runtime to avoid breaking custom Streamlit
    # components (tree/sortables) that rely on stable page paths.
    return


def _is_route_active_in_branch(item: dict[str, object], current_route: str) -> bool:
    route = str(item.get("route") or "").strip().lower()
    if route and route == current_route.strip().lower():
        return True
    for child in list(item.get("children") or []):
        if _is_route_active_in_branch(child, current_route):
            return True
    return False


def _build_menu_ul_li_html(
    items: list[dict[str, object]],
    *,
    current_route: str,
    nav_context: str | None = None,
) -> str:
    icon_map = {
        "home": "[]",
        "clipboard-list": "::",
        "users": "o",
        "shield": "🛡️",
        "menu": "🧭",
        "supply_report": "📦",
        "summary_report": "📈",
        "user": "👥",
    }

    def _icon_html(row: dict[str, object]) -> str:
        icon_name = str(row.get("icon") or "").strip().lower()
        symbol = icon_map.get(icon_name, "-")
        return f'<span class="navbar-icon">{symbol}</span>'

    def _build_list(rows: list[dict[str, object]], *, depth: int) -> str:
        if not rows:
            return ""
        ul_cls = "navbar-top-ul" if depth == 0 else "navbar-dropdown-ul"
        parts: list[str] = [f'<ul class="{ul_cls}" data-depth="{depth}">']
        for row in rows:
            title = html.escape(str(row.get("title") or ""))
            route = str(row.get("route") or "").strip()
            children = list(row.get("children") or [])
            active_cls = " active" if _is_route_active_in_branch(row, current_route) else ""
            has_children_cls = " has-children" if children else ""
            parts.append(f'<li class="navbar-menu-li depth-{depth}{active_cls}{has_children_cls}">')
            if route:
                href = _build_menu_nav_href(route, nav_context=nav_context)
                parts.append(
                    f'<a class="navbar-menu-link" href="{href}" target="_self">{_icon_html(row)}<span>{title}</span></a>',
                )
            else:
                parts.append(f'<span class="navbar-menu-link">{_icon_html(row)}<span>{title}</span></span>')
            if children:
                parts.append(_build_list(children, depth=depth + 1))
            parts.append("</li>")
        parts.append("</ul>")
        return "".join(parts)

    return _build_list(items, depth=0)


def _is_sidebar_root_menu(row: dict[str, object]) -> bool:
    key = str(row.get("menu_key") or "").strip().lower()
    return bool(key) and "." not in key


def _sidebar_menu_nodes(menu_tree: list[dict[str, object]]) -> list[dict[str, object]]:
    nodes: list[dict[str, object]] = []
    for row in menu_tree:
        if _is_sidebar_root_menu(row):
            copy_row = dict(row)
            # Sidebar only shows parent-level menus; child menus belong to navbar.
            copy_row["children"] = []
            nodes.append(copy_row)
    return nodes


def _report_menu_nodes_for_route(
    menu_tree: list[dict[str, object]],
    *,
    current_route: str,
) -> list[dict[str, object]]:
    for row in menu_tree:
        if not _is_sidebar_root_menu(row):
            continue
        if _is_route_active_in_branch(row, current_route):
            return list(row.get("children") or [])
    return []


def _find_active_root_menu(
    menu_tree: list[dict[str, object]],
    *,
    current_route: str,
) -> dict[str, object] | None:
    for row in menu_tree:
        if not _is_sidebar_root_menu(row):
            continue
        if _is_route_active_in_branch(row, current_route):
            return row
    return None


def _find_menu_by_key(menu_tree: list[dict[str, object]], *, menu_key: str) -> dict[str, object] | None:
    target = (menu_key or "").strip().lower()
    if not target:
        return None
    for item in menu_tree:
        if str(item.get("menu_key") or "").strip().lower() == target:
            return item
    return None


def _split_menu_tree_by_scope(
    menu_tree: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    # Backward-compatible helper used by tests and sidebar render.
    sidebar_menus = _sidebar_menu_nodes(menu_tree)
    report_menus: list[dict[str, object]] = []
    return report_menus, sidebar_menus


def _find_menu_by_route(menu_tree: list[dict[str, object]], *, route: str) -> dict[str, object] | None:
    target_route = (route or "").strip().lower()
    if not target_route:
        return None
    for item in menu_tree:
        item_route = str(item.get("route") or "").strip().lower()
        if item_route and item_route == target_route:
            return item
        children = list(item.get("children") or [])
        found = _find_menu_by_route(children, route=target_route)
        if found:
            return found
    return None


def _tree_contains_route(menu_tree: list[dict[str, object]], *, route: str) -> bool:
    return _find_menu_by_route(menu_tree, route=route) is not None


def _render_navbar_css() -> None:
    st.markdown(
        """
        <style>
        .sidebar-menu-shell {
            margin-top: 0;
            margin-bottom: 12px;
            padding: 10px;
            border: 1px solid #d8e0ea;
            border-radius: 12px;
            background: #fbfcff;
            box-shadow: 0 6px 16px rgba(32, 46, 92, 0.06);
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left {
            margin-top: 0.85rem;
            margin-bottom: 0.3rem;
            width: calc(100% + 1rem);
            margin-left: -0.5rem;
            margin-right: -0.5rem;
            padding: 0;
            border: 0;
            border-radius: 0;
            background: transparent;
            box-shadow: none;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .sidebar-menu-in-left-title {
            font-size: 0.88rem;
            font-weight: 700;
            color: var(--text-main, #1f2f46);
            margin: 0 0 0.35rem 0.7rem;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 0.15rem;
            padding-right: 0.4rem;
        }
        section[data-testid="stSidebar"] .sidebar-menu-toggle-btn {
            border: 1px solid var(--border-2, #dde5ef);
            background: var(--surface-1, #ffffff);
            color: var(--text-main, #1f2f46);
            border-radius: 6px;
            width: 24px;
            height: 22px;
            line-height: 1;
            font-weight: 700;
            cursor: pointer;
        }
        section[data-testid="stSidebar"] .sidebar-menu-toggle-btn:hover {
            background: rgba(82, 114, 255, 0.12);
        }
        section[data-testid="stSidebar"] .sidebar-menu-content.is-collapsed {
            display: none;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-top-ul {
            background: transparent;
            border: 0;
            padding: 0;
            margin: 0;
            display: flex;
            flex-direction: column;
            align-items: stretch;
            gap: 6px;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li {
            width: 100%;
            display: block;
            margin: 0 !important;
            border-radius: 8px;
            overflow: hidden;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-link {
            width: 100%;
            display: flex;
            box-sizing: border-box;
            border: 0;
            border-bottom: 1px solid var(--border-2, #dde5ef);
            background: transparent;
            border-radius: 8px;
            padding: 8px 10px 8px 10px;
            cursor: pointer;
            transition: background-color 0.2s ease, color 0.2s ease, border-color 0.2s ease;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-link:hover {
            background: rgba(82, 114, 255, 0.12);
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li.active > .navbar-menu-link {
            background: rgba(82, 114, 255, 0.18);
            color: #1f3fb5;
            font-weight: 700;
            border-bottom-color: rgba(63, 102, 227, 0.45);
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li:last-child > .navbar-menu-link {
            border-bottom: 1px solid var(--border-2, #dde5ef);
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li > .navbar-dropdown-ul {
            position: static;
            min-width: 0;
            margin-top: 4px;
            padding: 4px 0 2px 12px;
            box-shadow: none;
            background: transparent;
            border-radius: 0;
            display: none !important;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li.is-open > .navbar-dropdown-ul {
            display: flex !important;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-li > .navbar-dropdown-ul {
            left: 0;
            margin-left: 0;
            padding-left: 12px;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-li {
            margin: 0 !important;
            border-radius: 8px;
            overflow: hidden;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-link {
            background: var(--surface-2, #f3f6f9);
            border-bottom: 1px solid var(--border-2, #dde5ef);
            border-radius: 8px;
            transition: background-color 0.2s ease, color 0.2s ease, border-color 0.2s ease;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-link:hover {
            background: rgba(82, 114, 255, 0.16);
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-li.active > .navbar-menu-link {
            background: rgba(82, 114, 255, 0.20);
            color: #1f3fb5;
            font-weight: 700;
            border-bottom-color: rgba(63, 102, 227, 0.45);
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li.has-children > .navbar-menu-link {
            cursor: pointer;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li.has-children > .navbar-menu-link::after {
            content: "\\25BE";
            margin-left: auto;
            color: #6a7396;
        }
        section[data-testid="stSidebar"] .sidebar-menu-in-left .navbar-menu-li.has-children.is-open > .navbar-menu-link::after {
            content: "\\25B4";
        }
        .navbar-shell {
            position: sticky;
            top: 0;
            z-index: 20;
            margin-top: 0;
            margin-bottom: 14px;
            padding: 10px;
            border-radius: 14px;
            background: #ffffff;
            box-shadow: 0 8px 16px rgba(32, 46, 92, 0.08);
        }
        .navbar-top-ul {
            list-style: none;
            margin: 0;
            padding: 0;
            display: flex;
            gap: 6px;
            align-items: center;
            flex-wrap: wrap;
            justify-content: flex-start;
            background: rgba(250, 251, 255, 0.96);
            border-radius: 10px;
            padding: 8px;
        }
        .sidebar-menu-shell .navbar-top-ul {
            border: 1px solid rgba(82, 114, 255, 0.14);
            background: #ffffff;
        }
        .navbar-menu-li {
            position: relative;
            display: inline-flex;
            align-items: center;
        }
        .navbar-menu-link {
            display: inline-flex;
            align-items: center;
            padding: 8px 12px;
            border-radius: 8px;
            text-decoration: none !important;
            color: #1f2a44;
            background: transparent;
            line-height: 1.2;
            white-space: nowrap;
            font-weight: 500;
        }
        .navbar-menu-link:hover {
            background: rgba(82, 114, 255, 0.18);
            text-decoration: none !important;
        }
        .navbar-menu-link span {
            vertical-align: middle;
        }
        .navbar-icon {
            display: inline-flex;
            width: 18px;
            margin-right: 8px;
            color: #5566b8;
            font-size: 15px;
            justify-content: center;
        }
        .navbar-menu-li.active > .navbar-menu-link {
            background: rgba(82, 114, 255, 0.12);
            font-weight: 600;
            color: #2642d8;
        }
        .navbar-menu-li.has-children > .navbar-menu-link::after {
            content: " v";
            font-size: 0.8em;
            color: #6a7396;
            margin-left: 6px;
        }
        .navbar-menu-li > .navbar-dropdown-ul {
            display: none;
            position: absolute;
            top: 100%;
            left: 0;
            min-width: 280px;
            flex-direction: column;
            gap: 6px;
            margin: 0;
            padding: 10px;
            list-style: none;
            background: #f7f9ff;
            border-radius: 10px;
            z-index: 40;
            box-shadow: 0 12px 24px rgba(20, 38, 105, 0.20);
        }
        .navbar-menu-li:hover > .navbar-dropdown-ul,
        .navbar-menu-li:focus-within > .navbar-dropdown-ul {
            display: flex;
        }
        .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-li > .navbar-dropdown-ul {
            left: 100%;
            top: 0;
            margin-left: 14px;
        }
        .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-li.has-children::after {
            content: "";
            position: absolute;
            top: 0;
            right: -14px;
            width: 14px;
            height: 100%;
        }
        .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-link {
            width: 100%;
            background: transparent;
            border-radius: 6px;
            display: flex;
            align-items: center;
            padding: 10px 12px;
            box-sizing: border-box;
        }
        .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-li {
            width: 100%;
            border-radius: 8px;
            overflow: visible;
            padding: 0;
            margin: 0;
        }
        .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-li:hover {
            background: rgba(68,114,196,0.22);
        }
        .navbar-menu-li > .navbar-dropdown-ul .navbar-menu-li:hover > .navbar-menu-link {
            background: transparent;
        }
        .navbar-menu-li.depth-1.has-children > .navbar-menu-link::after,
        .navbar-menu-li.depth-2.has-children > .navbar-menu-link::after {
            content: ">";
            float: right;
            margin-left: auto;
            color: #6a7396;
        }
        @media (max-width: 920px) {
            .navbar-shell {
                position: static;
            }
            .navbar-top-ul {
                gap: 6px;
                padding: 8px;
            }
            .navbar-menu-link {
                padding: 6px 10px;
            }
            .navbar-menu-li > .navbar-dropdown-ul {
                position: static;
                box-shadow: none;
                border: 1px solid #e8e8e8;
                margin-top: 6px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_top_navbar(
    *,
    current_route: str,
    embed_sidebar_in_header: bool = False,
    render_sidebar_in_left_sidebar: bool = True,
) -> None:
    visible_menu_tree = resolve_visible_menu_tree(
        roles=st.session_state.get(KEY_ROLES, []),
        permissions=st.session_state.get(KEY_PERMISSIONS, []),
    )

    if not visible_menu_tree:
        return
    _handle_pending_menu_navigation(visible_menu_tree=visible_menu_tree, current_route=current_route)
    _sync_browser_path_with_route(current_route)

    active_root = _find_active_root_menu(visible_menu_tree, current_route=current_route)
    if active_root is not None:
        st.session_state[_NAV_CONTEXT_STATE_KEY] = str(active_root.get("menu_key") or "").strip().lower()

    context_key = str(st.session_state.get(_NAV_CONTEXT_STATE_KEY, "") or "").strip().lower()
    context_root = _find_menu_by_key(visible_menu_tree, menu_key=context_key)
    report_menu_tree: list[dict[str, object]]
    if context_root is not None:
        report_menu_tree = list(context_root.get("children") or [])
    else:
        report_menu_tree = _report_menu_nodes_for_route(visible_menu_tree, current_route=current_route)
        if active_root is not None:
            context_key = str(active_root.get("menu_key") or "").strip().lower()

    _, sidebar_menu_tree = _split_menu_tree_by_scope(visible_menu_tree)
    _render_navbar_css()

    if sidebar_menu_tree:
        if render_sidebar_in_left_sidebar:
            with st.sidebar:
                render_left_sidebar_menu(current_route=current_route, title="Menu")

    should_hide_menu = False
    current_menu = _find_menu_by_route(visible_menu_tree, route=current_route)
    if current_menu is not None:
        hide_value = current_menu.get("hide_menu")
        if hide_value is not None:
            should_hide_menu = bool(hide_value)

    if report_menu_tree and not should_hide_menu:
        report_menu_html = _build_menu_ul_li_html(
            report_menu_tree,
            current_route=current_route,
            nav_context=context_key or None,
        )
        st.markdown(f'<nav class="navbar-shell">{report_menu_html}</nav>', unsafe_allow_html=True)


def render_left_sidebar_menu(*, current_route: str, title: str = "Menu") -> None:
    visible_menu_tree = resolve_visible_menu_tree(
        roles=st.session_state.get(KEY_ROLES, []),
        permissions=st.session_state.get(KEY_PERMISSIONS, []),
    )
    if not visible_menu_tree:
        return
    _handle_pending_menu_navigation(visible_menu_tree=visible_menu_tree, current_route=current_route)
    _sync_browser_path_with_route(current_route)
    _render_navbar_css()
    _, sidebar_menu_tree = _split_menu_tree_by_scope(visible_menu_tree)
    if not sidebar_menu_tree:
        return

    sidebar_menu_html = _build_menu_ul_li_html(sidebar_menu_tree, current_route=current_route)
    title_html = html.escape(title)
    st.markdown(
        (
            '<div class="sidebar-menu-in-left">'
            '<div class="sidebar-menu-in-left-header">'
            f'<div class="sidebar-menu-in-left-title">{title_html}</div>'
            '<button id="left-sidebar-menu-toggle-btn" class="sidebar-menu-toggle-btn" type="button" title="Thu gọn/Mở menu">▲</button>'
            '</div>'
            f'<div id="left-sidebar-menu-content" class="sidebar-menu-content">{sidebar_menu_html}</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )
    components.html(
        """
        <script>
        (function () {
          const doc = window.parent && window.parent.document ? window.parent.document : document;
          const btn = doc.getElementById("left-sidebar-menu-toggle-btn");
          const content = doc.getElementById("left-sidebar-menu-content");
          if (!btn || !content) return;
          const storageKey = "left_sidebar_menu_collapsed";
          const apply = (collapsed) => {
            content.classList.toggle("is-collapsed", collapsed);
            btn.textContent = collapsed ? "▼" : "▲";
            btn.title = collapsed ? "Mở menu" : "Thu gọn menu";
          };
          let collapsed = false;
          try { collapsed = sessionStorage.getItem(storageKey) === "1"; } catch (e) {}
          apply(collapsed);

          const parentItems = content.querySelectorAll(".navbar-menu-li.has-children");
          parentItems.forEach((li) => {
            const trigger = li.querySelector(".navbar-menu-link");
            if (!trigger) return;
            if (li.classList.contains("active")) {
              li.classList.add("is-open");
            }
            if (trigger.dataset.sidebarBound === "1") return;
            trigger.dataset.sidebarBound = "1";
            trigger.addEventListener("click", function (event) {
              event.preventDefault();
              event.stopPropagation();
              li.classList.toggle("is-open");
            });
          });

          btn.onclick = function () {
            collapsed = !content.classList.contains("is-collapsed");
            apply(collapsed);
            try { sessionStorage.setItem(storageKey, collapsed ? "1" : "0"); } catch (e) {}
            return false;
          };
        })();
        </script>
        """,
        height=0,
        width=0,
    )



