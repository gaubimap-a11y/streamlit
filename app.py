import streamlit as st
from pathlib import Path
from urllib.parse import urlparse

from src.core.logging_setup import configure_logging
from src.ui.session.auth_session import KEY_AUTHENTICATED, switch_page_safely
from src.ui.pages.login_page import LoginPage
from src.ui.pages.menu_page import MenuPage

_NAV_QUERY_PARAM = "menu_nav"


def _get_query_param(name: str) -> str:
    if hasattr(st, "query_params"):
        raw = st.query_params.get(name, "")
        if isinstance(raw, list):
            return str(raw[-1] if raw else "").strip()
        return str(raw or "").strip()
    params = st.experimental_get_query_params()
    raw = params.get(name, "")
    if isinstance(raw, list):
        return str(raw[-1] if raw else "").strip()
    return str(raw or "").strip()


def _remove_query_param(name: str) -> None:
    if hasattr(st, "query_params"):
        try:
            del st.query_params[name]
        except Exception:
            pass
        return
    params = st.experimental_get_query_params()
    params.pop(name, None)
    st.experimental_set_query_params(**params)


def _route_to_page_path(route: str) -> str | None:
    normalized = (route or "").strip().lower()
    mapping = {
        "/menu": "pages/menu.py",
        "/dashboard": "pages/dashboard.py",
        "/users": "pages/users.py",
        "/summary_report": "pages/summary_report.py",
        "/supply_report": "pages/supply_report.py",
        "/admin": "pages/admin.py",
        "/menu_admin": "pages/menu_admin.py",
    }
    mapped = mapping.get(normalized)
    if mapped:
        return mapped

    slug = normalized.lstrip("/")
    if not slug or "/" in slug or ".." in slug:
        return None

    pages_dir = Path(__file__).resolve().parent / "pages"
    candidate_names = (slug, slug.replace("-", "_"))
    for candidate_name in candidate_names:
        if not candidate_name:
            continue
        page_file = pages_dir / f"{candidate_name}.py"
        if page_file.exists():
            return f"pages/{candidate_name}.py"
    return None


def _handle_pending_menu_navigation() -> None:
    route = _get_query_param(_NAV_QUERY_PARAM)
    if not route:
        return

    _remove_query_param(_NAV_QUERY_PARAM)
    page_path = _route_to_page_path(route)
    if not page_path:
        return

    switch_page_safely(page_path)
    st.stop()


def _current_route_path() -> str:
    try:
        raw_url = str(getattr(st.context, "url", "") or "").strip()
    except Exception:
        raw_url = ""
    if not raw_url:
        return "/"

    parsed = urlparse(raw_url)
    route_path = str(parsed.path or "/").strip()
    if not route_path.startswith("/"):
        route_path = f"/{route_path}"
    if route_path != "/":
        route_path = route_path.rstrip("/")
    return route_path.lower() or "/"


def _resolve_start_page():
    if st.session_state.get(KEY_AUTHENTICATED):
        current_route = _current_route_path()
        if current_route not in {"", "/"}:
            direct_page_path = _route_to_page_path(current_route)
            if direct_page_path:
                switch_page_safely(direct_page_path)
                st.stop()
        _handle_pending_menu_navigation()
        return MenuPage()
    return LoginPage()


configure_logging()
_resolve_start_page().run()
