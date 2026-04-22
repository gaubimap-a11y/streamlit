from __future__ import annotations

from abc import ABC, abstractmethod

import os

import streamlit as st

from src.ui.components.header import render_dashboard_header
from src.ui.components.sidebar import render_app_sidebar
from src.ui.session.auth_session import (
    KEY_REMEMBER_ME,
    get_current_display_name,
    get_current_username,
    require_auth,
)
from src.ui.session.browser_storage import sync_auth_to_browser_storage
from src.ui.styles.loader import inject_css
from src.ui.styles.loader import sync_theme_mode


_LOGIN_PAGE = "pages/login.py"

def _is_pytest_runtime() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))

def _render_page_link(page_path: str, *, label: str, icon: str) -> None:
    if _is_pytest_runtime():
        st.markdown(f"[{icon} {label}]({page_path})", unsafe_allow_html=True)
        return
    try:
        st.page_link(page_path, label=label, icon=icon)
    except Exception:
        st.markdown(f"[{icon} {label}]({page_path})", unsafe_allow_html=True)

class BasePage(ABC):
    def __init__(self) -> None:
        self._page_config_applied = False

    @property
    @abstractmethod
    def page_title(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def page_icon(self) -> str:
        raise NotImplementedError

    @property
    def layout(self) -> str:
        return "wide"

    @property
    def sidebar_state(self) -> str:
        return "expanded"

    @property
    def current_route(self) -> str:
        return ""

    def run(self) -> None:
        self._setup()
        self._apply_css()
        self.render()

    def _setup(self) -> None:
        if self._page_config_applied:
            return
        st.set_page_config(
            page_title=self.page_title,
            page_icon=self.page_icon,
            layout=self.layout,
            initial_sidebar_state=self.sidebar_state,
        )
        self._page_config_applied = True

    def _apply_css(self) -> None:
        inject_css("base.css")

    def _get_dark_mode(self) -> bool:
        return bool(st.session_state.get("ui_dark_mode", False))

    def _require_auth(self) -> None:
        require_auth()

    def _render_sidebar_extra(self) -> None:
        return

    def _render_header_left(self) -> None:
        render_dashboard_header(get_current_display_name() or get_current_username() or "Team")

    def _render_page_sidebar(self) -> None:
        with st.sidebar:
            render_app_sidebar(self.page_title, current_route=self.current_route)
            self._render_sidebar_extra()

    def _render_page_header(self, *, show_welcome: bool = True) -> None:
        self._render_page_sidebar()
        remember_me = bool(st.session_state.get(KEY_REMEMBER_ME, False))
        sync_auth_to_browser_storage(remember_me=remember_me)
        self._render_header_left()
        sync_theme_mode(self._get_dark_mode())

    @abstractmethod
    def render(self) -> None:
        raise NotImplementedError
