from __future__ import annotations

import datetime as dt

import streamlit as st

from src.ui.base.base_page import BasePage
from src.ui.components.header import render_dashboard_header
from src.ui.components.navbar import render_left_sidebar_menu, render_top_navbar
from src.core.i18n.translator import t
from src.ui.session.auth_session import (
    KEY_REMEMBER_ME,
    clear_session,
    get_current_username,
    require_auth,
    switch_page_safely,
)
from src.ui.session.browser_storage import clear_browser_storage_auth, sync_auth_to_browser_storage
from src.ui.styles.loader import inject_css, sync_theme_mode

_LOGIN_PAGE = "pages/login.py"
_LANGUAGES = {"vi": "Tiếng Việt", "ja": "日本語"}
_DEPARTMENTS = [
    "Tất cả",
    "住関衣料 (Gia dụng)",
    "日配・惣菜 (Thực phẩm)",
    "農産 (Nông sản)",
]


class MenuPage(BasePage):
    @property
    def page_title(self) -> str:
        return "Menu"

    @property
    def page_icon(self) -> str:
        return "📚"

    @property
    def sidebar_state(self) -> str:
        return "expanded"

    def render(self) -> None:
        require_auth()
        inject_css("dashboard.css")
        self._render_sidebar()
        sync_theme_mode(self._get_dark_mode())

        remember_me = bool(st.session_state.get(KEY_REMEMBER_ME, False))
        sync_auth_to_browser_storage(remember_me=remember_me)

        header_col, action_col = st.columns([7.5, 1.2], vertical_alignment="bottom")
        with action_col:
            st.markdown('<div class="logout-offset">', unsafe_allow_html=True)
            logout_pressed = st.button("Đăng xuất", key="menu_page_logout")
            st.markdown("</div>", unsafe_allow_html=True)
        with header_col:
            render_dashboard_header(get_current_username())

        if logout_pressed:
            clear_session()
            clear_browser_storage_auth()
            switch_page_safely(_LOGIN_PAGE)
            st.stop()

        render_top_navbar(current_route="/menu", render_sidebar_in_left_sidebar=False)

    def _render_sidebar(self) -> None:
        with st.sidebar:
            render_left_sidebar_menu(current_route="/menu")
            st.title(self.page_title)

            st.selectbox(
                t("ui.sidebar.language"),
                options=list(_LANGUAGES.keys()),
                format_func=lambda value: _LANGUAGES[value],
                key="locale",
            )
            st.toggle(t("ui.sidebar.dark_mode"), key="ui_dark_mode")

            st.markdown(
                f'<div class="sidebar-info-card">{t("ui.sidebar.info_card")}</div>',
                unsafe_allow_html=True,
            )
            st.selectbox(
                t("ui.sidebar.department_label"),
                _DEPARTMENTS,
                key="_menu_department",
            )
            today = dt.date.today()
            st.date_input(
                t("ui.sidebar.date_range_label"),
                value=(today - dt.timedelta(days=7), today),
                key="_menu_date_range",
                format="YYYY/MM/DD",
            )
            st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="sidebar-section-label">{t("ui.sidebar.connection_config")}</div>',
                unsafe_allow_html=True,
            )
            st.caption(t("ui.sidebar.engine_label"))
            st.caption(t("ui.sidebar.state_connected"))

