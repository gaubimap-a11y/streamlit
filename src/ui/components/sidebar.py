from __future__ import annotations

import streamlit as st

from src.core.i18n.translator import t
from src.ui.audit_events import record_ui_audit_event
from src.ui.components.navbar import render_left_sidebar_menu
from src.ui.session.auth_session import (
    clear_session,
    get_current_session,
)
from src.ui.session.browser_storage import clear_browser_storage_auth

def render_app_sidebar(page_title: str, *, current_route: str = "") -> None:
    st.title(page_title)
    st.selectbox(
        t("ui.sidebar.language"),
        options=["vi", "ja"],
        format_func=lambda x: "Tiếng Việt" if x == "vi" else "日本語",
        key="locale",
    )
    st.toggle(t("ui.sidebar.dark_mode"), key="ui_dark_mode")
    render_left_sidebar_menu(current_route=current_route, title="Menu")

    st.markdown('<div class="logout-offset">', unsafe_allow_html=True)
    logout_pressed = st.button(t("ui.header.logout"))
    st.markdown("</div>", unsafe_allow_html=True)

    if logout_pressed:
        record_ui_audit_event(
            get_current_session(),
            event_type="logout",
            resource="auth",
            action="logout",
            result="success",
        )
        clear_session()
        clear_browser_storage_auth(force_reload=True)
        st.stop()
