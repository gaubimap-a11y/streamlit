from __future__ import annotations

import datetime as dt
from html import escape

import streamlit as st


def render_dashboard_header(username: str) -> None:
    today_label = dt.datetime.now().strftime("%d/%m/%Y")
    initials = (username[0] if username else "U").upper()
    st.markdown(
        f"""
        <div class="topbar-card">
            <div class="topbar-brand">COOP KOBE</div>
            <div class="topbar-meta">
                <div class="topbar-meta-line">
                    <span>Hệ thống phân tích dữ liệu</span>
                    <span class="topbar-separator">|</span>
                    <span class="user-identity">
                        <span class="user-avatar">{escape(initials)}</span>
                        <b>{escape(username)}</b>
                    </span>
                    <span class="topbar-separator">|</span>
                    <span>{today_label}</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
