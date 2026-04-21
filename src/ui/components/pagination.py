from __future__ import annotations

import math

import streamlit as st


def render_pagination(total: int, current_page: int, page_size: int = 10) -> int:
    total_pages = max(1, math.ceil(total / page_size))

    st.markdown("---")
    _, center_wrap, _ = st.columns([1.2, 2.6, 1.2])

    next_page = current_page
    with center_wrap:
        col_prev, col_info, col_next = st.columns([0.4, 1.0, 0.4], vertical_alignment="center")

        with col_prev:
            if st.button(
                "<-  Trang trước",
                disabled=current_page == 1,
                key="product_prev_page",
                use_container_width=True,
            ):
                next_page = current_page - 1

        with col_info:
            st.markdown(
                (
                    "<div style='text-align:center;font-weight:600;padding-top:0.35rem;'>"
                    f"Trang {current_page} / {total_pages}"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

        with col_next:
            if st.button(
                "Trang sau  ->",
                disabled=current_page >= total_pages,
                key="product_next_page",
                use_container_width=True,
            ):
                next_page = current_page + 1

    return next_page
