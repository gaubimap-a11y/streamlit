import streamlit as st


def render_dashboard_footer() -> None:
    st.markdown(
        """
        <div class="footer-spacer"></div>
        <div class="footer-bar">
            (c) 2026 MD-Pro Migration Project | Powered by Databricks SQL &amp; Streamlit/Python | Confidential
        </div>
        """,
        unsafe_allow_html=True,
    )
