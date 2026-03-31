from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from application.demo_report_service import export_demo_report, generate_demo_report
from domain.models import DemoRequest
from domain.validation import ValidationError
from infrastructure.sql_warehouse_source import DatabricksSchemaError, DatabricksUnavailableError

THEME_CSS_PATH = Path(__file__).with_name("theme.css")


def _ensure_report_state() -> None:
    st.session_state.setdefault("report_result", None)
    st.session_state.setdefault("report_error", None)
    st.session_state.setdefault("report_input_value", "")
    st.session_state.setdefault("report_loading", False)
    st.session_state.setdefault("report_pending_product_name", None)
    st.session_state.setdefault("report_last_refresh", None)


def inject_styles() -> None:
    css = THEME_CSS_PATH.read_text(encoding="utf-8")
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def render_sidebar() -> str:
    current_value = st.session_state.get("report_input_value", "")
    with st.sidebar:
        st.markdown("### :material/tune: Filter Panel")

        with st.container(border=True):
            st.markdown("**:material/filter_alt: Product Filter**")
            product_name = st.text_input(
                ":material/inventory_2: Product name",
                value=current_value,
            )

        with st.container(border=True):
            st.markdown("**:material/database: Data Source**")
            st.write(":material/cloud_sync: Databricks SQL Warehouse")
        return product_name


def render_header() -> None:
    st.header("Sales Overview")

def render_actions() -> bool:
    action_col, helper_col = st.columns([1, 2.2], vertical_alignment="bottom")
    with action_col:
        return st.button(
            "Generating..." if st.session_state.get("report_loading", False) else "Generate report",
            type="primary",
            icon=":material/play_arrow:",
            use_container_width=True,
            disabled=st.session_state.get("report_loading", False),
        )
    with helper_col:
        st.markdown(
            '<div class="helper-text">Use the sidebar presets, then run the Databricks query. The button disables while the request is in progress.</div>',
            unsafe_allow_html=True,
        )
    return False


def render_empty_state() -> None:
    st.info("Choose a filter or preset in the sidebar, then click `Generate report` to load data from Databricks.")


def render_kpis(result) -> None:
    revenue_per_row = result.summary.total_revenue / result.summary.row_count if result.summary.row_count else 0.0
    cols = st.columns(4)
    cols[0].metric("Total revenue", f"{result.summary.total_revenue:,.2f}")
    cols[1].metric("Total sales", f"{result.summary.total_sales:,}")
    cols[2].metric("Visible rows", f"{result.summary.row_count:,}")
    cols[3].metric("Avg revenue per product", f"{revenue_per_row:,.2f}")


def _build_chart_frame(report_df: pd.DataFrame) -> pd.DataFrame:
    chart_df = report_df.sort_values("Total Revenue", ascending=False).head(10).copy()
    return chart_df.set_index("Product Name")


def render_results(result) -> None:
    render_kpis(result)
    overview_tab, data_tab, export_tab = st.tabs(["Overview", "Data", "Export"])

    with overview_tab:
        chart_frame = _build_chart_frame(result.report_df)
        left_col, right_col = st.columns(2)
        with left_col:
            with st.container(border=True):
                st.markdown("#### :material/bar_chart: Top Revenue by Product")
                st.bar_chart(chart_frame["Total Revenue"], color="#1769aa")
        with right_col:
            with st.container(border=True):
                st.markdown("#### :material/show_chart: Top Sales by Product")
                st.line_chart(chart_frame["Total Sales"], color="#f28c28")

        with st.container(border=True):
            st.markdown("#### :material/insights: Summary")
            top_product = chart_frame.index[0] if not chart_frame.empty else "N/A"
            st.write(f"Highest revenue product in the current view: `{top_product}`")

    with data_tab:
        with st.container(border=True):
            st.markdown("#### :material/table: Detailed Data")
            st.dataframe(result.report_df, use_container_width=True, hide_index=True)

    with export_tab:
        with st.container(border=True):
            st.markdown("#### :material/download: Export Actions")
            st.caption("Download the current dataset without generating sample files on disk.")
            file_name, csv_bytes, _ = export_demo_report(result, persist_sample=False)
            st.download_button(
                "Export CSV",
                data=csv_bytes,
                file_name=file_name,
                mime="text/csv",
                icon=":material/download:",
                use_container_width=True,
                disabled=st.session_state.get("report_loading", False),
            )


def _run_report(product_name: str) -> None:
    st.session_state.report_input_value = product_name
    st.session_state.report_error = None
    st.session_state.report_result = None
    st.session_state.report_loading = True

    try:
        with st.spinner("Loading data from Databricks..."):
            st.session_state.report_result = generate_demo_report(DemoRequest(product_name=product_name))
            st.session_state.report_last_refresh = datetime.now()
    except (ValidationError, DatabricksUnavailableError, DatabricksSchemaError) as exc:
        st.session_state.report_error = exc
    finally:
        st.session_state.report_loading = False
        st.session_state.report_pending_product_name = None


def render_state_feedback() -> None:
    if st.session_state.get("report_loading", False):
        st.warning("The Databricks query is running. Please wait until loading completes.")

    report_error = st.session_state.get("report_error")
    report_result = st.session_state.get("report_result")
    if report_error is not None:
        st.error(str(report_error))
    elif report_result is not None:
        st.success("Report generated successfully.")
        render_results(report_result)
    else:
        render_empty_state()


def render_app() -> None:
    st.set_page_config(page_title="Sales Demo", layout="wide", page_icon="📊")
    _ensure_report_state()
    inject_styles()

    product_name = render_sidebar()
    render_header()
    should_generate = render_actions()
    st.divider()

    if should_generate:
        st.session_state.report_input_value = product_name
        st.session_state.report_pending_product_name = product_name
        st.session_state.report_loading = True
        st.rerun()

    pending_product_name = st.session_state.get("report_pending_product_name")
    if pending_product_name is not None and st.session_state.get("report_loading", False):
        _run_report(pending_product_name)
        st.rerun()

    render_state_feedback()


if __name__ == "__main__":
    render_app()
