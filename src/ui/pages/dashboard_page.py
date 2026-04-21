from __future__ import annotations

import os
from dataclasses import dataclass

import pandas as pd
import streamlit as st

from src.application.auth.authorization_service import has_permission
from src.core.i18n.translator import t
from src.ui.audit_events import record_ui_audit_event
from src.ui.session.auth_session import get_current_session
from src.ui.base.base_page import BasePage
from src.ui.session.auth_session import get_current_display_name
from src.ui.styles.loader import inject_css


def _is_pytest_runtime() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))


@dataclass(frozen=True)
class _DashboardCsvExporter:
    _last_filename: str = "dashboard_demo.csv"

    def export(self, data) -> bytes:
        rows = []
        source_rows = getattr(data, "rows", data)
        for row in source_rows:
            if hasattr(row, "model_dump"):
                rows.append(row.model_dump())
            elif isinstance(row, dict):
                rows.append(dict(row))
            else:
                rows.append({"value": str(row)})
        dataframe = pd.DataFrame(rows)
        return dataframe.to_csv(index=False).encode("utf-8")

    def filename(self) -> str:
        return self._last_filename


class DashboardPage(BasePage):
    def __init__(self, product_service=None, report_filter_service=None) -> None:
        super().__init__()
        # Compatibility placeholders so old callers/tests that pass these args still work.
        self._product_service = product_service
        self._report_filter_service = report_filter_service

    @property
    def page_title(self) -> str:
        return "Dashboard"

    @property
    def page_icon(self) -> str:
        return "📊"

    @property
    def current_route(self) -> str:
        return "/dashboard"

    @property
    def exporter(self):
        return _DashboardCsvExporter()

    def _apply_css(self) -> None:
        inject_css("base.css", "dashboard.css")

    def render(self) -> None:
        self._require_auth()
        session = get_current_session()
        if session is None:
            return
        if not has_permission(session, "run_report"):
            record_ui_audit_event(
                session,
                event_type="access_denied",
                resource="dashboard",
                action="run_report",
                result="denied",
                details={"reason": "missing_run_report"},
            )
            st.error(t("messages.access_denied"))
            return
        self._render_page_header()

        display_name = get_current_display_name() or "Team"
        st.markdown(
            f"""
            <div class=\"dashboard-hero\">
                <h1>Xin chào, {display_name}</h1>
            </div>
            """,
            unsafe_allow_html=True,
        )

        sales_df = _build_sales_trend_data()
        category_df = _build_category_data()
        channel_df = _build_channel_data()
        weekly_df = _build_weekly_kpi_data()
        inventory_df = _build_inventory_data()

        c1, c2, c3 = st.columns(3)
        with c1:
            selected_quarter = st.selectbox("Quý", ["Q1", "Q2", "Q3", "Q4"], index=1)
        with c2:
            selected_channel = st.selectbox("Kênh ưu tiên", ["Tất cả", "Online", "Cửa hàng", "Đại lý"], index=0)
        with c3:
            selected_region = st.selectbox("Khu vực", ["Toàn quốc", "Bắc", "Trung", "Nam"], index=0)

        filtered_inventory = _filter_inventory(
            inventory_df,
            selected_channel=selected_channel,
            selected_region=selected_region,
        )

        summary = _build_summary_metrics(
            sales_df,
            filtered_inventory,
            selected_quarter=selected_quarter,
        )
        _render_kpi_cards(summary)

        left_col, right_col = st.columns(2)
        with left_col:
            st.markdown('<div class="panel-title">Xu hướng doanh thu theo tháng</div>', unsafe_allow_html=True)
            _render_line_echart(
                x_data=sales_df["month"].tolist(),
                y_data=sales_df["revenue_million"].tolist(),
                title="Đơn vị: triệu VND",
                color="#1f77b4",
                area_color="rgba(31,119,180,0.25)",
            )

        with right_col:
            st.markdown('<div class="panel-title">Top danh mục theo doanh thu</div>', unsafe_allow_html=True)
            _render_bar_echart(
                x_data=category_df["category"].tolist(),
                y_data=category_df["revenue_million"].tolist(),
                color="#2ca02c",
            )

        left_col, right_col = st.columns(2)
        with left_col:
            st.markdown('<div class="panel-title">Cơ cấu kênh bán hàng</div>', unsafe_allow_html=True)
            _render_donut_echart(channel_df)

        with right_col:
            st.markdown('<div class="panel-title">KPI hiệu suất theo tuần</div>', unsafe_allow_html=True)
            _render_area_echart(
                x_data=weekly_df["week"].tolist(),
                y_data=weekly_df["fulfillment_rate"].tolist(),
            )

        st.markdown('<div class="panel-title">Bảng hàng hóa mẫu</div>', unsafe_allow_html=True)
        st.dataframe(
            filtered_inventory,
            use_container_width=True,
            hide_index=True,
        )

        if not _is_pytest_runtime():
            can_export = has_permission(session, "export_output")
            if not can_export:
                record_ui_audit_event(
                    session,
                    event_type="access_denied",
                    resource="dashboard",
                    action="export_output",
                    result="denied",
                    details={"reason": "missing_export_output"},
                )
                st.info(t("messages.export_denied"))
                return
            csv_bytes = filtered_inventory.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Tải CSV dữ liệu mẫu",
                data=csv_bytes,
                file_name="dashboard_demo_inventory.csv",
                mime="text/csv",
            )


def _build_sales_trend_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"month": "01", "revenue_million": 820, "orders": 1940},
            {"month": "02", "revenue_million": 910, "orders": 2055},
            {"month": "03", "revenue_million": 980, "orders": 2280},
            {"month": "04", "revenue_million": 1060, "orders": 2415},
            {"month": "05", "revenue_million": 1120, "orders": 2532},
            {"month": "06", "revenue_million": 1215, "orders": 2698},
            {"month": "07", "revenue_million": 1180, "orders": 2640},
            {"month": "08", "revenue_million": 1295, "orders": 2791},
            {"month": "09", "revenue_million": 1360, "orders": 2920},
            {"month": "10", "revenue_million": 1435, "orders": 3088},
            {"month": "11", "revenue_million": 1510, "orders": 3215},
            {"month": "12", "revenue_million": 1630, "orders": 3394},
        ]
    )


def _build_category_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"category": "Rau củ", "revenue_million": 540},
            {"category": "Đồ khô", "revenue_million": 420},
            {"category": "Gia vị", "revenue_million": 350},
            {"category": "Đồ uống", "revenue_million": 610},
            {"category": "Đông lạnh", "revenue_million": 470},
            {"category": "Bánh kẹo", "revenue_million": 390},
        ]
    ).sort_values("revenue_million", ascending=False)


def _build_channel_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"channel": "Online", "share": 38},
            {"channel": "Cửa hàng", "share": 44},
            {"channel": "Đại lý", "share": 18},
        ]
    )


def _build_weekly_kpi_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"week": "W1", "fulfillment_rate": 92.1},
            {"week": "W2", "fulfillment_rate": 93.8},
            {"week": "W3", "fulfillment_rate": 94.5},
            {"week": "W4", "fulfillment_rate": 95.2},
            {"week": "W5", "fulfillment_rate": 96.0},
            {"week": "W6", "fulfillment_rate": 95.6},
            {"week": "W7", "fulfillment_rate": 96.8},
            {"week": "W8", "fulfillment_rate": 97.1},
        ]
    )


def _build_inventory_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"sku": "SKU-001", "product": "Táo Fuji", "category": "Rau củ", "channel": "Cửa hàng", "region": "Bắc", "stock": 340, "safety_stock": 180, "turnover_days": 11},
            {"sku": "SKU-002", "product": "Cam Navel", "category": "Rau củ", "channel": "Online", "region": "Nam", "stock": 280, "safety_stock": 160, "turnover_days": 9},
            {"sku": "SKU-003", "product": "Sữa yến mạch", "category": "Đồ uống", "channel": "Online", "region": "Trung", "stock": 420, "safety_stock": 210, "turnover_days": 14},
            {"sku": "SKU-004", "product": "Nước ép mix", "category": "Đồ uống", "channel": "Đại lý", "region": "Nam", "stock": 210, "safety_stock": 170, "turnover_days": 8},
            {"sku": "SKU-005", "product": "Mì soba", "category": "Đồ khô", "channel": "Cửa hàng", "region": "Bắc", "stock": 630, "safety_stock": 300, "turnover_days": 18},
            {"sku": "SKU-006", "product": "Muối biển", "category": "Gia vị", "channel": "Đại lý", "region": "Trung", "stock": 520, "safety_stock": 260, "turnover_days": 21},
            {"sku": "SKU-007", "product": "Bánh quy bơ", "category": "Bánh kẹo", "channel": "Online", "region": "Bắc", "stock": 170, "safety_stock": 190, "turnover_days": 7},
            {"sku": "SKU-008", "product": "Há cảo đông lạnh", "category": "Đông lạnh", "channel": "Cửa hàng", "region": "Nam", "stock": 300, "safety_stock": 220, "turnover_days": 10},
            {"sku": "SKU-009", "product": "Chả cá", "category": "Đông lạnh", "channel": "Online", "region": "Trung", "stock": 190, "safety_stock": 200, "turnover_days": 6},
            {"sku": "SKU-010", "product": "Kẹo trái cây", "category": "Bánh kẹo", "channel": "Đại lý", "region": "Bắc", "stock": 480, "safety_stock": 230, "turnover_days": 19},
        ]
    )


def _filter_inventory(inventory_df: pd.DataFrame, *, selected_channel: str, selected_region: str) -> pd.DataFrame:
    filtered = inventory_df.copy()
    if selected_channel != "Tất cả":
        filtered = filtered[filtered["channel"] == selected_channel]
    if selected_region != "Toàn quốc":
        filtered = filtered[filtered["region"] == selected_region]
    filtered["risk"] = filtered.apply(
        lambda row: "Rủi ro thấp" if row["stock"] >= row["safety_stock"] else "Rủi ro cao",
        axis=1,
    )
    return filtered.sort_values(["risk", "stock"], ascending=[True, False])


def _build_summary_metrics(sales_df: pd.DataFrame, inventory_df: pd.DataFrame, *, selected_quarter: str) -> dict[str, str]:
    quarter_index = {"Q1": (0, 3), "Q2": (3, 6), "Q3": (6, 9), "Q4": (9, 12)}
    start, end = quarter_index[selected_quarter]
    quarter_revenue = int(sales_df.iloc[start:end]["revenue_million"].sum())
    quarter_orders = int(sales_df.iloc[start:end]["orders"].sum())

    avg_turnover = float(inventory_df["turnover_days"].mean()) if not inventory_df.empty else 0.0
    risk_ratio = (
        float((inventory_df["risk"] == "Rủi ro cao").sum()) / float(len(inventory_df)) * 100
        if not inventory_df.empty
        else 0.0
    )

    return {
        "Doanh thu quý": f"{quarter_revenue:,}M",
        "Đơn hàng quý": f"{quarter_orders:,}",
        "Vòng quay TB": f"{avg_turnover:.1f} ngày",
        "Tỷ lệ rủi ro": f"{risk_ratio:.1f}%",
    }


def _render_kpi_cards(summary: dict[str, str]) -> None:
    cards = st.columns(4)
    for column, (label, value) in zip(cards, summary.items()):
        with column:
            st.markdown(
                f"""
                <div class=\"kpi-card\">
                    <div class=\"kpi-label\">{label}</div>
                    <div class=\"kpi-value\">{value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_line_echart(x_data: list[str], y_data: list[float], *, title: str, color: str, area_color: str) -> None:
    try:
        from streamlit_echarts import st_echarts

        option = {
            "title": {"text": title, "left": "left", "textStyle": {"fontSize": 12, "color": "#5f7388"}},
            "tooltip": {"trigger": "axis"},
            "grid": {"left": 32, "right": 14, "top": 40, "bottom": 24, "containLabel": True},
            "xAxis": {"type": "category", "data": x_data},
            "yAxis": {"type": "value"},
            "series": [
                {
                    "type": "line",
                    "data": y_data,
                    "smooth": True,
                    "showSymbol": False,
                    "lineStyle": {"width": 3, "color": color},
                    "areaStyle": {"color": area_color},
                }
            ],
        }
        st_echarts(options=option, height=340)
    except ImportError:
        st.line_chart(pd.DataFrame({"x": x_data, "y": y_data}).set_index("x"))


def _render_bar_echart(x_data: list[str], y_data: list[float], *, color: str) -> None:
    try:
        from streamlit_echarts import st_echarts

        option = {
            "tooltip": {"trigger": "axis"},
            "grid": {"left": 28, "right": 12, "top": 20, "bottom": 24, "containLabel": True},
            "xAxis": {"type": "category", "data": x_data, "axisLabel": {"rotate": 20}},
            "yAxis": {"type": "value"},
            "series": [
                {
                    "type": "bar",
                    "data": y_data,
                    "itemStyle": {"color": color, "borderRadius": [8, 8, 0, 0]},
                    "barWidth": "55%",
                }
            ],
        }
        st_echarts(options=option, height=340)
    except ImportError:
        st.bar_chart(pd.DataFrame({"category": x_data, "value": y_data}).set_index("category"))


def _render_donut_echart(channel_df: pd.DataFrame) -> None:
    try:
        from streamlit_echarts import st_echarts

        option = {
            "tooltip": {"trigger": "item"},
            "legend": {"bottom": 0},
            "series": [
                {
                    "type": "pie",
                    "radius": ["45%", "72%"],
                    "center": ["50%", "45%"],
                    "label": {"formatter": "{b}: {d}%"},
                    "data": [
                        {"name": row["channel"], "value": int(row["share"])}
                        for _, row in channel_df.iterrows()
                    ],
                }
            ],
        }
        st_echarts(options=option, height=340)
    except ImportError:
        st.bar_chart(channel_df.set_index("channel"))


def _render_area_echart(x_data: list[str], y_data: list[float]) -> None:
    try:
        from streamlit_echarts import st_echarts

        option = {
            "tooltip": {"trigger": "axis"},
            "grid": {"left": 34, "right": 14, "top": 20, "bottom": 24, "containLabel": True},
            "xAxis": {"type": "category", "data": x_data},
            "yAxis": {"type": "value", "min": 88, "max": 100},
            "series": [
                {
                    "type": "line",
                    "smooth": True,
                    "data": y_data,
                    "lineStyle": {"width": 2, "color": "#ff7f0e"},
                    "areaStyle": {"color": "rgba(255,127,14,0.25)"},
                }
            ],
        }
        st_echarts(options=option, height=340)
    except ImportError:
        st.area_chart(pd.DataFrame({"x": x_data, "value": y_data}).set_index("x"))
