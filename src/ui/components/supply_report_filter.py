from __future__ import annotations

import streamlit as st

from src.domain.supply_report_filter import (
    AXIS_DIMENSION_LABELS,
    AXIS_DIMENSIONS,
    AXIS_NONE,
    KPI_NAMES,
    SupplyReportFilter,
)

_REQUIRED_LABELS = [AXIS_DIMENSION_LABELS[dimension] for dimension in AXIS_DIMENSIONS]
_OPTIONAL_LABELS = [AXIS_NONE] + _REQUIRED_LABELS
_LABEL_TO_DIM = {label: dimension for dimension, label in AXIS_DIMENSION_LABELS.items()}


def _dim_index(dim_key: str, optional: bool = False) -> int:
    """Lấy vị trí index của dimension key trong danh sách options."""
    options = ([AXIS_NONE] + AXIS_DIMENSIONS) if optional else AXIS_DIMENSIONS
    try:
        return options.index(dim_key)
    except ValueError:
        return 0


def _find_duplicate_axes(axis_keys: list[str]) -> list[str]:
    """Trả về danh sách dimension bị chọn trùng (bỏ qua AXIS_NONE)."""
    seen: set[str] = set()
    duplicates: list[str] = []
    for axis_key in axis_keys:
        if not axis_key or axis_key == AXIS_NONE:
            continue
        if axis_key in seen and axis_key not in duplicates:
            duplicates.append(axis_key)
            continue
        seen.add(axis_key)
    return duplicates


def render_supply_report_filter(
    prefill: dict,
    metadata: dict[str, list[str]],
    is_running: bool = False,
) -> tuple[SupplyReportFilter, bool]:
    """Render UI filter + axis config cho supply report trong form submit."""
    with st.form("supply_report_filter_form", clear_on_submit=False):
        st.markdown("#### Bộ lọc (フィルター)")

        col1, col2, col3 = st.columns(3)

        with col1:
            product_opts = ["Tất cả"] + metadata.get("products", [])
            pref_prod = prefill.get("product_name") or "Tất cả"
            prod_idx = product_opts.index(pref_prod) if pref_prod in product_opts else 0
            product_val = st.selectbox(
                "Sản phẩm (商品)",
                product_opts,
                index=prod_idx,
                key="supply_filter_product",
            )
            product_name = None if product_val == "Tất cả" else product_val

        with col2:
            store_opts = ["Tất cả"] + metadata.get("stores", [])
            pref_store = prefill.get("store_name") or "Tất cả"
            store_idx = store_opts.index(pref_store) if pref_store in store_opts else 0
            store_val = st.selectbox(
                "Cửa hàng (店舗)",
                store_opts,
                index=store_idx,
                key="supply_filter_store",
            )
            store_name = None if store_val == "Tất cả" else store_val

        with col3:
            period_opts = ["Tất cả"] + metadata.get("periods", [])
            pref_period = prefill.get("period_id") or "Tất cả"
            period_idx = period_opts.index(pref_period) if pref_period in period_opts else 0
            period_val = st.selectbox(
                "Kỳ (期間)",
                period_opts,
                index=period_idx,
                key="supply_filter_period",
            )
            period_id = None if period_val == "Tất cả" else period_val

        col4, col5 = st.columns(2)

        with col4:
            class_opts = ["Tất cả", "定番", "家庭応援", "特売"]
            pref_class = prefill.get("classification") or "Tất cả"
            class_idx = class_opts.index(pref_class) if pref_class in class_opts else 0
            class_val = st.selectbox(
                "Phân loại (分類)",
                class_opts,
                index=class_idx,
                key="supply_filter_classification",
            )
            classification = None if class_val == "Tất cả" else class_val

        with col5:
            pref_eval = prefill.get("evaluation_items") or KPI_NAMES
            evaluation_items = st.multiselect(
                "Mục đánh giá (評価項目)",
                KPI_NAMES,
                default=[kpi for kpi in pref_eval if kpi in KPI_NAMES] or KPI_NAMES,
                key="supply_filter_eval_items",
            )
            if not evaluation_items:
                evaluation_items = KPI_NAMES

        st.markdown("---")
        st.markdown("#### Cấu hình trục (軸設定)")

        col_a, col_b, col_c, col_d = st.columns(4)

        with col_a:
            r1_pref = prefill.get("row_axis_1", "store_name")
            r1_label = st.selectbox(
                "Trục dọc 1 (縦軸①)",
                _REQUIRED_LABELS,
                index=_dim_index(r1_pref, optional=False),
                key="supply_axis_row1",
            )
            row_axis_1 = _LABEL_TO_DIM.get(r1_label, "store_name")

        with col_b:
            r2_pref = prefill.get("row_axis_2", AXIS_NONE)
            r2_label = st.selectbox(
                "Trục dọc 2 (縦軸②)",
                _OPTIONAL_LABELS,
                index=_dim_index(r2_pref, optional=True),
                key="supply_axis_row2",
            )
            row_axis_2 = AXIS_NONE if r2_label == AXIS_NONE else _LABEL_TO_DIM.get(r2_label, AXIS_NONE)

        with col_c:
            c1_pref = prefill.get("col_axis_1", "evaluation_item")
            c1_label = st.selectbox(
                "Trục ngang 1 (横軸①)",
                _REQUIRED_LABELS,
                index=_dim_index(c1_pref, optional=False),
                key="supply_axis_col1",
            )
            col_axis_1 = _LABEL_TO_DIM.get(c1_label, "evaluation_item")

        with col_d:
            c2_pref = prefill.get("col_axis_2", AXIS_NONE)
            c2_label = st.selectbox(
                "Trục ngang 2 (横軸②)",
                _OPTIONAL_LABELS,
                index=_dim_index(c2_pref, optional=True),
                key="supply_axis_col2",
            )
            col_axis_2 = AXIS_NONE if c2_label == AXIS_NONE else _LABEL_TO_DIM.get(c2_label, AXIS_NONE)

        duplicate_axes = _find_duplicate_axes([row_axis_1, row_axis_2, col_axis_1, col_axis_2])
        has_duplicate_axes = bool(duplicate_axes)
        if has_duplicate_axes:
            duplicate_labels = ", ".join(
                AXIS_DIMENSION_LABELS.get(axis_key, axis_key) for axis_key in duplicate_axes
            )
            st.warning(f"Các trục không được trùng nhau. Đang bị trùng: {duplicate_labels}.")

        submitted = st.form_submit_button(
            "Chạy báo cáo",
            type="primary",
            disabled=is_running,
        )

    return (
        SupplyReportFilter(
            product_name=product_name,
            store_name=store_name,
            period_id=period_id,
            classification=classification,
            evaluation_items=evaluation_items,
            row_axis_1=row_axis_1,
            row_axis_2=row_axis_2,
            col_axis_1=col_axis_1,
            col_axis_2=col_axis_2,
        ),
        submitted,
    )
