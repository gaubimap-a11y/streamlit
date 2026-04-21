from __future__ import annotations

from html import escape

import pandas as pd
import streamlit as st

from src.application.reporting.supply_report_service import (
    MatrixPayload,
    SupplyReportService,
    TOTAL_LABEL,
)

_RAW_COLS = ["_store_id", "_quantity_sold", "_sales_amount", "_customer_count"]
_FORMAT_0 = {"供給数量", "供給金額"}
_FORMAT_2 = {"平均売単価", "数量PI", "金額PI"}
_LEGACY_TAIL_COLUMN = "見切"

_DIM_DISPLAY: dict[str, str] = {
    "store_name": "Cửa hàng",
    "product_name": "Sản phẩm",
    "classification": "Phân loại",
    "period_id": "Kỳ",
    "evaluation_item": "Mục đánh giá",
}


def render_supply_report_table(payload: MatrixPayload) -> None:
    """Render matrix theo trục dọc và trục ngang đã chọn."""
    if payload.is_empty or payload.df.empty:
        st.info("Không tìm thấy dữ liệu phù hợp với bộ lọc và cấu hình trục đã chọn.")
        return

    st.markdown("### 📊 Cung ứng sản phẩm đơn lẻ (単品供給レポート)")

    row_dims = list(payload.row_axes)
    col_axes = list(payload.col_axes)
    kpi_cols = payload.kpi_cols

    normalized_row_dims, normalized_col_axes = _normalize_axes_for_layout(row_dims, col_axes)

    if _use_simple_vertical_layout(normalized_row_dims, normalized_col_axes):
        display_df = _build_simple_vertical_dataframe(
            df=payload.df,
            row_dim=normalized_row_dims[0],
            kpi_cols=kpi_cols,
        )
    else:
        display_df = _build_matrix_dataframe(
            df=payload.df,
            row_dims=normalized_row_dims,
            col_axes=normalized_col_axes,
            kpi_cols=kpi_cols,
        )
    _render_styled_dataframe(display_df, normalized_row_dims, kpi_cols)


def _normalize_axes_for_layout(
    row_dims: list[str],
    col_axes: list[str],
) -> tuple[list[str], list[str]]:
    """Giữ nguyên cấu hình trục; evaluation_item sẽ được bổ sung ở bước dựng cột."""
    effective_col_axes = [axis for axis in col_axes if axis]
    return row_dims, effective_col_axes


def _build_matrix_dataframe(
    df: pd.DataFrame,
    row_dims: list[str],
    col_axes: list[str],
    kpi_cols: list[str],
) -> pd.DataFrame:
    """Dựng bảng matrix cho report, giữ layout trục dọc giống màn cũ."""
    if "evaluation_item" in row_dims:
        return _build_matrix_dataframe_with_row_eval(
            df=df,
            row_dims=row_dims,
            col_axes=col_axes,
            kpi_cols=kpi_cols,
        )

    effective_col_axes = [axis for axis in col_axes if axis]
    if "evaluation_item" not in effective_col_axes:
        effective_col_axes.append("evaluation_item")
    non_eval_col_axes = [axis for axis in effective_col_axes if axis != "evaluation_item"]
    column_keys = _collect_column_keys(df, effective_col_axes, non_eval_col_axes, kpi_cols)
    metric_labels = [_column_label_from_key(key) for key in column_keys]
    is_vertical_legacy_layout = not non_eval_col_axes

    row_records: list[dict] = []
    if not row_dims:
        base_row = {
            label: value
            for label, value in zip(
                metric_labels,
                _metric_row_values(df, effective_col_axes, non_eval_col_axes, column_keys, kpi_cols),
            )
        }
        if is_vertical_legacy_layout:
            base_row[_LEGACY_TAIL_COLUMN] = 0
        row_records.append(base_row)
        columns = metric_labels + ([_LEGACY_TAIL_COLUMN] if is_vertical_legacy_layout else [])
        return pd.DataFrame(row_records, columns=columns)

    primary_dim = row_dims[0]
    detail_group_cols = row_dims

    for _, primary_df in df.groupby(primary_dim, sort=True):
        first_detail = True
        for detail_key, detail_df in primary_df.groupby(detail_group_cols, sort=True):
            detail_tuple = detail_key if isinstance(detail_key, tuple) else (detail_key,)
            row_record: dict[str, object] = {}
            for index, dim in enumerate(row_dims):
                dim_value = detail_tuple[index]
                if dim == primary_dim and not first_detail:
                    row_record[dim] = ""
                else:
                    row_record[dim] = dim_value
            first_detail = False
            metric_values = _metric_row_values(
                detail_df,
                effective_col_axes,
                non_eval_col_axes,
                column_keys,
                kpi_cols,
            )
            row_record.update({label: value for label, value in zip(metric_labels, metric_values)})
            if is_vertical_legacy_layout:
                row_record[_LEGACY_TAIL_COLUMN] = 0
            row_record["_is_total"] = False
            row_records.append(row_record)

        should_append_subtotal = len(row_dims) > 1 and not is_vertical_legacy_layout
        if should_append_subtotal:
            subtotal = {dim: "" for dim in row_dims}
            subtotal[row_dims[1]] = TOTAL_LABEL
            metric_values = _metric_row_values(
                primary_df,
                effective_col_axes,
                non_eval_col_axes,
                column_keys,
                kpi_cols,
            )
            subtotal.update({label: value for label, value in zip(metric_labels, metric_values)})
            subtotal["_is_total"] = True
            row_records.append(subtotal)

    ordered_columns = row_dims + metric_labels
    if is_vertical_legacy_layout:
        ordered_columns.append(_LEGACY_TAIL_COLUMN)
    ordered_columns.append("_is_total")
    return pd.DataFrame(row_records, columns=ordered_columns)


def _build_matrix_dataframe_with_row_eval(
    df: pd.DataFrame,
    row_dims: list[str],
    col_axes: list[str],
    kpi_cols: list[str],
) -> pd.DataFrame:
    """Dựng matrix khi `evaluation_item` nằm ở trục dọc."""
    effective_col_axes = [axis for axis in col_axes if axis]
    non_eval_col_axes = [axis for axis in effective_col_axes if axis != "evaluation_item"]
    column_keys = _collect_column_keys(df, effective_col_axes, non_eval_col_axes, kpi_cols)
    metric_labels = [_column_label_from_key(key) for key in column_keys]

    row_records: list[dict[str, object]] = []
    base_row_dims = [axis for axis in row_dims if axis != "evaluation_item"]

    if base_row_dims:
        grouped_rows = df.groupby(base_row_dims, sort=True)
    else:
        grouped_rows = [((), df)]

    def _append_row(base_map: dict[str, object], base_df: pd.DataFrame, eval_item: str) -> None:
        row_record: dict[str, object] = {}
        for dim in row_dims:
            if dim == "evaluation_item":
                row_record[dim] = eval_item
            else:
                row_record[dim] = base_map.get(dim, "")
        metric_values = _metric_row_values(
            base_df,
            effective_col_axes,
            non_eval_col_axes,
            column_keys,
            kpi_cols,
            row_eval_item=eval_item,
        )
        row_record.update({label: value for label, value in zip(metric_labels, metric_values)})
        row_record["_is_total"] = False
        row_records.append(row_record)

    grouped_entries: list[tuple[dict[str, object], pd.DataFrame]] = []
    for base_key, base_df in grouped_rows:
        base_tuple = base_key if isinstance(base_key, tuple) else (base_key,)
        base_map = dict(zip(base_row_dims, base_tuple))
        grouped_entries.append((base_map, base_df))

    # Bám đúng thứ tự trục dọc user chọn.
    # row1=evaluation_item -> KPI ngoài cùng, row2 bên trong (ví dụ Kỳ).
    if row_dims and row_dims[0] == "evaluation_item":
        for eval_item in kpi_cols:
            for base_map, base_df in grouped_entries:
                _append_row(base_map, base_df, eval_item)
    else:
        for base_map, base_df in grouped_entries:
            for eval_item in kpi_cols:
                _append_row(base_map, base_df, eval_item)

    if row_dims:
        primary_dim = row_dims[0]
        previous_value: object | None = None
        for row_record in row_records:
            current_value = row_record.get(primary_dim)
            if previous_value is not None and current_value == previous_value:
                row_record[primary_dim] = ""
            else:
                previous_value = current_value

    ordered_columns = row_dims + metric_labels + ["_is_total"]
    return pd.DataFrame(row_records, columns=ordered_columns)


def _use_simple_vertical_layout(row_dims: list[str], col_axes: list[str]) -> bool:
    """Case 1 trục dọc + ngang chỉ là mục đánh giá: render theo từng dimension value một hàng."""
    effective_col_axes = [axis for axis in col_axes if axis]
    return len(row_dims) == 1 and (
        not effective_col_axes or effective_col_axes == ["evaluation_item"]
    )


def _build_simple_vertical_dataframe(
    df: pd.DataFrame,
    row_dim: str,
    kpi_cols: list[str],
) -> pd.DataFrame:
    """Nhánh hiển thị đơn giản, ổn định cho layout chỉ có 1 trục dọc."""
    row_records: list[dict[str, object]] = []

    for dim_value, group_df in df.groupby(row_dim, sort=True):
        row_record: dict[str, object] = {row_dim: dim_value}
        metrics = SupplyReportService.aggregate_group_metrics(group_df, kpi_cols)
        for kpi in kpi_cols:
            row_record[kpi] = metrics[kpi]
        row_record[_LEGACY_TAIL_COLUMN] = 0
        row_record["_is_total"] = False
        row_records.append(row_record)

    ordered_columns = [row_dim] + list(kpi_cols) + [_LEGACY_TAIL_COLUMN, "_is_total"]
    return pd.DataFrame(row_records, columns=ordered_columns)


def _collect_column_keys(
    df: pd.DataFrame,
    effective_col_axes: list[str],
    non_eval_col_axes: list[str],
    kpi_cols: list[str],
) -> list[tuple[str, ...]]:
    """Thu thập danh sách cột động theo trục ngang và KPI."""
    if not non_eval_col_axes:
        return [tuple([kpi]) for kpi in kpi_cols]

    distinct_cols = (
        df[non_eval_col_axes]
        .drop_duplicates()
        .sort_values(non_eval_col_axes, kind="stable")
        .itertuples(index=False, name=None)
    )

    column_keys: list[tuple[str, ...]] = []
    for combo in distinct_cols:
        combo_map = dict(zip(non_eval_col_axes, combo))
        if "evaluation_item" in effective_col_axes:
            for kpi in kpi_cols:
                key = tuple(combo_map[axis] if axis != "evaluation_item" else kpi for axis in effective_col_axes)
                column_keys.append(key)
        else:
            key = tuple(combo_map[axis] for axis in effective_col_axes)
            column_keys.append(key)

    # Sắp thứ tự theo đúng cấu hình trục ngang user chọn.
    # Ví dụ:
    # - [period_id, evaluation_item] -> 2024-03/[KPI...], 2024-04/[KPI...]
    # - [evaluation_item, period_id] -> KPI1/[2024-03, 2024-04], KPI2/[...]
    kpi_order = {kpi: index for index, kpi in enumerate(kpi_cols)}
    axis_value_order: dict[str, dict[object, int]] = {}
    for axis in non_eval_col_axes:
        axis_values = (
            df[[axis]]
            .drop_duplicates()
            .sort_values(axis, kind="stable")[axis]
            .tolist()
        )
        axis_value_order[axis] = {value: index for index, value in enumerate(axis_values)}

    def _sort_key(key: tuple[str, ...]) -> tuple[int, ...]:
        orders: list[int] = []
        for axis, value in zip(effective_col_axes, key):
            if axis == "evaluation_item":
                orders.append(kpi_order.get(str(value), len(kpi_order)))
            else:
                axis_orders = axis_value_order.get(axis, {})
                orders.append(axis_orders.get(value, len(axis_orders)))
        return tuple(orders)

    return sorted(column_keys, key=_sort_key)


def _metric_row_values(
    df: pd.DataFrame,
    effective_col_axes: list[str],
    non_eval_col_axes: list[str],
    column_keys: list[tuple[str, ...]],
    kpi_cols: list[str],
    row_eval_item: str | None = None,
) -> list[object]:
    """Tính giá trị cho từng cột động của một hàng matrix."""
    if df.empty:
        return [""] * len(column_keys)

    key_to_value: dict[tuple[str, ...], object] = {}
    if not non_eval_col_axes:
        metrics = SupplyReportService.aggregate_group_metrics(df, kpi_cols)
        for kpi in kpi_cols:
            key_to_value[(kpi,)] = metrics[kpi]
    else:
        for combo, combo_df in df.groupby(non_eval_col_axes, sort=True):
            combo_tuple = combo if isinstance(combo, tuple) else (combo,)
            combo_map = dict(zip(non_eval_col_axes, combo_tuple))
            metrics = SupplyReportService.aggregate_group_metrics(combo_df, kpi_cols)
            if "evaluation_item" in effective_col_axes:
                for kpi in kpi_cols:
                    key = tuple(combo_map[axis] if axis != "evaluation_item" else kpi for axis in effective_col_axes)
                    key_to_value[key] = metrics[kpi]
            else:
                selected_kpi = row_eval_item or (kpi_cols[0] if kpi_cols else "")
                key = tuple(combo_map[axis] for axis in effective_col_axes)
                key_to_value[key] = metrics.get(selected_kpi, "")

    return [key_to_value.get(key, "") for key in column_keys]


def _column_label_from_key(key: tuple[str, ...]) -> str:
    return " / ".join(str(part) for part in key)


def _render_styled_dataframe(df: pd.DataFrame, row_dims: list[str], kpi_cols: list[str]) -> None:
    if df.empty:
        st.info("Không có dữ liệu để hiển thị.")
        return

    is_total = df.get("_is_total", pd.Series(dtype=bool)).fillna(False).astype(bool).reset_index(drop=True)
    display = df.drop(columns=["_is_total"], errors="ignore").copy()
    display = _normalize_total_label_layout(display, row_dims)
    display = display.rename(columns=_DIM_DISPLAY)
    display = _build_display_columns(display, row_dims)

    row_headers = {_DIM_DISPLAY.get(dim, dim) for dim in row_dims}
    for col in display.columns:
        col_name = col[-1] if isinstance(col, tuple) else col
        if col_name in row_headers:
            continue
        kpi_name = col_name.split(" / ")[-1]
        if kpi_name in _FORMAT_0:
            display[col] = display[col].apply(
                lambda value: f"{value:,.0f}" if isinstance(value, (int, float)) else str(value)
            )
        elif kpi_name in _FORMAT_2:
            display[col] = display[col].apply(
                lambda value: f"{value:,.2f}" if isinstance(value, (int, float)) else str(value)
            )

    st.markdown(_render_html_table(display, is_total, row_dims), unsafe_allow_html=True)


def _build_display_columns(display: pd.DataFrame, row_dims: list[str]) -> pd.DataFrame:
    """Đổi cột phẳng sang header 2 tầng để renderer HTML merge thật bằng colspan."""
    row_headers = {_DIM_DISPLAY.get(dim, dim) for dim in row_dims}
    if not any(isinstance(col, str) and " / " in col for col in display.columns if col not in row_headers):
        return display

    multi_columns: list[tuple[str, str]] = []
    for col in display.columns:
        col_name = str(col)
        if col in row_headers:
            multi_columns.append(("", col_name))
            continue

        if isinstance(col, str) and " / " in col:
            top, bottom = col.split(" / ", 1)
            multi_columns.append((top, bottom))
        else:
            multi_columns.append(("", col_name))

    display.columns = pd.MultiIndex.from_tuples(multi_columns)
    return display


def _render_html_table(display: pd.DataFrame, is_total: pd.Series, row_dims: list[str]) -> str:
    """Render bảng HTML để merge header thật bằng colspan."""
    classes = "supply-report-table"
    html_parts = [f'<div class="{classes}__wrap"><table class="{classes}">']

    if isinstance(display.columns, pd.MultiIndex):
        top_labels = [str(col[0]) for col in display.columns]
        bottom_labels = [str(col[1]) for col in display.columns]
        html_parts.append("<thead><tr>")
        for label, span in _collapse_header_spans(top_labels):
            header_text = escape(label) if label else "&nbsp;"
            html_parts.append(
                f'<th class="{classes}__group" colspan="{span}">{header_text}</th>'
            )
        html_parts.append("</tr><tr>")
        for label in bottom_labels:
            html_parts.append(f'<th class="{classes}__subhead">{escape(label)}</th>')
        html_parts.append("</tr></thead>")
    else:
        html_parts.append("<thead><tr>")
        for label in display.columns:
            html_parts.append(f'<th class="{classes}__subhead">{escape(str(label))}</th>')
        html_parts.append("</tr></thead>")

    html_parts.append("<tbody>")
    total_flags = is_total.tolist()
    row_header_count = len(row_dims)
    for row_index, row in display.iterrows():
        row_class = f' class="{classes}__total"' if total_flags[row_index] else ""
        html_parts.append(f"<tr{row_class}>")
        for col_index, value in enumerate(row.tolist()):
            is_total_row = total_flags[row_index]
            if is_total_row and row_header_count > 1 and col_index == 0:
                html_parts.append("<td></td>")
                continue

            if col_index < row_header_count and value == "":
                continue

            cell = "" if value is None else str(value)
            rowspan_attr = ""
            if col_index < row_header_count and cell and not is_total_row:
                rowspan = _rowspan_for_group(display, total_flags, row_index, col_index)
                if rowspan > 1:
                    rowspan_attr = f' rowspan="{rowspan}"'
            html_parts.append(f"<td{rowspan_attr}>{escape(cell)}</td>")
        html_parts.append("</tr>")
    html_parts.append("</tbody></table></div>")
    return "".join(html_parts)


def _collapse_header_spans(labels: list[str]) -> list[tuple[str, int]]:
    if not labels:
        return []

    spans: list[tuple[str, int]] = []
    current = labels[0]
    count = 1
    for label in labels[1:]:
        if label == current:
            count += 1
            continue
        spans.append((current, count))
        current = label
        count = 1
    spans.append((current, count))
    return spans


def _rowspan_for_group(
    display: pd.DataFrame,
    total_flags: list[bool],
    row_index: int,
    col_index: int,
) -> int:
    """Tính rowspan cho cột dọc chính dựa trên các dòng trống kế tiếp."""
    if col_index != 0:
        return 1

    span = 1
    next_index = row_index + 1
    while next_index < len(display):
        if total_flags[next_index]:
            break
        next_value = display.iat[next_index, col_index]
        if next_value != "":
            break
        span += 1
        next_index += 1
    return span


def _normalize_total_label_layout(display: pd.DataFrame, row_dims: list[str]) -> pd.DataFrame:
    """Tách `総供給` sang cột trục dọc kế bên nếu đang bị dồn chung trong một ô."""
    if len(row_dims) < 2:
        return display

    normalized = display.copy()
    for index in normalized.index:
        for dim_index, dim in enumerate(row_dims[:-1]):
            value = normalized.at[index, dim]
            if not isinstance(value, str):
                continue
            suffix = f" {TOTAL_LABEL}"
            if not value.endswith(suffix):
                continue
            next_dim = row_dims[dim_index + 1]
            next_value = normalized.at[index, next_dim]
            normalized.at[index, dim] = ""
            if not next_value:
                normalized.at[index, next_dim] = TOTAL_LABEL
            break

    return normalized
