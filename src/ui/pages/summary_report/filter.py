from __future__ import annotations

import re
import zipfile
from datetime import date, timedelta
from io import BytesIO
from typing import Any
from xml.etree import ElementTree as ET

import streamlit as st
import pandas as pd
from streamlit_tree_select import tree_select

from src.application.reporting.summary_axis_service import SummaryAxisService
from src.application.product.product_service import ProductService
from src.application.report_filters.report_filter_service import ReportFilterService
from src.core.exceptions import BusinessRuleError, DataAccessError
from src.domain.filters import ProductFilter
from src.domain.report_filters import (
    RecipientStatus,
    ReportFilterDefinition,
    SUMMARY_REPORT_CODE,
    SharedReportFilter,
    parse_report_filter_payload,
)
from src.ui.session.auth_session import get_current_username
from src.ui.session.filter_store import FilterStore

_SUMMARY_CODE_INPUT_QUERY_KEY = "summary_code_input_query"
_SUMMARY_CODE_INPUT_ITEMS_KEY = "summary_code_input_items"
_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY = "summary_code_input_selected_items"
_SUMMARY_CODE_INPUT_STATUS_KEY = "summary_code_input_status"
_SUMMARY_CODE_INPUT_STATUS_KIND_KEY = "summary_code_input_status_kind"
_SUMMARY_CODE_INPUT_AVAILABLE_PICK_KEY = "summary_code_input_available_pick"
_SUMMARY_CODE_INPUT_SELECTED_PICK_KEY = "summary_code_input_selected_pick"
_SUMMARY_CODE_INPUT_AVAILABLE_SIGNATURE_KEY = "summary_code_input_available_signature"
_SUMMARY_CODE_INPUT_SELECTED_SIGNATURE_KEY = "summary_code_input_selected_signature"
_SUMMARY_CODE_INPUT_AVAILABLE_CHECKBOX_KEYS = "summary_code_input_available_checkbox_keys"
_SUMMARY_CODE_INPUT_SELECTED_CHECKBOX_KEYS = "summary_code_input_selected_checkbox_keys"
_SUMMARY_CODE_INPUT_SHOW_SELECTED_DETAIL_KEY = "summary_code_input_show_selected_detail"
_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY = "summary_code_input_detail_map"
_SUMMARY_FILE_INPUT_UPLOADER_KEY = "summary_file_input_uploader"
_SUMMARY_FILE_INPUT_STATUS_KEY = "summary_file_input_status"
_SUMMARY_FILE_INPUT_STATUS_KIND_KEY = "summary_file_input_status_kind"
_SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY = "summary_file_input_preview_rows"
_SUMMARY_FILE_INPUT_MATCHED_IDS_KEY = "summary_file_input_matched_ids"
_SUMMARY_FUZZY_QUERY_KEY = "summary_fuzzy_query"
_SUMMARY_FUZZY_SEARCH_BY_KEY = "summary_fuzzy_search_by"
_SUMMARY_FUZZY_MATCH_MODE_KEY = "summary_fuzzy_match_mode"
_SUMMARY_FUZZY_CATEGORY_ENABLED_KEY = "summary_fuzzy_category_enabled"
_SUMMARY_FUZZY_CATEGORY_KEY = "summary_fuzzy_category"
_SUMMARY_FUZZY_PARTNER_ENABLED_KEY = "summary_fuzzy_partner_enabled"
_SUMMARY_FUZZY_PARTNER_KEY = "summary_fuzzy_partner"
_SUMMARY_FUZZY_ITEMS_KEY = "summary_fuzzy_items"
_SUMMARY_FUZZY_SELECTED_ITEMS_KEY = "summary_fuzzy_selected_items"
_SUMMARY_FUZZY_AVAILABLE_PICK_KEY = "summary_fuzzy_available_pick"
_SUMMARY_FUZZY_SELECTED_PICK_KEY = "summary_fuzzy_selected_pick"
_SUMMARY_FUZZY_AVAILABLE_SIGNATURE_KEY = "summary_fuzzy_available_signature"
_SUMMARY_FUZZY_SELECTED_SIGNATURE_KEY = "summary_fuzzy_selected_signature"
_SUMMARY_FUZZY_AVAILABLE_CHECKBOX_KEYS = "summary_fuzzy_available_checkbox_keys"
_SUMMARY_FUZZY_SELECTED_CHECKBOX_KEYS = "summary_fuzzy_selected_checkbox_keys"
_SUMMARY_CUSTOM_STATUS_KEY = "summary_custom_category_status"
_SUMMARY_CUSTOM_STATUS_KIND_KEY = "summary_custom_category_status_kind"
_SUMMARY_CUSTOM_AVAILABLE_PICK_KEY = "summary_custom_available_pick"
_SUMMARY_CUSTOM_SELECTED_PICK_KEY = "summary_custom_selected_pick"
_SUMMARY_CUSTOM_AVAILABLE_SIGNATURE_KEY = "summary_custom_available_signature"
_SUMMARY_CUSTOM_SELECTED_SIGNATURE_KEY = "summary_custom_selected_signature"
_SUMMARY_CUSTOM_AVAILABLE_CHECKBOX_KEYS = "summary_custom_available_checkbox_keys"
_SUMMARY_CUSTOM_SELECTED_CHECKBOX_KEYS = "summary_custom_selected_checkbox_keys"
_SUMMARY_CUSTOM_SHOW_SELECTED_DETAIL_KEY = "summary_custom_show_selected_detail"
_SUMMARY_CUSTOM_TREE_CHECKED_KEY = "summary_custom_tree_checked"
_SUMMARY_CUSTOM_TREE_EXPANDED_KEY = "summary_custom_tree_expanded"
_SUMMARY_CUSTOM_TREE_SOURCE_READY_KEY = "summary_custom_tree_source_ready"
_SUMMARY_CUSTOM_TREE_CATEGORIES_KEY = "summary_custom_tree_categories"
_SUMMARY_CUSTOM_TREE_QUERY_KEY = "summary_custom_tree_query"
_SUMMARY_AXIS_SELECTED_STORE_IDS_KEY = "summary_axis_selected_store_ids"
_SUMMARY_AXIS_SELECTED_STORE_LABELS_KEY = "summary_axis_selected_store_labels"
_SUMMARY_AXIS_TREE_CHECKED_KEY = "summary_axis_tree_checked"
_SUMMARY_AXIS_TREE_EXPANDED_KEY = "summary_axis_tree_expanded"
_SUMMARY_AXIS_TREE_MODE_KEY = "summary_axis_tree_mode"
_SUMMARY_AXIS_TREE_QUERY_KEY = "summary_axis_tree_query"
_SUMMARY_AXIS_TREE_COMPONENT_KEY = "summary_axis_tree_component"
_SUMMARY_AXIS_TREE_COMPONENT_VERSION_KEY = "summary_axis_tree_component_version"
_SUMMARY_AXIS_TREE_IGNORE_COMPONENT_ONCE_KEY = "summary_axis_tree_ignore_component_once"
_SUMMARY_AXIS_SELECTED_PICK_KEY = "summary_axis_selected_pick"
_SUMMARY_AXIS_SELECTED_SIGNATURE_KEY = "summary_axis_selected_signature"
_SUMMARY_AXIS_SELECTED_CHECKBOX_KEYS = "summary_axis_selected_checkbox_keys"
_SUMMARY_AXIS_LABEL_TO_ID_MAP_KEY = "summary_axis_label_to_id_map"
_SUMMARY_AXIS_DIALOG_OPEN_KEY = "summary_axis_type_dialog_open"
_SUMMARY_AXIS_STATUS_KEY = "summary_axis_status"
_SUMMARY_AXIS_STATUS_KIND_KEY = "summary_axis_status_kind"
_SUMMARY_AXIS_TYPE_KEY = "summary_axis_type"
_SUMMARY_DATA_TYPE_KEY = "summary_data_type"
_SUMMARY_PREVIOUS_DATA_TYPE_KEY = "summary_previous_data_type"
_SUMMARY_DATA_MONTH_FROM_KEY = "summary_data_month_from"
_SUMMARY_DATA_MONTH_TO_KEY = "summary_data_month_to"
_SUMMARY_PERIOD_FROM_YEAR_KEY = "summary_period_from_year"
_SUMMARY_PERIOD_FROM_MONTH_KEY = "summary_period_from_month"
_SUMMARY_PERIOD_FROM_DAY_KEY = "summary_period_from_day"
_SUMMARY_PERIOD_TO_YEAR_KEY = "summary_period_to_year"
_SUMMARY_PERIOD_TO_MONTH_KEY = "summary_period_to_month"
_SUMMARY_PERIOD_TO_DAY_KEY = "summary_period_to_day"
_SUMMARY_WEEK_PERIOD_CHECKBOX_KEYS_KEY = "summary_week_period_checkbox_keys"
_SUMMARY_WEEK_PERIOD_KEY_TO_LABEL_KEY = "summary_week_period_key_to_label"
_SUMMARY_WEEK_PERIOD_SELECTION_ORDER_KEY = "summary_week_period_selection_order"
_SUMMARY_WEEK_PERIOD_SELECTED_LABELS_KEY = "summary_week_period_selected_labels"
_SUMMARY_WEEK_PERIOD_SIGNATURE_KEY = "summary_week_period_signature"
_SUMMARY_WEEK_PERIOD_CHECKBOX_PREFIX = "summary_week_period_chk"
_SUMMARY_WEEK_PERIOD_CHECKBOX_KEY_PREFIX = f"{_SUMMARY_WEEK_PERIOD_CHECKBOX_PREFIX}_"
_SUMMARY_WEEK_PERIOD_PICKER_OK_BTN_KEY = "summary_week_period_picker_ok"
_SUMMARY_CUSTOM_TREE_COMPONENT_KEY = "summary_tree_component"
_SUMMARY_REPORT_LOGOUT_BTN_KEY = "summary_report_logout"
_SUMMARY_SUPPLY_CATEGORY_BTN_KEY = "summary_supply_category_btn"
_SUMMARY_SELECT_PERIOD_BTN_KEY = "summary_select_period_btn"
_SUMMARY_FIELD_SELECTOR_BTN_KEY = "summary_field_selector_btn"
_SUMMARY_LOAD_FILE_BTN_KEY = "summary_load_file_btn"
_SUMMARY_FUZZY_SEARCH_BTN_KEY = "summary_fuzzy_search_btn"
_SUMMARY_CUSTOM_CATEGORY_BTN_KEY = "summary_custom_category_btn"
_SUMMARY_CUSTOM_CATEGORY_OK_BTN_KEY = "summary_custom_category_ok_btn"
_SUMMARY_CUSTOM_CATEGORY_CANCEL_BTN_KEY = "summary_custom_category_cancel_btn"
_SUMMARY_CUSTOM_MOVE_ALL_RIGHT_BTN_KEY = "summary_custom_move_all_right_btn"
_SUMMARY_CUSTOM_MOVE_RIGHT_BTN_KEY = "summary_custom_move_right_btn"
_SUMMARY_CUSTOM_MOVE_LEFT_BTN_KEY = "summary_custom_move_left_btn"
_SUMMARY_CUSTOM_MOVE_ALL_LEFT_BTN_KEY = "summary_custom_move_all_left_btn"
_SUMMARY_CUSTOM_MOVE_RIGHT_TREE_BTN_KEY = "summary_custom_move_right_tree_btn"
_SUMMARY_CUSTOM_REMOVE_SELECTED_BTN_KEY = "summary_custom_remove_selected_btn"
_SUMMARY_CUSTOM_SHOW_SELECTED_DETAIL_BTN_KEY = "summary_custom_show_selected_detail_btn"
_SUMMARY_CODE_INPUT_BTN_KEY = "summary_code_input_btn"
_SUMMARY_RESET_DEFAULT_BTN_KEY = "summary_reset_default_btn"
_SUMMARY_SAVE_FILTER_BTN_KEY = "summary_save_filter_btn"
_SUMMARY_OPEN_FILTER_BTN_KEY = "summary_open_filter_btn"
_SUMMARY_SETTING_BTN_KEY = "summary_setting_btn"
_SUMMARY_START_BTN_KEY = "summary_start_btn"
_SUMMARY_CLOSE_BTN_KEY = "summary_close_btn"
_SUMMARY_FUZZY_SEARCH_SUBMIT_BTN_KEY = "summary_fuzzy_search_submit_btn"
_SUMMARY_CODE_INPUT_MOVE_ALL_RIGHT_BTN_KEY = "summary_code_input_move_all_right_btn"
_SUMMARY_CODE_INPUT_MOVE_RIGHT_BTN_KEY = "summary_code_input_move_right_btn"
_SUMMARY_CODE_INPUT_MOVE_LEFT_BTN_KEY = "summary_code_input_move_left_btn"
_SUMMARY_CODE_INPUT_MOVE_ALL_LEFT_BTN_KEY = "summary_code_input_move_all_left_btn"
_SUMMARY_CODE_INPUT_OK_BTN_KEY = "summary_code_input_ok_btn"
_SUMMARY_CODE_INPUT_SHOW_SELECTED_DETAIL_BTN_KEY = "summary_code_input_show_selected_detail_btn"
_SUMMARY_FILE_INPUT_OK_BTN_KEY = "summary_file_input_ok_btn"
_SUMMARY_FILE_INPUT_CANCEL_BTN_KEY = "summary_file_input_cancel_btn"
_SUMMARY_FUZZY_MOVE_ALL_RIGHT_BTN_KEY = "summary_fuzzy_move_all_right_btn"
_SUMMARY_FUZZY_MOVE_RIGHT_BTN_KEY = "summary_fuzzy_move_right_btn"
_SUMMARY_FUZZY_MOVE_LEFT_BTN_KEY = "summary_fuzzy_move_left_btn"
_SUMMARY_FUZZY_MOVE_ALL_LEFT_BTN_KEY = "summary_fuzzy_move_all_left_btn"
_SUMMARY_FUZZY_OK_BTN_KEY = "summary_fuzzy_ok_btn"
_SUMMARY_FUZZY_CANCEL_BTN_KEY = "summary_fuzzy_cancel_btn"
_SUMMARY_TRANSIENT_WIDGET_KEYS = {
    _SUMMARY_WEEK_PERIOD_CHECKBOX_KEYS_KEY,
    _SUMMARY_WEEK_PERIOD_KEY_TO_LABEL_KEY,
    _SUMMARY_WEEK_PERIOD_SELECTION_ORDER_KEY,
    _SUMMARY_WEEK_PERIOD_SIGNATURE_KEY,
    _SUMMARY_REPORT_LOGOUT_BTN_KEY,
    _SUMMARY_SUPPLY_CATEGORY_BTN_KEY,
    _SUMMARY_SELECT_PERIOD_BTN_KEY,
    _SUMMARY_FIELD_SELECTOR_BTN_KEY,
    _SUMMARY_LOAD_FILE_BTN_KEY,
    _SUMMARY_FUZZY_SEARCH_BTN_KEY,
    _SUMMARY_CUSTOM_CATEGORY_BTN_KEY,
    _SUMMARY_CUSTOM_CATEGORY_OK_BTN_KEY,
    _SUMMARY_CUSTOM_CATEGORY_CANCEL_BTN_KEY,
    _SUMMARY_CUSTOM_MOVE_ALL_RIGHT_BTN_KEY,
    _SUMMARY_CUSTOM_MOVE_RIGHT_BTN_KEY,
    _SUMMARY_CUSTOM_MOVE_LEFT_BTN_KEY,
    _SUMMARY_CUSTOM_MOVE_ALL_LEFT_BTN_KEY,
    _SUMMARY_CUSTOM_MOVE_RIGHT_TREE_BTN_KEY,
    _SUMMARY_CUSTOM_REMOVE_SELECTED_BTN_KEY,
    _SUMMARY_CUSTOM_SHOW_SELECTED_DETAIL_BTN_KEY,
    _SUMMARY_CODE_INPUT_BTN_KEY,
    _SUMMARY_RESET_DEFAULT_BTN_KEY,
    _SUMMARY_SAVE_FILTER_BTN_KEY,
    _SUMMARY_OPEN_FILTER_BTN_KEY,
    _SUMMARY_SETTING_BTN_KEY,
    _SUMMARY_START_BTN_KEY,
    _SUMMARY_CLOSE_BTN_KEY,
    _SUMMARY_FUZZY_SEARCH_SUBMIT_BTN_KEY,
    _SUMMARY_WEEK_PERIOD_PICKER_OK_BTN_KEY,
    _SUMMARY_CODE_INPUT_MOVE_ALL_RIGHT_BTN_KEY,
    _SUMMARY_CODE_INPUT_MOVE_RIGHT_BTN_KEY,
    _SUMMARY_CODE_INPUT_MOVE_LEFT_BTN_KEY,
    _SUMMARY_CODE_INPUT_MOVE_ALL_LEFT_BTN_KEY,
    _SUMMARY_CODE_INPUT_OK_BTN_KEY,
    _SUMMARY_CODE_INPUT_SHOW_SELECTED_DETAIL_BTN_KEY,
    _SUMMARY_FILE_INPUT_OK_BTN_KEY,
    _SUMMARY_FILE_INPUT_CANCEL_BTN_KEY,
    _SUMMARY_FUZZY_MOVE_ALL_RIGHT_BTN_KEY,
    _SUMMARY_FUZZY_MOVE_RIGHT_BTN_KEY,
    _SUMMARY_FUZZY_MOVE_LEFT_BTN_KEY,
    _SUMMARY_FUZZY_MOVE_ALL_LEFT_BTN_KEY,
    _SUMMARY_FUZZY_OK_BTN_KEY,
    _SUMMARY_FUZZY_CANCEL_BTN_KEY,
}


@st.cache_data(show_spinner=False, ttl=30)
def _get_cached_filter_list(report_code: str, actor_user_id: str, cache_token: int, _service: Any):
    return _service.list_saved_filters(report_code, actor_user_id)


def _toast(message: str, kind: str = "info") -> None:
    text = str(message or "").strip()
    if not text:
        return
    icon_map = {
        "success": "✅",
        "warning": "⚠️",
        "error": "❗",
        "info": "ℹ️",
    }
    st.toast(text, icon=icon_map.get(str(kind or "").strip().lower(), "ℹ️"))


def _toast_and_clear_status(kind_key: str, message_key: str) -> None:
    kind = str(st.session_state.get(kind_key, "info") or "info").strip().lower()
    message = str(st.session_state.get(message_key, "") or "").strip()
    if message:
        _toast(message, kind=kind)
    st.session_state[message_key] = ""
    st.session_state[kind_key] = None


@st.cache_data(show_spinner=False, ttl=300)
def _get_cached_product_categories(_service: Any) -> list[str]:
    return [str(item) for item in _service.get_categories() if str(item).strip()]


@st.cache_data(show_spinner=False, ttl=180)
def _get_cached_products_by_category(category: str, page_limit: int, _service: Any) -> list[dict[str, Any]]:
    category_text = str(category or "").strip()
    if not category_text:
        return []

    product_filter = ProductFilter(category=category_text)
    total_count = int(_service.get_total_count(product_filter))
    if total_count <= 0:
        return []

    page_size = 10
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    max_pages = min(total_pages, max(1, int(page_limit)))
    pages = list(range(1, max_pages + 1))
    page_map = _service.get_product_pages(product_filter, pages)

    products: list[dict[str, Any]] = []
    for page in pages:
        for item in page_map.get(page, []):
            products.append(
                {
                    "product_id": getattr(item, "product_id", None),
                    "product_name": getattr(item, "product_name", ""),
                    "category": getattr(item, "category", ""),
                    "price": getattr(item, "price", None),
                    "unit": getattr(item, "unit", ""),
                    "description": getattr(item, "description", ""),
                    "stock_quantity": getattr(item, "stock_quantity", None),
                }
            )
    return products


@st.cache_data(show_spinner=False, ttl=180)
def _get_cached_summary_axis_rows(_service: Any) -> list[tuple]:
    return list(_service.get_store_hierarchy_rows())

def _month_options(start_year: int = 2024, end_year: int | None = None) -> list[str]:
    today = date.today()
    if end_year is None:
        end_year = max(start_year, today.year)

    options: list[str] = []
    for year in range(start_year, end_year + 1):
        month_limit = 12
        if year == today.year and end_year == today.year:
            month_limit = today.month

        for month in range(1, month_limit + 1):
            options.append(f"{year:04d}/{month:02d}")
    return options


def _month_bounds(month_from: str, month_to: str) -> tuple[date, date]:
    from_year, from_month = (int(part) for part in month_from.split("/"))
    to_year, to_month = (int(part) for part in month_to.split("/"))

    start_month = date(from_year, from_month, 1)
    end_month = date(to_year, to_month, 1)
    if start_month > end_month:
        start_month, end_month = end_month, start_month

    if end_month.month == 12:
        next_month = date(end_month.year + 1, 1, 1)
    else:
        next_month = date(end_month.year, end_month.month + 1, 1)
    end_date = next_month - timedelta(days=1)
    return start_month, end_date


def _weekly_period_options(month_from: str, month_to: str) -> list[tuple[str, date, date]]:
    start_date, end_date = _month_bounds(month_from, month_to)
    first_week_start = start_date - timedelta(days=start_date.weekday())

    options: list[tuple[str, date, date]] = []
    cursor = first_week_start
    while cursor <= end_date:
        week_start = cursor
        week_end = cursor + timedelta(days=6)

        if week_end >= start_date and week_start <= end_date:
            week_code = week_start.isocalendar()
            label = (
                f"{week_code.year}/{week_code.week:02d}週 "
                f"({week_start.month:02d}月{week_start.day:02d}日～{week_end.month:02d}月{week_end.day:02d}日)"
            )
            options.append((label, week_start, week_end))

        cursor += timedelta(days=7)

    return options


def _safe_month_value(year_value: str | None, month_value: str | None) -> str | None:
    year_text = str(year_value or "").strip()
    month_text = str(month_value or "").strip()
    if not year_text or not month_text:
        return None
    if not (year_text.isdigit() and month_text.isdigit()):
        return None
    year_num = int(year_text)
    month_num = int(month_text)
    if month_num < 1 or month_num > 12:
        return None
    return f"{year_num:04d}/{month_num:02d}"


def _sync_header_month_range() -> None:
    from_month = _safe_month_value(
        st.session_state.get(_SUMMARY_PERIOD_FROM_YEAR_KEY),
        st.session_state.get(_SUMMARY_PERIOD_FROM_MONTH_KEY),
    )
    to_month = _safe_month_value(
        st.session_state.get(_SUMMARY_PERIOD_TO_YEAR_KEY),
        st.session_state.get(_SUMMARY_PERIOD_TO_MONTH_KEY),
    )

    if from_month and to_month:
        from_key = tuple(int(part) for part in from_month.split("/"))
        to_key = tuple(int(part) for part in to_month.split("/"))
        if from_key <= to_key:
            st.session_state[_SUMMARY_DATA_MONTH_FROM_KEY] = from_month
            st.session_state[_SUMMARY_DATA_MONTH_TO_KEY] = to_month
        else:
            st.session_state[_SUMMARY_DATA_MONTH_FROM_KEY] = to_month
            st.session_state[_SUMMARY_DATA_MONTH_TO_KEY] = from_month


def _reset_summary_period_values() -> None:
    st.session_state[_SUMMARY_PERIOD_FROM_YEAR_KEY] = ""
    st.session_state[_SUMMARY_PERIOD_FROM_MONTH_KEY] = ""
    st.session_state[_SUMMARY_PERIOD_FROM_DAY_KEY] = ""
    st.session_state[_SUMMARY_PERIOD_TO_YEAR_KEY] = ""
    st.session_state[_SUMMARY_PERIOD_TO_MONTH_KEY] = ""
    st.session_state[_SUMMARY_PERIOD_TO_DAY_KEY] = ""


def _format_period_display(year_value: str | None, month_value: str | None, day_value: str | None, show_day: bool) -> str:
    year_text = str(year_value or "").strip()
    month_text = str(month_value or "").strip()
    day_text = str(day_value or "").strip()

    display_year = year_text if year_text else "----"
    display_month = month_text if month_text else "--"
    if show_day:
        display_day = day_text if day_text else "--"
        return f"{display_year}/{display_month}/{display_day}"
    return f"{display_year}/{display_month}"


def _render_two_column_radio(label: str, options: list[str], key: str) -> str | None:
    if not options:
        return None

    if key not in st.session_state:
        st.session_state[key] = options[0]

    selected = st.session_state.get(key)
    left_options = options[::2]
    right_options = options[1::2]
    left_key = f"{key}__left"
    right_key = f"{key}__right"

    if selected in left_options:
        st.session_state[left_key] = selected
        st.session_state[right_key] = None
    elif selected in right_options:
        st.session_state[right_key] = selected
        st.session_state[left_key] = None
    else:
        st.session_state[key] = options[0]
        st.session_state[left_key] = options[0]
        st.session_state[right_key] = None

    def _on_left_change() -> None:
        choice = st.session_state.get(left_key)
        if choice:
            st.session_state[key] = choice
            st.session_state[right_key] = None

    def _on_right_change() -> None:
        choice = st.session_state.get(right_key)
        if choice:
            st.session_state[key] = choice
            st.session_state[left_key] = None

    col_left, col_right = st.columns(2)
    with col_left:
        st.radio(
            label,
            options=left_options,
            key=left_key,
            index=None,
            label_visibility="collapsed",
            on_change=_on_left_change,
        )
    with col_right:
        st.radio(
            label,
            options=right_options,
            key=right_key,
            index=None,
            label_visibility="collapsed",
            on_change=_on_right_change,
        )

    return st.session_state.get(key)


def _on_week_period_checkbox_change(changed_key: str) -> None:
    option_keys = st.session_state.get(_SUMMARY_WEEK_PERIOD_CHECKBOX_KEYS_KEY, [])
    selection_order = list(st.session_state.get(_SUMMARY_WEEK_PERIOD_SELECTION_ORDER_KEY, []))

    if changed_key not in option_keys:
        return

    is_checked = bool(st.session_state.get(changed_key, False))
    if is_checked:
        if changed_key in selection_order:
            selection_order.remove(changed_key)
        selection_order.append(changed_key)
    else:
        selection_order = [key for key in selection_order if key != changed_key]

    selected_in_order = [key for key in selection_order if bool(st.session_state.get(key, False))]
    st.session_state[_SUMMARY_WEEK_PERIOD_SELECTION_ORDER_KEY] = selected_in_order


def _reset_week_period_dialog_state() -> None:
    for old_key in st.session_state.get(_SUMMARY_WEEK_PERIOD_CHECKBOX_KEYS_KEY, []):
        st.session_state.pop(old_key, None)

    reset_keys = [
        _SUMMARY_WEEK_PERIOD_CHECKBOX_KEYS_KEY,
        _SUMMARY_WEEK_PERIOD_KEY_TO_LABEL_KEY,
        _SUMMARY_WEEK_PERIOD_SELECTION_ORDER_KEY,
        _SUMMARY_WEEK_PERIOD_SELECTED_LABELS_KEY,
        _SUMMARY_WEEK_PERIOD_SIGNATURE_KEY,
    ]
    for key in reset_keys:
        st.session_state.pop(key, None)


def _read_excel_ids_from_col_a(file_bytes: bytes) -> list[int]:
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

    with zipfile.ZipFile(BytesIO(file_bytes)) as workbook_zip:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in workbook_zip.namelist():
            shared_xml = workbook_zip.read("xl/sharedStrings.xml")
            shared_root = ET.fromstring(shared_xml)
            for si in shared_root.findall("x:si", ns):
                chunks = [node.text or "" for node in si.findall(".//x:t", ns)]
                shared_strings.append("".join(chunks))

        sheet_name = "xl/worksheets/sheet1.xml"
        if sheet_name not in workbook_zip.namelist():
            sheets = sorted(
                [name for name in workbook_zip.namelist() if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")]
            )
            if not sheets:
                return []
            sheet_name = sheets[0]

        sheet_xml = workbook_zip.read(sheet_name)
        sheet_root = ET.fromstring(sheet_xml)

    ids: list[int] = []
    seen_ids: set[int] = set()

    for cell in sheet_root.findall(".//x:sheetData/x:row/x:c", ns):
        ref = str(cell.get("r", ""))
        if not ref.startswith("A"):
            continue

        value_node = cell.find("x:v", ns)
        inline_text_nodes = cell.findall(".//x:is/x:t", ns)
        if inline_text_nodes:
            raw_value = "".join((node.text or "") for node in inline_text_nodes)
        elif value_node is not None:
            raw_value = str(value_node.text or "")
            if str(cell.get("t", "")) == "s":
                try:
                    shared_idx = int(raw_value)
                    raw_value = shared_strings[shared_idx]
                except (ValueError, IndexError):
                    continue
        else:
            continue

        normalized = raw_value.strip()
        if not normalized:
            continue

        if re.fullmatch(r"\d+(\.0+)?", normalized):
            product_id = int(float(normalized))
        else:
            continue

        if product_id in seen_ids:
            continue
        seen_ids.add(product_id)
        ids.append(product_id)

    return ids


class SummaryReportFilterSection:
    report_code = SUMMARY_REPORT_CODE
    _TRANSIENT_STATE_KEYS = {
        _SUMMARY_CODE_INPUT_QUERY_KEY,
        _SUMMARY_CODE_INPUT_STATUS_KEY,
        _SUMMARY_CODE_INPUT_STATUS_KIND_KEY,
        _SUMMARY_CODE_INPUT_AVAILABLE_PICK_KEY,
        _SUMMARY_CODE_INPUT_SELECTED_PICK_KEY,
        _SUMMARY_CODE_INPUT_AVAILABLE_SIGNATURE_KEY,
        _SUMMARY_CODE_INPUT_SELECTED_SIGNATURE_KEY,
        _SUMMARY_CODE_INPUT_AVAILABLE_CHECKBOX_KEYS,
        _SUMMARY_CODE_INPUT_SELECTED_CHECKBOX_KEYS,
        _SUMMARY_CODE_INPUT_SHOW_SELECTED_DETAIL_KEY,
        _SUMMARY_FILE_INPUT_UPLOADER_KEY,
        _SUMMARY_FILE_INPUT_STATUS_KEY,
        _SUMMARY_FILE_INPUT_STATUS_KIND_KEY,
        _SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY,
        _SUMMARY_FILE_INPUT_MATCHED_IDS_KEY,
        _SUMMARY_FUZZY_QUERY_KEY,
        _SUMMARY_FUZZY_ITEMS_KEY,
        _SUMMARY_FUZZY_AVAILABLE_PICK_KEY,
        _SUMMARY_FUZZY_SELECTED_PICK_KEY,
        _SUMMARY_FUZZY_AVAILABLE_SIGNATURE_KEY,
        _SUMMARY_FUZZY_SELECTED_SIGNATURE_KEY,
        _SUMMARY_FUZZY_AVAILABLE_CHECKBOX_KEYS,
        _SUMMARY_FUZZY_SELECTED_CHECKBOX_KEYS,
        _SUMMARY_CUSTOM_STATUS_KEY,
        _SUMMARY_CUSTOM_STATUS_KIND_KEY,
        _SUMMARY_CUSTOM_AVAILABLE_PICK_KEY,
        _SUMMARY_CUSTOM_SELECTED_PICK_KEY,
        _SUMMARY_CUSTOM_AVAILABLE_SIGNATURE_KEY,
        _SUMMARY_CUSTOM_SELECTED_SIGNATURE_KEY,
        _SUMMARY_CUSTOM_AVAILABLE_CHECKBOX_KEYS,
        _SUMMARY_CUSTOM_SELECTED_CHECKBOX_KEYS,
        _SUMMARY_CUSTOM_SHOW_SELECTED_DETAIL_KEY,
        _SUMMARY_CUSTOM_TREE_SOURCE_READY_KEY,
        _SUMMARY_CUSTOM_TREE_CATEGORIES_KEY,
        _SUMMARY_CUSTOM_TREE_QUERY_KEY,
        *_SUMMARY_TRANSIENT_WIDGET_KEYS,
        _SUMMARY_AXIS_DIALOG_OPEN_KEY,
        _SUMMARY_AXIS_TREE_CHECKED_KEY,
        _SUMMARY_AXIS_TREE_EXPANDED_KEY,
        _SUMMARY_AXIS_TREE_MODE_KEY,
        _SUMMARY_AXIS_TREE_QUERY_KEY,
        _SUMMARY_AXIS_TREE_COMPONENT_KEY,
        _SUMMARY_AXIS_TREE_IGNORE_COMPONENT_ONCE_KEY,
        _SUMMARY_AXIS_SELECTED_PICK_KEY,
        _SUMMARY_AXIS_SELECTED_SIGNATURE_KEY,
        _SUMMARY_AXIS_SELECTED_CHECKBOX_KEYS,
        _SUMMARY_AXIS_LABEL_TO_ID_MAP_KEY,
        _SUMMARY_AXIS_STATUS_KEY,
        _SUMMARY_AXIS_STATUS_KIND_KEY,
    }

    def __init__(
        self,
        product_service: ProductService | None = None,
        report_filter_service: ReportFilterService | None = None,
        summary_axis_service: SummaryAxisService | None = None,
    ) -> None:
        self._product_service = product_service or ProductService()
        self._report_filter_service = report_filter_service or ReportFilterService()
        self._summary_axis_service = summary_axis_service or SummaryAxisService()

    @staticmethod
    def _pick_best_product_match(query_text: str, products: list) -> object:
        normalized_query = query_text.strip().lower()
        if not normalized_query:
            return products[0]

        for product in products:
            if str(product.product_name).strip().lower() == normalized_query:
                return product

        for product in products:
            if str(product.product_name).strip().lower().startswith(normalized_query):
                return product

        return products[0]

    @staticmethod
    def _get_product_field(product: object, field_name: str, default: Any = None) -> Any:
        if isinstance(product, dict):
            return product.get(field_name, default)
        return getattr(product, field_name, default)

    @staticmethod
    def _extract_product_detail(product: object) -> dict[str, object]:
        return {
            "category": str(SummaryReportFilterSection._get_product_field(product, "category", "") or ""),
            "price": SummaryReportFilterSection._get_product_field(product, "price", None),
            "unit": str(SummaryReportFilterSection._get_product_field(product, "unit", "") or ""),
            "description": str(SummaryReportFilterSection._get_product_field(product, "description", "") or ""),
            "stock_quantity": SummaryReportFilterSection._get_product_field(product, "stock_quantity", None),
        }

    @staticmethod
    def _to_item_text(product: object) -> str:
        return (
            f"{SummaryReportFilterSection._get_product_field(product, 'product_id', '')}"
            f" - {SummaryReportFilterSection._get_product_field(product, 'product_name', '')}"
        )

    def _merge_products_to_available(self, products: list[object]) -> tuple[int, int]:
        available_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_ITEMS_KEY, []))
        selected_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, []))
        detail_map = dict(st.session_state.get(_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY, {}))
        added_count = 0
        duplicated_count = 0

        for product in products:
            item_text = self._to_item_text(product)
            detail_map[item_text] = self._extract_product_detail(product)
            if item_text in available_items or item_text in selected_items:
                duplicated_count += 1
                continue
            available_items.append(item_text)
            added_count += 1

        st.session_state[_SUMMARY_CODE_INPUT_ITEMS_KEY] = available_items
        st.session_state[_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY] = detail_map
        return added_count, duplicated_count

    def _merge_products_to_selected(self, products: list[object]) -> tuple[int, int]:
        available_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_ITEMS_KEY, []))
        selected_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, []))
        detail_map = dict(st.session_state.get(_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY, {}))
        added_count = 0
        duplicated_count = 0

        for product in products:
            item_text = self._to_item_text(product)
            detail_map[item_text] = self._extract_product_detail(product)

            if item_text in selected_items:
                duplicated_count += 1
                continue

            if item_text in available_items:
                available_items.remove(item_text)

            selected_items.append(item_text)
            added_count += 1

        st.session_state[_SUMMARY_CODE_INPUT_ITEMS_KEY] = available_items
        st.session_state[_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY] = selected_items
        st.session_state[_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY] = detail_map
        return added_count, duplicated_count

    def _on_code_input_submit(self) -> None:
        query_text = str(st.session_state.get(_SUMMARY_CODE_INPUT_QUERY_KEY, "")).strip()
        if not query_text:
            return

        try:
            products = self._product_service.get_product_page(
                ProductFilter(name=query_text),
                page=1,
            )
        except DataAccessError:
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KIND_KEY] = "error"
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KEY] = "Không thể tìm sản phẩm. Vui lòng thử lại."
            return

        if not products:
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KIND_KEY] = "warning"
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KEY] = f"Không tìm thấy sản phẩm cho từ khóa: {query_text}"
            return

        matched_product = self._pick_best_product_match(query_text, products)
        item_text = f"{matched_product.product_id} - {matched_product.product_name}"
        available_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_ITEMS_KEY, []))
        selected_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, []))
        detail_map = dict(st.session_state.get(_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY, {}))
        detail_map[item_text] = self._extract_product_detail(matched_product)
        st.session_state[_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY] = detail_map

        if item_text in available_items or item_text in selected_items:
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KIND_KEY] = "info"
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KEY] = "Sản phẩm đã có trong danh sách."
        else:
            available_items.append(item_text)
            st.session_state[_SUMMARY_CODE_INPUT_ITEMS_KEY] = available_items
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KIND_KEY] = "success"
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KEY] = f"Đã thêm: {item_text}"

        st.session_state[_SUMMARY_CODE_INPUT_QUERY_KEY] = ""

    @staticmethod
    def _set_custom_category_status(kind: str, message: str) -> None:
        st.session_state[_SUMMARY_CUSTOM_STATUS_KIND_KEY] = str(kind or "").strip()
        st.session_state[_SUMMARY_CUSTOM_STATUS_KEY] = str(message or "").strip()

    def _ensure_custom_tree_source(self) -> None:
        if bool(st.session_state.get(_SUMMARY_CUSTOM_TREE_SOURCE_READY_KEY, False)):
            return

        try:
            categories = _get_cached_product_categories(self._product_service)
            st.session_state[_SUMMARY_CUSTOM_TREE_CATEGORIES_KEY] = list(categories)
            st.session_state[_SUMMARY_CUSTOM_SELECTED_PICK_KEY] = []
            st.session_state[_SUMMARY_CUSTOM_TREE_CHECKED_KEY] = []
            st.session_state[_SUMMARY_CUSTOM_TREE_EXPANDED_KEY] = ["root"]
            st.session_state[_SUMMARY_CUSTOM_TREE_SOURCE_READY_KEY] = True
            self._set_custom_category_status("", "")
        except DataAccessError:
            self._set_custom_category_status("error", "Không thể tải sản phẩm theo category. Vui lòng thử lại.")
        except Exception:
            self._set_custom_category_status("error", "Đã xảy ra lỗi khi tải danh sách sản phẩm.")

    @staticmethod
    def _move_items(source_key: str, target_key: str, moved_items: list[str]) -> None:
        if not moved_items:
            return

        source_items = list(st.session_state.get(source_key, []))
        target_items = list(st.session_state.get(target_key, []))
        remaining = [item for item in source_items if item not in moved_items]
        combined = [*target_items, *[item for item in moved_items if item not in target_items]]

        st.session_state[source_key] = remaining
        st.session_state[target_key] = combined

    def _on_move_all_right(self) -> None:
        self._move_items(
            source_key=_SUMMARY_CODE_INPUT_ITEMS_KEY,
            target_key=_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY,
            moved_items=list(st.session_state.get(_SUMMARY_CODE_INPUT_ITEMS_KEY, [])),
        )
        st.session_state[_SUMMARY_CODE_INPUT_AVAILABLE_PICK_KEY] = []

    def _on_move_right(self) -> None:
        self._move_items(
            source_key=_SUMMARY_CODE_INPUT_ITEMS_KEY,
            target_key=_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY,
            moved_items=list(st.session_state.get(_SUMMARY_CODE_INPUT_AVAILABLE_PICK_KEY, [])),
        )
        st.session_state[_SUMMARY_CODE_INPUT_AVAILABLE_PICK_KEY] = []

    def _on_move_left(self) -> None:
        self._move_items(
            source_key=_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY,
            target_key=_SUMMARY_CODE_INPUT_ITEMS_KEY,
            moved_items=list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_PICK_KEY, [])),
        )
        st.session_state[_SUMMARY_CODE_INPUT_SELECTED_PICK_KEY] = []

    def _on_move_all_left(self) -> None:
        self._move_items(
            source_key=_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY,
            target_key=_SUMMARY_CODE_INPUT_ITEMS_KEY,
            moved_items=list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, [])),
        )
        st.session_state[_SUMMARY_CODE_INPUT_SELECTED_PICK_KEY] = []

    @staticmethod
    def _to_custom_tree_category_value(category_name: str) -> str:
        return f"cat::{category_name}"

    @staticmethod
    def _from_custom_tree_category_value(tree_value: str) -> str | None:
        value = str(tree_value or "")
        if not value.startswith("cat::"):
            return None
        return value.replace("cat::", "", 1)

    def _build_custom_tree_nodes(self, categories: list[str]) -> list[dict[str, Any]]:
        category_nodes = [
            {
                "label": category,
                "value": self._to_custom_tree_category_value(category),
            }
            for category in categories
        ]

        return [
            {
                "label": "Category",
                "value": "root",
                "children": category_nodes,
            }
        ]

    @staticmethod
    def _filter_custom_categories(categories: list[str], query: str) -> list[str]:
        query_text = str(query or "").strip()
        if not query_text:
            return list(categories)
        query_folded = query_text.casefold()
        return [item for item in categories if query_folded in str(item).casefold()]

    def _on_custom_move_right(self) -> None:
        checked_values = list(st.session_state.get(_SUMMARY_CUSTOM_TREE_CHECKED_KEY, []))
        all_categories = list(st.session_state.get(_SUMMARY_CUSTOM_TREE_CATEGORIES_KEY, []))
        query_text = str(st.session_state.get(_SUMMARY_CUSTOM_TREE_QUERY_KEY, ""))
        visible_categories = self._filter_custom_categories(all_categories, query_text)
        selected_categories = [
            category
            for category in (
                self._from_custom_tree_category_value(checked_value) for checked_value in checked_values
            )
            if category
        ]

        if "root" in checked_values and not selected_categories:
            selected_categories = visible_categories

        if not selected_categories:
            self._set_custom_category_status("warning", "Vui lòng chọn ít nhất 1 category trong cây.")
            return

        products_to_add: list[object] = []
        try:
            for category in selected_categories:
                products = _get_cached_products_by_category(
                    category=category,
                    page_limit=50,
                    _service=self._product_service,
                )
                products_to_add.extend(products)
        except DataAccessError:
            self._set_custom_category_status("error", "Không thể tải sản phẩm theo category. Vui lòng thử lại.")
            return
        except Exception:
            self._set_custom_category_status("error", "Đã xảy ra lỗi khi tải danh sách sản phẩm.")
            return

        if not products_to_add:
            self._set_custom_category_status("warning", "Không có sản phẩm thuộc category đã chọn.")
            return

        added_count, duplicated_count = self._merge_products_to_selected(products_to_add)
        message_parts = [f"Đã thêm {added_count} sản phẩm"]
        if duplicated_count:
            message_parts.append(f"trùng {duplicated_count}")
        self._set_custom_category_status("success", " | ".join(message_parts) + ".")

    def _on_custom_remove_selected(self) -> None:
        selected_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, []))
        removed_items = set(st.session_state.get(_SUMMARY_CUSTOM_SELECTED_PICK_KEY, []))
        st.session_state[_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY] = [
            item for item in selected_items if item not in removed_items
        ]
        st.session_state[_SUMMARY_CUSTOM_SELECTED_PICK_KEY] = []

    @staticmethod
    def _toggle_custom_selected_detail() -> None:
        current = bool(st.session_state.get(_SUMMARY_CUSTOM_SHOW_SELECTED_DETAIL_KEY, False))
        st.session_state[_SUMMARY_CUSTOM_SHOW_SELECTED_DETAIL_KEY] = not current

    @staticmethod
    def _ensure_fuzzy_dialog_state() -> None:
        if _SUMMARY_FUZZY_SELECTED_ITEMS_KEY not in st.session_state:
            st.session_state[_SUMMARY_FUZZY_SELECTED_ITEMS_KEY] = []

    def _on_fuzzy_move_all_right(self) -> None:
        self._move_items(
            source_key=_SUMMARY_FUZZY_ITEMS_KEY,
            target_key=_SUMMARY_FUZZY_SELECTED_ITEMS_KEY,
            moved_items=list(st.session_state.get(_SUMMARY_FUZZY_ITEMS_KEY, [])),
        )
        st.session_state[_SUMMARY_FUZZY_AVAILABLE_PICK_KEY] = []

    def _on_fuzzy_move_right(self) -> None:
        self._move_items(
            source_key=_SUMMARY_FUZZY_ITEMS_KEY,
            target_key=_SUMMARY_FUZZY_SELECTED_ITEMS_KEY,
            moved_items=list(st.session_state.get(_SUMMARY_FUZZY_AVAILABLE_PICK_KEY, [])),
        )
        st.session_state[_SUMMARY_FUZZY_AVAILABLE_PICK_KEY] = []

    def _on_fuzzy_move_left(self) -> None:
        self._move_items(
            source_key=_SUMMARY_FUZZY_SELECTED_ITEMS_KEY,
            target_key=_SUMMARY_FUZZY_ITEMS_KEY,
            moved_items=list(st.session_state.get(_SUMMARY_FUZZY_SELECTED_PICK_KEY, [])),
        )
        st.session_state[_SUMMARY_FUZZY_SELECTED_PICK_KEY] = []

    def _on_fuzzy_move_all_left(self) -> None:
        self._move_items(
            source_key=_SUMMARY_FUZZY_SELECTED_ITEMS_KEY,
            target_key=_SUMMARY_FUZZY_ITEMS_KEY,
            moved_items=list(st.session_state.get(_SUMMARY_FUZZY_SELECTED_ITEMS_KEY, [])),
        )
        st.session_state[_SUMMARY_FUZZY_SELECTED_PICK_KEY] = []

    @staticmethod
    def _toggle_selected_detail() -> None:
        current = bool(st.session_state.get(_SUMMARY_CODE_INPUT_SHOW_SELECTED_DETAIL_KEY, False))
        st.session_state[_SUMMARY_CODE_INPUT_SHOW_SELECTED_DETAIL_KEY] = not current

    @staticmethod
    def _render_checkbox_list_box(
        *,
        label: str,
        items: list[str],
        pick_key: str,
        signature_key: str,
        checkbox_keys_state_key: str,
        checkbox_prefix: str,
    ) -> None:
        signature = "||".join(items)
        prev_signature = str(st.session_state.get(signature_key, ""))
        selected_items = set(st.session_state.get(pick_key, []))

        if signature != prev_signature:
            for old_key in st.session_state.get(checkbox_keys_state_key, []):
                st.session_state.pop(old_key, None)
            st.session_state[signature_key] = signature

        st.markdown(f"**{label}**")
        checkbox_keys: list[str] = []
        with st.container(border=True, height=280):
            for idx, item in enumerate(items):
                checkbox_key = f"{checkbox_prefix}_{idx}"
                checkbox_keys.append(checkbox_key)
                if checkbox_key not in st.session_state:
                    st.session_state[checkbox_key] = item in selected_items
                st.checkbox(item, key=checkbox_key)

        st.session_state[checkbox_keys_state_key] = checkbox_keys
        st.session_state[pick_key] = [
            item for idx, item in enumerate(items) if bool(st.session_state.get(f"{checkbox_prefix}_{idx}", False))
        ]

    @st.dialog("Chọn kỳ tổng hợp", width="small")
    def show_week_period_dialog(self, options: list[tuple[str, date, date]]) -> None:
        st.caption("Có thể chọn nhiều kỳ. Hệ thống sẽ lấy kỳ nhỏ nhất và lớn nhất.")

        option_map = {label: (start_date, end_date) for label, start_date, end_date in options}
        labels = list(option_map.keys())
        signature = "||".join(labels)
        prev_signature = str(st.session_state.get(_SUMMARY_WEEK_PERIOD_SIGNATURE_KEY, ""))
        if signature != prev_signature:
            for old_key in st.session_state.get(_SUMMARY_WEEK_PERIOD_CHECKBOX_KEYS_KEY, []):
                st.session_state.pop(old_key, None)
            st.session_state[_SUMMARY_WEEK_PERIOD_SIGNATURE_KEY] = signature
            st.session_state[_SUMMARY_WEEK_PERIOD_SELECTION_ORDER_KEY] = []

        existing_selected = set(st.session_state.get(_SUMMARY_WEEK_PERIOD_SELECTED_LABELS_KEY, []))
        checkbox_keys: list[str] = []
        key_to_label: dict[str, str] = {}

        with st.container(border=True, height=340):
            for idx, label in enumerate(labels):
                checkbox_key = f"{_SUMMARY_WEEK_PERIOD_CHECKBOX_PREFIX}_{idx}"
                checkbox_keys.append(checkbox_key)
                key_to_label[checkbox_key] = label
                if checkbox_key not in st.session_state:
                    st.session_state[checkbox_key] = label in existing_selected
                st.checkbox(
                    label,
                    key=checkbox_key,
                    on_change=_on_week_period_checkbox_change,
                    args=(checkbox_key,),
                )

        st.session_state[_SUMMARY_WEEK_PERIOD_CHECKBOX_KEYS_KEY] = checkbox_keys
        st.session_state[_SUMMARY_WEEK_PERIOD_KEY_TO_LABEL_KEY] = key_to_label

        selected_labels = [
            key_to_label[key]
            for key in checkbox_keys
            if key in key_to_label and bool(st.session_state.get(key, False))
        ]

        if st.button("OK", type="primary", use_container_width=True, key=_SUMMARY_WEEK_PERIOD_PICKER_OK_BTN_KEY):
            if not selected_labels:
                _toast("Vui lòng chọn ít nhất 1 kỳ trước khi nhấn OK.", kind="warning")
                return

            selected_ranges = [option_map[label] for label in selected_labels]
            selected_ranges.sort(key=lambda item: item[0])

            from_date = selected_ranges[0][0]
            to_date = selected_ranges[-1][1]

            st.session_state[_SUMMARY_PERIOD_FROM_YEAR_KEY] = f"{from_date.year:04d}"
            st.session_state[_SUMMARY_PERIOD_FROM_MONTH_KEY] = f"{from_date.month:02d}"
            st.session_state[_SUMMARY_PERIOD_FROM_DAY_KEY] = f"{from_date.day:02d}"
            st.session_state[_SUMMARY_PERIOD_TO_YEAR_KEY] = f"{to_date.year:04d}"
            st.session_state[_SUMMARY_PERIOD_TO_MONTH_KEY] = f"{to_date.month:02d}"
            st.session_state[_SUMMARY_PERIOD_TO_DAY_KEY] = f"{to_date.day:02d}"
            st.session_state[_SUMMARY_WEEK_PERIOD_SELECTED_LABELS_KEY] = selected_labels
            st.rerun()

    @staticmethod
    def _on_axis_type_change() -> None:
        axis_type = str(st.session_state.get(_SUMMARY_AXIS_TYPE_KEY, "")).strip()
        prev_axis_type = str(st.session_state.get(_SUMMARY_AXIS_TREE_MODE_KEY, "")).strip()
        if axis_type != prev_axis_type:
            st.session_state.pop(_SUMMARY_AXIS_TREE_CHECKED_KEY, None)
            st.session_state.pop(_SUMMARY_AXIS_TREE_EXPANDED_KEY, None)
            st.session_state[_SUMMARY_AXIS_TREE_MODE_KEY] = axis_type
            st.session_state[_SUMMARY_AXIS_SELECTED_PICK_KEY] = []
            st.session_state.pop(_SUMMARY_AXIS_TREE_IGNORE_COMPONENT_ONCE_KEY, None)
            SummaryReportFilterSection._bump_axis_tree_component_version()
        if axis_type == "Không phân tầng":
            st.session_state[_SUMMARY_AXIS_SELECTED_STORE_IDS_KEY] = []
            st.session_state[_SUMMARY_AXIS_SELECTED_STORE_LABELS_KEY] = []
            st.session_state[_SUMMARY_AXIS_LABEL_TO_ID_MAP_KEY] = {}
            st.session_state[_SUMMARY_AXIS_DIALOG_OPEN_KEY] = False

    @staticmethod
    def _open_axis_type_dialog() -> None:
        axis_type = str(st.session_state.get(_SUMMARY_AXIS_TYPE_KEY, "")).strip()
        st.session_state[_SUMMARY_AXIS_DIALOG_OPEN_KEY] = axis_type != "Không phân tầng"

    @staticmethod
    def _axis_tree_supported(axis_type: str) -> bool:
        return axis_type in {
            "Theo hợp tác xã",
            "Theo khu vực",
            "Theo mô hình hoạt động",
            "Hợp tác xã ➡ Khu vực",
        }

    @staticmethod
    def _to_store_node_value(store_id: str) -> str:
        return f"s::{store_id}"

    @staticmethod
    def _from_store_node_value(node_value: str) -> str | None:
        value = str(node_value or "").strip()
        if not value.startswith("s::"):
            return None
        return value.replace("s::", "", 1)

    @staticmethod
    def _row_to_axis_item(row: tuple) -> dict[str, str]:
        return {
            "store_id": str(row[0] or "").strip(),
            "store_name": str(row[1] or "").strip(),
            "area_id": str(row[2] or "").strip(),
            "area_name": str(row[3] or "").strip(),
            "coop_id": str(row[4] or "").strip(),
            "coop_name": str(row[5] or "").strip(),
            "biz_model_id": str(row[6] or "").strip(),
            "biz_model_name": str(row[7] or "").strip(),
        }

    def _build_axis_tree_data(
        self,
        axis_type: str,
        rows: list[tuple],
        excluded_store_ids: set[str] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, set[str]], dict[str, str]]:
        excluded = {str(item) for item in (excluded_store_ids or set())}
        items = [self._row_to_axis_item(row) for row in rows]
        nodes: list[dict[str, Any]] = []
        parent_store_map: dict[str, set[str]] = {}
        store_lookup: dict[str, str] = {}

        for item in items:
            store_id = item["store_id"]
            store_name = item["store_name"] or f"Store {store_id}"
            if store_id:
                store_lookup[store_id] = store_name

        if axis_type == "Theo hợp tác xã":
            coop_map: dict[str, dict[str, Any]] = {}
            for item in items:
                coop_id = item["coop_id"]
                store_id = item["store_id"]
                if not coop_id or not store_id:
                    continue
                if store_id in excluded:
                    continue
                coop_name = item["coop_name"] or f"COOP {coop_id}"
                parent_value = f"c::{coop_id}"
                group = coop_map.setdefault(
                    coop_id,
                    {"label": coop_name, "value": parent_value, "children": []},
                )
                group["children"].append(
                    {
                        "label": item["store_name"],
                        "value": self._to_store_node_value(store_id),
                    }
                )
                parent_store_map.setdefault(parent_value, set()).add(store_id)
            nodes = list(coop_map.values())

        elif axis_type == "Theo khu vực":
            area_map: dict[str, dict[str, Any]] = {}
            for item in items:
                area_id = item["area_id"]
                store_id = item["store_id"]
                if not area_id or not store_id:
                    continue
                if store_id in excluded:
                    continue
                area_name = item["area_name"] or f"Area {area_id}"
                parent_value = f"a::{area_id}"
                group = area_map.setdefault(
                    area_id,
                    {"label": area_name, "value": parent_value, "children": []},
                )
                group["children"].append(
                    {
                        "label": item["store_name"],
                        "value": self._to_store_node_value(store_id),
                    }
                )
                parent_store_map.setdefault(parent_value, set()).add(store_id)
            nodes = list(area_map.values())

        elif axis_type == "Theo mô hình hoạt động":
            biz_map: dict[str, dict[str, Any]] = {}
            for item in items:
                biz_model_id = item["biz_model_id"]
                store_id = item["store_id"]
                if not biz_model_id or not store_id:
                    continue
                if store_id in excluded:
                    continue
                biz_model_name = item["biz_model_name"] or f"Model {biz_model_id}"
                parent_value = f"b::{biz_model_id}"
                group = biz_map.setdefault(
                    biz_model_id,
                    {"label": biz_model_name, "value": parent_value, "children": []},
                )
                group["children"].append(
                    {
                        "label": item["store_name"],
                        "value": self._to_store_node_value(store_id),
                    }
                )
                parent_store_map.setdefault(parent_value, set()).add(store_id)
            nodes = list(biz_map.values())

        elif axis_type == "Hợp tác xã ➡ Khu vực":
            coop_map: dict[str, dict[str, Any]] = {}
            for item in items:
                coop_id = item["coop_id"]
                area_id = item["area_id"]
                store_id = item["store_id"]
                if not coop_id or not area_id or not store_id:
                    continue
                if store_id in excluded:
                    continue

                coop_name = item["coop_name"] or f"COOP {coop_id}"
                area_name = item["area_name"] or f"Area {area_id}"
                coop_value = f"c::{coop_id}"
                area_value = f"ca::{coop_id}::{area_id}"

                coop_node = coop_map.setdefault(
                    coop_id,
                    {"label": coop_name, "value": coop_value, "children": []},
                )
                area_node: dict[str, Any] | None = next(
                    (node for node in coop_node["children"] if str(node.get("value", "")) == area_value),
                    None,
                )
                if area_node is None:
                    area_node = {"label": area_name, "value": area_value, "children": []}
                    coop_node["children"].append(area_node)

                area_node["children"].append(
                    {
                        "label": item["store_name"],
                        "value": self._to_store_node_value(store_id),
                    }
                )
                parent_store_map.setdefault(area_value, set()).add(store_id)
                parent_store_map.setdefault(coop_value, set()).add(store_id)
            nodes = list(coop_map.values())

        return nodes, parent_store_map, store_lookup

    @staticmethod
    def _axis_collect_expand_values(nodes: list[dict[str, Any]]) -> list[str]:
        values: list[str] = []

        def _walk(node_list: list[dict[str, Any]]) -> None:
            for node in node_list:
                child_nodes = node.get("children", [])
                has_children = isinstance(child_nodes, list) and len(child_nodes) > 0
                node_value = str(node.get("value", "")).strip()
                if has_children and node_value:
                    values.append(node_value)
                if has_children:
                    _walk(child_nodes)

        _walk(nodes)
        return values

    @staticmethod
    def _axis_collect_all_node_values(nodes: list[dict[str, Any]]) -> list[str]:
        values: list[str] = []

        def _walk(node_list: list[dict[str, Any]]) -> None:
            for node in node_list:
                node_value = str(node.get("value", "")).strip()
                if node_value:
                    values.append(node_value)
                child_nodes = node.get("children", [])
                if isinstance(child_nodes, list) and child_nodes:
                    _walk(child_nodes)

        _walk(nodes)
        return values

    @staticmethod
    def _axis_filter_tree_nodes_by_store_name(nodes: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
        query_text = str(query or "").strip()
        if not query_text:
            return list(nodes)

        query_folded = query_text.casefold()

        def _filter_node(node: dict[str, Any]) -> dict[str, Any] | None:
            label = str(node.get("label", "") or "")
            children = node.get("children", [])
            has_children = isinstance(children, list) and len(children) > 0

            filtered_children: list[dict[str, Any]] = []
            if has_children:
                for child in children:
                    if not isinstance(child, dict):
                        continue
                    child_result = _filter_node(child)
                    if child_result is not None:
                        filtered_children.append(child_result)

            label_matches = query_folded in label.casefold()
            if filtered_children:
                cloned = dict(node)
                cloned["children"] = filtered_children
                return cloned
            if label_matches and not has_children:
                return dict(node)
            return None

        filtered: list[dict[str, Any]] = []
        for raw_node in nodes:
            if not isinstance(raw_node, dict):
                continue
            node_result = _filter_node(raw_node)
            if node_result is not None:
                filtered.append(node_result)
        return filtered

    def _axis_collect_store_ids_from_tree(
        self,
        checked_values: list[str],
        parent_store_map: dict[str, set[str]],
    ) -> set[str]:
        selected_store_ids: set[str] = set()
        for value in checked_values:
            store_id = self._from_store_node_value(str(value))
            if store_id:
                selected_store_ids.add(store_id)
                continue
            selected_store_ids.update(parent_store_map.get(str(value), set()))
        return selected_store_ids

    @staticmethod
    def _axis_build_selected_labels(
        selected_store_ids: set[str],
        store_lookup: dict[str, str],
    ) -> tuple[list[str], list[tuple[str, str]], dict[str, str]]:
        selected_rows = sorted(
            [(store_id, store_lookup.get(store_id, f"Store {store_id}")) for store_id in selected_store_ids],
            key=lambda item: (item[1], item[0]),
        )
        selected_labels = [f"{store_name} ({store_id})" for store_id, store_name in selected_rows]
        label_to_id = {f"{store_name} ({store_id})": store_id for store_id, store_name in selected_rows}
        return selected_labels, selected_rows, label_to_id

    @staticmethod
    def _normalize_tree_select_values(values: Any) -> list[str]:
        if not isinstance(values, list):
            return []
        return [str(value) for value in values]

    @staticmethod
    def _bump_axis_tree_component_version() -> None:
        current = int(st.session_state.get(_SUMMARY_AXIS_TREE_COMPONENT_VERSION_KEY, 0) or 0)
        st.session_state[_SUMMARY_AXIS_TREE_COMPONENT_VERSION_KEY] = current + 1

    def _on_axis_move_right_tree(
        self,
        parent_store_map: dict[str, set[str]],
        store_lookup: dict[str, str],
    ) -> None:
        checked_values = list(st.session_state.get(_SUMMARY_AXIS_TREE_CHECKED_KEY, []))
        moved_store_ids = self._axis_collect_store_ids_from_tree(checked_values, parent_store_map)
        if not moved_store_ids:
            st.session_state[_SUMMARY_AXIS_STATUS_KIND_KEY] = "warning"
            st.session_state[_SUMMARY_AXIS_STATUS_KEY] = "Vui lòng chọn ít nhất 1 cửa hàng trong cây."
            st.session_state[_SUMMARY_AXIS_TREE_CHECKED_KEY] = []
            st.session_state[_SUMMARY_AXIS_TREE_IGNORE_COMPONENT_ONCE_KEY] = True
            return

        existing_store_ids = {str(item) for item in st.session_state.get(_SUMMARY_AXIS_SELECTED_STORE_IDS_KEY, [])}
        merged_store_ids = existing_store_ids.union(moved_store_ids)
        selected_labels, selected_rows, label_to_id = self._axis_build_selected_labels(merged_store_ids, store_lookup)
        st.session_state[_SUMMARY_AXIS_SELECTED_STORE_IDS_KEY] = [store_id for store_id, _ in selected_rows]
        st.session_state[_SUMMARY_AXIS_SELECTED_STORE_LABELS_KEY] = selected_labels
        st.session_state[_SUMMARY_AXIS_LABEL_TO_ID_MAP_KEY] = label_to_id
        st.session_state[_SUMMARY_AXIS_SELECTED_PICK_KEY] = []
        st.session_state[_SUMMARY_AXIS_TREE_CHECKED_KEY] = []
        st.session_state[_SUMMARY_AXIS_TREE_IGNORE_COMPONENT_ONCE_KEY] = True
        self._bump_axis_tree_component_version()

    def _on_axis_remove_selected(self) -> None:
        selected_labels = list(st.session_state.get(_SUMMARY_AXIS_SELECTED_STORE_LABELS_KEY, []))
        removed_labels = set(str(item) for item in st.session_state.get(_SUMMARY_AXIS_SELECTED_PICK_KEY, []))
        if not removed_labels:
            st.session_state[_SUMMARY_AXIS_STATUS_KIND_KEY] = "warning"
            st.session_state[_SUMMARY_AXIS_STATUS_KEY] = "Vui lòng chọn ít nhất 1 cửa hàng ở danh sách được chọn."
            return

        remaining_labels = [item for item in selected_labels if item not in removed_labels]
        label_to_id = dict(st.session_state.get(_SUMMARY_AXIS_LABEL_TO_ID_MAP_KEY, {}))
        remaining_ids = [str(label_to_id[item]) for item in remaining_labels if item in label_to_id]
        st.session_state[_SUMMARY_AXIS_SELECTED_STORE_LABELS_KEY] = remaining_labels
        st.session_state[_SUMMARY_AXIS_SELECTED_STORE_IDS_KEY] = remaining_ids
        st.session_state[_SUMMARY_AXIS_SELECTED_PICK_KEY] = []
        st.session_state[_SUMMARY_AXIS_TREE_CHECKED_KEY] = []
        st.session_state[_SUMMARY_AXIS_TREE_IGNORE_COMPONENT_ONCE_KEY] = True
        self._bump_axis_tree_component_version()

    @st.dialog("Chọn trục ngữ nghĩa", width="medium")
    def show_axis_type_dialog(self, axis_type: str) -> None:
        _toast_and_clear_status(_SUMMARY_AXIS_STATUS_KIND_KEY, _SUMMARY_AXIS_STATUS_KEY)

        if not self._axis_tree_supported(axis_type):
            st.write(f'Bạn đã chọn kiểu trục: "{axis_type}".')
            st.caption("Hệ thống đã ghi nhận cấu hình trục ngữ nghĩa.")
            if st.button("Đóng", type="primary", use_container_width=True, key="summary_axis_type_dialog_close_btn"):
                st.session_state[_SUMMARY_AXIS_DIALOG_OPEN_KEY] = False
                st.rerun()
            return

        try:
            rows = _get_cached_summary_axis_rows(self._summary_axis_service)
        except DataAccessError:
            st.warning("Không thể tải danh sách cửa hàng. Vui lòng thử lại.")
            if st.button("Đóng", type="primary", use_container_width=True, key="summary_axis_type_dialog_error_close_btn"):
                st.session_state[_SUMMARY_AXIS_DIALOG_OPEN_KEY] = False
                st.rerun()
            return

        existing_store_ids = {str(item) for item in st.session_state.get(_SUMMARY_AXIS_SELECTED_STORE_IDS_KEY, [])}
        all_store_lookup = {
            item["store_id"]: (item["store_name"] or f'Store {item["store_id"]}')
            for item in (self._row_to_axis_item(row) for row in rows)
            if item["store_id"]
        }
        nodes, parent_store_map, store_lookup = self._build_axis_tree_data(
            axis_type,
            rows,
            excluded_store_ids=existing_store_ids,
        )

        st.caption("Chọn danh sách cửa hàng theo trục ngữ nghĩa.")
        checked = list(st.session_state.get(_SUMMARY_AXIS_TREE_CHECKED_KEY, []))

        expanded = list(st.session_state.get(_SUMMARY_AXIS_TREE_EXPANDED_KEY, []))
        if not expanded:
            expanded = self._axis_collect_expand_values(nodes)
            st.session_state[_SUMMARY_AXIS_TREE_EXPANDED_KEY] = list(expanded)
        selected_labels, selected_rows, label_to_id = self._axis_build_selected_labels(existing_store_ids, all_store_lookup)
        st.session_state[_SUMMARY_AXIS_LABEL_TO_ID_MAP_KEY] = label_to_id
        current_selected_picks = [
            item for item in st.session_state.get(_SUMMARY_AXIS_SELECTED_PICK_KEY, []) if item in selected_labels
        ]
        st.session_state[_SUMMARY_AXIS_SELECTED_PICK_KEY] = current_selected_picks

        list_col_left, action_col, list_col_right = st.columns([1.6, 0.3, 1.6], gap="small")
        with list_col_left:
            st.markdown("**Danh sách cửa hàng theo cây**")
            tree_query = str(st.session_state.get(_SUMMARY_AXIS_TREE_QUERY_KEY, ""))
            filtered_nodes = self._axis_filter_tree_nodes_by_store_name(nodes, tree_query)
            valid_node_values = set(self._axis_collect_all_node_values(filtered_nodes))
            checked = [value for value in checked if value in valid_node_values]
            expanded = [value for value in expanded if value in valid_node_values]
            st.session_state[_SUMMARY_AXIS_TREE_CHECKED_KEY] = checked
            st.session_state[_SUMMARY_AXIS_TREE_EXPANDED_KEY] = expanded
            if filtered_nodes:
                component_version = int(st.session_state.get(_SUMMARY_AXIS_TREE_COMPONENT_VERSION_KEY, 0) or 0)
                component_key = f"{_SUMMARY_AXIS_TREE_COMPONENT_KEY}_{component_version}"
                selected_tree = tree_select(
                    nodes=filtered_nodes,
                    check_model="all",
                    checked=checked,
                    expanded=expanded,
                    show_expand_all=True,
                    key=component_key,
                )
                if isinstance(selected_tree, dict):
                    ignore_once = bool(st.session_state.pop(_SUMMARY_AXIS_TREE_IGNORE_COMPONENT_ONCE_KEY, False))
                    if ignore_once:
                        st.session_state[_SUMMARY_AXIS_TREE_CHECKED_KEY] = []
                    else:
                        tree_checked = [
                            value
                            for value in self._normalize_tree_select_values(selected_tree.get("checked"))
                            if value in valid_node_values
                        ]
                        st.session_state[_SUMMARY_AXIS_TREE_CHECKED_KEY] = tree_checked
                    tree_expanded = [
                        value
                        for value in self._normalize_tree_select_values(selected_tree.get("expanded"))
                        if value in valid_node_values
                    ]
                    st.session_state[_SUMMARY_AXIS_TREE_EXPANDED_KEY] = tree_expanded
                st.text_input(
                    "",
                    placeholder="Tìm theo tên cửa hàng...",
                    label_visibility="collapsed",
                    key=_SUMMARY_AXIS_TREE_QUERY_KEY,
                )
            else:
                if tree_query.strip():
                    st.caption("Không tìm thấy cửa hàng phù hợp.")
                else:
                    st.caption("Không còn cửa hàng ở bên trái. Hãy xóa ở bên phải để đưa lại.")
                st.text_input(
                    "",
                    placeholder="Tìm theo tên cửa hàng...",
                    label_visibility="collapsed",
                    key=_SUMMARY_AXIS_TREE_QUERY_KEY,
                )

        with action_col:
            st.markdown("<div style='height: 72px;'></div>", unsafe_allow_html=True)
            st.button(
                ">>",
                use_container_width=True,
                key="summary_axis_move_right_tree_btn",
                on_click=self._on_axis_move_right_tree,
                args=(parent_store_map, all_store_lookup),
            )
            st.button(
                "<<",
                use_container_width=True,
                key="summary_axis_move_left_selected_btn",
                on_click=self._on_axis_remove_selected,
            )
            st.markdown("<div style='height: 32px;'></div>", unsafe_allow_html=True)

        with list_col_right:
            self._render_checkbox_list_box(
                label="Danh sách được chọn",
                items=selected_labels,
                pick_key=_SUMMARY_AXIS_SELECTED_PICK_KEY,
                signature_key=_SUMMARY_AXIS_SELECTED_SIGNATURE_KEY,
                checkbox_keys_state_key=_SUMMARY_AXIS_SELECTED_CHECKBOX_KEYS,
                checkbox_prefix="summary_axis_selected_chk",
            )

        st.divider()
        action_cols = st.columns(2, gap="small")
        with action_cols[0]:
            if st.button("OK", type="primary", use_container_width=True, key="summary_axis_type_dialog_ok_btn"):
                final_ids = [str(item) for item in st.session_state.get(_SUMMARY_AXIS_SELECTED_STORE_IDS_KEY, [])]
                if not final_ids:
                    st.session_state[_SUMMARY_AXIS_STATUS_KIND_KEY] = "warning"
                    st.session_state[_SUMMARY_AXIS_STATUS_KEY] = "Vui lòng chọn ít nhất 1 cửa hàng."
                    return
                final_labels, _, final_label_to_id = self._axis_build_selected_labels(set(final_ids), all_store_lookup)
                st.session_state[_SUMMARY_AXIS_SELECTED_STORE_LABELS_KEY] = final_labels
                st.session_state[_SUMMARY_AXIS_LABEL_TO_ID_MAP_KEY] = final_label_to_id
                st.session_state[_SUMMARY_AXIS_DIALOG_OPEN_KEY] = False
                st.rerun()
        with action_cols[1]:
            if st.button("Cancel", use_container_width=True, key="summary_axis_type_dialog_cancel_btn"):
                st.session_state[_SUMMARY_AXIS_DIALOG_OPEN_KEY] = False
                st.rerun()

    @st.dialog("Nhập mã", width="medium")
    def show_code_input_dialog(self) -> None:
        st.text_input(
            "Tên sản phẩm",
            key=_SUMMARY_CODE_INPUT_QUERY_KEY,
            placeholder="Nhập tên sản phẩm rồi nhấn Enter",
            on_change=self._on_code_input_submit,
        )

        status_kind = st.session_state.get(_SUMMARY_CODE_INPUT_STATUS_KIND_KEY)
        status_message = str(st.session_state.get(_SUMMARY_CODE_INPUT_STATUS_KEY, "")).strip()
        if status_message:
            _toast(status_message, kind=str(status_kind or "info"))
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KEY] = ""
            st.session_state[_SUMMARY_CODE_INPUT_STATUS_KIND_KEY] = None

        available_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_ITEMS_KEY, []))
        selected_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, []))
        available_picks = [
            item for item in st.session_state.get(_SUMMARY_CODE_INPUT_AVAILABLE_PICK_KEY, []) if item in available_items
        ]
        selected_picks = [
            item for item in st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_PICK_KEY, []) if item in selected_items
        ]
        st.session_state[_SUMMARY_CODE_INPUT_AVAILABLE_PICK_KEY] = available_picks
        st.session_state[_SUMMARY_CODE_INPUT_SELECTED_PICK_KEY] = selected_picks

        list_col_left, action_col, list_col_right = st.columns([1.6, 0.3, 1.6], gap="small")

        with list_col_left:
            self._render_checkbox_list_box(
                label="Danh sách khả dụng",
                items=available_items,
                pick_key=_SUMMARY_CODE_INPUT_AVAILABLE_PICK_KEY,
                signature_key=_SUMMARY_CODE_INPUT_AVAILABLE_SIGNATURE_KEY,
                checkbox_keys_state_key=_SUMMARY_CODE_INPUT_AVAILABLE_CHECKBOX_KEYS,
                checkbox_prefix="summary_code_input_available_chk",
            )

        with action_col:
            st.markdown("<div style='height: 72px;'></div>", unsafe_allow_html=True)
            st.button(
                "",
                icon=":material/keyboard_double_arrow_right:",
                help="Chuyển toàn bộ sang danh sách được chọn",
                use_container_width=True,
                key=_SUMMARY_CODE_INPUT_MOVE_ALL_RIGHT_BTN_KEY,
                on_click=self._on_move_all_right,
            )
            st.button(
                "",
                icon=":material/chevron_right:",
                help="Chuyển các mục đang chọn sang danh sách được chọn",
                use_container_width=True,
                key=_SUMMARY_CODE_INPUT_MOVE_RIGHT_BTN_KEY,
                on_click=self._on_move_right,
            )
            st.button(
                "",
                icon=":material/chevron_left:",
                help="Đưa các mục đang chọn về danh sách khả dụng",
                use_container_width=True,
                key=_SUMMARY_CODE_INPUT_MOVE_LEFT_BTN_KEY,
                on_click=self._on_move_left,
            )
            st.button(
                "",
                icon=":material/keyboard_double_arrow_left:",
                help="Đưa toàn bộ về danh sách khả dụng",
                use_container_width=True,
                key=_SUMMARY_CODE_INPUT_MOVE_ALL_LEFT_BTN_KEY,
                on_click=self._on_move_all_left,
            )
            st.markdown("<div style='height: 64px;'></div>", unsafe_allow_html=True)

        with list_col_right:
            self._render_checkbox_list_box(
                label="Danh sách được chọn",
                items=selected_items,
                pick_key=_SUMMARY_CODE_INPUT_SELECTED_PICK_KEY,
                signature_key=_SUMMARY_CODE_INPUT_SELECTED_SIGNATURE_KEY,
                checkbox_keys_state_key=_SUMMARY_CODE_INPUT_SELECTED_CHECKBOX_KEYS,
                checkbox_prefix="summary_code_input_selected_chk",
            )

        st.divider()
        footer_col_left, footer_col_right = st.columns([1.0, 2.2], gap="small")

        with footer_col_left:
            if st.button(
                "OK",
                type="primary",
                use_container_width=True,
                key=_SUMMARY_CODE_INPUT_OK_BTN_KEY,
            ):
                st.session_state[_SUMMARY_CODE_INPUT_SHOW_SELECTED_DETAIL_KEY] = False
                st.rerun()


        with footer_col_right:
            st.button(
                "Chi tiết DS đã chọn",
                use_container_width=True,
                key=_SUMMARY_CODE_INPUT_SHOW_SELECTED_DETAIL_BTN_KEY,
                on_click=self._toggle_selected_detail,
            )

        if bool(st.session_state.get(_SUMMARY_CODE_INPUT_SHOW_SELECTED_DETAIL_KEY, False)):
            current_selected_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, []))
            st.markdown("**Chi tiết danh sách được chọn**")
            with st.container(border=True, height=180):
                if not current_selected_items:
                    st.caption("Chưa có sản phẩm nào trong danh sách được chọn.")
                else:
                    detail_map = dict(st.session_state.get(_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY, {}))
                    detail_rows: list[dict[str, object]] = []
                    for item in current_selected_items:
                        detail = detail_map.get(item, {})
                        detail_rows.append(
                            {
                                "item": item,
                                "category": detail.get("category", ""),
                                "price": detail.get("price", None),
                                "unit": detail.get("unit", ""),
                                "description": detail.get("description", ""),
                                "stock_quantity": detail.get("stock_quantity", None),
                            }
                        )
                    st.dataframe(
                        detail_rows,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "item": st.column_config.TextColumn("Sản phẩm", width="medium"),
                            "category": st.column_config.TextColumn("Category"),
                            "price": st.column_config.NumberColumn("Price", format="%.2f"),
                            "unit": st.column_config.TextColumn("Unit"),
                            "description": st.column_config.TextColumn("Description", width="large"),
                            "stock_quantity": st.column_config.NumberColumn("Stock Quantity"),
                        },
                    )

    @st.dialog("Đọc file Excel", width="medium")
    def show_file_input_dialog(self) -> None:
        st.caption("Upload file Excel `.xlsx` và hệ thống sẽ đọc mã sản phẩm ở cột A.")
        uploaded_file = st.file_uploader(
            "Tệp Excel",
            type=["xlsx"],
            key=_SUMMARY_FILE_INPUT_UPLOADER_KEY,
        )

        if uploaded_file is None:
            _toast("Vui lòng chọn file để xem dữ liệu.", kind="info")
        else:
            try:
                product_ids = _read_excel_ids_from_col_a(uploaded_file.getvalue())
            except (zipfile.BadZipFile, ET.ParseError):
                st.session_state[_SUMMARY_FILE_INPUT_STATUS_KIND_KEY] = "error"
                st.session_state[_SUMMARY_FILE_INPUT_STATUS_KEY] = "File không đúng định dạng Excel `.xlsx` hợp lệ."
                st.session_state[_SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY] = []
                st.session_state[_SUMMARY_FILE_INPUT_MATCHED_IDS_KEY] = []
            except Exception:
                st.session_state[_SUMMARY_FILE_INPUT_STATUS_KIND_KEY] = "error"
                st.session_state[_SUMMARY_FILE_INPUT_STATUS_KEY] = "Không thể đọc dữ liệu từ file. Vui lòng thử lại."
                st.session_state[_SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY] = []
                st.session_state[_SUMMARY_FILE_INPUT_MATCHED_IDS_KEY] = []
            else:
                if not product_ids:
                    st.session_state[_SUMMARY_FILE_INPUT_STATUS_KIND_KEY] = "warning"
                    st.session_state[_SUMMARY_FILE_INPUT_STATUS_KEY] = "Không tìm thấy mã hợp lệ ở cột A."
                    st.session_state[_SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY] = []
                    st.session_state[_SUMMARY_FILE_INPUT_MATCHED_IDS_KEY] = []
                else:
                    try:
                        matched_products = self._product_service.get_products_by_ids(product_ids)
                    except DataAccessError:
                        st.session_state[_SUMMARY_FILE_INPUT_STATUS_KIND_KEY] = "error"
                        st.session_state[_SUMMARY_FILE_INPUT_STATUS_KEY] = "Không thể tải danh sách sản phẩm theo mã. Vui lòng thử lại."
                        st.session_state[_SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY] = []
                        st.session_state[_SUMMARY_FILE_INPUT_MATCHED_IDS_KEY] = []
                    else:
                        found_ids = {int(getattr(product, "product_id", 0)) for product in matched_products}
                        missing_ids = [product_id for product_id in product_ids if product_id not in found_ids]
                        matched_ids = sorted(found_ids)

                        preview_rows = [
                            {
                                "Mã sản phẩm": getattr(product, "product_id", None),
                                "Tên sản phẩm": getattr(product, "product_name", ""),
                                "Category": getattr(product, "category", ""),
                            }
                            for product in matched_products
                        ]
                        st.session_state[_SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY] = preview_rows
                        st.session_state[_SUMMARY_FILE_INPUT_MATCHED_IDS_KEY] = matched_ids

                        status_parts = [
                            f"Tìm thấy {len(matched_products)}/{len(product_ids)} sản phẩm",
                        ]
                        if missing_ids:
                            status_parts.append(f"không tìm thấy {len(missing_ids)}")

                        st.session_state[_SUMMARY_FILE_INPUT_STATUS_KIND_KEY] = "success" if matched_products else "warning"
                        st.session_state[_SUMMARY_FILE_INPUT_STATUS_KEY] = ". ".join(status_parts) + "."

        status_kind = st.session_state.get(_SUMMARY_FILE_INPUT_STATUS_KIND_KEY)
        status_message = str(st.session_state.get(_SUMMARY_FILE_INPUT_STATUS_KEY, "")).strip()
        if status_message:
            _toast(status_message, kind=str(status_kind or "info"))
            st.session_state[_SUMMARY_FILE_INPUT_STATUS_KEY] = ""
            st.session_state[_SUMMARY_FILE_INPUT_STATUS_KIND_KEY] = None

        preview_rows = list(st.session_state.get(_SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY, []))
        if preview_rows:
            st.dataframe(
                preview_rows,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Mã sản phẩm": st.column_config.NumberColumn("Mã sản phẩm"),
                    "Tên sản phẩm": st.column_config.TextColumn("Tên sản phẩm", width="large"),
                    "Category": st.column_config.TextColumn("Category"),
                },
            )

        st.divider()
        action_cols = st.columns(2, gap="small")
        with action_cols[0]:
            if st.button("OK", type="primary", use_container_width=True, key=_SUMMARY_FILE_INPUT_OK_BTN_KEY):
                matched_ids = list(st.session_state.get(_SUMMARY_FILE_INPUT_MATCHED_IDS_KEY, []))
                if not matched_ids:
                    _toast("Không có sản phẩm hợp lệ để thêm vào danh sách được chọn.", kind="warning")
                else:
                    try:
                        matched_products = self._product_service.get_products_by_ids(matched_ids)
                    except DataAccessError:
                        _toast("Không thể thêm sản phẩm vào danh sách được chọn. Vui lòng thử lại.", kind="error")
                    else:
                        added_count, duplicated_count = self._merge_products_to_selected(matched_products)
                        message_parts = [f"Đã thêm {added_count} sản phẩm vào DS được chọn"]
                        if duplicated_count:
                            message_parts.append(f"trùng {duplicated_count}")
                        st.session_state[_SUMMARY_CODE_INPUT_STATUS_KIND_KEY] = "success"
                        st.session_state[_SUMMARY_CODE_INPUT_STATUS_KEY] = ". ".join(message_parts) + "."
                        st.session_state[_SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY] = []
                        st.session_state[_SUMMARY_FILE_INPUT_MATCHED_IDS_KEY] = []
                        st.session_state[_SUMMARY_FILE_INPUT_STATUS_KEY] = ""
                        st.session_state[_SUMMARY_FILE_INPUT_STATUS_KIND_KEY] = None
                        st.rerun()
        with action_cols[1]:
            if st.button("Cancel", use_container_width=True, key=_SUMMARY_FILE_INPUT_CANCEL_BTN_KEY):
                st.session_state[_SUMMARY_FILE_INPUT_PREVIEW_ROWS_KEY] = []
                st.session_state[_SUMMARY_FILE_INPUT_MATCHED_IDS_KEY] = []
                st.session_state[_SUMMARY_FILE_INPUT_STATUS_KEY] = ""
                st.session_state[_SUMMARY_FILE_INPUT_STATUS_KIND_KEY] = None
                st.rerun()

    @st.dialog("Tìm kiếm mơ hồ", width="large")
    def show_fuzzy_search_dialog(self) -> None:
        self._ensure_fuzzy_dialog_state()

        search_mode_cols = st.columns(2, gap="small")
        with search_mode_cols[0]:
            st.selectbox(
                "Cách tìm",
                options=[
                    "Tìm bằng tên sp (full-width)",
                    "Tìm bằng tên sp (half-width)",
                    "Tìm bằng mã sp",
                ],
                key=_SUMMARY_FUZZY_SEARCH_BY_KEY,
            )
        with search_mode_cols[1]:
            st.selectbox(
                "Kiểu khớp",
                options=[
                    "Tìm những mục khớp chính xác từ đầu chuỗi (prefix match)",
                    "Tìm những mục khớp chính xác từ cuối chuỗi (suffix match)",
                    "Tìm những mục có chứa chuỗi cần tìm ở bất kỳ vị trí nào",
                ],
                key=_SUMMARY_FUZZY_MATCH_MODE_KEY,
            )

        filter_mode_cols = st.columns(2, gap="small")

        with filter_mode_cols[0]:
            st.checkbox("Tìm theo phân loại", key=_SUMMARY_FUZZY_CATEGORY_ENABLED_KEY)
            st.selectbox(
                "Phân loại",
                options=["Rau", "Củ", "Quả", "Đồ khô"],
                key=_SUMMARY_FUZZY_CATEGORY_KEY,
                disabled=not bool(st.session_state.get(_SUMMARY_FUZZY_CATEGORY_ENABLED_KEY, False)),
                label_visibility="collapsed",
            )
        with filter_mode_cols[1]:
            st.checkbox("Tìm theo đối tác", key=_SUMMARY_FUZZY_PARTNER_ENABLED_KEY)
            st.selectbox(
                "Đối tác",
                options=["Đối tác A", "Đối tác B", "Đối tác C"],
                key=_SUMMARY_FUZZY_PARTNER_KEY,
                disabled=not bool(st.session_state.get(_SUMMARY_FUZZY_PARTNER_ENABLED_KEY, False)),
                label_visibility="collapsed",
            )

        keyword_col, search_btn_col = st.columns([5.0, 1.2], gap="small")
        with keyword_col:
            st.text_input(
                "Từ khóa",
                key=_SUMMARY_FUZZY_QUERY_KEY,
                placeholder="Nhập từ khóa tìm kiếm...",
            )
        with search_btn_col:
            st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
            st.button("Tìm kiếm", type="primary", use_container_width=True, key=_SUMMARY_FUZZY_SEARCH_SUBMIT_BTN_KEY)

        available_items = list(st.session_state.get(_SUMMARY_FUZZY_ITEMS_KEY, []))
        selected_items = list(st.session_state.get(_SUMMARY_FUZZY_SELECTED_ITEMS_KEY, []))
        available_picks = [
            item for item in st.session_state.get(_SUMMARY_FUZZY_AVAILABLE_PICK_KEY, []) if item in available_items
        ]
        selected_picks = [
            item for item in st.session_state.get(_SUMMARY_FUZZY_SELECTED_PICK_KEY, []) if item in selected_items
        ]
        st.session_state[_SUMMARY_FUZZY_AVAILABLE_PICK_KEY] = available_picks
        st.session_state[_SUMMARY_FUZZY_SELECTED_PICK_KEY] = selected_picks

        list_col_left, action_col, list_col_right = st.columns([1.6, 0.3, 1.6], gap="small")
        with list_col_left:
            self._render_checkbox_list_box(
                label="Danh sách khả dụng",
                items=available_items,
                pick_key=_SUMMARY_FUZZY_AVAILABLE_PICK_KEY,
                signature_key=_SUMMARY_FUZZY_AVAILABLE_SIGNATURE_KEY,
                checkbox_keys_state_key=_SUMMARY_FUZZY_AVAILABLE_CHECKBOX_KEYS,
                checkbox_prefix="summary_fuzzy_available_chk",
            )
        with action_col:
            st.markdown("<div style='height: 72px;'></div>", unsafe_allow_html=True)
            st.button(
                "",
                icon=":material/keyboard_double_arrow_right:",
                use_container_width=True,
                key=_SUMMARY_FUZZY_MOVE_ALL_RIGHT_BTN_KEY,
                on_click=self._on_fuzzy_move_all_right,
            )
            st.button(
                "",
                icon=":material/chevron_right:",
                use_container_width=True,
                key=_SUMMARY_FUZZY_MOVE_RIGHT_BTN_KEY,
                on_click=self._on_fuzzy_move_right,
            )
            st.button(
                "",
                icon=":material/chevron_left:",
                use_container_width=True,
                key=_SUMMARY_FUZZY_MOVE_LEFT_BTN_KEY,
                on_click=self._on_fuzzy_move_left,
            )
            st.button(
                "",
                icon=":material/keyboard_double_arrow_left:",
                use_container_width=True,
                key=_SUMMARY_FUZZY_MOVE_ALL_LEFT_BTN_KEY,
                on_click=self._on_fuzzy_move_all_left,
            )
            st.markdown("<div style='height: 64px;'></div>", unsafe_allow_html=True)
        with list_col_right:
            self._render_checkbox_list_box(
                label="Danh sách được chọn",
                items=selected_items,
                pick_key=_SUMMARY_FUZZY_SELECTED_PICK_KEY,
                signature_key=_SUMMARY_FUZZY_SELECTED_SIGNATURE_KEY,
                checkbox_keys_state_key=_SUMMARY_FUZZY_SELECTED_CHECKBOX_KEYS,
                checkbox_prefix="summary_fuzzy_selected_chk",
            )

        st.divider();
        action_cols = st.columns(2, gap="small")
        with action_cols[0]:
            if st.button("OK", type="primary", use_container_width=True, key=_SUMMARY_FUZZY_OK_BTN_KEY):
                st.rerun()
        with action_cols[1]:
            if st.button("Cancel", use_container_width=True, key=_SUMMARY_FUZZY_CANCEL_BTN_KEY):
                st.rerun()

    @st.dialog("Phân loại tùy chọn", width="medium")
    def show_custom_category_dialog(self) -> None:
        self._ensure_custom_tree_source()

        status_kind = st.session_state.get(_SUMMARY_CUSTOM_STATUS_KIND_KEY)
        status_message = str(st.session_state.get(_SUMMARY_CUSTOM_STATUS_KEY, "")).strip()
        if status_message:
            _toast(status_message, kind=str(status_kind or "info"))
            st.session_state[_SUMMARY_CUSTOM_STATUS_KEY] = ""
            st.session_state[_SUMMARY_CUSTOM_STATUS_KIND_KEY] = None

        all_category_items = list(st.session_state.get(_SUMMARY_CUSTOM_TREE_CATEGORIES_KEY, []))
        tree_query = str(st.session_state.get(_SUMMARY_CUSTOM_TREE_QUERY_KEY, ""))
        category_items = self._filter_custom_categories(all_category_items, tree_query)
        selected_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, []))
        selected_picks = [
            item for item in st.session_state.get(_SUMMARY_CUSTOM_SELECTED_PICK_KEY, []) if item in selected_items
        ]
        st.session_state[_SUMMARY_CUSTOM_SELECTED_PICK_KEY] = selected_picks

        list_col_left, action_col, list_col_right = st.columns([1.6, 0.3, 1.6], gap="small")

        with list_col_left:
            st.markdown("**Phân loại**")
            tree_nodes = self._build_custom_tree_nodes(category_items)
            checked = list(st.session_state.get(_SUMMARY_CUSTOM_TREE_CHECKED_KEY, []))
            expanded = list(st.session_state.get(_SUMMARY_CUSTOM_TREE_EXPANDED_KEY, ["root"]))
            current_category_values = {self._to_custom_tree_category_value(item) for item in all_category_items}
            checked = [
                value
                for value in checked
                if value == "root" or value in current_category_values
            ]
            st.session_state[_SUMMARY_CUSTOM_TREE_CHECKED_KEY] = checked

            selected_tree = tree_select(
                nodes=tree_nodes,
                check_model="all",
                checked=checked,
                expanded=expanded,
                show_expand_all=True,
                key=_SUMMARY_CUSTOM_TREE_COMPONENT_KEY,
            )
            if isinstance(selected_tree, dict):
                st.session_state[_SUMMARY_CUSTOM_TREE_CHECKED_KEY] = list(selected_tree.get("checked", []))
                st.session_state[_SUMMARY_CUSTOM_TREE_EXPANDED_KEY] = list(selected_tree.get("expanded", []))
            st.text_input(
                "",
                placeholder="Tìm theo tên phân loại...",
                label_visibility="collapsed",
                key=_SUMMARY_CUSTOM_TREE_QUERY_KEY,
            )

        with action_col:
            st.markdown("<div style='height: 72px;'></div>", unsafe_allow_html=True)
            st.button(
                ">>",
                help="Thêm sản phẩm theo category đã chọn",
                use_container_width=True,
                key=_SUMMARY_CUSTOM_MOVE_RIGHT_TREE_BTN_KEY,
                on_click=self._on_custom_move_right,
            )
            st.markdown("<div style='height: 64px;'></div>", unsafe_allow_html=True)

        with list_col_right:
            self._render_checkbox_list_box(
                label="Danh sách được chọn",
                items=selected_items,
                pick_key=_SUMMARY_CUSTOM_SELECTED_PICK_KEY,
                signature_key=_SUMMARY_CUSTOM_SELECTED_SIGNATURE_KEY,
                checkbox_keys_state_key=_SUMMARY_CUSTOM_SELECTED_CHECKBOX_KEYS,
                checkbox_prefix="summary_custom_selected_chk",
            )
            st.button(
                "Xóa khỏi danh sách",
                use_container_width=True,
                key=_SUMMARY_CUSTOM_REMOVE_SELECTED_BTN_KEY,
                on_click=self._on_custom_remove_selected,
            )

        st.divider()
        footer_col_left, footer_col_right = st.columns([1.0, 2.2], gap="small")
        with footer_col_left:
            if st.button("OK", type="primary", use_container_width=True, key=_SUMMARY_CUSTOM_CATEGORY_OK_BTN_KEY):
                st.session_state[_SUMMARY_CUSTOM_SHOW_SELECTED_DETAIL_KEY] = False
                st.rerun()
        with footer_col_right:
            st.button(
                "Chi tiết DS đã chọn",
                use_container_width=True,
                key=_SUMMARY_CUSTOM_SHOW_SELECTED_DETAIL_BTN_KEY,
                on_click=self._toggle_custom_selected_detail,
            )

        if bool(st.session_state.get(_SUMMARY_CUSTOM_SHOW_SELECTED_DETAIL_KEY, False)):
            current_selected_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, []))
            st.markdown("**Chi tiết danh sách được chọn**")
            with st.container(border=True, height=180):
                if not current_selected_items:
                    st.caption("Chưa có sản phẩm nào trong danh sách được chọn.")
                else:
                    detail_map = dict(st.session_state.get(_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY, {}))
                    detail_rows: list[dict[str, object]] = []
                    for item in current_selected_items:
                        detail = detail_map.get(item, {})
                        detail_rows.append(
                            {
                                "item": item,
                                "category": detail.get("category", ""),
                                "price": detail.get("price", None),
                                "unit": detail.get("unit", ""),
                                "description": detail.get("description", ""),
                                "stock_quantity": detail.get("stock_quantity", None),
                            }
                        )
                    st.dataframe(
                        detail_rows,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "item": st.column_config.TextColumn("Sản phẩm", width="medium"),
                            "category": st.column_config.TextColumn("Category"),
                            "price": st.column_config.NumberColumn("Price", format="%.2f"),
                            "unit": st.column_config.TextColumn("Unit"),
                            "description": st.column_config.TextColumn("Description", width="large"),
                            "stock_quantity": st.column_config.NumberColumn("Stock Quantity"),
                        },
                    )

    def _serialize_filter(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key in sorted(st.session_state.keys()):
            if not self._is_filter_state_key(key):
                continue
            value = st.session_state.get(key)
            normalized = self._normalize_json_compatible(value)
            if normalized is None and value is not None:
                continue
            payload[key] = normalized
        return payload

    def _apply_payload_to_state(self, payload: dict[str, Any]) -> None:
        existing_filter_keys = [key for key in st.session_state.keys() if self._is_filter_state_key(key)]
        for key in existing_filter_keys:
            st.session_state.pop(key, None)
        for key, value in payload.items():
            if not self._is_filter_state_key(key):
                continue
            st.session_state[key] = value

    @classmethod
    def _is_filter_state_key(cls, key: str) -> bool:
        if not str(key).startswith("summary_"):
            return False
        if key in cls._TRANSIENT_STATE_KEYS:
            return False
        transient_prefixes = (
            "summary_code_input_available_chk_",
            "summary_code_input_selected_chk_",
            "summary_fuzzy_available_chk_",
            "summary_fuzzy_selected_chk_",
            "summary_custom_available_chk_",
            "summary_custom_selected_chk_",
            _SUMMARY_WEEK_PERIOD_CHECKBOX_KEY_PREFIX,
            "summary_share_",
        )
        if key.startswith(transient_prefixes):
            return False
        if key.endswith("__left") or key.endswith("__right"):
            return False
        # Button widgets cannot be assigned via st.session_state.
        if key.endswith("_btn"):
            return False
        return True

    @staticmethod
    def _normalize_json_compatible(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (list, tuple)):
            items: list[Any] = []
            for item in value:
                normalized = SummaryReportFilterSection._normalize_json_compatible(item)
                if normalized is None and item is not None:
                    return None
                items.append(normalized)
            return items
        if isinstance(value, dict):
            normalized_map: dict[str, Any] = {}
            for key, item in value.items():
                key_text = str(key or "").strip()
                if not key_text:
                    continue
                normalized = SummaryReportFilterSection._normalize_json_compatible(item)
                if normalized is None and item is not None:
                    return None
                normalized_map[key_text] = normalized
            return normalized_map
        return None

    def _clear_filters_cache(self) -> None:
        token_key = f"_{self.report_code}_filter_cache_token"
        st.session_state[token_key] = int(st.session_state.get(token_key, 0)) + 1

    @st.dialog("Luu bo loc moi")
    def _show_save_filter_dialog(self, actor_user_id: str) -> None:
        st.write("Nhap ten de luu bo loc hien tai.")
        filter_name = st.text_input("Ten bo loc", placeholder="Vi du: Bao cao tuan 1, Bao cao cua hang...")

        if st.button("Xac nhan luu", type="secondary", use_container_width=True):
            if not filter_name.strip():
                _toast("Vui long nhap ten bo loc.", kind="error")
                return

            try:
                self._report_filter_service.save_filter(
                    report_code=self.report_code,
                    actor_user_id=actor_user_id,
                    filter_name=filter_name,
                    raw_filter_payload=self._serialize_filter(),
                )
                self._clear_filters_cache()
                _toast(f"Da luu bo loc '{filter_name}' thanh cong.", kind="success")
                st.rerun()
            except (BusinessRuleError, ValueError) as exc:
                _toast(str(exc), kind="error")

    @st.dialog("Quản lý bộ lọc", width="large")
    def _show_management_dialog(self, filter_store: FilterStore) -> None:
        actor_user_id = self._report_filter_service.resolve_user_id(get_current_username())
        if not actor_user_id:
            _toast("Khong xac dinh duoc user hien tai.", kind="error")
            return

        try:
            with st.spinner("Đang tải danh sách bộ lọc..."):
                saved = _get_cached_filter_list(
                    self.report_code,
                    actor_user_id,
                    int(st.session_state.get(f"_{self.report_code}_filter_cache_token", 0)),
                    self._report_filter_service,
                )
        except Exception:
            _toast("Khong tai duoc danh sach bo loc.", kind="error")
            return

        tab_my, tab_shared = st.tabs(["Bộ lọc đã lưu", "Bộ lọc chia sẻ"])
        with tab_my:
            self._render_my_filters_table(filter_store, actor_user_id, saved.my_filters)
        with tab_shared:
            self._render_shared_filters_table(filter_store, actor_user_id, saved.shared_with_me)

        st.divider()
        close_cols = st.columns([8.8, 1.2], vertical_alignment="center")
        with close_cols[1]:
            if st.button("Đóng", type="primary", use_container_width=True, key="summary_manage_close_btn"):
                st.session_state.pop(f"_{self.report_code}_pending_share_filter_id", None)
                st.session_state.pop(f"_{self.report_code}_pending_share_filter_name", None)
                st.rerun()


    def _render_my_filters_table(
        self,
        filter_store: FilterStore,
        actor_user_id: str,
        my_filters: list[ReportFilterDefinition],
    ) -> None:
        if not my_filters:
            st.caption("Chua co bo loc ca nhan.")
            return

        cols = st.columns([6.7, 1.0, 1.3], gap=None)
        cols[0].markdown("<div class='cols-table-head'>Tên bộ lọc</div>", unsafe_allow_html=True)
        cols[1].markdown("<div class='cols-table-head'>Thời gian cập nhật</div>",   unsafe_allow_html=True)
        cols[2].markdown("<div class='cols-table-head'>Hành động</div>",  unsafe_allow_html=True)

        for idx, item in enumerate(my_filters):
            with st.container():
                c1, c2, c3 = st.columns([6.7, 1.0, 1.3], gap=None)

                alt_class = " alt" if idx % 2 else ""
                row_class  = "cols-table-cell" + (" cols-table-cell-alt" if idx % 2 else "")

                c1.markdown(f"<div class='{row_class}'>{item.filter_name}</div>", unsafe_allow_html=True)
                c2.markdown(f"<div class='{row_class}'>{item.updated_at.strftime('%Y-%m-%d %H:%M')}</div>", unsafe_allow_html=True)

                with c3:
                    st.markdown(f'<div class="cols-table-body{alt_class}"></div>', unsafe_allow_html=True)

                    btn_cols = st.columns(3, gap=None)

                    if btn_cols[0].button(
                        "",
                        icon=":material/play_arrow:",
                        help="Áp dụng",
                        key=f"summary_apply_{item.filter_definition_id}",
                        use_container_width=True,
                    ):
                        self._apply_filter_logic(
                            filter_store,
                            actor_user_id,
                            item.filter_definition_id,
                            cached_payload=item.filter_payload,
                        )
                        st.rerun()

                    if btn_cols[1].button(
                        "",
                        icon=":material/share:",
                        help="Chia sẻ",
                        key=f"summary_share_toggle_{item.filter_definition_id}",
                        use_container_width=True,
                    ):
                        st.session_state[f"_{self.report_code}_pending_share_filter_id"] = item.filter_definition_id
                        st.session_state[f"_{self.report_code}_pending_share_filter_name"] = item.filter_name

                    if btn_cols[2].button(
                        "",
                        type="primary",
                        icon=":material/delete:",
                        help="Xóa",
                        key=f"summary_delete_{item.filter_definition_id}",
                        use_container_width=True,
                    ):
                        self._delete_selected_filter(actor_user_id, item.filter_definition_id)

        pending_share_filter_id = st.session_state.get(f"_{self.report_code}_pending_share_filter_id")
        pending_share_filter_name = str(st.session_state.get(f"_{self.report_code}_pending_share_filter_name", "")).strip()
        if pending_share_filter_id:
            label_name = pending_share_filter_name or str(pending_share_filter_id)
            with st.expander(f"Chia sẻ bộ lọc: {label_name}", expanded=True):
                self._render_sharing_sub_section(actor_user_id, str(pending_share_filter_id), label_name)

        # st.divider()
        # close_cols = st.columns([8.8, 1.2], vertical_alignment="center")
        # with close_cols[1]:
        #     if st.button("Đóng", type="primary", use_container_width=True, key="summary_manage_close_btn"):
        #         st.session_state.pop(f"_{self.report_code}_pending_share_filter_id", None)
        #         st.session_state.pop(f"_{self.report_code}_pending_share_filter_name", None)
        #         st.rerun()

    def _render_sharing_sub_section(self, actor_user_id: str, filter_definition_id: str, filter_name: str) -> None:
        share_target_key = f"summary_share_target_{filter_definition_id}"
        default_shared_user_ids: list[str] = []

        with st.spinner("Đang tải dữ liệu chia sẻ..."):
            recipients = [r for r in self._report_filter_service.list_recipients() if r[0] != actor_user_id]
            if share_target_key not in st.session_state:
                try:
                    current_recipients = self._report_filter_service.get_share_recipients(filter_definition_id, actor_user_id)
                    default_shared_user_ids = [
                        recipient.recipient_user_id
                        for recipient in current_recipients
                        if recipient.revoked_at is None and recipient.recipient_status == RecipientStatus.ACTIVE
                    ]
                except Exception:
                    default_shared_user_ids = []

        recipient_map = {uid: name for uid, name in recipients}

        if share_target_key not in st.session_state:
            st.session_state[share_target_key] = [uid for uid in default_shared_user_ids if uid in recipient_map]

        with st.form(key=f"summary_share_form_{filter_definition_id}", border=False):
            st.multiselect(
                "Người nhận",
                options=list(recipient_map.keys()),
                format_func=lambda uid: f"{recipient_map[uid]} ({uid})",
                key=share_target_key,
            )
            submitted = st.form_submit_button("Chia sẻ ngay", use_container_width=True)

        if submitted:
            selected = list(st.session_state.get(share_target_key, []))
            try:
                self._report_filter_service.share_filter(filter_definition_id, actor_user_id, selected)
                _toast("Da chia se.", kind="success")
                self._clear_filters_cache()
                st.session_state.pop(f"_{self.report_code}_pending_share_filter_id", None)
                st.session_state.pop(f"_{self.report_code}_pending_share_filter_name", None)
                st.rerun()
            except Exception as exc:
                _toast(str(exc), kind="error")

    def _render_shared_filters_table(
        self,
        filter_store: FilterStore,
        actor_user_id: str,
        shared_filters: list[SharedReportFilter],
    ) -> None:
        if not shared_filters:
            st.caption("Chua co bo loc duoc chia se.")
            return

        cols = st.columns([3.5, 2.5, 2.0, 3.0])
        cols[0].markdown("<div class='cols-table-head'>Ten bo loc</div>", unsafe_allow_html=True)
        cols[1].markdown("<div class='cols-table-head'>Nguoi so huu</div>", unsafe_allow_html=True)
        cols[2].markdown("<div class='cols-table-head'>Cap nhat</div>", unsafe_allow_html=True)
        cols[3].markdown("<div class='cols-table-head'>Hanh dong</div>", unsafe_allow_html=True)

        for idx, item in enumerate(shared_filters):
            c1, c2, c3, c4 = st.columns([3.5, 2.5, 2.0, 3.0])
            row_class = "cols-table-cell cols-table-cell-alt" if idx % 2 else "cols-table-cell"
            owner_text = item.owner_username or item.owner_user_id
            c1.markdown(f"<div class='{row_class}'>{item.filter_name}</div>", unsafe_allow_html=True)
            c2.markdown(f"<div class='{row_class}'>{owner_text}</div>", unsafe_allow_html=True)
            c3.markdown(f"<div class='{row_class}'>{item.updated_at.strftime('%Y-%m-%d %H:%M')}</div>", unsafe_allow_html=True)
            c4.markdown(
                f"<div class='{row_class} cols-table-actions-spacer'>&nbsp;</div>",
                unsafe_allow_html=True,
            )

            btn_cols = c4.columns(2)
            if btn_cols[0].button("Ap dung", key=f"summary_apply_shared_{item.filter_definition_id}"):
                self._apply_filter_logic(
                    filter_store,
                    actor_user_id,
                    item.filter_definition_id,
                    cached_payload=item.filter_payload,
                )
                st.rerun()
            if btn_cols[1].button("Luu moi", key=f"summary_copy_shared_{item.filter_definition_id}"):
                self._report_filter_service.save_as_new(
                    item.filter_definition_id,
                    actor_user_id,
                    f"{item.filter_name} - copy",
                )
                self._clear_filters_cache()
                _toast("Da tao ban sao.", kind="success")
                st.rerun()

        st.markdown("<div class='cols-table-bottom-gap'></div>", unsafe_allow_html=True)

    def _apply_filter_logic(
        self,
        filter_store: FilterStore,
        actor_user_id: str,
        filter_definition_id: str,
        cached_payload: dict[str, Any] | None = None,
    ) -> None:
        try:
            if cached_payload is not None:
                apply_result = parse_report_filter_payload(self.report_code, cached_payload)
            else:
                apply_result = self._report_filter_service.get_apply_payload(
                    filter_definition_id=filter_definition_id,
                    actor_user_id=actor_user_id,
                    current_report_code=self.report_code,
                )
            self._apply_payload_to_state(apply_result.payload)
            filter_store.replace_payload(apply_result.payload)
            if apply_result.ignored_fields:
                _toast(
                    "Mot so truong khong con hop le da duoc bo qua: " + ", ".join(apply_result.ignored_fields),
                    kind="warning",
                )
        except BusinessRuleError as exc:
            _toast(str(exc), kind="warning")
        except Exception as exc:
            _toast(f"Loi he thong: {str(exc)}", kind="warning")

    def _delete_selected_filter(self, actor_user_id: str, filter_definition_id: str) -> None:
        try:
            self._report_filter_service.delete_filter(
                filter_definition_id=filter_definition_id,
                actor_user_id=actor_user_id,
            )
            self._clear_filters_cache()
            _toast("Da xoa bo loc.", kind="success")
            st.rerun()
        except Exception as exc:
            _toast(str(exc), kind="warning")

    def _render_action_buttons_row(self, key_prefix: str | None = None) -> None:
        def _btn_key(suffix: str) -> str:
            if key_prefix:
                return f"{key_prefix}_{suffix}"
            key_map = {
                "reset_default_btn": _SUMMARY_RESET_DEFAULT_BTN_KEY,
                "save_filter_btn": _SUMMARY_SAVE_FILTER_BTN_KEY,
                "open_filter_btn": _SUMMARY_OPEN_FILTER_BTN_KEY,
                "setting_btn": _SUMMARY_SETTING_BTN_KEY,
                "start_btn": _SUMMARY_START_BTN_KEY,
                "close_btn": _SUMMARY_CLOSE_BTN_KEY,
            }
            return key_map.get(suffix, f"summary_{suffix}")

        action_cols = st.columns(6)
        action_cols[0].button("Mặc định", use_container_width=True, key=_btn_key("reset_default_btn"))
        if action_cols[1].button("Lưu", use_container_width=True, key=_btn_key("save_filter_btn")):
            actor_user_id = self._report_filter_service.resolve_user_id(get_current_username())
            if actor_user_id:
                self._show_save_filter_dialog(actor_user_id)
            else:
                _toast("Khong xac dinh duoc user.", kind="error")
        if action_cols[2].button("Quản lý", use_container_width=True, key=_btn_key("open_filter_btn")):
            self._show_management_dialog(FilterStore(self.report_code))
        action_cols[3].button("Cài đặt", use_container_width=True, key=_btn_key("setting_btn"))
        if action_cols[4].button("Phân tích", type="primary", use_container_width=True, key=_btn_key("start_btn")):
            from datetime import datetime

            _toast(f"Đã ghi nhận cấu hình bộ lọc lúc {datetime.now().strftime('%H:%M:%S')}.", kind="success")
        if action_cols[5].button("Kết thúc", use_container_width=True, key=_btn_key("close_btn")):
            _toast("Bạn đã chọn kết thúc thao tác trên màn hình Báo cáo tổng hợp.", kind="info")

    def render(self) -> None:
        st.markdown("### Bộ lọc báo cáo")

        month_choices = _month_options()
        _sync_header_month_range()
        selected_data_type = str(st.session_state.get(_SUMMARY_DATA_TYPE_KEY, "Dữ liệu hàng tháng"))
        previous_data_type = st.session_state.get(_SUMMARY_PREVIOUS_DATA_TYPE_KEY)
        if previous_data_type is None:
            st.session_state[_SUMMARY_PREVIOUS_DATA_TYPE_KEY] = selected_data_type
        elif previous_data_type != selected_data_type:
            _reset_summary_period_values()
            _reset_week_period_dialog_state()
            st.session_state[_SUMMARY_WEEK_PERIOD_SELECTED_LABELS_KEY] = []
            st.session_state[_SUMMARY_PREVIOUS_DATA_TYPE_KEY] = selected_data_type
        show_day_in_header = selected_data_type != "Dữ liệu hàng tháng"
        period_from_display = _format_period_display(
            st.session_state.get(_SUMMARY_PERIOD_FROM_YEAR_KEY),
            st.session_state.get(_SUMMARY_PERIOD_FROM_MONTH_KEY),
            st.session_state.get(_SUMMARY_PERIOD_FROM_DAY_KEY),
            show_day=show_day_in_header,
        )
        period_to_display = _format_period_display(
            st.session_state.get(_SUMMARY_PERIOD_TO_YEAR_KEY),
            st.session_state.get(_SUMMARY_PERIOD_TO_MONTH_KEY),
            st.session_state.get(_SUMMARY_PERIOD_TO_DAY_KEY),
            show_day=show_day_in_header,
        )
        month_col_from, month_col_to = st.columns(2)
        with month_col_from:
            st.text_input("Dữ liệu từ", value=period_from_display, disabled=True)
        with month_col_to:
            st.text_input("đến", value=period_to_display, disabled=True)

        left_col, right_col = st.columns(2, gap="small")

        with left_col:
            with st.container(border=True):
                st.markdown("**1. Tên báo cáo**")
                st.text_input(
                    "Tên báo cáo",
                    value="Báo cáo tổng hợp",
                    label_visibility="collapsed",
                    key="summary_report_name",
                )

            with st.container(border=True):
                st.markdown("**2. Loại báo cáo**")
                st.selectbox(
                    "Loại báo cáo",
                    options=["Tổng hợp theo sản phẩm", "Tổng hợp theo nhóm", "Tổng hợp theo cửa hàng"],
                    index=0,
                    label_visibility="collapsed",
                    key="summary_report_type",
                )

            with st.container(border=True):
                st.markdown("**3. Chỉ định trục dọc**")
                y1, y2 = st.columns(2)
                y1.selectbox(
                    "Trục dọc 1",
                    options=["Sản phẩm", "Cửa hàng", "Kỳ", "Phân loại", "Các mục đánh giá"],
                    key="summary_vertical_axis_1",
                )
                y2.selectbox(
                    "Trục dọc 2",
                    options=["Không", "Sản phẩm", "Cửa hàng", "Kỳ", "Phân loại", "Các mục đánh giá"],
                    index=0,
                    key="summary_vertical_axis_2",
                )

            with st.container(border=True):
                st.markdown("**4. Chỉ định trục ngang**")
                x1, x2 = st.columns(2)
                x1.selectbox(
                    "Trục ngang 1",
                    options=["Sản phẩm", "Cửa hàng", "Kỳ", "Phân loại", "Các mục đánh giá"],
                    index=1,
                    key="summary_horizontal_axis_1",
                )
                y2_horizontal = x2.selectbox(
                    "Trục ngang 2",
                    options=["Không", "Sản phẩm", "Cửa hàng", "Kỳ", "Phân loại", "Các mục đánh giá"],
                    index=0,
                    key="summary_horizontal_axis_2",
                )

            with st.container(border=True):
                st.markdown("**5. Loại dữ liệu**")
                selected_data_type = _render_two_column_radio(
                    "",
                    options=["Dữ liệu hàng tháng", "Dữ liệu hàng tuần", "Dữ liệu hàng ngày"],
                    key=_SUMMARY_DATA_TYPE_KEY,
                )

                st.button("Phân loại cung ứng", use_container_width=True, key=_SUMMARY_SUPPLY_CATEGORY_BTN_KEY)

            with st.container(border=True):
                st.markdown("**6. Kỳ tổng hợp**")
                current_year = max(2020, date.today().year)
                year_options = ["", *[str(year) for year in range(2020, current_year + 1)]]
                month_number_options = ["", *[f"{month:02d}" for month in range(1, 13)]]
                day_number_options = ["", *[f"{day:02d}" for day in range(1, 32)]]

                is_monthly = selected_data_type == "Dữ liệu hàng tháng"
                is_weekly = selected_data_type == "Dữ liệu hàng tuần"

                if is_weekly:
                    month_from = str(st.session_state.get(_SUMMARY_DATA_MONTH_FROM_KEY, month_choices[0]))
                    month_to = str(st.session_state.get(_SUMMARY_DATA_MONTH_TO_KEY, month_choices[-1]))
                    weekly_options = _weekly_period_options(month_from=month_from, month_to=month_to)
                    if st.button("Chọn kỳ", use_container_width=True, key=_SUMMARY_SELECT_PERIOD_BTN_KEY):
                        _reset_week_period_dialog_state()
                        self.show_week_period_dialog(weekly_options)

                if is_monthly:
                    from_row_cols = st.columns(2)
                else:
                    from_row_cols = st.columns(3)

                from_row_cols[0].selectbox(
                    "Từ năm",
                    options=year_options,
                    index=0,
                    format_func=lambda v: "--" if v == "" else v,
                    key=_SUMMARY_PERIOD_FROM_YEAR_KEY,
                    disabled=is_weekly,
                )
                from_row_cols[1].selectbox(
                    "Từ tháng",
                    options=month_number_options,
                    index=0,
                    format_func=lambda v: "--" if v == "" else v,
                    key=_SUMMARY_PERIOD_FROM_MONTH_KEY,
                    disabled=is_weekly,
                )
                if not is_monthly:
                    from_row_cols[2].selectbox(
                        "Từ ngày",
                        options=day_number_options,
                        index=0,
                        format_func=lambda v: "--" if v == "" else v,
                        key=_SUMMARY_PERIOD_FROM_DAY_KEY,
                        disabled=is_weekly,
                    )
                else:
                    st.session_state[_SUMMARY_PERIOD_FROM_DAY_KEY] = ""

                if is_monthly:
                    to_row_cols = st.columns(2)
                else:
                    to_row_cols = st.columns(3)

                to_row_cols[0].selectbox(
                    "Đến năm",
                    options=year_options,
                    index=0,
                    format_func=lambda v: "--" if v == "" else v,
                    key=_SUMMARY_PERIOD_TO_YEAR_KEY,
                    disabled=is_weekly,
                )
                to_row_cols[1].selectbox(
                    "Đến tháng",
                    options=month_number_options,
                    index=0,
                    format_func=lambda v: "--" if v == "" else v,
                    key=_SUMMARY_PERIOD_TO_MONTH_KEY,
                    disabled=is_weekly,
                )
                if not is_monthly:
                    to_row_cols[2].selectbox(
                        "Đến ngày",
                        options=day_number_options,
                        index=0,
                        format_func=lambda v: "--" if v == "" else v,
                        key=_SUMMARY_PERIOD_TO_DAY_KEY,
                        disabled=is_weekly,
                    )
                else:
                    st.session_state[_SUMMARY_PERIOD_TO_DAY_KEY] = ""

        with right_col:
            with st.container(border=True):
                st.markdown("**7. Chọn trục ngữ nghĩa**")
                st.selectbox(
                    "Chỉ định trục",
                    options=[
                        "Không phân tầng",
                        "Theo hợp tác xã",
                        "Theo khu vực",
                        "Theo mô hình hoạt động",
                        "Hợp tác xã ➡ Mô hình hoạt động",
                        "Hợp tác xã ➡ Khu vực",
                        "Mô hình hoạt động ➡ Khu vực",
                        "Mô hình hoạt động ➡ Hợp tác xã",
                        "Khu vực ➡ Mô hình hoạt động",
                        "Hợp tác xã ➡ Mô hình hoạt động ➡ Khu vực",
                    ],
                    key=_SUMMARY_AXIS_TYPE_KEY,
                    on_change=self._on_axis_type_change,
                )
                axis_type_selected = str(st.session_state.get(_SUMMARY_AXIS_TYPE_KEY, "Không phân tầng"))
                if bool(st.session_state.get(_SUMMARY_AXIS_DIALOG_OPEN_KEY, False)) and axis_type_selected:
                    self.show_axis_type_dialog(axis_type_selected)
                st.selectbox(
                    "Chỉ định tầng",
                    options=[
                        "Cửa hàng",
                        "Khu vực",
                        "Giá trị thống kê của mỗi cửa hàng và tổng toàn bộ cửa hàng",
                        "Tổng toàn bộ cửa hàng",
                    ],
                    key="summary_level_type",
                )
                st.button(
                    "Chọn trường",
                    use_container_width=True,
                    key=_SUMMARY_FIELD_SELECTOR_BTN_KEY,
                    on_click=self._open_axis_type_dialog,
                )
                selected_stores = list(st.session_state.get(_SUMMARY_AXIS_SELECTED_STORE_LABELS_KEY, []))
                if selected_stores:
                    with st.expander(f"DS cửa hàng đã chọn ({len(selected_stores)})", expanded=True):
                        st.dataframe(
                            [{"Cửa hàng": item} for item in selected_stores],
                            use_container_width=True,
                            hide_index=True,
                            height=180,
                        )

            with st.container(border=True):
                st.markdown("**8. Chọn sản phẩm đơn lẻ**")
                row_1 = st.columns(2)
                row_2 = st.columns(2)
                if row_1[0].button("Đọc file", use_container_width=True, key=_SUMMARY_LOAD_FILE_BTN_KEY):
                    self.show_file_input_dialog()
                if row_1[1].button(
                    "Tìm kiếm mơ hồ (aka. sương sương)",
                    use_container_width=True,
                    key=_SUMMARY_FUZZY_SEARCH_BTN_KEY,
                ):
                    self.show_fuzzy_search_dialog()
                if row_2[0].button("Phân loại tùy chọn", use_container_width=True, key=_SUMMARY_CUSTOM_CATEGORY_BTN_KEY):
                    self.show_custom_category_dialog()
                if row_2[1].button("Nhập mã", use_container_width=True, key=_SUMMARY_CODE_INPUT_BTN_KEY):
                    self.show_code_input_dialog()

                selected_items = list(st.session_state.get(_SUMMARY_CODE_INPUT_SELECTED_ITEMS_KEY, []))
                if selected_items:
                    with st.expander(f"DS sản phẩm đã chọn ({len(selected_items)})", expanded=True):
                        detail_map = dict(st.session_state.get(_SUMMARY_CODE_INPUT_DETAIL_MAP_KEY, {}))
                        detail_rows: list[dict[str, object]] = []
                        for item in selected_items:
                            detail = detail_map.get(item, {})
                            detail_rows.append(
                                {
                                    "item": item,
                                    "category": detail.get("category", ""),
                                    "price": detail.get("price", None),
                                    "unit": detail.get("unit", ""),
                                    "description": detail.get("description", ""),
                                    "stock_quantity": detail.get("stock_quantity", None),
                                }
                            )
                        st.dataframe(
                            detail_rows,
                            use_container_width=True,
                            hide_index=True,
                            height=180,
                            column_config={
                                "item": st.column_config.TextColumn("Sản phẩm", width="medium"),
                                "category": st.column_config.TextColumn("Category"),
                                "price": st.column_config.NumberColumn("Price", format="%.2f"),
                                "unit": st.column_config.TextColumn("Unit"),
                                "description": st.column_config.TextColumn("Description", width="large"),
                                "stock_quantity": st.column_config.NumberColumn("Stock Quantity"),
                            },
                        )

            with st.container(border=True):
                st.markdown("**9. Đầu ra**")
                _render_two_column_radio(
                    "",
                    options=["Báo cáo", "Worksheet", "Biểu đồ", "Portfolio"],
                    key="summary_output_target",
                )
                st.markdown("<div style='margin-top: 0.85rem;'></div>", unsafe_allow_html=True)

            st.divider()

            self._render_action_buttons_row()
