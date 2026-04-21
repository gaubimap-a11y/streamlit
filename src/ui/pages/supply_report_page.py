from __future__ import annotations

import unicodedata

import streamlit as st
from pydantic import ValidationError

from src.application.reporting.supply_report_service import MatrixPayload, SupplyReportService
from src.core.exceptions import DataAccessError
from src.core.reporting import ReportData
from src.domain.auth_validation import PermissionDeniedError
from src.domain.supply_report_filter import (
    AXIS_NONE,
    AXIS_DIMENSION_LABELS,
    KPI_NAMES,
    SupplyReportFilter,
)
from src.ui.base.base_report import BaseReport
from src.ui.components.footer import render_dashboard_footer
from src.ui.components.supply_report_filter import render_supply_report_filter
from src.ui.components.supply_report_table import render_supply_report_table
from src.ui.audit_events import get_audit_writer
from src.ui.session.auth_session import get_current_session, require_auth
from src.ui.session.filter_store import FilterStore
from src.ui.styles.loader import inject_css


class SupplyReportPage(BaseReport):
    """Page báo cáo cung ứng sản phẩm đơn lẻ (KOBE-00006).

    Override `render()` để:
    - Có nút `Chạy báo cáo` thay vì auto-run, phù hợp với matrix report
    - Có empty state riêng cho supply report
    - Không dùng pagination vì matrix không phân trang

    Tái sử dụng từ `BaseReport`:
    - `require_auth()`, `has_current_permission()`
    - `_render_page_header()`, `FilterStore`
    - error handling pattern chung
    """

    def __init__(self, supply_service: SupplyReportService | None = None) -> None:
        super().__init__()
        self._supply_service = supply_service or SupplyReportService(audit_writer=get_audit_writer())

    @property
    def report_id(self) -> str:
        return "supply_report"

    @property
    def page_title(self) -> str:
        return "Cung ứng sản phẩm đơn lẻ"

    @property
    def page_icon(self) -> str:
        return "📦"

    @property
    def current_route(self) -> str:
        return "/supply_report"

    def render(self) -> None:
        # AC-11: auth guard đầu trang
        require_auth()
        session = get_current_session()
        if session is None:
            render_dashboard_footer()
            return
        try:
            self._supply_service.ensure_page_access(session)
        except PermissionDeniedError:
            st.error("Tài khoản không có quyền truy cập trang báo cáo.")
            render_dashboard_footer()
            return
        inject_css("supply_report.css")
        self._render_page_header()

        filter_store = FilterStore(self.report_id)
        prefill = filter_store.load()
        running_key = f"_{self.report_id}_is_running"
        pending_run_key = f"_{self.report_id}_pending_run"
        data_key = f"_{self.report_id}_report_cache"
        is_running = bool(st.session_state.get(running_key, False))
        pending_run = bool(st.session_state.get(pending_run_key, False))

        # Load metadata cho dropdown filter
        try:
            metadata = self._get_cached_metadata()
        except DataAccessError:
            st.warning("Không tải được danh sách options. Vui lòng thử lại sau.")
            metadata = {"stores": [], "products": [], "classifications": [], "periods": []}

        # Render filter widget; nếu duplicate dimension thì ValueError
        try:
            validated_filter, run_clicked = render_supply_report_filter(
                prefill,
                metadata,
                is_running=is_running,
            )
        except (ValidationError, ValueError) as exc:
            st.session_state[running_key] = False
            st.error(self._friendly_filter_error(exc))
            render_dashboard_footer()
            return
        validated_filter = self._sync_filters_from_widgets(validated_filter)
        duplicate_axes = self._find_duplicate_axes(validated_filter)
        has_duplicate_axes = bool(duplicate_axes)

        # Phát hiện filter thay đổi -> xóa cache và yêu cầu chạy lại
        filter_changed = filter_store.detect_change_reset_page(validated_filter)
        if filter_changed:
            st.session_state.pop(data_key, None)
            st.session_state[f"_{self.report_id}_has_result"] = False
            st.session_state[pending_run_key] = False
            st.session_state[running_key] = False
        filter_store.save(validated_filter)

        # Kiểm tra có cached result không
        has_result = st.session_state.get(f"_{self.report_id}_has_result", False)
        cached_data = st.session_state.get(data_key)

        if run_clicked:
            st.session_state.pop(data_key, None)
            if has_duplicate_axes:
                st.session_state[f"_{self.report_id}_has_result"] = False
                st.session_state[running_key] = False
                st.session_state[pending_run_key] = False
                has_result = False
            else:
                st.session_state[f"_{self.report_id}_has_result"] = True
                st.session_state[running_key] = True
                st.session_state[pending_run_key] = True
                has_result = True
                st.rerun()

        if has_duplicate_axes:
            st.session_state[running_key] = False
            st.session_state[pending_run_key] = False
            render_dashboard_footer()
            return

        if not has_result:
            st.session_state[running_key] = False
            st.session_state[pending_run_key] = False
            st.info("Chọn filter và cấu hình trục, sau đó bấm **Chạy báo cáo**.")
            render_dashboard_footer()
            return

        data: ReportData | None = cached_data if isinstance(cached_data, ReportData) else None
        if pending_run:
            try:
                effective_filter = self._build_effective_filter(validated_filter)
            except (ValidationError, ValueError) as exc:
                st.session_state[running_key] = False
                st.session_state[pending_run_key] = False
                st.session_state[f"_{self.report_id}_has_result"] = False
                st.session_state.pop(data_key, None)
                st.error(self._friendly_filter_error(exc))
                render_dashboard_footer()
                return

            try:
                with st.spinner("Đang chạy báo cáo..."):
                    data = self.fetch_data(effective_filter, 1)
            except PermissionDeniedError:
                st.session_state[running_key] = False
                st.session_state[pending_run_key] = False
                st.session_state[f"_{self.report_id}_has_result"] = False
                st.session_state.pop(data_key, None)
                st.error("Tài khoản không có quyền chạy báo cáo.")
                render_dashboard_footer()
                return
            except DataAccessError:
                st.session_state[running_key] = False
                st.session_state[pending_run_key] = False
                st.session_state[f"_{self.report_id}_has_result"] = False
                st.session_state.pop(data_key, None)
                st.error("Không lấy được dữ liệu. Vui lòng thử lại sau.")
                render_dashboard_footer()
                return

            st.session_state[data_key] = data
            st.session_state[running_key] = False
            st.session_state[pending_run_key] = False
            st.rerun()

        if data is None:
            st.session_state[f"_{self.report_id}_has_result"] = False
            st.info("Chọn filter và cấu hình trục, sau đó bấm **Chạy báo cáo**.")
            render_dashboard_footer()
            return

        if data.is_empty():
            st.info("Không tìm thấy dữ liệu phù hợp với bộ lọc và cấu hình trục đã chọn.")
            render_dashboard_footer()
            return

        self.render_result(data)
        render_dashboard_footer()

    # Abstract method implementations

    def render_filter_widget(self, prefill):
        metadata = self._get_cached_metadata()
        supply_filter, _ = render_supply_report_filter(prefill, metadata, is_running=False)
        return supply_filter

    def fetch_data(self, supply_filter: SupplyReportFilter, page: int) -> ReportData:
        session = get_current_session()
        if session is None:
            raise PermissionDeniedError("Missing authenticated session.")
        payload = self._supply_service.run_report(session, supply_filter)

        if payload.is_empty:
            return ReportData(total=0, rows=[])

        return ReportData(total=1, rows=[payload])

    def render_result(self, data: ReportData) -> None:
        if not data.rows:
            return
        payload = data.rows[0]
        if isinstance(payload, MatrixPayload):
            render_supply_report_table(payload)

    # Helpers

    def _get_cached_metadata(self) -> dict[str, list[str]]:
        cache_key = f"_{self.report_id}_metadata_cache"
        cached = st.session_state.get(cache_key)
        if isinstance(cached, dict):
            return cached
        metadata = self._supply_service.get_filter_metadata()
        st.session_state[cache_key] = metadata
        return metadata

    def _friendly_filter_error(self, exc: ValidationError | ValueError) -> str:
        """Xử lý thông báo lỗi thân thiện, đặc biệt là lỗi Tiếng Việt từ Pydantic."""
        if isinstance(exc, ValidationError):
            errors = exc.errors()
            if errors:
                # Lấy message đầu tiên
                msg = str(errors[0].get("msg", ""))
                # Loại bỏ các prefix kỹ thuật của Pydantic để giữ lại nội dung Tiếng Việt
                prefixes = ["Value error,", "Assertion failed,"]
                for prefix in prefixes:
                    if msg.startswith(prefix):
                        msg = msg.replace(prefix, "", 1).strip()
                
                if msg:
                    # Đảm bảo viết hoa chữ cái đầu cho thẩm mỹ UI
                    return msg[0].upper() + msg[1:] if len(msg) > 1 else msg
            
            return "Cấu hình trục không hợp lệ. Vui lòng kiểm tra lại lựa chọn."

        # Xử lý ValueError trực tiếp
        message = str(exc).strip()
        return message if message else "Cấu hình trục không hợp lệ. Vui lòng kiểm tra lại lựa chọn."

    def _sync_filters_from_widgets(self, supply_filter: SupplyReportFilter) -> SupplyReportFilter:
        """Đồng bộ cứng filter từ widget state để tránh lệch giữa UI và payload."""
        def _to_filter_value(widget_key: str) -> str | None:
            value = st.session_state.get(widget_key)
            if not isinstance(value, str):
                return None
            normalized = value.strip()
            if not normalized or self._is_all_sentinel(normalized):
                return None
            return normalized

        updates = {
            "product_name": _to_filter_value("supply_filter_product"),
            "store_name": _to_filter_value("supply_filter_store"),
            "period_id": _to_filter_value("supply_filter_period"),
            "classification": _to_filter_value("supply_filter_classification"),
        }
        return supply_filter.model_copy(update=updates)

    def _build_effective_filter(self, fallback: SupplyReportFilter) -> SupplyReportFilter:
        """Dựng lại filter từ session_state tại thời điểm chạy report."""
        required_axis_dims = set(AXIS_DIMENSION_LABELS.keys())
        optional_axis_dims = required_axis_dims | {AXIS_NONE}
        label_to_dim = {v: k for k, v in AXIS_DIMENSION_LABELS.items()}

        def _normalize(value: object) -> str | None:
            if not isinstance(value, str):
                return None
            normalized = value.strip()
            if not normalized or self._is_all_sentinel(normalized):
                return None
            return normalized

        def _axis_dim(widget_key: str, optional: bool, default_dim: str) -> str:
            raw = st.session_state.get(widget_key)
            if isinstance(raw, str):
                normalized = raw.strip()
                valid_dims = optional_axis_dims if optional else required_axis_dims
                # State mới: selectbox lưu trực tiếp dimension key.
                if normalized in valid_dims:
                    return normalized
                # Tương thích ngược: state cũ có thể đang lưu label hiển thị.
                mapped_dim = label_to_dim.get(normalized)
                if mapped_dim and mapped_dim in required_axis_dims:
                    return mapped_dim
            return default_dim

        eval_items_raw = st.session_state.get("supply_filter_eval_items")
        if isinstance(eval_items_raw, list):
            eval_items = [str(item) for item in eval_items_raw if str(item) in KPI_NAMES]
        else:
            eval_items = []
        
        if not eval_items:
            eval_items = list(fallback.evaluation_items or KPI_NAMES)

        return SupplyReportFilter(
            product_name=_normalize(st.session_state.get("supply_filter_product")),
            store_name=_normalize(st.session_state.get("supply_filter_store")),
            period_id=_normalize(st.session_state.get("supply_filter_period")),
            classification=_normalize(st.session_state.get("supply_filter_classification")),
            evaluation_items=eval_items,
            row_axis_1=_axis_dim("supply_axis_row1", optional=False, default_dim=fallback.row_axis_1),
            row_axis_2=_axis_dim("supply_axis_row2", optional=True, default_dim=fallback.row_axis_2),
            col_axis_1=_axis_dim("supply_axis_col1", optional=False, default_dim=fallback.col_axis_1),
            col_axis_2=_axis_dim("supply_axis_col2", optional=True, default_dim=fallback.col_axis_2),
        )

    def _is_all_sentinel(self, value: str) -> bool:
        raw = value.strip()
        if not raw:
            return True
        if raw.casefold() in {"tất cả", "tat ca", "all", "(all)"}:
            return True

        normalized = unicodedata.normalize("NFKD", raw.casefold())
        ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        compact = "".join(ch for ch in ascii_text if ch.isalnum())
        return compact in {"tatca", "all"}

    def _find_duplicate_axes(self, supply_filter: SupplyReportFilter) -> list[str]:
        """Trả về danh sách axis key bị chọn trùng (bỏ qua AXIS_NONE)."""
        axes = [
            supply_filter.row_axis_1,
            supply_filter.row_axis_2,
            supply_filter.col_axis_1,
            supply_filter.col_axis_2,
        ]
        seen: set[str] = set()
        duplicates: list[str] = []
        for axis in axes:
            if not axis or axis == AXIS_NONE:
                continue
            if axis in seen and axis not in duplicates:
                duplicates.append(axis)
                continue
            seen.add(axis)
        return duplicates
