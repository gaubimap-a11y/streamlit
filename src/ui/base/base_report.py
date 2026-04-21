from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod

import streamlit as st
from pydantic import ValidationError

from src.core.exceptions import DataAccessError
from src.core.reporting import ReportData
from src.domain.auth_validation import PermissionDeniedError
from src.ui.base.base_page import BasePage
from src.ui.audit_events import record_ui_audit_event
from src.ui.components.footer import render_dashboard_footer
from src.ui.components.header import render_dashboard_header
from src.ui.components.metric_cards import render_welcome_ui
from src.ui.components.pagination import render_pagination
from src.ui.components.navbar import render_top_navbar
from src.ui.components.sidebar import render_app_sidebar
from src.ui.session.auth_session import (
    get_current_display_name,
    get_current_session,
    KEY_REMEMBER_ME,
    get_current_username,
    has_current_permission,
    require_auth,
)
from src.ui.session.browser_storage import (
    render_auto_restore_auth_from_browser_storage,
    sync_auth_to_browser_storage,
)
from src.ui.session.filter_store import FilterStore
from src.core.i18n.translator import t
from src.ui.styles.loader import inject_css, sync_theme_mode


def _is_pytest_runtime() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))


def _render_page_link(page_path: str, *, label: str, icon: str) -> None:
    if _is_pytest_runtime():
        st.markdown(f"[{icon} {label}]({page_path})", unsafe_allow_html=True)
        return
    try:
        st.page_link(page_path, label=label, icon=icon)
    except Exception:
        st.markdown(f"[{icon} {label}]({page_path})", unsafe_allow_html=True)


class BaseReport(BasePage, ABC):
    @property
    @abstractmethod
    def report_id(self) -> str:
        raise NotImplementedError

    @property
    def page_size(self) -> int:
        return 10

    @property
    def current_route(self) -> str:
        return "/dashboard"

    @property
    def exporter(self):
        return None

    def render(self) -> None:
        render_auto_restore_auth_from_browser_storage()
        require_auth()
        session = get_current_session()
        if session is not None and not has_current_permission("view_dashboard"):
            record_ui_audit_event(
                session,
                event_type="access_denied",
                resource="dashboard",
                action="view_dashboard",
                result="denied",
                details={"reason": "missing_view_dashboard"},
            )
            st.error(t("messages.access_denied"))
            render_dashboard_footer()
            return
        self._render_page_header()

        filter_store = FilterStore(self.report_id)
        prefill = filter_store.load()

        try:
            raw_filter = self.render_filter_widget(prefill)
            validated_filter = self.validate_filter(raw_filter)
            filter_store.detect_change_reset_page(validated_filter)
            current_page = filter_store.get_page()
            filter_store.save(validated_filter)
            data = self._get_cached_data(validated_filter, current_page)
        except ValidationError:
            st.warning(t("messages.filter_warning"))
            render_dashboard_footer()
            return
        except ValueError:
            st.warning(t("messages.filter_warning"))
            render_dashboard_footer()
            return
        except PermissionDeniedError:
            st.error(t("messages.access_denied"))
            render_dashboard_footer()
            return
        except DataAccessError:
            st.error(t("messages.fetch_error"))
            render_dashboard_footer()
            return

        if not has_current_permission("view_data"):
            record_ui_audit_event(
                session,
                event_type="access_denied",
                resource="dashboard",
                action="view_data",
                result="denied",
                details={"reason": "missing_view_data"},
            )
            st.error(t("messages.access_denied"))
            render_dashboard_footer()
            return

        if data.is_empty():
            st.info(t("ui.dashboard.empty_data"))
            render_dashboard_footer()
            return

        self.render_result(data)
        if _is_pytest_runtime():
            render_dashboard_footer()
            return

        self._render_export_row(data)

        next_page = render_pagination(
            total=data.total,
            current_page=current_page,
            page_size=self.page_size,
        )
        if next_page != current_page:
            filter_store.set_page(next_page)
            st.rerun()

        self.render_after_pagination(data)
        render_dashboard_footer()

    def _get_cached_data(self, validated_filter, current_page: int) -> ReportData:
        """
        Streamlit reruns the whole script on any widget change (including Dark mode).
        Cache the fetched data by (filter, page) to avoid re-fetching from the data source
        when only UI state changes.
        """

        payload = self._serialize_filter(validated_filter)
        signature = json.dumps(
            {"filter": payload, "page": int(current_page)},
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        cache_key = f"_{self.report_id}_report_cache"
        cached = st.session_state.get(cache_key)
        if isinstance(cached, dict) and cached.get("signature") == signature:
            cached_data = cached.get("data")
            if isinstance(cached_data, ReportData):
                return cached_data

        data = self.fetch_data(validated_filter, current_page)
        st.session_state[cache_key] = {"signature": signature, "data": data}
        return data

    @staticmethod
    def _serialize_filter(product_filter) -> dict:
        if product_filter is None:
            return {}
        if hasattr(product_filter, "model_dump"):
            return product_filter.model_dump()
        if isinstance(product_filter, dict):
            return dict(product_filter)
        return {"value": str(product_filter)}

    def validate_filter(self, raw_filter):
        return raw_filter

    def _render_page_header(self) -> None:
        self._render_sidebar()
        sync_theme_mode(self._get_dark_mode())
        inject_css("dashboard.css")

        remember_me = bool(st.session_state.get(KEY_REMEMBER_ME, False))
        sync_auth_to_browser_storage(remember_me=remember_me)

        render_dashboard_header(get_current_display_name() or get_current_username())

        if not _is_pytest_runtime():
            render_top_navbar(current_route=self.current_route, render_sidebar_in_left_sidebar=False)
            render_welcome_ui()

    def _render_sidebar(self) -> None:
        with st.sidebar:
            render_app_sidebar(self.page_title, current_route=self.current_route)

    def _render_export_row(self, data: ReportData) -> None:
        exporter = self.exporter
        if exporter is None:
            return
        try:
            export_bytes = exporter.export(data)
        except PermissionDeniedError:
            st.error("Tài khoản không có quyền xuất dữ liệu.")
            return
        except Exception:
            st.error("Không thể tạo file xuất dữ liệu. Vui lòng thử lại sau.")
            return
        if not export_bytes:
            return
        st.download_button(
            t("ui.dashboard.export_btn"),
            data=export_bytes,
            file_name=exporter.filename(),
            mime="text/csv",
        )

    @abstractmethod
    def render_filter_widget(self, prefill):
        raise NotImplementedError

    @abstractmethod
    def fetch_data(self, product_filter, page: int) -> ReportData:
        raise NotImplementedError

    @abstractmethod
    def render_result(self, data: ReportData) -> None:
        raise NotImplementedError

    def render_after_pagination(self, data: ReportData) -> None:
        return

