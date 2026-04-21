from __future__ import annotations

import streamlit as st

from src.core.exceptions import DataAccessError
from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.user_repository import UserRepository
from src.ui.base.base_page import BasePage
from src.ui.components.user_table import render_user_table


_USERS_PENDING_ACTION_KEY = "users_pending_action"
_USERS_PENDING_USER_ID_KEY = "users_pending_user_id"
_USERS_TABLE_VERSION_KEY = "users_table_version"


class UsersPage(BasePage):
    def __init__(self, user_repository: UserRepository | None = None) -> None:
        super().__init__()
        self._user_repository = user_repository or UserRepository()

    @property
    def page_title(self) -> str:
        return "Users"

    @property
    def page_icon(self) -> str:
        return "👥"

    @property
    def current_route(self) -> str:
        return "/users"

    def render(self) -> None:
        self._require_auth()
        self._render_page_header()

        try:
            rows = self._get_users()
        except DataAccessError:
            st.error("Không lấy được dữ liệu người dùng. Vui lòng thử lại sau.")
            
            return

        st.markdown("### Danh sách người dùng")
        if not rows:
            st.info("Không tìm thấy người dùng nào.")
            
            return

        st.caption(f"Tổng số người dùng: {len(rows)}")
        self._render_pending_action_dialog(rows)

        action_payload = render_user_table(rows, key=self._get_table_widget_key())
        if not self._has_pending_action():
            self._handle_table_actions(action_payload, rows)
        

    def _get_users(self):
        cache_key = "_users_dataset_cache"
        cached = st.session_state.get(cache_key)
        if isinstance(cached, list):
            return cached

        with databricks_connection() as conn:
            rows = self._user_repository.list_users(conn)
        st.session_state[cache_key] = rows
        return rows

    def _handle_table_actions(self, action_payload, rows) -> None:
        if not action_payload:
            return

        action, user_id = action_payload
        available_ids = {str(row.user_id) for row in rows}
        if user_id not in available_ids:
            st.error("Người dùng không tồn tại hoặc đã thay đổi dữ liệu.")
            return

        if action not in {"edit", "delete"}:
            st.error("Hành động không hợp lệ.")
            return

        st.session_state[_USERS_PENDING_ACTION_KEY] = action
        st.session_state[_USERS_PENDING_USER_ID_KEY] = user_id
        st.session_state[_USERS_TABLE_VERSION_KEY] = self._get_table_version() + 1
        st.rerun()

    def _render_pending_action_dialog(self, rows) -> None:
        action = str(st.session_state.pop(_USERS_PENDING_ACTION_KEY, "")).strip().lower()
        user_id = str(st.session_state.pop(_USERS_PENDING_USER_ID_KEY, "")).strip()
        if not action or not user_id:
            return

        available_ids = {str(row.user_id) for row in rows}
        if user_id not in available_ids:
            st.error("Người dùng không tồn tại hoặc đã thay đổi dữ liệu.")
            return

        if action == "edit":
            self._show_edit_user_dialog(user_id)
        elif action == "delete":
            self._show_delete_user_dialog(user_id)
        else:
            st.error("Hành động không hợp lệ.")

    @staticmethod
    def _has_pending_action() -> bool:
        action = str(st.session_state.get(_USERS_PENDING_ACTION_KEY, "")).strip().lower()
        user_id = str(st.session_state.get(_USERS_PENDING_USER_ID_KEY, "")).strip()
        return bool(action and user_id)

    @staticmethod
    def _get_table_version() -> int:
        value = st.session_state.get(_USERS_TABLE_VERSION_KEY, 0)
        return int(value) if isinstance(value, int) else 0

    def _get_table_widget_key(self) -> str:
        return f"users_table_editor_{self._get_table_version()}"

    @st.dialog("Sửa người dùng", width="small")
    def _show_edit_user_dialog(self, user_id: str) -> None:
        st.write(f"Bạn đang mở chức năng sửa cho user: `{user_id}`")
        st.caption("Màn hình cập nhật chi tiết user chưa triển khai.")
        if st.button("Đóng", key=f"close_edit_dialog_{user_id}", use_container_width=True):
            st.rerun()

    @st.dialog("Xóa người dùng", width="small")
    def _show_delete_user_dialog(self, user_id: str) -> None:
        st.warning(f"Bạn có chắc muốn xóa user `{user_id}` không?")
        st.caption("Chức năng xóa backend chưa triển khai.")
        col_cancel, col_confirm = st.columns(2)
        with col_cancel:
            if st.button("Hủy", key=f"cancel_delete_dialog_{user_id}", use_container_width=True):
                st.rerun()
        with col_confirm:
            if st.button("Xác nhận xóa", key=f"confirm_delete_dialog_{user_id}", type="primary", use_container_width=True):
                st.info("Đã nhận yêu cầu xóa. Backend delete chưa được triển khai.")
                st.rerun()
