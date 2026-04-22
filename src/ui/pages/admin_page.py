from __future__ import annotations

from dataclasses import asdict, is_dataclass
from html import escape
import time

import pandas as pd
import streamlit as st
from streamlit.delta_generator_singletons import get_last_dg_added_to_context_stack

from src.application.auth.security_admin_service import ADMIN_GATE_PERMISSIONS, SecurityAdminService
from src.core.exceptions import AuthError
from src.domain.auth_validation import AuthenticationValidationError
from src.ui.audit_events import get_audit_writer, record_ui_audit_event
from src.ui.base.base_page import BasePage
from src.ui.components.footer import render_dashboard_footer
from src.ui.components.header import render_dashboard_header
from src.core.i18n.translator import t
from src.ui.session.auth_session import (
    KEY_REMEMBER_ME,
    clear_session,
    get_current_display_name,
    get_current_session,
    get_current_username,
    require_auth,
    switch_page_safely,
)
from src.ui.session.browser_storage import (
    render_auto_restore_auth_from_browser_storage,
    sync_auth_to_browser_storage,
)
from src.ui.styles.loader import inject_css, sync_theme_mode


_LOGIN_PAGE = "pages/login.py"
_DASHBOARD_PAGE = "pages/dashboard.py"
_USER_MODAL_STATE_KEY = "admin_user_modal_state"
_ROLE_MODAL_STATE_KEY = "admin_role_modal_state"
_PERMISSION_MODAL_STATE_KEY = "admin_permission_modal_state"
_MODAL_STATE_KEYS = (_USER_MODAL_STATE_KEY, _ROLE_MODAL_STATE_KEY, _PERMISSION_MODAL_STATE_KEY)
_ADMIN_ACTIVE_TAB_KEY = "admin_active_tab"
_ADMIN_TAB_USERS = "users"
_ADMIN_TAB_ROLES = "roles"
_ADMIN_TAB_PERMISSIONS = "permissions"
_ADMIN_TAB_AUDIT = "audit"
_ADMIN_TAB_OPTIONS = (_ADMIN_TAB_USERS, _ADMIN_TAB_ROLES, _ADMIN_TAB_PERMISSIONS, _ADMIN_TAB_AUDIT)
_ADMIN_FEEDBACK_KEY = "admin_feedback"
_ADMIN_USER_MODAL_FEEDBACK_KEY = "admin_user_modal_feedback"
_ADMIN_USER_MODAL_PENDING_CLOSE_KEY = "admin_user_modal_pending_close"
_ADMIN_ROLE_ADD_MODAL_PENDING_CLOSE_KEY = "admin_role_add_modal_pending_close"
_ADMIN_ROLE_EDIT_MODAL_PENDING_CLOSE_KEY = "admin_role_edit_modal_pending_close"
_ADMIN_PERMISSION_ADD_MODAL_PENDING_CLOSE_KEY = "admin_permission_add_modal_pending_close"
_ADMIN_PERMISSION_EDIT_MODAL_PENDING_CLOSE_KEY = "admin_permission_edit_modal_pending_close"
_ADMIN_CACHE_STATE_KEY = "admin_data_cache"
_ADMIN_CACHE_TTL_SECONDS = 60.0


@st.cache_resource
def _get_admin_service(_factory_identity: int) -> SecurityAdminService:
    return SecurityAdminService.from_current_config(audit_writer=get_audit_writer())


def _render_page_link(page_path: str, *, label: str, icon: str) -> None:
    try:
        st.page_link(page_path, label=label, icon=icon)
    except Exception:
        st.markdown(f"[{icon} {label}]({page_path})", unsafe_allow_html=True)


def _close_current_dialog_without_rerun() -> None:
    current = get_last_dg_added_to_context_stack()
    hops = 0
    while current is not None and hops < 20:
        close_fn = getattr(current, "close", None)
        if callable(close_fn):
            close_fn()
            return
        current = getattr(current, "_parent", None)
        hops += 1


class SecurityAdminPage(BasePage):
    def __init__(self, admin_service: SecurityAdminService | None = None) -> None:
        super().__init__()
        self._admin_service = admin_service or _get_admin_service(id(SecurityAdminService.from_current_config))

    @property
    def page_title(self) -> str:
        return t("ui.admin.title")

    @property
    def page_icon(self) -> str:
        return "🛡️"

    def render(self) -> None:
        inject_css("dashboard.css")
        render_auto_restore_auth_from_browser_storage()
        require_auth()
        session = get_current_session()
        if session is None:
            switch_page_safely(_LOGIN_PAGE)
            st.stop()
            return

        if not self._admin_service.can_access_admin(session):
            self._admin_service.record_admin_access_denied(session)
            st.error("Tài khoản không có quyền truy cập trang quản trị bảo mật.")
            render_dashboard_footer()
            return

        sync_theme_mode(self._get_dark_mode())
        self._render_sidebar(session)
        self._render_header(session)
        self._render_security_banner()
        # Modal được gọi trực tiếp từ các button hành động để tránh rerun toàn trang
        self._render_active_admin_section(session)
        render_dashboard_footer()

    @st.fragment
    def _render_active_admin_section(self, session) -> None:
        active_tab = st.segmented_control(
            "Admin modules",
            options=_ADMIN_TAB_OPTIONS,
            default=_ADMIN_TAB_USERS,
            format_func=lambda tab: t(f"ui.admin.tabs.{tab}"),
            key=_ADMIN_ACTIVE_TAB_KEY,
            label_visibility="collapsed",
            width="stretch",
        )
        if active_tab == _ADMIN_TAB_ROLES:
            self._render_roles_tab(session)
            return
        if active_tab == _ADMIN_TAB_PERMISSIONS:
            self._render_permissions_tab(session)
            return
        if active_tab == _ADMIN_TAB_AUDIT:
            self._render_audit_tab(session)
            return
        self._render_users_tab(session)

    def _render_security_banner(self) -> None:
        st.markdown(
            f"""
            <div class="security-hero">
                <div class="security-hero-copy">
                    <div class="security-hero-eyebrow">{t("ui.admin.banner_title")}</div>
                    <div class="security-hero-title">{t("ui.admin.banner_subtitle")}</div>
                    <div class="security-hero-body">
                        {t("ui.admin.banner_body")}
                    </div>
                </div>
                <div class="security-hero-chip-list">
                    <span class="security-hero-chip">Immutable IDs</span>
                    <span class="security-hero-chip">Fail-closed guards</span>
                    <span class="security-hero-chip">Audit trail on</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    def _render_sidebar(self, session) -> None:
        with st.sidebar:
            st.title(f"🛡️ {t('ui.admin.title')}")
            
            # Language Switcher
            st.selectbox(
                t("ui.sidebar.language"),
                options=["vi", "ja"],
                format_func=lambda x: "Tiếng Việt" if x == "vi" else "日本語",
                key="locale"
            )

            st.toggle(t("ui.sidebar.dark_mode"), key="ui_dark_mode")
            st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

            st.markdown('<div class="back-container">', unsafe_allow_html=True)
            _render_page_link(_DASHBOARD_PAGE, label=t("ui.sidebar.back_to_dashboard"), icon="📊")
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
            st.info(t("ui.sidebar.info_card"))

    def _render_header(self, session) -> None:
        remember_me = bool(st.session_state.get(KEY_REMEMBER_ME, False))
        sync_auth_to_browser_storage(remember_me=remember_me)

        header_col, action_col = st.columns([8.2, 0.8], vertical_alignment="center")
        with action_col:
            st.markdown('<div class="logout-btn-container">', unsafe_allow_html=True)
            logout_pressed = st.button(t("ui.header.logout"), key="admin_logout")
            st.markdown("</div>", unsafe_allow_html=True)
        with header_col:
            render_dashboard_header(get_current_display_name() or get_current_username())

        if logout_pressed:
            record_ui_audit_event(
                session,
                event_type="logout",
                resource="auth",
                action="logout",
                result="success",
                details={"page": "admin"},
            )
            clear_session()
            switch_page_safely(_LOGIN_PAGE)
            st.stop()

    def _render_users_tab(self, session) -> None:
        st.markdown(f"#### {t('ui.admin.tabs.users')}")
        self._render_feedback()
        if not self._can_manage_users(session):
            st.warning("Bạn không có quyền quản lý người dùng.")
            return
        search_term = st.text_input(t("ui.admin.users.search_label"), key="admin_user_search")
        status_col, auth_source_col = st.columns(2)
        with status_col:
            status_filter = st.selectbox(
                t("ui.admin.users.status_label"),
                ["all", "active", "inactive"],
                format_func=lambda x: t(f"ui.admin.common.{x}"),
                key="admin_user_status",
            )
        with auth_source_col:
            auth_source_filter = st.selectbox(
                t("ui.admin.users.auth_source"),
                ["all", "internal", "sso"],
                format_func=lambda x: t(f"ui.admin.common.{x}") if x == "all" else x,
                key="admin_user_auth_source",
            )

        try:
            users = self._list_users(
                session,
                search_term=search_term,
                status_filter=status_filter,
                auth_source_filter=auth_source_filter,
            )
        except (AuthenticationValidationError, AuthError) as exc:
            st.error(str(exc))
            return

        self._render_metric_strip(
            [
                ("Filtered users", len(users), "Matches the current search and filter state"),
                ("Active users", sum(1 for user in users if user.is_active), "Currently enabled accounts"),
                ("SSO users", sum(1 for user in users if user.auth_source == "sso"), "Accounts coming from SSO"),
            ]
        )

        self._render_badge_row(
            [
                ("Active", "active", sum(1 for user in users if user.is_active)),
                ("Inactive", "inactive", sum(1 for user in users if not user.is_active)),
                ("Internal", "info", sum(1 for user in users if user.auth_source == "internal")),
                ("SSO", "neutral", sum(1 for user in users if user.auth_source == "sso")),
            ]
        )

        with st.container(border=True):
            st.caption("Danh sách user. Dùng cột thao tác để mở modal chỉnh sửa hoặc xóa.")
            self._render_user_action_table(session, users, can_manage=True)

    def _render_roles_tab(self, session) -> None:
        st.markdown(f"#### {t('ui.admin.tabs.roles')}")
        self._render_feedback()
        if not self._can_manage_roles(session):
            st.warning("Bạn không có quyền quản lý vai trò.")
            return
        actions_col, _ = st.columns([2, 8])
        with actions_col:
            if st.button("+ Add role", key="admin_add_role"):
                self._show_role_add_modal(session)
        search_term = st.text_input(t("ui.admin.roles.search_label"), key="admin_role_search")
        status_filter = st.selectbox(
            t("ui.admin.users.status_label"),
            ["all", "active", "inactive"],
            format_func=lambda x: t(f"ui.admin.common.{x}"),
            key="admin_role_status",
        )

        try:
            roles = self._list_roles(session, search_term=search_term, status_filter=status_filter)
        except (AuthenticationValidationError, AuthError) as exc:
            st.error(str(exc))
            return

        self._render_metric_strip(
            [
                ("Filtered roles", len(roles), "Matches the current search and filter state"),
                ("Active roles", sum(1 for role in roles if role.is_active), "Currently enabled roles"),
                ("Roles with perms", sum(1 for role in roles if role.permissions_count > 0), "Roles with assigned permissions"),
            ]
        )

        self._render_badge_row(
            [
                ("Active", "active", sum(1 for role in roles if role.is_active)),
                ("Inactive", "inactive", sum(1 for role in roles if not role.is_active)),
                ("Privileged", "danger", sum(1 for role in roles if role.permissions_count > 0)),
            ]
        )

        with st.container(border=True):
            st.caption("Danh sách vai trò. Dùng cột thao tác để mở modal chỉnh sửa hoặc xóa.")
            self._render_role_action_table(session, roles, can_manage=True)

    def _render_permissions_tab(self, session) -> None:
        st.markdown("#### Permissions")
        self._render_feedback()
        if not self._can_manage_permissions(session):
            st.warning("Bạn không có quyền quản lý quyền hạn.")
            return
        actions_col, _ = st.columns([2, 8])
        with actions_col:
            if st.button("+ Add permission", key="admin_add_permission"):
                self._show_permission_add_modal(session)
        search_term = st.text_input("Search permissions", key="admin_permission_search")
        try:
            permissions = self._list_permissions(session, search_term=search_term)
        except (AuthenticationValidationError, AuthError) as exc:
            st.error(str(exc))
            return

        self._render_metric_strip(
            [
                ("Permissions", len(permissions), "Catalog items currently visible"),
                ("Active", sum(1 for permission in permissions if permission.is_active), "Enabled permissions"),
                ("Used by roles", sum(1 for permission in permissions if permission.role_count > 0), "Permissions assigned to at least one role"),
            ]
        )

        self._render_badge_row(
            [
                ("Active", "active", sum(1 for permission in permissions if permission.is_active)),
                ("Inactive", "inactive", sum(1 for permission in permissions if not permission.is_active)),
                ("Used", "info", sum(1 for permission in permissions if permission.role_count > 0)),
                ("Unused", "neutral", sum(1 for permission in permissions if permission.role_count == 0)),
            ]
        )

        with st.container(border=True):
            st.caption("Danh sách permission. Dùng cột thao tác để mở modal chỉnh sửa hoặc xóa.")
            self._render_permission_action_table(session, permissions, can_manage=True)

    def _render_user_action_table(self, session, users, *, can_manage: bool) -> None:
        if not users:
            st.info("Không có dữ liệu.")
            return
        header = st.columns([1.9, 1.8, 2.4, 1.2, 1.1, 1.6])
        for column, label in zip(header, ("User ID", "Username", "Email", "Auth source", "Status", "Actions")):
            with column:
                st.markdown(f"**{label}**")
        st.markdown('<div class="admin-action-table-divider"></div>', unsafe_allow_html=True)
        for principal in users:
            row = st.columns([1.9, 1.8, 2.4, 1.2, 1.1, 1.6], vertical_alignment="center")
            with row[0]:
                st.caption(principal.principal_id)
            with row[1]:
                st.write(principal.username)
            with row[2]:
                st.write(principal.email or "-")
            with row[3]:
                st.write(principal.auth_source)
            with row[4]:
                st.markdown(
                    self._status_badge_html("Active" if principal.is_active else "Inactive", "active" if principal.is_active else "inactive"),
                    unsafe_allow_html=True,
                )
            with row[5]:
                safe_id = _widget_safe_key(principal.principal_id)
                edit_pressed, delete_pressed = self._render_action_icons(
                    edit_key=f"admin_user_edit_{safe_id}",
                    delete_key=f"admin_user_delete_{safe_id}",
                    can_manage=can_manage,
                )
            if edit_pressed:
                self._show_user_edit_modal(session, principal.principal_id)
            if delete_pressed:
                self._show_user_delete_modal(session, principal.principal_id)

    def _render_role_action_table(self, session, roles, *, can_manage: bool) -> None:
        if not roles:
            st.info("Không có dữ liệu.")
            return
        header = st.columns([1.5, 1.9, 2.4, 1.1, 1.3, 1.6])
        for column, label in zip(header, ("Role ID", "Role name", "Description", "Status", "Perm count", "Actions")):
            with column:
                st.markdown(f"**{label}**")
        st.markdown('<div class="admin-action-table-divider"></div>', unsafe_allow_html=True)
        for role in roles:
            row = st.columns([1.5, 1.9, 2.4, 1.1, 1.3, 1.6], vertical_alignment="center")
            with row[0]:
                st.caption(role.role_id)
            with row[1]:
                st.write(role.role_name)
            with row[2]:
                st.write(role.description or "-")
            with row[3]:
                st.markdown(
                    self._status_badge_html("Active" if role.is_active else "Inactive", "active" if role.is_active else "inactive"),
                    unsafe_allow_html=True,
                )
            with row[4]:
                st.write(role.permissions_count)
            with row[5]:
                safe_id = _widget_safe_key(role.role_id)
                edit_pressed, delete_pressed = self._render_action_icons(
                    edit_key=f"admin_role_edit_{safe_id}",
                    delete_key=f"admin_role_delete_{safe_id}",
                    can_manage=can_manage,
                )
            if edit_pressed:
                self._show_role_edit_modal(session, role.role_id)
            if delete_pressed:
                self._show_role_delete_modal(session, role.role_id)

    def _render_permission_action_table(self, session, permissions, *, can_manage: bool) -> None:
        if not permissions:
            st.info("Không có dữ liệu.")
            return
        header = st.columns([1.4, 1.8, 2.5, 1.0, 1.2, 1.6])
        for column, label in zip(header, ("Permission ID", "Name", "Description", "Status", "Role count", "Actions")):
            with column:
                st.markdown(f"**{label}**")
        st.markdown('<div class="admin-action-table-divider"></div>', unsafe_allow_html=True)
        for permission in permissions:
            row = st.columns([1.4, 1.8, 2.5, 1.0, 1.2, 1.6], vertical_alignment="center")
            with row[0]:
                st.caption(permission.permission_id)
            with row[1]:
                st.write(permission.permission_name)
            with row[2]:
                st.write(permission.description or "-")
            with row[3]:
                st.markdown(
                    self._status_badge_html("Active" if permission.is_active else "Inactive", "active" if permission.is_active else "inactive"),
                    unsafe_allow_html=True,
                )
            with row[4]:
                st.write(permission.role_count)
            with row[5]:
                safe_id = _widget_safe_key(permission.permission_id)
                edit_pressed, delete_pressed = self._render_action_icons(
                    edit_key=f"admin_permission_edit_{safe_id}",
                    delete_key=f"admin_permission_delete_{safe_id}",
                    can_manage=can_manage,
                )
            if edit_pressed:
                self._show_permission_edit_modal(session, permission.permission_id)
            if delete_pressed:
                self._show_permission_delete_modal(session, permission.permission_id)

    def _render_action_icons(self, *, edit_key: str, delete_key: str, can_manage: bool) -> tuple[bool, bool]:
        edit_col, delete_col = st.columns(2, gap="small")
        with edit_col:
            edit_pressed = st.button("✏️", key=edit_key, help="Chỉnh sửa", disabled=not can_manage)
        with delete_col:
            delete_pressed = st.button("🗑️", key=delete_key, help="Xóa", disabled=not can_manage)
        return edit_pressed, delete_pressed

    def _show_user_edit_modal(self, session, principal_id: str) -> None:
        @st.dialog(
            f"Edit user: {principal_id}",
            width="large",
            on_dismiss=self._on_user_edit_modal_dismiss,
        )
        def _dialog():
            if bool(st.session_state.pop(_ADMIN_USER_MODAL_PENDING_CLOSE_KEY, False)):
                st.session_state.pop(_ADMIN_USER_MODAL_FEEDBACK_KEY, None)
                self._close_user_modal_with_users_refresh()
                return

            if not self._can_manage_users(session):
                st.error("Bạn không có quyền chỉnh sửa người dùng.")
                if st.button(
                    "Đóng",
                    key=f"admin_user_modal_close_denied_{_widget_safe_key(principal_id)}",
                    width="stretch",
                ):
                    st.session_state.pop(_ADMIN_USER_MODAL_FEEDBACK_KEY, None)
                    self._close_user_modal_with_users_refresh()
                return
            try:
                with st.spinner("Đang tải thông tin người dùng..."):
                    detail = self._get_user_detail(session, principal_id=principal_id)
                    role_catalog = self._safe_roles(session)
                    permission_catalog = self._safe_permissions(session)
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))
                if st.button(
                    "Đóng",
                    key=f"admin_user_modal_close_{_widget_safe_key(principal_id)}",
                    width="stretch",
                ):
                    st.session_state.pop(_ADMIN_USER_MODAL_FEEDBACK_KEY, None)
                    self._close_user_modal_with_users_refresh()
                return

            role_by_id = {role.role_id: role for role in role_catalog}
            available_role_ids = [role.role_id for role in self._sorted_role_catalog(role_catalog)]
            permission_by_id = {permission.permission_id: permission for permission in permission_catalog}
            safe_principal_id = _widget_safe_key(principal_id)
            effective_permission_badges = []
            for permission_id in detail.effective_permissions:
                matched = permission_by_id.get(permission_id)
                if matched is None:
                    effective_permission_badges.append((permission_id, "neutral", None))
                    continue
                badge_label = f"{matched.permission_name} ({permission_id})"
                effective_permission_badges.append((badge_label, "neutral", None))

            with st.form(f"admin_user_modal_form_{safe_principal_id}"):
                identity_col, status_col = st.columns([2.2, 1.0], vertical_alignment="bottom")
                with identity_col:
                    st.text_input("principal_id", value=detail.principal.principal_id, disabled=True)
                with status_col:
                    is_active = st.checkbox("is_active", value=detail.principal.is_active)

                profile_col, auth_col = st.columns(2, vertical_alignment="bottom")
                with profile_col:
                    username = st.text_input("username", value=detail.principal.username)
                    display_name = st.text_input("display_name", value=detail.principal.display_name)
                with auth_col:
                    email = st.text_input("email", value=detail.principal.email)
                    auth_source = detail.principal.auth_source
                    st.text_input("auth_source", value=auth_source, disabled=True)

                role_ids = st.multiselect(
                    "roles",
                    options=available_role_ids,
                    format_func=lambda role_id: self._role_option_label(role_by_id.get(role_id)) or role_id,
                    default=_filter_multiselect_default(available_role_ids, detail.assigned_roles),
                )

                st.markdown("##### Quyền hiện có của user")
                self._render_badge_row(
                    effective_permission_badges
                    or [("No effective permissions", "neutral", None)]
                )
                st.caption("Danh sách này là read-only, được cập nhật theo role đã gán.")

                cancel_col, submit_col = st.columns(2)
                with cancel_col:
                    st.form_submit_button(
                        "Hủy",
                        width="stretch",
                        on_click=self._mark_user_edit_modal_for_close,
                    )
                with submit_col:
                    submitted = st.form_submit_button("Lưu thay đổi", type="primary", width="stretch")

            if not submitted:
                return
            try:
                with st.spinner("Đang lưu thay đổi người dùng..."):
                    self._admin_service.sync_user_roles(
                        session,
                        principal_id=principal_id,
                        username=username,
                        email=email,
                        display_name=display_name,
                        auth_source=auth_source,
                        is_active=is_active,
                        target_role_ids=tuple(role_ids),
                    )
                # Surgical invalidate
                self._invalidate_admin_cache(bucket="user_detail", key=(principal_id.strip().lower(),))
                self._invalidate_admin_cache(bucket="permissions")
                self._set_feedback("Đã cập nhật user thành công.", level="success")
                st.session_state[_ADMIN_USER_MODAL_PENDING_CLOSE_KEY] = True
                st.rerun(scope="fragment")
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))

        _dialog()

    def _on_user_edit_modal_dismiss(self) -> None:
        self._invalidate_admin_cache(bucket="users")

    def _mark_user_edit_modal_for_close(self) -> None:
        st.session_state[_ADMIN_USER_MODAL_PENDING_CLOSE_KEY] = True

    def _close_user_modal_with_users_refresh(self) -> None:
        self._invalidate_admin_cache(bucket="users")
        st.rerun()

    def _show_user_delete_modal(self, session, principal_id: str) -> None:
        @st.dialog(
            f"Delete user: {principal_id}",
            width="large",
        )
        def _dialog():
            if not self._can_manage_users(session):
                st.error("Bạn không có quyền xóa người dùng.")
                if st.button(
                    "Đóng",
                    key=f"admin_user_delete_close_denied_{_widget_safe_key(principal_id)}",
                    width="stretch",
                ):
                    _close_current_dialog_without_rerun()
                return
            st.warning("Bạn có chắc muốn xóa user này? Dữ liệu sẽ được soft-delete.")
            st.caption(f"Đối tượng: `{principal_id}`")
            cancel_col, confirm_col = st.columns(2)
            with cancel_col:
                cancel = st.button("Hủy", key=f"admin_user_delete_cancel_{_widget_safe_key(principal_id)}", width="stretch")
            with confirm_col:
                confirm = st.button(
                    "Xóa user",
                    type="primary",
                    key=f"admin_user_delete_confirm_{_widget_safe_key(principal_id)}",
                    width="stretch",
                )

            if cancel:
                _close_current_dialog_without_rerun()
                return
            if not confirm:
                return
            try:
                self._admin_service.delete_user(session, principal_id=principal_id)
                self._invalidate_admin_cache()
                self._set_feedback("Đã xóa user thành công.", level="success")
                st.rerun()
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))

        _dialog()

    def _show_role_add_modal(self, session) -> None:
        @st.dialog(
            "Add role",
            width="large",
        )
        def _dialog():
            if bool(st.session_state.pop(_ADMIN_ROLE_ADD_MODAL_PENDING_CLOSE_KEY, False)):
                self._close_role_modal_with_roles_refresh()
                return

            if not self._can_manage_roles(session):
                st.error("Bạn không có quyền thêm vai trò.")
                if st.button("Đóng", key="admin_role_add_close_denied", width="stretch"):
                    _close_current_dialog_without_rerun()
                return

            can_manage_permissions = self._can_manage_permissions(session)
            permission_catalog = self._safe_permissions(session) if can_manage_permissions else []
            permission_by_id = {permission.permission_id: permission for permission in permission_catalog}
            available_permission_ids = [permission.permission_id for permission in self._sorted_permission_catalog(permission_catalog)]

            with st.form("admin_role_add_modal_form"):
                identity_col, status_col = st.columns([2.2, 1.0], vertical_alignment="bottom")
                with identity_col:
                    st.caption("role_id sẽ được hệ thống tự sinh sau khi tạo.")
                with status_col:
                    is_active = st.checkbox("is_active", value=True)

                role_name = st.text_input("role_name")
                description = st.text_area("description")
                permission_ids = st.multiselect(
                    "permissions",
                    options=available_permission_ids,
                    format_func=lambda permission_id: self._permission_option_label(permission_by_id.get(permission_id)) or permission_id,
                    disabled=not can_manage_permissions,
                )
                if not can_manage_permissions:
                    st.caption("Không có quyền quản lý permission, role sẽ được tạo mà chưa gán permission.")
                cancel_col, submit_col = st.columns(2)
                with cancel_col:
                    st.form_submit_button(
                        "Hủy",
                        width="stretch",
                        on_click=self._mark_role_add_modal_for_close,
                    )
                with submit_col:
                    submitted = st.form_submit_button("Tạo role", type="primary", width="stretch")

            if not submitted:
                return

            try:
                created_role_id = self._admin_service.create_role(
                    session,
                    role_name=role_name,
                    description=description,
                    is_active=is_active,
                )
                if can_manage_permissions and permission_ids:
                    self._admin_service.assign_permissions(session, role_id=created_role_id, permission_ids=permission_ids)
                self._invalidate_admin_cache()
                self._set_feedback("Đã tạo role thành công.", level="success")
                st.session_state[_ADMIN_ROLE_ADD_MODAL_PENDING_CLOSE_KEY] = True
                st.rerun(scope="fragment")
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))

        _dialog()

    def _mark_role_add_modal_for_close(self) -> None:
        st.session_state[_ADMIN_ROLE_ADD_MODAL_PENDING_CLOSE_KEY] = True

    def _show_role_edit_modal(self, session, role_id: str) -> None:
        @st.dialog(
            f"Edit role: {role_id}",
            width="large",
            on_dismiss=self._on_role_edit_modal_dismiss,
        )
        def _dialog():
            if bool(st.session_state.pop(_ADMIN_ROLE_EDIT_MODAL_PENDING_CLOSE_KEY, False)):
                self._close_role_modal_with_roles_refresh()
                return

            if not self._can_manage_roles(session):
                st.error("Bạn không có quyền chỉnh sửa vai trò.")
                if st.button(
                    "Đóng",
                    key=f"admin_role_modal_close_denied_{_widget_safe_key(role_id)}",
                    width="stretch",
                ):
                    self._close_role_modal_with_roles_refresh()
                return

            try:
                with st.spinner("Đang tải thông tin vai trò..."):
                    detail = self._get_role_detail(session, role_id=role_id)
                    permission_catalog = self._safe_permissions(session) if self._can_manage_permissions(session) else []
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))
                if st.button(
                    "Đóng",
                    key=f"admin_role_modal_close_{_widget_safe_key(role_id)}",
                    width="stretch",
                ):
                    self._close_role_modal_with_roles_refresh()
                return

            can_manage_permissions = self._can_manage_permissions(session)
            permission_by_id = {permission.permission_id: permission for permission in permission_catalog}
            available_permission_ids = [permission.permission_id for permission in self._sorted_permission_catalog(permission_catalog)]
            assigned_permission_ids = self._permission_ids_for_assigned_permissions(permission_catalog, detail.assigned_permissions)

            with st.form(f"admin_role_modal_form_{_widget_safe_key(role_id)}"):
                identity_col, status_col = st.columns([2.2, 1.0], vertical_alignment="bottom")
                with identity_col:
                    st.text_input("role_id", value=detail.role.role_id, disabled=True)
                with status_col:
                    is_active = st.checkbox("is_active", value=detail.role.is_active)

                role_name = st.text_input("role_name", value=detail.role.role_name)
                description = st.text_area("description", value=detail.role.description)
                permission_ids = st.multiselect(
                    "permissions",
                    options=available_permission_ids,
                    format_func=lambda permission_id: self._permission_option_label(permission_by_id.get(permission_id)) or permission_id,
                    default=_filter_multiselect_default(available_permission_ids, assigned_permission_ids),
                    disabled=not can_manage_permissions,
                )
                if not can_manage_permissions:
                    st.caption("Không có quyền cập nhật permissions của role.")
                cancel_col, submit_col = st.columns(2)
                with cancel_col:
                    st.form_submit_button(
                        "Hủy",
                        width="stretch",
                        on_click=self._mark_role_edit_modal_for_close,
                    )
                with submit_col:
                    submitted = st.form_submit_button("Lưu thay đổi", type="primary", width="stretch")

            if not submitted:
                return
            try:
                self._admin_service.sync_role_permissions(
                    session,
                    role_id=role_id,
                    role_name=role_name,
                    description=description,
                    is_active=is_active,
                    target_permission_ids=tuple(permission_ids) if can_manage_permissions else None,
                )
                # Surgical invalidate
                self._invalidate_admin_cache(bucket="role_detail", key=(role_id.strip().lower(),))
                self._invalidate_admin_cache(bucket="roles")
                self._invalidate_admin_cache(bucket="permissions")
                self._set_feedback("Đã cập nhật role thành công.", level="success")
                st.session_state[_ADMIN_ROLE_EDIT_MODAL_PENDING_CLOSE_KEY] = True
                st.rerun(scope="fragment")
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))

        _dialog()

    def _on_role_edit_modal_dismiss(self) -> None:
        self._invalidate_admin_cache(bucket="roles")

    def _mark_role_edit_modal_for_close(self) -> None:
        st.session_state[_ADMIN_ROLE_EDIT_MODAL_PENDING_CLOSE_KEY] = True

    def _close_role_modal_with_roles_refresh(self) -> None:
        self._invalidate_admin_cache(bucket="roles")
        st.rerun()

    def _show_role_delete_modal(self, session, role_id: str) -> None:
        @st.dialog(
            f"Delete role: {role_id}",
            width="large",
        )
        def _dialog():
            if not self._can_manage_roles(session):
                st.error("Bạn không có quyền xóa vai trò.")
                if st.button(
                    "Đóng",
                    key=f"admin_role_delete_close_denied_{_widget_safe_key(role_id)}",
                    width="stretch",
                ):
                    _close_current_dialog_without_rerun()
                return
            st.warning("Bạn có chắc muốn xóa role này? Dữ liệu sẽ được soft-delete.")
            st.caption(f"Đối tượng: `{role_id}`")
            cancel_col, confirm_col = st.columns(2)
            with cancel_col:
                cancel = st.button("Hủy", key=f"admin_role_delete_cancel_{_widget_safe_key(role_id)}", width="stretch")
            with confirm_col:
                confirm = st.button(
                    "Xóa role",
                    type="primary",
                    key=f"admin_role_delete_confirm_{_widget_safe_key(role_id)}",
                    width="stretch",
                )
            if cancel:
                _close_current_dialog_without_rerun()
                return
            if not confirm:
                return
            try:
                self._admin_service.delete_role(session, role_id=role_id)
                self._invalidate_admin_cache()
                self._set_feedback("Đã xóa role thành công.", level="success")
                st.rerun()
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))

        _dialog()

    def _show_permission_add_modal(self, session) -> None:
        @st.dialog(
            "Add permission",
            width="large",
        )
        def _dialog():
            if bool(st.session_state.pop(_ADMIN_PERMISSION_ADD_MODAL_PENDING_CLOSE_KEY, False)):
                self._close_permission_modal_with_permissions_refresh()
                return

            if not self._can_manage_permissions(session):
                st.error("Bạn không có quyền thêm permission.")
                if st.button("Đóng", key="admin_permission_add_close_denied", width="stretch"):
                    _close_current_dialog_without_rerun()
                return

            with st.form("admin_permission_add_modal_form"):
                identity_col, status_col = st.columns([2.2, 1.0], vertical_alignment="bottom")
                with identity_col:
                    st.caption("permission_id sẽ được hệ thống tự sinh sau khi tạo.")
                with status_col:
                    is_active = st.checkbox("is_active", value=True)

                permission_name = st.text_input("permission_name")
                description = st.text_area("description")
                cancel_col, submit_col = st.columns(2)
                with cancel_col:
                    st.form_submit_button(
                        "Hủy",
                        width="stretch",
                        on_click=self._mark_permission_add_modal_for_close,
                    )
                with submit_col:
                    submitted = st.form_submit_button("Tạo permission", type="primary", width="stretch")

            if not submitted:
                return
            try:
                self._admin_service.create_permission(
                    session,
                    permission_name=permission_name,
                    description=description,
                    is_active=is_active,
                )
                self._invalidate_admin_cache()
                self._set_feedback("Đã tạo permission thành công.", level="success")
                st.session_state[_ADMIN_PERMISSION_ADD_MODAL_PENDING_CLOSE_KEY] = True
                st.rerun(scope="fragment")
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))

        _dialog()

    def _mark_permission_add_modal_for_close(self) -> None:
        st.session_state[_ADMIN_PERMISSION_ADD_MODAL_PENDING_CLOSE_KEY] = True

    def _show_permission_edit_modal(self, session, permission_id: str) -> None:
        @st.dialog(
            f"Edit permission: {permission_id}",
            width="large",
            on_dismiss=self._on_permission_edit_modal_dismiss,
        )
        def _dialog():
            if bool(st.session_state.pop(_ADMIN_PERMISSION_EDIT_MODAL_PENDING_CLOSE_KEY, False)):
                self._close_permission_modal_with_permissions_refresh()
                return

            if not self._can_manage_permissions(session):
                st.error("Bạn không có quyền chỉnh sửa permission.")
                if st.button(
                    "Đóng",
                    key=f"admin_permission_modal_close_denied_{_widget_safe_key(permission_id)}",
                    width="stretch",
                ):
                    self._close_permission_modal_with_permissions_refresh()
                return
            try:
                permission = self._get_permission_detail(session, permission_id=permission_id)
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))
                if st.button(
                    "Đóng",
                    key=f"admin_permission_modal_close_{_widget_safe_key(permission_id)}",
                    width="stretch",
                ):
                    self._close_permission_modal_with_permissions_refresh()
                return

            with st.form(f"admin_permission_modal_form_{_widget_safe_key(permission_id)}"):
                identity_col, status_col = st.columns([2.2, 1.0], vertical_alignment="bottom")
                with identity_col:
                    st.text_input("permission_id", value=permission.permission_id, disabled=True)
                with status_col:
                    is_active = st.checkbox("is_active", value=permission.is_active)

                permission_name = st.text_input("permission_name", value=permission.permission_name)
                description = st.text_area("description", value=permission.description)
                cancel_col, submit_col = st.columns(2)
                with cancel_col:
                    st.form_submit_button(
                        "Hủy",
                        width="stretch",
                        on_click=self._mark_permission_edit_modal_for_close,
                    )
                with submit_col:
                    submitted = st.form_submit_button("Lưu thay đổi", type="primary", width="stretch")

            if not submitted:
                return
            try:
                self._admin_service.save_permission(
                    session,
                    permission_id=permission_id,
                    permission_name=permission_name,
                    description=description,
                    is_active=is_active,
                )
                # Surgical invalidate
                self._invalidate_admin_cache(bucket="permission_detail", key=(permission_id.strip().lower(),))
                self._invalidate_admin_cache(bucket="permissions")
                self._set_feedback("Đã cập nhật permission thành công.", level="success")
                st.session_state[_ADMIN_PERMISSION_EDIT_MODAL_PENDING_CLOSE_KEY] = True
                st.rerun(scope="fragment")
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))

        _dialog()

    def _on_permission_edit_modal_dismiss(self) -> None:
        self._invalidate_admin_cache(bucket="permissions")

    def _mark_permission_edit_modal_for_close(self) -> None:
        st.session_state[_ADMIN_PERMISSION_EDIT_MODAL_PENDING_CLOSE_KEY] = True

    def _close_permission_modal_with_permissions_refresh(self) -> None:
        self._invalidate_admin_cache(bucket="permissions")
        st.rerun()

    def _show_permission_delete_modal(self, session, permission_id: str) -> None:
        @st.dialog(
            f"Delete permission: {permission_id}",
            width="large",
        )
        def _dialog():
            if not self._can_manage_permissions(session):
                st.error("Bạn không có quyền xóa permission.")
                if st.button(
                    "Đóng",
                    key=f"admin_permission_delete_close_denied_{_widget_safe_key(permission_id)}",
                    width="stretch",
                ):
                    _close_current_dialog_without_rerun()
                return
            st.warning("Bạn có chắc muốn xóa permission này? Dữ liệu sẽ được soft-delete.")
            st.caption(f"Đối tượng: `{permission_id}`")
            cancel_col, confirm_col = st.columns(2)
            with cancel_col:
                cancel = st.button("Hủy", key=f"admin_permission_delete_cancel_{_widget_safe_key(permission_id)}", width="stretch")
            with confirm_col:
                confirm = st.button(
                    "Xóa permission",
                    type="primary",
                    key=f"admin_permission_delete_confirm_{_widget_safe_key(permission_id)}",
                    width="stretch",
                )
            if cancel:
                _close_current_dialog_without_rerun()
                return
            if not confirm:
                return
            try:
                self._admin_service.delete_permission(session, permission_id=permission_id)
                self._invalidate_admin_cache()
                self._set_feedback("Đã xóa permission thành công.", level="success")
                st.rerun()
            except (AuthenticationValidationError, AuthError) as exc:
                st.error(str(exc))

        _dialog()

    def _render_feedback(self) -> None:
        feedback = st.session_state.pop(_ADMIN_FEEDBACK_KEY, None)
        if not isinstance(feedback, dict):
            return
        message = str(feedback.get("message", "")).strip()
        level = str(feedback.get("level", "success")).strip().lower()
        if not message:
            return
        if level == "error":
            st.error(message)
            return
        st.success(message)

    def _set_feedback(self, message: str, *, level: str) -> None:
        st.session_state[_ADMIN_FEEDBACK_KEY] = {"message": message, "level": level}

    def _list_users(self, session, *, search_term: str, status_filter: str, auth_source_filter: str):
        key = (search_term.strip().lower(), status_filter, auth_source_filter)
        return self._cached_query(
            session,
            bucket="users",
            key=key,
            loader=lambda: self._admin_service.list_users(
                session,
                search_term=search_term,
                status_filter=status_filter,
                auth_source_filter=auth_source_filter,
            ),
        )

    def _list_roles(self, session, *, search_term: str, status_filter: str):
        key = (search_term.strip().lower(), status_filter)
        return self._cached_query(
            session,
            bucket="roles",
            key=key,
            loader=lambda: self._admin_service.list_roles(session, search_term=search_term, status_filter=status_filter),
        )

    def _list_permissions(self, session, *, search_term: str):
        key = (search_term.strip().lower(),)
        return self._cached_query(
            session,
            bucket="permissions",
            key=key,
            loader=lambda: self._admin_service.list_permissions(session, search_term=search_term),
        )

    def _list_audit(self, session, *, limit: int):
        key = (limit,)
        return self._cached_query(
            session,
            bucket="audit",
            key=key,
            loader=lambda: self._admin_service.list_audit(session, limit=limit),
        )

    def _get_user_detail(self, session, *, principal_id: str):
        key = (principal_id.strip().lower(),)
        return self._cached_query(
            session,
            bucket="user_detail",
            key=key,
            loader=lambda: self._admin_service.get_user_detail(session, principal_id=principal_id),
        )

    def _get_role_detail(self, session, *, role_id: str):
        key = (role_id.strip().lower(),)
        return self._cached_query(
            session,
            bucket="role_detail",
            key=key,
            loader=lambda: self._admin_service.get_role_detail(session, role_id=role_id),
        )

    def _get_permission_detail(self, session, *, permission_id: str):
        key = (permission_id.strip().lower(),)
        return self._cached_query(
            session,
            bucket="permission_detail",
            key=key,
            loader=lambda: self._admin_service.get_permission_detail(session, permission_id=permission_id),
        )

    def _cached_query(self, session, *, bucket: str, key: tuple[str, ...] | tuple[int, ...], loader):
        root_cache = st.session_state.setdefault(_ADMIN_CACHE_STATE_KEY, {})
        bucket_cache = root_cache.setdefault(bucket, {})
        scope = self._cache_scope(session)
        scoped_key = (scope, key)
        cached = bucket_cache.get(scoped_key)
        now = time.time()
        if isinstance(cached, dict):
            cached_at = float(cached.get("ts", 0.0))
            if now - cached_at <= _ADMIN_CACHE_TTL_SECONDS:
                return cached.get("value")
        value = loader()
        bucket_cache[scoped_key] = {"ts": now, "value": value}
        return value

    def _cache_scope(self, session) -> tuple[str, str]:
        if session is None:
            return ("anonymous", "")
        principal_id = str(getattr(session, "principal_id", "")).strip().lower()
        permissions = tuple(sorted(str(permission) for permission in getattr(session, "permissions", ())))
        return (principal_id, ",".join(permissions))

    def _invalidate_admin_cache(self, *, bucket: str | None = None, key: object | None = None) -> None:
        if _ADMIN_CACHE_STATE_KEY not in st.session_state:
            return
        
        if not bucket:
            st.session_state.pop(_ADMIN_CACHE_STATE_KEY, None)
            return

        root_cache = st.session_state.get(_ADMIN_CACHE_STATE_KEY, {})
        if bucket not in root_cache:
            return

        if key is None:
            # Clear whole bucket
            root_cache[bucket] = {}
        else:
            # Surgical pop
            bucket_cache = root_cache[bucket]
            # Try both scoped and unscoped if applicable
            # (In our impl, scoped is (scope, key))
            scope = self._cache_scope(get_current_session())
            scoped_key = (scope, key)
            bucket_cache.pop(scoped_key, None)
            bucket_cache.pop(key, None)

    def _render_audit_tab(self, session) -> None:
        st.markdown("#### Audit")
        limit = st.selectbox("Limit", [50, 100, 200, 500], index=2, key="admin_audit_limit")
        try:
            records = self._list_audit(session, limit=int(limit))
        except (AuthenticationValidationError, AuthError) as exc:
            st.error(str(exc))
            return

        self._render_metric_strip(
            [
                ("Audit rows", len(records), "Latest records returned by the backend"),
                ("Limit", int(limit), "Selected row cap"),
                ("Denied events", sum(1 for record in records if record.result == "denied"), "Access or action denials"),
            ]
        )

        self._render_badge_row(
            [
                ("Success", "active", sum(1 for record in records if record.result == "success")),
                ("Denied", "danger", sum(1 for record in records if record.result == "denied")),
                ("Failed", "inactive", sum(1 for record in records if record.result not in {"success", "denied"})),
            ]
        )

        with st.container(border=True):
            st.caption("Timeline-style audit list. Most recent records are shown first.")
            self._render_dataframe(records, height=300)

    def _render_dataframe(self, records, *, height: int | None = None) -> None:
        if not records:
            st.info("Không có dữ liệu.")
            return
        st.dataframe(
            pd.DataFrame([_record_to_dict(record) for record in records]),
            use_container_width=True,
            hide_index=True,
            height=height,
        )

    def _safe_users(self, session) -> list:
        try:
            return list(self._list_users(session, search_term="", status_filter="all", auth_source_filter="all"))
        except Exception:
            return []

    def _safe_roles(self, session) -> list:
        try:
            return list(self._list_roles(session, search_term="", status_filter="all"))
        except Exception:
            return []

    def _safe_permissions(self, session) -> list:
        try:
            return list(self._list_permissions(session, search_term=""))
        except Exception:
            return []

    def _sorted_role_catalog(self, roles) -> list:
        return sorted(roles, key=lambda role: (self._role_category(role), role.role_name.lower(), role.role_id.lower()))

    def _sorted_permission_catalog(self, permissions) -> list:
        return sorted(
            permissions,
            key=lambda permission: (self._permission_category(permission), permission.permission_name.lower(), permission.permission_id.lower()),
        )

    def _role_category(self, role) -> str:
        text = f"{role.role_id} {role.role_name}".lower()
        if any(keyword in text for keyword in ("admin", "security", "manage")):
            return "Admin"
        return "Standard"

    def _permission_category(self, permission) -> str:
        permission_id = permission.permission_id.lower()
        if permission_id in {"manage_users", "manage_roles", "manage_permissions"}:
            return "Administration"
        if permission_id in {"view_security_audit"}:
            return "Audit"
        if permission_id in {"app_access"}:
            return "Access"
        return "Business"

    def _role_option_label(self, role) -> str:
        if role is None:
            return ""
        return f"{self._role_category(role)} · {role.role_id} | {role.role_name}"

    def _permission_option_label(self, permission) -> str:
        if permission is None:
            return ""
        return f"{self._permission_category(permission)} · {permission.permission_id} | {permission.permission_name}"

    def _permission_ids_for_assigned_permissions(self, permissions, assigned_permissions) -> list[str]:
        permission_id_by_name = {permission.permission_name: permission.permission_id for permission in permissions}
        available_permission_ids = {permission.permission_id for permission in permissions}
        resolved_ids: list[str] = []
        seen: set[str] = set()
        for permission in assigned_permissions:
            resolved_id = permission if permission in available_permission_ids else permission_id_by_name.get(permission, "")
            if not resolved_id or resolved_id in seen:
                continue
            seen.add(resolved_id)
            resolved_ids.append(resolved_id)
        return resolved_ids

    def _principal_option_label(self, principal) -> str:
        if principal is None:
            return ""
        return f"{principal.principal_id} | {principal.username}"

    def _render_metric_strip(self, metrics: list[tuple[str, int | str, str]]) -> None:
        columns = st.columns(len(metrics))
        for column, (label, value, help_text) in zip(columns, metrics):
            with column:
                st.metric(label, value)
                st.caption(help_text)

    def _render_summary_grid(self, rows: list[tuple[str, str]]) -> None:
        if not rows:
            return
        columns = st.columns(2)
        for index, (label, value) in enumerate(rows):
            with columns[index % 2]:
                st.markdown(f"**{escape(label)}**")
                st.caption(value)

    def _render_badge_row(self, badges: list[tuple[str, str, int | None]]) -> None:
        if not badges:
            return
        rendered = []
        for label, tone, count in badges:
            suffix = f" {count}" if count is not None else ""
            rendered.append(f'<span class="status-badge {tone}">{escape(label)}{suffix}</span>')
        st.markdown(f'<div class="badge-row">{"".join(rendered)}</div>', unsafe_allow_html=True)

    def _status_badge_html(self, label: str, tone: str) -> str:
        safe_tone = tone if tone in {"active", "inactive", "info", "neutral", "danger"} else "neutral"
        return f'<span class="status-badge {safe_tone}">{escape(label)}</span>'

    def _can_manage_users(self, session) -> bool:
        return self._has_any_permission(session, ("manage_users", "security_admin"))

    def _can_manage_roles(self, session) -> bool:
        return self._has_any_permission(session, ("manage_roles", "security_admin"))

    def _can_manage_permissions(self, session) -> bool:
        return self._has_any_permission(session, ("manage_permissions", "security_admin"))

    def _has_any_permission(self, session, permissions: tuple[str, ...]) -> bool:
        if session is None:
            return False
        return any(permission in session.permissions for permission in permissions)

    def _has_admin_gate(self, permissions: tuple[str, ...]) -> bool:
        return any(permission in ADMIN_GATE_PERMISSIONS for permission in permissions)


def _record_to_dict(record) -> dict[str, object]:
    if is_dataclass(record):
        return asdict(record)
    if hasattr(record, "model_dump"):
        return record.model_dump()
    if isinstance(record, dict):
        return dict(record)
    return {"value": str(record)}


def _dataclass_to_dict(value) -> dict[str, object]:
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, dict):
        return dict(value)
    return {"value": str(value)}


def _filter_multiselect_default(options: list[str], selected: tuple[str, ...] | list[str]) -> list[str]:
    available = set(options)
    normalized: list[str] = []
    seen: set[str] = set()
    for value in selected:
        if value not in available or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _widget_safe_key(value: str) -> str:
    normalized = [character if character.isalnum() else "_" for character in value]
    output = "".join(normalized).strip("_")
    return output or "item"
