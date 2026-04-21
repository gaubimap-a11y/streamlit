"""Integration tests for ReportFilterService with real Databricks persistence.

Run:
    pytest webapp/tests/test_report_filter_service_integration.py -m integration -v

Skip in offline / CI without DB:
    pytest -m "not integration"
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from src.application.report_filters.report_filter_service import ReportFilterService
from src.core.config import get_settings
from src.core.exceptions import DataAccessError
from src.domain.report_filters import PRODUCT_REPORT_CODE, RecipientStatus
from src.infrastructure.databricks.client import databricks_connection


def _must_resolve_user_id(service: ReportFilterService, username: str) -> str:
    try:
        user_id = service.resolve_user_id(username)
    except DataAccessError:
        pytest.skip("Integration DB is unavailable in current environment.")
    if not user_id:
        pytest.skip(f"Missing seeded user `{username}` in integration database.")
    return user_id


def _insert_inactive_user(user_id: str, username: str) -> None:
    now = datetime.now(tz=timezone.utc)
    catalog = get_settings().databricks.catalog
    sql = (
        f"INSERT INTO {catalog}.auth.users "
        "(user_id, username, email, password_hash, auth_source, last_login_at, is_active, "
        "created_at, updated_at, deleted_at, created_by, updated_by, deleted_by, is_deleted) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    params = [
        user_id,
        username,
        f"{username}@example.com",
        "integration-hash",
        "internal",
        None,
        False,
        now,
        now,
        None,
        "integration-test",
        "integration-test",
        None,
        False,
    ]
    with databricks_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)


@pytest.mark.integration
def test_report_filter_service_e2e_persistence_flows():
    service = ReportFilterService()
    owner_user_id = _must_resolve_user_id(service, "admin")
    recipient_user_id = _must_resolve_user_id(service, "manager")
    suffix = uuid4().hex[:8]

    source_filter_id: str | None = None
    copied_filter_id: str | None = None
    try:
        source_filter_id = service.save_filter(
            report_code=PRODUCT_REPORT_CODE,
            actor_user_id=owner_user_id,
            filter_name=f"it-source-{suffix}",
            raw_filter_payload={"name": "apple", "price_min": 1000},
        )

        owner_list = service.list_saved_filters(PRODUCT_REPORT_CODE, owner_user_id)
        assert any(item.filter_definition_id == source_filter_id for item in owner_list.my_filters)

        apply_result = service.get_apply_payload(source_filter_id, owner_user_id, PRODUCT_REPORT_CODE)
        assert apply_result.payload == {"name": "apple", "price_min": 1000.0}

        share_result = service.share_filter(
            filter_definition_id=source_filter_id,
            actor_user_id=owner_user_id,
            recipient_user_ids=[recipient_user_id],
        )
        assert share_result.invalid_recipient_user_ids == []

        recipient_list = service.list_saved_filters(PRODUCT_REPORT_CODE, recipient_user_id)
        shared_before_update = next(
            item
            for item in recipient_list.shared_with_me
            if item.filter_definition_id == source_filter_id
        )
        assert shared_before_update.filter_payload == {"name": "apple", "price_min": 1000.0}

        service.update_filter(
            filter_definition_id=source_filter_id,
            actor_user_id=owner_user_id,
            filter_name=f"it-source-updated-{suffix}",
            raw_filter_payload={"name": "banana", "price_max": 9000},
        )

        recipient_after_update = service.list_saved_filters(PRODUCT_REPORT_CODE, recipient_user_id)
        shared_after_update = next(
            item
            for item in recipient_after_update.shared_with_me
            if item.filter_definition_id == source_filter_id
        )
        assert shared_after_update.filter_name == f"it-source-updated-{suffix}"
        assert shared_after_update.filter_payload == {"name": "banana", "price_max": 9000.0}

        copied_filter_id = service.save_as_new(
            source_filter_definition_id=source_filter_id,
            actor_user_id=recipient_user_id,
            new_filter_name=f"it-copy-{suffix}",
        )
        recipient_my_filters = service.list_saved_filters(PRODUCT_REPORT_CODE, recipient_user_id).my_filters
        assert any(item.filter_definition_id == copied_filter_id for item in recipient_my_filters)

        service.revoke_share(
            filter_definition_id=source_filter_id,
            actor_user_id=owner_user_id,
            recipient_user_id=recipient_user_id,
        )
        recipient_after_revoke = service.list_saved_filters(PRODUCT_REPORT_CODE, recipient_user_id)
        assert all(item.filter_definition_id != source_filter_id for item in recipient_after_revoke.shared_with_me)
    finally:
        if copied_filter_id:
            service.delete_filter(copied_filter_id, recipient_user_id)
        if source_filter_id:
            service.delete_filter(source_filter_id, owner_user_id)


@pytest.mark.integration
def test_share_filter_handles_inactive_recipient_and_merge_upsert():
    service = ReportFilterService()
    owner_user_id = _must_resolve_user_id(service, "admin")
    active_recipient_user_id = _must_resolve_user_id(service, "manager")
    suffix = uuid4().hex[:8]
    inactive_user_id = f"I{uuid4().hex[:11].upper()}"
    inactive_username = f"it_inactive_{suffix}"
    _insert_inactive_user(inactive_user_id, inactive_username)

    source_filter_id: str | None = None
    try:
        source_filter_id = service.save_filter(
            report_code=PRODUCT_REPORT_CODE,
            actor_user_id=owner_user_id,
            filter_name=f"it-share-{suffix}",
            raw_filter_payload={"category": "Any"},
        )

        share_result = service.share_filter(
            filter_definition_id=source_filter_id,
            actor_user_id=owner_user_id,
            recipient_user_ids=[active_recipient_user_id, inactive_user_id],
        )
        assert share_result.invalid_recipient_user_ids == [inactive_user_id]

        recipients = service.get_share_recipients(source_filter_id, owner_user_id)
        inactive_row = next(item for item in recipients if item.recipient_user_id == inactive_user_id)
        assert inactive_row.recipient_status == RecipientStatus.INVALID
        assert inactive_row.revoked_at is None

        service.revoke_share(
            filter_definition_id=source_filter_id,
            actor_user_id=owner_user_id,
            recipient_user_id=active_recipient_user_id,
        )
        service.share_filter(
            filter_definition_id=source_filter_id,
            actor_user_id=owner_user_id,
            recipient_user_ids=[active_recipient_user_id],
        )

        recipients_after_re_share = service.get_share_recipients(source_filter_id, owner_user_id)
        active_rows = [
            item for item in recipients_after_re_share if item.recipient_user_id == active_recipient_user_id
        ]
        assert len(active_rows) == 1
        assert active_rows[0].revoked_at is None
        assert active_rows[0].recipient_status == RecipientStatus.ACTIVE
    finally:
        if source_filter_id:
            service.delete_filter(source_filter_id, owner_user_id)
