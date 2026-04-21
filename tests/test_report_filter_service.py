from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from src.application.report_filters.report_filter_service import ReportFilterService
from src.core.exceptions import BusinessRuleError
from src.domain.report_filters import RecipientStatus, ReportFilterDefinition, SharedReportFilter
from src.domain.user import UserRow


def _mock_connection(mocker):
    conn = object()

    @contextmanager
    def _cm():
        yield conn

    mocker.patch("src.application.report_filters.report_filter_service.databricks_connection", _cm)
    return conn


def _definition(
    *,
    filter_definition_id: str = "f1",
    report_code: str = "product",
    owner_user_id: str = "u1",
    filter_name: str = "Tên filter",
    filter_payload: dict | None = None,
) -> ReportFilterDefinition:
    now = datetime.now(tz=timezone.utc)
    return ReportFilterDefinition(
        filter_definition_id=filter_definition_id,
        report_code=report_code,
        owner_user_id=owner_user_id,
        filter_name=filter_name,
        filter_payload=filter_payload or {"name": "apple"},
        is_active=True,
        created_at=now,
        updated_at=now,
    )


def test_save_filter_rejects_duplicate_name(mocker):
    conn = _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    definition_repo.find_by_name.return_value = _definition()
    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=mocker.MagicMock(),
        user_repository=mocker.MagicMock(),
    )

    with pytest.raises(BusinessRuleError):
        service.save_filter(
            report_code="product",
            actor_user_id="u1",
            filter_name="Tên filter",
            raw_filter_payload={"name": "apple"},
        )

    definition_repo.find_by_name.assert_called_once_with("u1", "product", "Tên filter", conn)


def test_update_filter_rejects_non_owner(mocker):
    _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    definition_repo.find_by_id.return_value = _definition(owner_user_id="u_owner")
    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=mocker.MagicMock(),
        user_repository=mocker.MagicMock(),
    )

    with pytest.raises(BusinessRuleError):
        service.update_filter(
            filter_definition_id="f1",
            actor_user_id="u_not_owner",
            filter_name="new name",
            raw_filter_payload={"name": "apple"},
        )


def test_get_apply_payload_rejects_cross_report(mocker):
    _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    definition_repo.find_by_id.return_value = _definition(report_code="users")
    share_repo = mocker.MagicMock()
    share_repo.has_active_share.return_value = True
    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=share_repo,
        user_repository=mocker.MagicMock(),
    )

    with pytest.raises(BusinessRuleError):
        service.get_apply_payload(
            filter_definition_id="f1",
            actor_user_id="u_recipient",
            current_report_code="product",
        )


def test_get_apply_payload_ignores_legacy_fields(mocker):
    _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    definition_repo.find_by_id.return_value = _definition(
        owner_user_id="u_owner",
        filter_payload={"name": "apple", "legacy": "x", "price_min": 10},
    )
    share_repo = mocker.MagicMock()
    share_repo.has_active_share.return_value = True
    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=share_repo,
        user_repository=mocker.MagicMock(),
    )

    result = service.get_apply_payload(
        filter_definition_id="f1",
        actor_user_id="u_recipient",
        current_report_code="product",
    )

    assert result.payload == {"name": "apple", "price_min": 10.0}
    assert result.ignored_fields == ["legacy"]


def test_save_as_new_allows_recipient(mocker):
    conn = _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    source = _definition(owner_user_id="u_owner", filter_name="Nguồn")
    definition_repo.find_by_id.return_value = source
    definition_repo.find_by_name.return_value = None
    share_repo = mocker.MagicMock()
    share_repo.has_active_share.return_value = True

    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=share_repo,
        user_repository=mocker.MagicMock(),
    )

    new_id = service.save_as_new(
        source_filter_definition_id="f1",
        actor_user_id="u_recipient",
        new_filter_name="Copy",
    )

    assert isinstance(new_id, str)
    definition_repo.create.assert_called_once()
    assert definition_repo.create.call_args.kwargs["owner_user_id"] == "u_recipient"
    definition_repo.find_by_name.assert_called_once_with(
        owner_user_id="u_recipient",
        report_code=source.report_code,
        filter_name="Copy",
        conn=conn,
    )


def test_share_filter_marks_invalid_recipient(mocker):
    _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    definition_repo.find_by_id.return_value = _definition(owner_user_id="u_owner")
    user_repo = mocker.MagicMock()
    user_repo.find_by_user_id.side_effect = lambda user_id, conn: {
        "u_valid": UserRow(
            user_id="u_valid",
            username="valid",
            email="valid@example.com",
            password_hash="hash",
            is_active=True,
        ),
        "u_inactive": UserRow(
            user_id="u_inactive",
            username="inactive",
            email="inactive@example.com",
            password_hash="hash",
            is_active=False,
        ),
    }.get(user_id)
    share_repo = mocker.MagicMock()

    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=share_repo,
        user_repository=user_repo,
    )

    result = service.share_filter(
        filter_definition_id="f1",
        actor_user_id="u_owner",
        recipient_user_ids=["u_valid", "u_inactive", "u_invalid"],
    )

    assert result.invalid_recipient_user_ids == ["u_inactive", "u_invalid"]
    assert share_repo.share_with_recipients.call_args.kwargs["invalid_recipient_user_ids"] == {
        "u_inactive",
        "u_invalid",
    }


def test_list_saved_filters_returns_two_groups(mocker):
    _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    my_filter = _definition(owner_user_id="u1")
    definition_repo.list_my_filters.return_value = [my_filter]

    share_repo = mocker.MagicMock()
    shared_item = SharedReportFilter(
        filter_definition_id="f_shared",
        report_code="product",
        filter_name="shared-name",
        filter_payload={"name": "apple"},
        owner_user_id="u_owner",
        recipient_user_id="u1",
        recipient_status=RecipientStatus.ACTIVE,
        updated_at=datetime.now(tz=timezone.utc),
    )
    share_repo.list_shared_with_me.return_value = [shared_item]

    owner = mocker.MagicMock()
    owner.username = "owner-name"
    user_repo = mocker.MagicMock()
    user_repo.find_by_user_id.return_value = owner

    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=share_repo,
        user_repository=user_repo,
    )

    result = service.list_saved_filters("product", "u1")

    assert len(result.my_filters) == 1
    assert len(result.shared_with_me) == 1
    assert result.shared_with_me[0].owner_username == "owner-name"


def test_save_filter_persists_only_normalized_payload(mocker):
    conn = _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    definition_repo.find_by_name.return_value = None
    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=mocker.MagicMock(),
        user_repository=mocker.MagicMock(),
    )

    service.save_filter(
        report_code="product",
        actor_user_id="u1",
        filter_name="Ten filter",
        raw_filter_payload={"name": " apple ", "page": 2, "sort": "desc"},
    )

    definition_repo.create.assert_called_once()
    assert definition_repo.create.call_args.kwargs["filter_payload"] == {"name": "apple"}
    definition_repo.find_by_name.assert_called_once_with("u1", "product", "Ten filter", conn)


def test_get_apply_payload_rejects_non_owner_without_share(mocker):
    _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    definition_repo.find_by_id.return_value = _definition(owner_user_id="u_owner")
    share_repo = mocker.MagicMock()
    share_repo.has_active_share.return_value = False
    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=share_repo,
        user_repository=mocker.MagicMock(),
    )

    with pytest.raises(BusinessRuleError):
        service.get_apply_payload(
            filter_definition_id="f1",
            actor_user_id="u_other",
            current_report_code="product",
        )


def test_share_filter_rejects_empty_recipient_list(mocker):
    _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    definition_repo.find_by_id.return_value = _definition(owner_user_id="u_owner")
    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=mocker.MagicMock(),
        user_repository=mocker.MagicMock(),
    )

    with pytest.raises(BusinessRuleError):
        service.share_filter(
            filter_definition_id="f1",
            actor_user_id="u_owner",
            recipient_user_ids=["", "u_owner", "  "],
        )


def test_revoke_share_rejects_empty_recipient_user_id(mocker):
    _mock_connection(mocker)
    service = ReportFilterService(
        definition_repository=mocker.MagicMock(),
        share_repository=mocker.MagicMock(),
        user_repository=mocker.MagicMock(),
    )

    with pytest.raises(BusinessRuleError):
        service.revoke_share(
            filter_definition_id="f1",
            actor_user_id="u_owner",
            recipient_user_id=" ",
        )


def test_save_as_new_rejects_user_without_access_to_source_filter(mocker):
    _mock_connection(mocker)
    definition_repo = mocker.MagicMock()
    definition_repo.find_by_id.return_value = _definition(owner_user_id="u_owner")
    share_repo = mocker.MagicMock()
    share_repo.has_active_share.return_value = False
    service = ReportFilterService(
        definition_repository=definition_repo,
        share_repository=share_repo,
        user_repository=mocker.MagicMock(),
    )

    with pytest.raises(BusinessRuleError):
        service.save_as_new(
            source_filter_definition_id="f1",
            actor_user_id="u_not_owner",
            new_filter_name="Copy",
        )
