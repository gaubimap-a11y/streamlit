from src.core.exceptions import BusinessRuleError
from src.domain.filters import ProductFilter
from src.domain.report_filters import FilterApplyResult
from src.ui.pages.dashboard_page import DashboardPage


def _create_page(mocker) -> DashboardPage:
    product_service = mocker.MagicMock()
    product_service.get_categories.return_value = ["Any"]
    report_filter_service = mocker.MagicMock()
    return DashboardPage(
        product_service=product_service,
        report_filter_service=report_filter_service,
    )


def test_apply_filter_updates_store_and_notice_when_payload_has_ignored_fields(mocker):
    page = _create_page(mocker)
    page.clear_report_cache = mocker.MagicMock()
    filter_store = mocker.MagicMock()

    page._report_filter_service.get_apply_payload.return_value = FilterApplyResult(
        payload={"name": "apple"},
        ignored_fields=["legacy_field"],
    )

    session_state = {"_product_dataset_cache": {"cached": True}}
    mocker.patch("src.ui.pages.dashboard_page.st.session_state", session_state)
    rerun = mocker.patch("src.ui.pages.dashboard_page.st.rerun")

    page._apply_filter(
        filter_store=filter_store,
        actor_user_id="u1",
        filter_definition_id="f1",
    )

    filter_store.replace_payload.assert_called_once_with({"name": "apple"})
    page.clear_report_cache.assert_called_once()
    assert session_state["_product_is_searched"] is True
    assert "legacy_field" in session_state["_product_apply_notice"]
    rerun.assert_called_once()


def test_apply_filter_shows_warning_on_business_rule_error(mocker):
    page = _create_page(mocker)
    filter_store = mocker.MagicMock()
    page._report_filter_service.get_apply_payload.side_effect = BusinessRuleError("Khong co quyen.")
    session_state = {}
    mocker.patch("src.ui.pages.dashboard_page.st.session_state", session_state)

    page._apply_filter_logic(
        filter_store=filter_store,
        actor_user_id="u1",
        filter_definition_id="f1",
    )

    filter_store.replace_payload.assert_not_called()
    assert session_state["_product_apply_error"] == "Khong co quyen."


def test_apply_filter_logic_marks_report_as_searched(mocker):
    page = _create_page(mocker)
    page.clear_report_cache = mocker.MagicMock()
    filter_store = mocker.MagicMock()

    page._report_filter_service.get_apply_payload.return_value = FilterApplyResult(
        payload={"name": "banana"},
        ignored_fields=[],
    )

    session_state = {}
    mocker.patch("src.ui.pages.dashboard_page.st.session_state", session_state)

    page._apply_filter_logic(
        filter_store=filter_store,
        actor_user_id="u1",
        filter_definition_id="f1",
    )

    assert session_state["_product_is_searched"] is True
    filter_store.replace_payload.assert_called_once_with({"name": "banana"})


def test_update_selected_filter_shows_warning_when_service_validation_fails(mocker):
    page = _create_page(mocker)
    page._report_filter_service.update_filter.side_effect = ValueError("Payload khong hop le.")

    warning = mocker.patch("src.ui.pages.dashboard_page.st.warning")
    rerun = mocker.patch("src.ui.pages.dashboard_page.st.rerun")

    page._update_selected_filter(
        actor_user_id="u1",
        filter_definition_id="f1",
        filter_name="New filter",
        current_filter=ProductFilter(),
    )

    warning.assert_called_once_with("Payload khong hop le.")
    rerun.assert_not_called()
