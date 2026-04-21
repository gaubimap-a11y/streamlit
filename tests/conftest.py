from __future__ import annotations

import pytest
from unittest.mock import MagicMock as _MagicMock
from unittest.mock import ANY as _ANY
from unittest.mock import patch as _patch


def pytest_configure() -> None:
    # Swallow tempfile cleanup PermissionError (WinError 5) that can appear at
    # interpreter shutdown and pollute test output.
    import shutil
    import tempfile

    def _safe_rmtree(cls, name: str, ignore_errors: bool = False, onexc=None) -> None:  # type: ignore[no-untyped-def]
        try:
            shutil.rmtree(name, ignore_errors=ignore_errors, onexc=onexc)  # type: ignore[arg-type]
        except PermissionError:
            return

    tempfile.TemporaryDirectory._rmtree = classmethod(_safe_rmtree)  # type: ignore[assignment]


@pytest.fixture
def mocker(request):
    class _SimpleMocker:
        MagicMock = _MagicMock
        ANY = _ANY

        def patch(self, target, *args, **kwargs):
            patched = _patch(target, *args, **kwargs)
            value = patched.start()
            request.addfinalizer(patched.stop)
            return value

    return _SimpleMocker()


@pytest.fixture(autouse=True)
def _disable_databricks_for_ui_tests(mocker, request):
    """
    UI tests execute Streamlit scripts via AppTest. Those scripts call
    `ProductService`, which normally opens a real Databricks connection.

    Patch the service methods globally so the suite runs offline.
    """

    nodeid = str(getattr(request, "node", ""))
    is_ui_test = any(
        name in nodeid
        for name in (
            "test_login_page.py",
            "test_dashboard.py",
            "test_e2e_login.py",
            "test_admin_page.py",
        )
    )
    if not is_ui_test:
        return

    from src.core.reporting import ReportData

    class _Settings:
        session_timeout_hours = 8

    mocker.patch(
        "src.application.product.product_service.ProductService.get_categories",
        return_value=["Any"],
    )
    mocker.patch(
        "src.application.product.product_service.ProductService.get_products",
        return_value=ReportData(total=1, rows=[{"product_name": "x"}]),
    )
    mocker.patch(
        "src.application.product.product_service.ProductService.get_report_meta",
        return_value=(1, []),
    )
    mocker.patch(
        "src.application.product.product_service.ProductService.get_product_page",
        return_value=[{"product_name": "x"}],
    )
    mocker.patch(
        "src.application.product.product_service.ProductService.get_product_pages",
        return_value={1: [{"product_name": "x"}]},
    )
    mocker.patch(
        "src.application.product.product_service.ProductService.get_chart_data",
        return_value=[],
    )
    mocker.patch(
        "src.ui.session.auth_session.get_settings",
        return_value=_Settings(),
    )
    mocker.patch("src.ui.audit_events.get_audit_writer", return_value=None)
    mocker.patch(
        "src.ui.session.auth_session._sync_auth_query_params_from_session",
        return_value=None,
    )
