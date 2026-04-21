from contextlib import contextmanager

import pytest

from src.application.product.product_service import ProductService
from src.core.exceptions import DataAccessError
from src.core.reporting import ReportData
from src.domain.filters import ProductFilter


def test_get_products_opens_one_connection_and_calls_count_and_page(mocker):
    conn = object()

    @contextmanager
    def _cm():
        yield conn

    mocker.patch("src.application.product.product_service.databricks_connection", _cm)

    repo = mocker.MagicMock()
    repo.get_count.return_value = 100
    repo.get_page.return_value = []

    service = ProductService(product_repository=repo)
    data = service.get_products(ProductFilter(), 2)

    assert isinstance(data, ReportData)
    assert data.total == 100
    repo.get_count.assert_called_once_with(mocker.ANY, conn)
    repo.get_page.assert_called_once()


def test_get_products_propagates_data_access_error(mocker):
    conn = object()

    @contextmanager
    def _cm():
        yield conn

    mocker.patch("src.application.product.product_service.databricks_connection", _cm)

    repo = mocker.MagicMock()
    repo.get_count.side_effect = DataAccessError("boom")

    service = ProductService(product_repository=repo)
    with pytest.raises(DataAccessError):
        service.get_products(ProductFilter(), 1)


def test_get_chart_data_returns_list(mocker):
    conn = object()

    @contextmanager
    def _cm():
        yield conn

    mocker.patch("src.application.product.product_service.databricks_connection", _cm)

    repo = mocker.MagicMock()
    repo.get_chart_data.return_value = [{"category": "A", "total_stock": 1}]

    service = ProductService(product_repository=repo)
    result = service.get_chart_data(ProductFilter())
    assert isinstance(result, list)
