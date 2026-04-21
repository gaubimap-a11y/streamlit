"""Integration tests for ProductRepository — require real Databricks connection.

Run:
    pytest webapp/tests/test_product_repository_integration.py -m integration -v

Skip in offline / CI without DB:
    pytest -m "not integration"
"""

import pytest

from src.domain.filters import ProductFilter
from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.product_repository import ProductRepository


@pytest.fixture(scope="module")
def db_connection():
    with databricks_connection() as conn:
        yield conn


@pytest.mark.integration
def test_get_categories_returns_non_empty_list(db_connection):
    repo = ProductRepository()
    result = repo.get_categories(db_connection)
    assert isinstance(result, list)
    assert len(result) > 0


@pytest.mark.integration
def test_get_count_returns_non_negative_integer(db_connection):
    repo = ProductRepository()
    result = repo.get_count(ProductFilter(), db_connection)
    assert isinstance(result, int)
    assert result >= 0


@pytest.mark.integration
def test_get_page_returns_rows(db_connection):
    repo = ProductRepository()
    result = repo.get_page(ProductFilter(), 1, db_connection)
    assert isinstance(result, list)


@pytest.mark.integration
def test_get_chart_data_returns_grouped_by_category(db_connection):
    repo = ProductRepository()
    result = repo.get_chart_data(ProductFilter(), db_connection)
    assert isinstance(result, list)
