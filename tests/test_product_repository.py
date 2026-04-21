import pytest

from src.analytics.shared.sql_builder import WhereClauseBuilder
from src.core.exceptions import DataAccessError
from src.domain.filters import ProductFilter
from src.domain.product import ProductRow
from src.infrastructure.repositories.product_repository import ProductRepository


def _make_connection_with_rows(mock_rows):
    class MockCursor:
        def __init__(self):
            self.last_sql = None
            self.last_params = None

        def execute(self, sql, params=None):
            self.last_sql = sql
            self.last_params = params or []

        def fetchall(self):
            return mock_rows

        def fetchone(self):
            return mock_rows[0] if mock_rows else None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class MockConnection:
        def __init__(self):
            self._cursor = MockCursor()

        def cursor(self):
            return self._cursor

    return MockConnection()


def test_where_clause_returns_empty_when_no_filters():
    where, params = WhereClauseBuilder.from_product_filter(ProductFilter())
    assert where == ""
    assert params == []


def test_where_clause_with_name_filter():
    where, params = WhereClauseBuilder.from_product_filter(ProductFilter(name="apple"))
    assert "ILIKE" in where
    assert "%apple%" in params


def test_where_clause_with_category_filter():
    where, params = WhereClauseBuilder.from_product_filter(ProductFilter(category="Gia dụng"))
    assert "category = ?" in where
    assert "Gia dụng" in params


def test_where_clause_with_price_bounds():
    where, params = WhereClauseBuilder.from_product_filter(
        ProductFilter(price_min=100.0, price_max=500.0)
    )
    assert "price >= ?" in where
    assert "price <= ?" in where
    assert 100.0 in params
    assert 500.0 in params


def test_get_categories_returns_sorted_list():
    repo = ProductRepository()
    mock_rows = [("Gia dụng",), ("Thực phẩm",), ("Nông sản",)]
    conn = _make_connection_with_rows(mock_rows)

    result = repo.get_categories(conn)
    assert result == ["Gia dụng", "Thực phẩm", "Nông sản"]


def test_get_count_no_filter_executes_without_where():
    repo = ProductRepository()
    conn = _make_connection_with_rows([(100,)])

    result = repo.get_count(ProductFilter(), conn)
    assert result == 100
    assert "WHERE" not in conn.cursor().last_sql


def test_get_count_with_name_filter_uses_ilike():
    repo = ProductRepository()
    conn = _make_connection_with_rows([(5,)])

    repo.get_count(ProductFilter(name="apple"), conn)
    assert "ILIKE" in conn.cursor().last_sql


def test_get_page_uses_limit_and_correct_offset():
    repo = ProductRepository()
    conn = _make_connection_with_rows([])

    repo.get_page(ProductFilter(), 2, conn, page_size=10)
    cursor = conn.cursor()
    assert "LIMIT ? OFFSET ?" in cursor.last_sql
    assert cursor.last_params[-2:] == [10, 10]


def test_get_page_returns_list_of_product_rows():
    repo = ProductRepository()
    mock_rows = [(1, "Apple", "Gia dụng", 10.0, "kg", "desc", 100)]
    conn = _make_connection_with_rows(mock_rows)

    result = repo.get_page(ProductFilter(), 1, conn)
    assert isinstance(result, list)
    assert isinstance(result[0], ProductRow)
    assert result[0].product_name == "Apple"


def test_get_chart_data_groups_by_category():
    repo = ProductRepository()
    conn = _make_connection_with_rows([("Gia dụng", 500), ("Thực phẩm", 300)])

    result = repo.get_chart_data(ProductFilter(), conn)
    assert "GROUP BY category" in conn.cursor().last_sql
    assert isinstance(result, list)
    assert result[0]["category"] == "Gia dụng"


def test_repository_wraps_db_errors_as_data_access_error():
    repo = ProductRepository()

    class BrokenCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args, **kwargs):
            raise RuntimeError("db")

    class BrokenConn:
        def cursor(self):
            return BrokenCursor()

    with pytest.raises(DataAccessError):
        repo.get_categories(BrokenConn())
