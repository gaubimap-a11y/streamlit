import pytest

from src.core.exceptions import DataAccessError
from src.domain.user import UserRow
from src.infrastructure.repositories.user_repository import UserRepository


def _make_connection(mock_row):
    class MockCursor:
        def __init__(self):
            self.last_sql = None
            self.last_params = None

        def execute(self, sql, params):
            self.last_sql = sql
            self.last_params = params

        def fetchone(self):
            return mock_row

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


def test_find_by_username_returns_user_row_when_found():
    repo = UserRepository()
    conn = _make_connection(("user-001", "admin", "admin@example.com", "somehash", True))
    result = repo.find_by_username("admin", conn)
    assert isinstance(result, UserRow)
    assert result.username == "admin"
    assert result.is_active is True


def test_find_by_username_returns_none_when_not_found():
    repo = UserRepository()
    conn = _make_connection(None)
    assert repo.find_by_username("ghost", conn) is None


def test_find_by_username_uses_parameterized_query():
    repo = UserRepository()
    conn = _make_connection(None)
    repo.find_by_username("admin", conn)
    cursor = conn.cursor()
    assert "?" in cursor.last_sql
    assert cursor.last_params == ["admin"]


def test_find_by_username_wraps_db_error_as_data_access_error():
    repo = UserRepository()

    class BrokenCursor:
        def execute(self, sql, params):
            raise RuntimeError("DB connection lost")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class BrokenConnection:
        def cursor(self):
            return BrokenCursor()

    with pytest.raises(DataAccessError):
        repo.find_by_username("admin", BrokenConnection())
