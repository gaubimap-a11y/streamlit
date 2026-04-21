"""Integration tests for UserRepository — require real Databricks connection.

Run:
    pytest webapp/tests/test_user_repository_integration.py -m integration -v

Skip in offline / CI without DB:
    pytest -m "not integration"
"""

import pytest

from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.user_repository import UserRepository


@pytest.fixture(scope="module")
def db_connection():
    with databricks_connection() as conn:
        yield conn


@pytest.mark.integration
def test_find_by_username_returns_user_from_real_db(db_connection):
    repo = UserRepository()
    result = repo.find_by_username("admin", db_connection)
    assert result is not None
    assert result.username == "admin"


@pytest.mark.integration
def test_find_by_username_returns_none_for_nonexistent_user(db_connection):
    repo = UserRepository()
    result = repo.find_by_username("nonexistent_user_kobe_test", db_connection)
    assert result is None


@pytest.mark.integration
def test_list_users_returns_rows(db_connection):
    repo = UserRepository()
    result = repo.list_users(db_connection)
    assert len(result) >= 1
    assert all(row.username for row in result)
