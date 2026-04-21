from __future__ import annotations

from datetime import datetime, timezone

from werkzeug.security import check_password_hash, generate_password_hash

from src.infrastructure.repositories.auth_user_store import DatabricksAuthUserStore
from src.infrastructure.repositories.sql_warehouse_source import DatabricksConfig


def _store() -> DatabricksAuthUserStore:
    return DatabricksAuthUserStore(
        config=DatabricksConfig(
            host="https://example.databricks.com",
            token="token",
            warehouse_id="wh-123",
        )
    )


def test_get_user_by_username_reads_tmn_kobe_auth_users(mocker):
    password_hash = generate_password_hash("password123")
    response = {
        "statement_id": "stmt-1",
        "status": {"state": "SUCCEEDED"},
        "manifest": {
            "schema": {
                "columns": [
                    {"name": "user_id"},
                    {"name": "username"},
                    {"name": "email"},
                    {"name": "password_hash"},
                    {"name": "created_at"},
                    {"name": "last_login_at"},
                    {"name": "is_active"},
                    {"name": "display_name"},
                ]
            }
        },
        "result": {
            "data_array": [
                [
                    "U001",
                    "admin",
                    "admin@tmnkobe.com",
                    password_hash,
                    "2024-04-10 00:00:00",
                    None,
                    True,
                    "Admin User",
                ]
            ]
        },
    }
    request = mocker.patch(
        "src.infrastructure.repositories.auth_user_store._databricks_api_request",
        return_value=response,
    )

    user = _store().get_user_by_username("admin")

    assert user is not None
    assert user.user_id == "U001"
    assert user.username == "admin"
    assert user.email == "admin@tmnkobe.com"
    assert user.password_hash == password_hash
    assert user.display_name == "Admin User"
    statement = request.call_args.kwargs["payload"]["statement"]
    assert "tmn_kobe.auth.users" in statement
    assert "COALESCE(display_name, username) AS display_name" in statement
    assert "LOWER(COALESCE(auth_source, 'internal')) = 'internal'" in statement
    assert "WHERE LOWER(username) = LOWER(:username)" in statement


def test_update_last_login_targets_principal_id(mocker):
    response = {"statement_id": "stmt-2", "status": {"state": "SUCCEEDED"}}
    request = mocker.patch(
        "src.infrastructure.repositories.auth_user_store._databricks_api_request",
        return_value=response,
    )

    _store().update_last_login("U001", datetime(2026, 4, 10, 13, 30, tzinfo=timezone.utc))

    statement = request.call_args.kwargs["payload"]["statement"]
    assert "UPDATE tmn_kobe.auth.users" in statement
    assert "WHERE user_id = :user_id" in statement


def test_verify_password_and_hash_password_use_werkzeug():
    store = _store()
    password_hash = store.hash_password("password123")

    assert check_password_hash(password_hash, "password123")
    assert store.verify_password("password123", password_hash) is True
    assert store.verify_password("wrong-password", password_hash) is False
