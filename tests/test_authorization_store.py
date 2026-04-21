from __future__ import annotations

from src.infrastructure.repositories.authorization_store import DatabricksAuthorizationStore
from src.infrastructure.repositories.sql_warehouse_source import DatabricksConfig


def _store() -> DatabricksAuthorizationStore:
    return DatabricksAuthorizationStore(
        config=DatabricksConfig(
            host="https://example.databricks.com",
            token="token",
            warehouse_id="wh-123",
        )
    )


def test_has_principal_mapping_reads_tmn_kobe_auth_users(mocker):
    response = {
        "statement_id": "stmt-1",
        "status": {"state": "SUCCEEDED"},
        "manifest": {"schema": {"columns": [{"name": "mapped"}]}},
        "result": {"data_array": [[1]]},
    }
    request = mocker.patch(
        "src.infrastructure.repositories.authorization_store._databricks_api_request",
        return_value=response,
    )

    result = _store().has_principal_mapping(
        principal_id="U001",
        username="admin",
        email="admin@tmnkobe.com",
        auth_source="sso",
    )

    assert result is True
    statement = request.call_args.kwargs["payload"]["statement"]
    assert "FROM tmn_kobe.auth.users u" in statement
    assert "LOWER(COALESCE(u.auth_source, 'internal')) = LOWER(:auth_source)" in statement


def test_resolve_permissions_returns_permission_names_from_tmn_kobe_auth(mocker):
    response = {
        "statement_id": "stmt-2",
        "status": {"state": "SUCCEEDED"},
        "manifest": {"schema": {"columns": [{"name": "permission_name"}]}},
        "result": {"data_array": [["app_access"], ["view_dashboard"], ["run_report"]]},
    }
    request = mocker.patch(
        "src.infrastructure.repositories.authorization_store._databricks_api_request",
        return_value=response,
    )

    permissions = _store().resolve_permissions(
        principal_id="U001",
        username="admin",
        email="admin@tmnkobe.com",
        auth_source="internal",
    )

    assert permissions == ("app_access", "view_dashboard", "run_report")
    statement = request.call_args.kwargs["payload"]["statement"]
    assert "FROM tmn_kobe.auth.users u" in statement
    assert "INNER JOIN tmn_kobe.auth.user_roles user_role" in statement
    assert "INNER JOIN tmn_kobe.auth.permissions perm" in statement

