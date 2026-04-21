import pytest

from src.core.config import DatabricksConfig
from src.core.exceptions import DataAccessError
from src.infrastructure.databricks.client import databricks_connection


def test_databricks_connection_calls_connect_and_closes(mocker):
    cfg = DatabricksConfig(
        server_hostname="host.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
        access_token="dapi123",
        socket_timeout_seconds=10,
        retry_stop_after_attempts_count=1,
        retry_stop_after_attempts_duration_seconds=10,
    )
    mocker.patch("src.infrastructure.databricks.client.read_databricks_config", return_value=cfg)

    mock_conn = mocker.MagicMock()
    mock_connect = mocker.patch(
        "src.infrastructure.databricks.client.databricks.sql.connect",
        return_value=mock_conn,
    )

    with databricks_connection() as conn:
        assert conn is mock_conn

    mock_connect.assert_called_once_with(
        server_hostname="host.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
        access_token="dapi123",
        _socket_timeout=10,
        _retry_stop_after_attempts_count=1,
        _retry_stop_after_attempts_duration=10,
    )
    mock_conn.close.assert_called_once()


def test_databricks_connection_raises_data_access_error_on_connect_failure(mocker):
    cfg = DatabricksConfig(
        server_hostname="h",
        http_path="p",
        access_token="t",
        socket_timeout_seconds=10,
        retry_stop_after_attempts_count=1,
        retry_stop_after_attempts_duration_seconds=10,
    )
    mocker.patch("src.infrastructure.databricks.client.read_databricks_config", return_value=cfg)
    mocker.patch(
        "src.infrastructure.databricks.client.databricks.sql.connect",
        side_effect=Exception("boom"),
    )

    with pytest.raises(DataAccessError):
        with databricks_connection():
            pass
