from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

try:
    import databricks.sql as _databricks_sql
except ModuleNotFoundError:  # pragma: no cover - exercised when connector is absent
    def _missing_connect(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise ModuleNotFoundError("databricks-sql-connector is not installed.")

    databricks = SimpleNamespace(sql=SimpleNamespace(connect=_missing_connect))
else:
    databricks = SimpleNamespace(sql=_databricks_sql)

from src.core.exceptions import DataAccessError
from src.infrastructure.databricks.config_reader import read_databricks_config


@contextmanager
def databricks_connection():
    config = read_databricks_config()
    connection = None
    try:
        connection = databricks.sql.connect(
            server_hostname=config.server_hostname,
            http_path=config.http_path,
            access_token=config.access_token,
            _socket_timeout=config.socket_timeout_seconds,
            _retry_stop_after_attempts_count=config.retry_stop_after_attempts_count,
            _retry_stop_after_attempts_duration=config.retry_stop_after_attempts_duration_seconds,
        )
    except Exception as exc:
        raise DataAccessError("Unable to connect to Databricks.") from exc

    try:
        yield connection
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
