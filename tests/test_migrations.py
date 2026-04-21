from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import mock_open

from src.core.exceptions import DataAccessError
from migrations import migrate


@contextmanager
def _missing_connector():
    raise DataAccessError("Unable to connect to Databricks.") from ModuleNotFoundError(
        "databricks-sql-connector is not installed."
    )
    yield  # pragma: no cover


def test_run_migrations_returns_false_when_connector_is_missing(mocker, caplog):
    mocker.patch("migrations.migrate.databricks_connection", _missing_connector)

    with caplog.at_level(logging.ERROR, logger="migrations"):
        result = migrate.run_migrations()

    assert result is False
    assert "databricks-sql-connector is not installed" in caplog.text


def test_pending_migrations_keeps_same_day_suffix_migrations_with_legacy_history():
    sql_files = [
        Path("20240410_001_master_products_schema.sql"),
        Path("20240410_002_auth_table_schema.sql"),
        Path("20240410_003_menu_table_schema.sql"),
        Path("20260414_003_fact_tables_schema.sql"),
    ]
    applied = {"20240410"}

    pending = migrate._pending_migrations(sql_files, applied)

    assert [file.name for file in pending] == [
        "20240410_002_auth_table_schema.sql",
        "20240410_003_menu_table_schema.sql",
        "20260414_003_fact_tables_schema.sql",
    ]


def test_apply_migration_records_full_version(mocker):
    migration_file = Path("20240410_003_menu_table_schema.sql")
    mocker.patch("builtins.open", mock_open(read_data="SELECT 1;"))

    executed: list[str] = []

    class _Cursor:
        def execute(self, statement: str) -> None:
            executed.append(statement.strip())

    migrate.apply_migration(_Cursor(), migration_file)

    assert any(
        "VALUES ('20240410_003'" in statement
        for statement in executed
    )
