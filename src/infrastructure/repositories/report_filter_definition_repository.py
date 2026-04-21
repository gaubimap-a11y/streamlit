from __future__ import annotations

import json
from datetime import datetime, timezone

from src.core.config import get_settings
from src.core.exceptions import DataAccessError
from src.domain.report_filters import ReportFilterDefinition


class ReportFilterDefinitionRepository:
    def _table_name(self) -> str:
        catalog = get_settings().databricks.catalog
        # Saved report filters live in the shared config schema alongside shares.
        return f"{catalog}.config.report_filter_definitions"

    def list_my_filters(self, owner_user_id: str, report_code: str, conn) -> list[ReportFilterDefinition]:
        sql = (
            "SELECT filter_definition_id, report_code, owner_user_id, filter_name, "
            "filter_payload_json, is_active, created_at, updated_at "
            f"FROM {self._table_name()} "
            "WHERE owner_user_id = ? AND report_code = ? AND is_active = TRUE "
            "ORDER BY updated_at DESC"
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, [owner_user_id, report_code])
                rows = cursor.fetchall()
        except Exception as exc:
            raise DataAccessError("Failed to list owner filters.") from exc
        return [self._to_definition(row) for row in rows]

    def find_by_id(self, filter_definition_id: str, conn) -> ReportFilterDefinition | None:
        sql = (
            "SELECT filter_definition_id, report_code, owner_user_id, filter_name, "
            "filter_payload_json, is_active, created_at, updated_at "
            f"FROM {self._table_name()} "
            "WHERE filter_definition_id = ?"
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, [filter_definition_id])
                row = cursor.fetchone()
        except Exception as exc:
            raise DataAccessError("Failed to load filter definition by id.") from exc
        if not row:
            return None
        return self._to_definition(row)

    def find_by_name(
        self,
        owner_user_id: str,
        report_code: str,
        filter_name: str,
        conn,
    ) -> ReportFilterDefinition | None:
        sql = (
            "SELECT filter_definition_id, report_code, owner_user_id, filter_name, "
            "filter_payload_json, is_active, created_at, updated_at "
            f"FROM {self._table_name()} "
            "WHERE owner_user_id = ? AND report_code = ? AND LOWER(filter_name) = LOWER(?) "
            "AND is_active = TRUE"
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, [owner_user_id, report_code, filter_name])
                row = cursor.fetchone()
        except Exception as exc:
            raise DataAccessError("Failed to load filter definition by name.") from exc
        if not row:
            return None
        return self._to_definition(row)

    def create(
        self,
        filter_definition_id: str,
        report_code: str,
        owner_user_id: str,
        filter_name: str,
        filter_payload: dict,
        actor_user_id: str,
        conn,
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        sql = (
            f"INSERT INTO {self._table_name()} "
            "(filter_definition_id, report_code, owner_user_id, filter_name, "
            "filter_payload_json, is_active, created_at, updated_at, created_by, updated_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = [
            filter_definition_id,
            report_code,
            owner_user_id,
            filter_name,
            json.dumps(filter_payload, ensure_ascii=False, sort_keys=True),
            True,
            now,
            now,
            actor_user_id,
            actor_user_id,
        ]
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
        except Exception as exc:
            raise DataAccessError("Failed to create filter definition.") from exc

    def update(
        self,
        filter_definition_id: str,
        filter_name: str,
        filter_payload: dict,
        actor_user_id: str,
        conn,
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        sql = (
            f"UPDATE {self._table_name()} "
            "SET filter_name = ?, filter_payload_json = ?, updated_at = ?, updated_by = ? "
            "WHERE filter_definition_id = ? AND is_active = TRUE"
        )
        params = [
            filter_name,
            json.dumps(filter_payload, ensure_ascii=False, sort_keys=True),
            now,
            actor_user_id,
            filter_definition_id,
        ]
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
        except Exception as exc:
            raise DataAccessError("Failed to update filter definition.") from exc

    def deactivate(self, filter_definition_id: str, actor_user_id: str, conn) -> None:
        now = datetime.now(tz=timezone.utc)
        sql = (
            f"UPDATE {self._table_name()} "
            "SET is_active = FALSE, updated_at = ?, updated_by = ? "
            "WHERE filter_definition_id = ? AND is_active = TRUE"
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, [now, actor_user_id, filter_definition_id])
        except Exception as exc:
            raise DataAccessError("Failed to deactivate filter definition.") from exc

    @staticmethod
    def _to_definition(row) -> ReportFilterDefinition:
        payload_raw = row[4]
        try:
            payload = json.loads(payload_raw) if payload_raw else {}
        except (TypeError, json.JSONDecodeError):
            payload = {}
        return ReportFilterDefinition(
            filter_definition_id=str(row[0]),
            report_code=str(row[1]),
            owner_user_id=str(row[2]),
            filter_name=str(row[3]),
            filter_payload=payload,
            is_active=bool(row[5]),
            created_at=row[6],
            updated_at=row[7],
        )
