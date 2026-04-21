from __future__ import annotations

from datetime import datetime, timezone

from src.core.config import get_settings
from src.core.exceptions import DataAccessError
from src.domain.report_filters import RecipientStatus, ShareRecipient, SharedReportFilter


class ReportFilterShareRepository:
    def _shares_table(self) -> str:
        catalog = get_settings().databricks.catalog
        return f"{catalog}.config.report_filter_shares"

    def _definitions_table(self) -> str:
        catalog = get_settings().databricks.catalog
        return f"{catalog}.config.report_filter_definitions"

    def list_shared_with_me(
        self,
        recipient_user_id: str,
        report_code: str,
        conn,
    ) -> list[SharedReportFilter]:
        sql = (
            "SELECT d.filter_definition_id, d.report_code, d.filter_name, d.filter_payload_json, "
            "d.owner_user_id, s.recipient_user_id, s.recipient_status, d.updated_at "
            f"FROM {self._definitions_table()} d "
            f"JOIN {self._shares_table()} s ON d.filter_definition_id = s.filter_definition_id "
            "WHERE s.recipient_user_id = ? AND d.report_code = ? "
            "AND d.is_active = TRUE AND s.revoked_at IS NULL "
            "ORDER BY d.updated_at DESC"
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, [recipient_user_id, report_code])
                rows = cursor.fetchall()
        except Exception as exc:
            raise DataAccessError("Failed to list filters shared with recipient.") from exc
        items: list[SharedReportFilter] = []
        for row in rows:
            payload_json = row[3]
            try:
                import json

                payload = json.loads(payload_json) if payload_json else {}
            except (TypeError, json.JSONDecodeError):
                payload = {}
            items.append(
                SharedReportFilter(
                    filter_definition_id=str(row[0]),
                    report_code=str(row[1]),
                    filter_name=str(row[2]),
                    filter_payload=payload,
                    owner_user_id=str(row[4]),
                    recipient_user_id=str(row[5]),
                    recipient_status=RecipientStatus(str(row[6] or RecipientStatus.ACTIVE.value)),
                    updated_at=row[7],
                )
            )
        return items

    def list_filter_recipients(self, filter_definition_id: str, conn) -> list[ShareRecipient]:
        sql = (
            "SELECT recipient_user_id, recipient_status, revoked_at "
            f"FROM {self._shares_table()} "
            "WHERE filter_definition_id = ? "
            "ORDER BY recipient_user_id"
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, [filter_definition_id])
                rows = cursor.fetchall()
        except Exception as exc:
            raise DataAccessError("Failed to list share recipients.") from exc
        return [
            ShareRecipient(
                recipient_user_id=str(row[0]),
                recipient_status=RecipientStatus(str(row[1] or RecipientStatus.ACTIVE.value)),
                revoked_at=row[2],
            )
            for row in rows
        ]

    def has_active_share(self, filter_definition_id: str, recipient_user_id: str, conn) -> bool:
        sql = (
            "SELECT 1 "
            f"FROM {self._shares_table()} "
            "WHERE filter_definition_id = ? AND recipient_user_id = ? AND revoked_at IS NULL "
            "LIMIT 1"
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, [filter_definition_id, recipient_user_id])
                row = cursor.fetchone()
        except Exception as exc:
            raise DataAccessError("Failed to verify active share.") from exc
        return bool(row)

    def share_with_recipients(
        self,
        filter_definition_id: str,
        recipient_user_ids: list[str],
        actor_user_id: str,
        invalid_recipient_user_ids: set[str],
        conn,
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        upsert_sql = (
            f"MERGE INTO {self._shares_table()} AS target "
            "USING (SELECT ? AS filter_definition_id, ? AS recipient_user_id, ? AS recipient_status, "
            "? AS now_ts, ? AS actor_user_id) AS source "
            "ON target.filter_definition_id = source.filter_definition_id "
            "AND target.recipient_user_id = source.recipient_user_id "
            "WHEN MATCHED THEN UPDATE SET "
            "target.recipient_status = source.recipient_status, "
            "target.revoked_at = NULL, "
            "target.updated_by = source.actor_user_id "
            "WHEN NOT MATCHED THEN INSERT "
            "(filter_share_id, filter_definition_id, recipient_user_id, recipient_status, shared_at, revoked_at, created_by, updated_by) "
            "VALUES (uuid(), source.filter_definition_id, source.recipient_user_id, source.recipient_status, source.now_ts, NULL, source.actor_user_id, source.actor_user_id)"
        )
        try:
            with conn.cursor() as cursor:
                for recipient_user_id in recipient_user_ids:
                    status = (
                        RecipientStatus.INVALID.value
                        if recipient_user_id in invalid_recipient_user_ids
                        else RecipientStatus.ACTIVE.value
                    )
                    cursor.execute(
                        upsert_sql,
                        [filter_definition_id, recipient_user_id, status, now, actor_user_id],
                    )
        except Exception as exc:
            raise DataAccessError("Failed to share filter with recipients.") from exc

    def revoke_share(
        self,
        filter_definition_id: str,
        recipient_user_id: str,
        actor_user_id: str,
        conn,
    ) -> None:
        now = datetime.now(tz=timezone.utc)
        sql = (
            f"UPDATE {self._shares_table()} "
            "SET revoked_at = ?, updated_by = ? "
            "WHERE filter_definition_id = ? AND recipient_user_id = ? AND revoked_at IS NULL"
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, [now, actor_user_id, filter_definition_id, recipient_user_id])
        except Exception as exc:
            raise DataAccessError("Failed to revoke share recipient.") from exc
