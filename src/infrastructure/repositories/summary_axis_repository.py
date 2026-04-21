from __future__ import annotations

from src.core.config import get_settings
from src.core.exceptions import DataAccessError


class SummaryAxisRepository:
    def _get_store_hierarchy_sql(self) -> str:
        catalog = get_settings().databricks.catalog
        return (
            "SELECT "
            "s.store_id, s.store_name, "
            "a.area_id, a.area_name, "
            "c.coop_id, c.coop_name, "
            "b.biz_model_id, b.biz_model_name "
            f"FROM {catalog}.master.m_store AS s "
            f"LEFT JOIN {catalog}.master.m_area AS a ON a.area_id = s.area_id "
            f"LEFT JOIN {catalog}.master.m_coop AS c ON c.coop_id = a.coop_id "
            f"LEFT JOIN {catalog}.master.m_biz_model AS b ON b.biz_model_id = s.biz_model_id "
            "ORDER BY c.coop_name, a.area_name, b.biz_model_name, s.store_name"
        )

    def get_store_hierarchy_rows(self, conn) -> list[tuple]:
        try:
            with conn.cursor() as cursor:
                cursor.execute(self._get_store_hierarchy_sql())
                return cursor.fetchall()
        except Exception as exc:
            raise DataAccessError("Failed to fetch summary axis store hierarchy.") from exc
