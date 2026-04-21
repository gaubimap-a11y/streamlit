from __future__ import annotations

from src.core.exceptions import DataAccessError
from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.summary_axis_repository import SummaryAxisRepository


class SummaryAxisService:
    def __init__(self, repository: SummaryAxisRepository | None = None) -> None:
        self._repository = repository or SummaryAxisRepository()

    def get_store_hierarchy_rows(self) -> list[tuple]:
        try:
            with databricks_connection() as conn:
                return self._repository.get_store_hierarchy_rows(conn)
        except DataAccessError:
            raise
        except Exception as exc:
            raise DataAccessError("Failed to fetch summary axis hierarchy data.") from exc
