from __future__ import annotations

from collections.abc import Sequence

from src.core.exceptions import DataAccessError
from src.core.reporting import ReportData
from src.domain.filters import ProductFilter
from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.product_repository import ProductRepository


class ProductService:
    def __init__(self, product_repository: ProductRepository | None = None) -> None:
        self._product_repository = product_repository or ProductRepository()

    def get_categories(self) -> list[str]:
        with databricks_connection() as conn:
            return self._product_repository.get_categories(conn)

    def search_name_options(self, keyword: str | None, limit: int = 200) -> list[tuple[int, str]]:
        try:
            with databricks_connection() as conn:
                return self._product_repository.search_name_options(keyword=keyword, conn=conn, limit=limit)
        except DataAccessError:
            raise
        except Exception as exc:
            raise DataAccessError("Failed to search products by name.") from exc

    def get_products(self, product_filter: ProductFilter, page: int) -> ReportData:
        try:
            with databricks_connection() as conn:
                total = self._product_repository.get_count(product_filter, conn)
                rows = self._product_repository.get_page(product_filter, page, conn)
        except DataAccessError:
            raise
        except Exception as exc:
            raise DataAccessError("Failed to fetch product report.") from exc
        return ReportData(total=total, rows=rows)

    def get_total_count(self, product_filter: ProductFilter) -> int:
        try:
            with databricks_connection() as conn:
                return self._product_repository.get_count(product_filter, conn)
        except DataAccessError:
            raise
        except Exception as exc:
            raise DataAccessError("Failed to fetch product count.") from exc

    def get_report_meta(self, product_filter: ProductFilter) -> tuple[int, list[dict]]:
        try:
            with databricks_connection() as conn:
                total = self._product_repository.get_count(product_filter, conn)
                chart_data = self._product_repository.get_chart_data(product_filter, conn)
        except DataAccessError:
            raise
        except Exception as exc:
            raise DataAccessError("Failed to fetch product report metadata.") from exc
        return total, chart_data

    def get_product_page(self, product_filter: ProductFilter, page: int):
        try:
            with databricks_connection() as conn:
                return self._product_repository.get_page(product_filter, page, conn)
        except DataAccessError:
            raise
        except Exception as exc:
            raise DataAccessError("Failed to fetch product page.") from exc

    def get_product_pages(
        self,
        product_filter: ProductFilter,
        pages: Sequence[int],
    ) -> dict[int, list]:
        normalized_pages = sorted({max(1, int(page)) for page in pages})
        if not normalized_pages:
            return {}
        try:
            with databricks_connection() as conn:
                return {
                    page: self._product_repository.get_page(product_filter, page, conn)
                    for page in normalized_pages
                }
        except DataAccessError:
            raise
        except Exception as exc:
            raise DataAccessError("Failed to fetch product pages.") from exc

    def get_chart_data(self, product_filter: ProductFilter) -> list[dict]:
        try:
            with databricks_connection() as conn:
                return self._product_repository.get_chart_data(product_filter, conn)
        except DataAccessError:
            raise
        except Exception as exc:
            raise DataAccessError("Failed to fetch product chart data.") from exc

    def get_products_by_ids(self, product_ids: Sequence[int]) -> list:
        normalized_ids = sorted({int(product_id) for product_id in product_ids})
        if not normalized_ids:
            return []
        try:
            with databricks_connection() as conn:
                return self._product_repository.get_by_ids(normalized_ids, conn)
        except DataAccessError:
            raise
        except Exception as exc:
            raise DataAccessError("Failed to fetch products by IDs.") from exc
