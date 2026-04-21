from __future__ import annotations

from typing import Any

from src.core.config import get_settings
from src.analytics.product.product_query import ProductQuery
from src.core.exceptions import DataAccessError
from src.domain.filters import ProductFilter
from src.domain.product import ProductRow


class ProductRepository:
    @staticmethod
    def _map_product_rows(rows: list[tuple]) -> list[ProductRow]:
        return [
            ProductRow(
                product_id=row[0],
                product_name=row[1],
                category=row[2],
                price=row[3],
                unit=row[4],
                description=row[5],
                stock_quantity=row[6],
            )
            for row in rows
        ]

    def _get_categories_sql(self) -> str:
        catalog = get_settings().databricks.catalog
        return (
            f"SELECT DISTINCT category "
            f"FROM {catalog}.master.products "
            "ORDER BY category"
        )

    def get_categories(self, conn) -> list[str]:
        try:
            with conn.cursor() as cursor:
                cursor.execute(self._get_categories_sql())
                rows = cursor.fetchall()
        except Exception as exc:
            raise DataAccessError("Failed to fetch product categories.") from exc
        return [row[0] for row in rows]

    def get_count(self, product_filter: ProductFilter, conn) -> int:
        sql, params = ProductQuery.build_count(product_filter)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
        except Exception as exc:
            raise DataAccessError("Failed to fetch product count.") from exc
        return int(row[0]) if row else 0

    def get_page(
        self,
        product_filter: ProductFilter,
        page: int,
        conn,
        page_size: int = 10,
    ) -> list[ProductRow]:
        sql, params = ProductQuery.build_page(product_filter, page, page_size=page_size)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        except Exception as exc:
            raise DataAccessError("Failed to fetch product page.") from exc
        return self._map_product_rows(rows)

    def get_by_ids(self, product_ids: list[int], conn) -> list[ProductRow]:
        normalized_ids = sorted({int(product_id) for product_id in product_ids})
        if not normalized_ids:
            return []

        placeholders = ", ".join("?" for _ in normalized_ids)
        catalog = get_settings().databricks.catalog
        sql = (
            "SELECT product_id, product_name, category, price, unit, description, stock_quantity "
            f"FROM {catalog}.master.products "
            f"WHERE product_id IN ({placeholders}) "
            "ORDER BY product_id"
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, normalized_ids)
                rows = cursor.fetchall()
        except Exception as exc:
            raise DataAccessError("Failed to fetch products by IDs.") from exc
        return self._map_product_rows(rows)

    def get_chart_data(self, product_filter: ProductFilter, conn) -> list[dict[str, Any]]:
        sql, params = ProductQuery.build_chart(product_filter)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        except Exception as exc:
            raise DataAccessError("Failed to fetch chart data.") from exc
        return [{"category": row[0], "total_stock": row[1]} for row in rows]
