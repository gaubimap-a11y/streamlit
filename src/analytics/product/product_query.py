from src.core.config import get_settings
from src.analytics.shared.sql_builder import WhereClauseBuilder
from src.domain.filters import ProductFilter


class ProductQuery:
    @staticmethod
    def _get_table_name() -> str:
        catalog = get_settings().databricks.catalog
        return f"{catalog}.master.products"

    @staticmethod
    def build_count(product_filter: ProductFilter | None = None) -> tuple[str, list[object]]:
        where_clause, params = WhereClauseBuilder.from_product_filter(product_filter)
        sql = f"SELECT COUNT(*) FROM {ProductQuery._get_table_name()} {where_clause}".strip()
        return sql, params

    @staticmethod
    def build_page(
        product_filter: ProductFilter | None,
        page: int,
        page_size: int = 10,
    ) -> tuple[str, list[object]]:
        safe_page = max(page, 1)
        offset = (safe_page - 1) * page_size
        where_clause, params = WhereClauseBuilder.from_product_filter(product_filter)
        sql = (
            "SELECT product_id, product_name, category, price, unit, description, stock_quantity "
            f"FROM {ProductQuery._get_table_name()} "
            f"{where_clause} "
            "ORDER BY product_id "
            "LIMIT ? OFFSET ?"
        ).strip()
        return sql, [*params, page_size, offset]

    @staticmethod
    def build_chart(product_filter: ProductFilter | None = None) -> tuple[str, list[object]]:
        where_clause, params = WhereClauseBuilder.from_product_filter(product_filter)
        sql = (
            "SELECT category, SUM(stock_quantity) AS total_stock "
            f"FROM {ProductQuery._get_table_name()} "
            f"{where_clause} "
            "GROUP BY category "
            "ORDER BY category"
        ).strip()
        return sql, params
