from __future__ import annotations

from src.domain.filters import ProductFilter


class WhereClauseBuilder:
    @staticmethod
    def from_product_filter(product_filter: ProductFilter | None) -> tuple[str, list[object]]:
        if product_filter is None:
            return "", []

        conditions: list[str] = []
        params: list[object] = []

        if product_filter.name:
            conditions.append("product_name ILIKE ?")
            params.append(f"%{product_filter.name}%")
        if product_filter.category:
            conditions.append("category = ?")
            params.append(product_filter.category)
        if product_filter.price_min is not None:
            conditions.append("price >= ?")
            params.append(product_filter.price_min)
        if product_filter.price_max is not None:
            conditions.append("price <= ?")
            params.append(product_filter.price_max)

        if not conditions:
            return "", []
        return "WHERE " + " AND ".join(conditions), params
