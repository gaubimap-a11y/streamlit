from __future__ import annotations

import logging
from decimal import Decimal

from src.analytics.reporting.supply_report_query import SupplyReportQuery
from src.core.exceptions import DataAccessError
from src.domain.supply_report_filter import SupplyReportFilter
from src.domain.supply_report_row import SupplyReportAggRow

logger = logging.getLogger(__name__)


class SupplyReportRepository:
    """Data access cho report cung ứng sản phẩm đơn lẻ.

    Tách rõ hai trách nhiệm:
    - load_filter_metadata : lấy danh sách options cho dropdown UI
    - load_dataset         : lấy dataset đã aggregate để tính KPI
    """

    _TABLE_CANDIDATES: tuple[tuple[str, str], ...] = (
        ("fact.daily_sales", "fact.daily_customer_count"),
        ("fact.fact_daily_sales", "fact.fact_daily_customer_count"),
    )

    def load_filter_metadata(self, conn) -> dict[str, list[str]]:
        """Trả về distinct values cho từng filter dimension.

        Returns:
            {
                "stores": [...],
                "products": [...],
                "classifications": [...],
                "periods": [...],
            }
        """
        rows = self._execute_first_success_filter_options(conn)

        stores: set[str] = set()
        products: set[str] = set()
        classifications: set[str] = set()
        periods: set[str] = set()

        for row in rows:
            if row[0]:
                stores.add(str(row[0]))
            if row[1]:
                products.add(str(row[1]))
            if row[2]:
                classifications.add(str(row[2]))
            if row[3]:
                periods.add(str(row[3]))

        return {
            "stores": sorted(stores),
            "products": sorted(products),
            "classifications": sorted(classifications),
            "periods": sorted(periods),
        }

    def load_dataset(
        self,
        supply_filter: SupplyReportFilter,
        conn,
    ) -> list[SupplyReportAggRow]:
        """Trả về dataset đã aggregate theo store × product × classification × period."""
        rows = self._execute_best_effort_dataset(conn, supply_filter)

        return [
            SupplyReportAggRow(
                store_id=str(row[0]),
                store_name=str(row[1]),
                product_name=str(row[2]),
                classification=str(row[3]),
                period_id=str(row[4]),
                quantity_sold=int(row[5]) if row[5] is not None else 0,
                sales_amount=Decimal(str(row[6])) if row[6] is not None else Decimal("0"),
                customer_count=int(row[7]) if row[7] is not None else 0,
            )
            for row in rows
        ]

    def _execute_first_success_filter_options(self, conn):
        last_exc: Exception | None = None
        for sales_table, _ in self._TABLE_CANDIDATES:
            sql, params = SupplyReportQuery.build_filter_options(
                sales_table_ref=sales_table
            )
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    return cursor.fetchall()
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Filter options query that bai voi sales_table=%s, thu candidate ke tiep.",
                    sales_table,
                    exc_info=True,
                )
                continue
        logger.error("Tat ca candidate table deu fail khi load filter options.")
        raise DataAccessError("Không lấy được danh sách options cho filter.") from last_exc

    def _execute_best_effort_dataset(self, conn, supply_filter: SupplyReportFilter):
        """Thử nhiều convention tên bảng để tránh lệch nguồn dữ liệu giữa các môi trường."""
        best_rows = []
        best_unique_products = -1
        last_exc: Exception | None = None

        for sales_table, customer_table in self._TABLE_CANDIDATES:
            sql, params = SupplyReportQuery.build_dataset(
                supply_filter,
                sales_table_ref=sales_table,
                customer_table_ref=customer_table,
            )
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Dataset query that bai voi sales=%s, customer=%s, thu candidate ke tiep.",
                    sales_table,
                    customer_table,
                    exc_info=True,
                )
                continue

            if supply_filter.product_name is not None:
                return rows

            unique_products = len({str(row[2]) for row in rows if row[2] is not None})
            if unique_products > best_unique_products:
                best_unique_products = unique_products
                best_rows = rows

            if unique_products > 1:
                return rows

        if best_rows:
            return best_rows
        logger.error("Tat ca candidate table deu fail khi load dataset supply_report.")
        raise DataAccessError("Không lấy được dữ liệu báo cáo cung ứng.") from last_exc
