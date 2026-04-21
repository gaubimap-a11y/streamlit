from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class SupplyReportAggRow:
    """Một dòng dữ liệu đã aggregate từ fact.daily_sales JOIN fact.daily_customer_count.

    Grain sau aggregate: store_name × product_name × classification × period_id.
    customer_count được tổng hợp ở grain ngày × cửa hàng (không có product_id),
    phù hợp với spec AC-8/AC-9.
    """

    store_id: str
    store_name: str
    product_name: str
    classification: str
    period_id: str
    quantity_sold: int
    sales_amount: Decimal
    customer_count: int
