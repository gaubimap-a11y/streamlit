from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DemoRequest:
    product_name: str = ""


@dataclass(frozen=True)
class DemoSummary:
    total_revenue: float
    total_sales: int
    row_count: int
    source: str
