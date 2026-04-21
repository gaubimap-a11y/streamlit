from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel


class ProductRow(BaseModel):
    product_id: int
    product_name: str
    category: str
    price: Decimal
    unit: str
    description: str
    stock_quantity: int

    model_config = {"frozen": True}
