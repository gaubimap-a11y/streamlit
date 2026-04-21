from __future__ import annotations

from pydantic import BaseModel, field_validator

# Giá trị đặc biệt biểu thị "không chọn trục phụ"
AXIS_NONE = "(Không chọn)なし"

# Các dimension có thể chọn làm trục
AXIS_DIMENSIONS: list[str] = [
    "store_name",
    "product_name",
    "classification",
    "period_id",
    "evaluation_item",
]

# Nhãn hiển thị (tiếng Nhật / tiếng Việt) cho từng dimension
AXIS_DIMENSION_LABELS: dict[str, str] = {
    "store_name":      "Cửa hàng (店舗)",
    "product_name":    "Sản phẩm (商品)",
    "classification":  "Phân loại (分類)",
    "period_id":       "Kỳ (期間)",
    "evaluation_item": "Mục đánh giá (評価項目)",
}

# Tên các KPI được hỗ trợ
KPI_NAMES: list[str] = [
    "供給数量",
    "供給金額",
    "平均売単価",
    "数量PI",
    "金額PI",
]

# Các cột dimension thực sự trong DB (không kể evaluation_item)
DB_DIMENSION_COLUMNS: list[str] = [
    "store_name",
    "product_name",
    "classification",
    "period_id",
]

_ALL_SENTINELS = {
    "tất cả",
    "tat ca",
    "all",
    "(all)",
}


class SupplyReportFilter(BaseModel):
    """Filter và cấu hình trục cho report cung ứng sản phẩm đơn lẻ."""

    # ── Bộ lọc ────────────────────────────────────────────────────────────────
    product_name: str | None = None
    store_name: str | None = None
    period_id: str | None = None
    classification: str | None = None
    evaluation_items: list[str] = KPI_NAMES.copy()

    # ── Cấu hình trục ─────────────────────────────────────────────────────────
    row_axis_1: str = "store_name"
    row_axis_2: str = "product_name"
    col_axis_1: str = "evaluation_item"
    col_axis_2: str = "period_id"

    @field_validator("product_name", "store_name", "period_id", "classification", mode="before")
    @classmethod
    def _strip_blank(cls, v: object) -> object:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
            if v.casefold() in _ALL_SENTINELS:
                return None
            return v
        return v

