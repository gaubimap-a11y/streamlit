from __future__ import annotations

from pydantic import BaseModel, field_validator, model_validator


class ProductFilter(BaseModel):
    name: str | None = None
    category: str | None = None
    price_min: float | None = None
    price_max: float | None = None

    @field_validator("name", "category", mode="before")
    @classmethod
    def _strip_blank_strings(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @model_validator(mode="after")
    def _validate_price_range(self) -> "ProductFilter":
        if (
            self.price_min is not None
            and self.price_max is not None
            and self.price_min > self.price_max
        ):
            raise ValueError("price_min must be less than or equal to price_max.")
        return self
