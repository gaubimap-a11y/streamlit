from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


PRODUCT_REPORT_CODE = "product"
SUMMARY_REPORT_CODE = "summary_report"
_ALLOWED_PRODUCT_FILTER_FIELDS = {"name", "category", "price_min", "price_max"}


class RecipientStatus(str, Enum):
    ACTIVE = "active"
    INVALID = "invalid"


class FilterApplyResult(BaseModel):
    payload: dict[str, Any]
    ignored_fields: list[str] = Field(default_factory=list)


class ReportFilterDefinition(BaseModel):
    filter_definition_id: str
    report_code: str
    owner_user_id: str
    filter_name: str
    filter_payload: dict[str, Any]
    is_active: bool = True
    created_at: datetime
    updated_at: datetime

    @field_validator("filter_name", mode="before")
    @classmethod
    def _normalize_filter_name(cls, value: str) -> str:
        name = str(value or "").strip()
        if not name:
            raise ValueError("filter_name is required.")
        return name


class SharedReportFilter(BaseModel):
    filter_definition_id: str
    report_code: str
    filter_name: str
    filter_payload: dict[str, Any]
    owner_user_id: str
    owner_username: str | None = None
    recipient_user_id: str
    recipient_status: RecipientStatus = RecipientStatus.ACTIVE
    updated_at: datetime


class SavedFilterList(BaseModel):
    my_filters: list[ReportFilterDefinition] = Field(default_factory=list)
    shared_with_me: list[SharedReportFilter] = Field(default_factory=list)


class ShareRecipient(BaseModel):
    recipient_user_id: str
    recipient_status: RecipientStatus
    revoked_at: datetime | None = None


class SaveFilterCommand(BaseModel):
    report_code: str
    owner_user_id: str
    filter_name: str
    filter_payload: dict[str, Any]

    @field_validator("report_code", "owner_user_id", mode="before")
    @classmethod
    def _required_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Required text field is empty.")
        return text

    @field_validator("filter_name", mode="before")
    @classmethod
    def _filter_name_required(cls, value: str) -> str:
        name = str(value or "").strip()
        if not name:
            raise ValueError("Tên bộ lọc không được để trống.")
        return name

    @model_validator(mode="after")
    def _filter_name_length(self) -> "SaveFilterCommand":
        if len(self.filter_name) > 120:
            raise ValueError("Tên bộ lọc không được vượt quá 120 ký tự.")
        return self


def normalize_product_filter_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    return parse_product_filter_payload(payload).payload


def parse_product_filter_payload(payload: dict[str, Any] | None) -> FilterApplyResult:
    raw_payload = dict(payload or {})
    normalized: dict[str, Any] = {}
    ignored_fields: list[str] = []

    for key, value in raw_payload.items():
        if key not in _ALLOWED_PRODUCT_FILTER_FIELDS:
            ignored_fields.append(key)
            continue

        if key in {"name", "category"}:
            if value is None:
                continue
            text = str(value).strip()
            if text:
                normalized[key] = text
            continue

        if value is None or value == "":
            continue

        try:
            price = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Giá trị `{key}` không hợp lệ.") from exc

        if price < 0:
            raise ValueError(f"Giá trị `{key}` không được âm.")
        normalized[key] = price

    price_min = normalized.get("price_min")
    price_max = normalized.get("price_max")
    if price_min is not None and price_max is not None and price_min > price_max:
        raise ValueError("Khoảng giá không hợp lệ: giá tối thiểu lớn hơn giá tối đa.")

    return FilterApplyResult(payload=normalized, ignored_fields=ignored_fields)


def normalize_report_filter_payload(report_code: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    return parse_report_filter_payload(report_code, payload).payload


def parse_report_filter_payload(report_code: str, payload: dict[str, Any] | None) -> FilterApplyResult:
    code = str(report_code or "").strip()
    if code == PRODUCT_REPORT_CODE:
        return parse_product_filter_payload(payload)
    if code == SUMMARY_REPORT_CODE:
        return _parse_summary_filter_payload(payload)
    raise ValueError(f"Unsupported report_code: {code}")


def _parse_summary_filter_payload(payload: dict[str, Any] | None) -> FilterApplyResult:
    raw_payload = dict(payload or {})
    normalized: dict[str, Any] = {}
    ignored_fields: list[str] = []

    for key, value in raw_payload.items():
        key_text = str(key or "").strip()
        if not key_text.startswith("summary_"):
            ignored_fields.append(key_text)
            continue
        normalized_value = _normalize_json_compatible_value(value)
        if normalized_value is None and value is not None:
            ignored_fields.append(key_text)
            continue
        normalized[key_text] = normalized_value

    return FilterApplyResult(payload=normalized, ignored_fields=ignored_fields)


def _normalize_json_compatible_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        normalized_items: list[Any] = []
        for item in value:
            normalized_item = _normalize_json_compatible_value(item)
            if normalized_item is None and item is not None:
                return None
            normalized_items.append(normalized_item)
        return normalized_items
    if isinstance(value, dict):
        normalized_map: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key or "").strip()
            if not key_text:
                continue
            normalized_item = _normalize_json_compatible_value(item)
            if normalized_item is None and item is not None:
                return None
            normalized_map[key_text] = normalized_item
        return normalized_map
    return None
