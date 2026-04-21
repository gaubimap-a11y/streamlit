import pytest

from src.domain.report_filters import SaveFilterCommand, normalize_product_filter_payload, parse_product_filter_payload


def test_parse_product_filter_payload_ignores_unknown_fields():
    result = parse_product_filter_payload(
        {
            "name": "  Apple  ",
            "category": "Food",
            "price_min": "100",
            "price_max": 200,
            "page": 3,
            "sort": "desc",
        }
    )

    assert result.payload == {
        "name": "Apple",
        "category": "Food",
        "price_min": 100.0,
        "price_max": 200.0,
    }
    assert sorted(result.ignored_fields) == ["page", "sort"]


def test_parse_product_filter_payload_rejects_invalid_price_range():
    with pytest.raises(ValueError):
        parse_product_filter_payload({"price_min": 300, "price_max": 100})


def test_normalize_product_filter_payload_omits_empty_values():
    result = normalize_product_filter_payload(
        {
            "name": "   ",
            "category": "  ",
            "price_min": "",
            "price_max": None,
            "legacy": "x",
        }
    )
    assert result == {}


def test_save_filter_command_rejects_too_long_filter_name():
    with pytest.raises(ValueError):
        SaveFilterCommand(
            report_code="product",
            owner_user_id="u1",
            filter_name="a" * 121,
            filter_payload={"name": "apple"},
        )
