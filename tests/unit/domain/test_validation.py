from __future__ import annotations

from domain.models import DemoRequest
from domain.validation import validate_demo_request


def test_validate_demo_request_accepts_valid_input() -> None:
    request = DemoRequest(product_name="Coffee A")

    validate_demo_request(request)

def test_validate_demo_request_accepts_empty_product_name() -> None:
    request = DemoRequest(product_name=" ")
    validate_demo_request(request)
