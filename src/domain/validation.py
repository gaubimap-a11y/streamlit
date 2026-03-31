from __future__ import annotations

from domain.models import DemoRequest


class ValidationError(ValueError):
    """Raised when user input does not meet the demo contract."""


def validate_demo_request(request: DemoRequest) -> None:
    if not isinstance(request.product_name, str):
        raise ValidationError("product_name must be a string.")

    # product_name is optional; empty means query all products.
    if len(request.product_name) > 200:
        raise ValidationError("product_name must be 200 characters or fewer.")

    for char in request.product_name:
        if ord(char) < 32 and char not in {"\t", "\n", "\r"}:
            raise ValidationError("product_name contains unsupported control characters.")
