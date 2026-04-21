from __future__ import annotations

from dataclasses import dataclass
import re

from src.domain.auth_models import AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SSO
from src.domain.auth_validation import AuthenticationValidationError

_EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
_USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9._@\-]{3,200}$")

@dataclass(frozen=True)
class ValidatedAdminText:
    raw_value: str
    normalized_value: str


def normalize_admin_text(value: str, field_name: str, *, max_length: int = 200) -> ValidatedAdminText:
    if not isinstance(value, str):
        raise AuthenticationValidationError(f"{field_name} must be a string.")

    normalized = value.strip()
    if normalized == "":
        raise AuthenticationValidationError(f"{field_name} is required.")
    if len(normalized) > max_length:
        raise AuthenticationValidationError(f"{field_name} must be {max_length} characters or fewer.")
    return ValidatedAdminText(raw_value=value, normalized_value=normalized)


def normalize_optional_admin_text(value: str, field_name: str, *, max_length: int = 500) -> str:
    if not isinstance(value, str):
        raise AuthenticationValidationError(f"{field_name} must be a string.")
    normalized = value.strip()
    if len(normalized) > max_length:
        raise AuthenticationValidationError(f"{field_name} must be {max_length} characters or fewer.")
    return normalized


def normalize_bulk_items(items: tuple[str, ...] | list[str], field_name: str) -> tuple[str, ...]:
    if not isinstance(items, (tuple, list)):
        raise AuthenticationValidationError(f"{field_name} must be a list.")
    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized_item = normalize_admin_text(str(item), field_name, max_length=200).normalized_value
        lowered = normalized_item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(normalized_item)
    if not normalized:
        raise AuthenticationValidationError(f"{field_name} is required.")
    return tuple(normalized)


def validate_admin_email(email: str) -> str:
    normalized_email = normalize_admin_text(email, "email").normalized_value.lower()
    if _EMAIL_PATTERN.fullmatch(normalized_email) is None:
        raise AuthenticationValidationError("Email format is invalid.")
    return normalized_email


def validate_admin_username(username: str) -> str:
    normalized_username = normalize_admin_text(username, "username").normalized_value
    if _USERNAME_PATTERN.fullmatch(normalized_username) is None:
        raise AuthenticationValidationError("Username format is invalid.")
    return normalized_username


def validate_admin_auth_source(auth_source: str) -> str:
    normalized_auth_source = normalize_admin_text(auth_source, "auth_source").normalized_value.lower()
    if normalized_auth_source not in {AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SSO}:
        raise AuthenticationValidationError("auth_source must be internal or sso.")
    return normalized_auth_source
