from __future__ import annotations

from datetime import datetime, timezone

from src.application.auth.sso_auth_service import (
    DEFAULT_SSO_BASIC_PERMISSIONS,
    authenticate_sso_user,
)
from src.domain.auth_models import AUTH_SOURCE_SSO, SsoLoginRequest


class _StubAuthorizationStore:
    def __init__(self, *, mapped: bool, permissions: tuple[str, ...] = ()) -> None:
        self._mapped = mapped
        self._permissions = permissions
        self.upsert_calls: list[dict[str, str]] = []
        self.mapping_checks: list[dict[str, str]] = []
        self.permission_resolutions: list[dict[str, str]] = []

    def has_principal_mapping(self, *, principal_id: str, username: str, email: str, auth_source: str) -> bool:
        self.mapping_checks.append(
            {
                "principal_id": principal_id,
                "username": username,
                "email": email,
                "auth_source": auth_source,
            }
        )
        return self._mapped

    def resolve_permissions(self, *, principal_id: str, username: str, email: str, auth_source: str) -> tuple[str, ...]:
        self.permission_resolutions.append(
            {
                "principal_id": principal_id,
                "username": username,
                "email": email,
                "auth_source": auth_source,
            }
        )
        return self._permissions

    def upsert_basic_sso_principal(
        self,
        *,
        principal_id: str,
        username: str,
        email: str,
        display_name: str,
        password_hash: str,
    ) -> None:
        self.upsert_calls.append(
            {
                "principal_id": principal_id,
                "username": username,
                "email": email,
                "display_name": display_name,
                "password_hash": password_hash,
            }
        )


def test_authenticate_sso_user_provisions_principal_when_unmapped():
    store = _StubAuthorizationStore(mapped=False, permissions=("should_not_be_used",))
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    request = SsoLoginRequest(
        principal_id="google-sub-001",
        email="user@gmail.com",
        display_name="Google User",
        provider_id="google",
    )

    session = authenticate_sso_user(request, now=now, authorization_store=store)

    assert session.auth_source == AUTH_SOURCE_SSO
    assert session.user_id == "google-sub-001"
    assert session.username == "user@gmail.com"
    assert session.email == "user@gmail.com"
    assert session.display_name == "Google User"
    assert session.permissions == DEFAULT_SSO_BASIC_PERMISSIONS

    assert len(store.mapping_checks) == 1
    assert len(store.upsert_calls) == 1
    assert store.upsert_calls[0]["principal_id"] == "google-sub-001"
    assert store.upsert_calls[0]["username"] == "user@gmail.com"
    assert store.upsert_calls[0]["email"] == "user@gmail.com"
    assert store.upsert_calls[0]["display_name"] == "Google User"
    assert store.upsert_calls[0]["password_hash"] == ""
    assert store.permission_resolutions == []


def test_authenticate_sso_user_uses_resolved_permissions_when_mapped():
    store = _StubAuthorizationStore(mapped=True, permissions=("app_access", "view_dashboard", "view_data"))
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    request = SsoLoginRequest(
        principal_id="google-sub-001",
        email="user@gmail.com",
        display_name="Google User",
        provider_id="google",
    )

    session = authenticate_sso_user(request, now=now, authorization_store=store)

    assert session.auth_source == AUTH_SOURCE_SSO
    assert session.permissions == ("app_access", "view_dashboard", "view_data")
    assert store.upsert_calls == []
    assert len(store.permission_resolutions) == 1

