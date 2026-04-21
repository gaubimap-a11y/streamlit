from __future__ import annotations

from dataclasses import dataclass

from src.domain.auth_models import AUTH_SOURCE_INTERNAL


@dataclass(frozen=True)
class PrincipalAuthorizationProfile:
    principal_id: str
    username: str
    email: str = ""
    display_name: str = ""
    auth_source: str = AUTH_SOURCE_INTERNAL
    permissions: tuple[str, ...] = ()
    correlation_id: str = ""

    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions
