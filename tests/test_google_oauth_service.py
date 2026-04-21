from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

from src.application.auth.google_oauth_service import (
    build_google_oauth_authorization_url,
    handle_google_oauth_callback,
)
from src.core.config import GoogleOAuthConfig
from src.domain.auth_models import AUTH_SOURCE_SSO, AuthenticatedSession


def _google_config() -> GoogleOAuthConfig:
    return GoogleOAuthConfig(
        client_id="google-client-id",
        client_secret="google-client-secret",
        redirect_uri="http://localhost:8501/login",
    )


def test_build_google_oauth_authorization_url_contains_expected_params():
    config = _google_config()
    fixed_now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    url = build_google_oauth_authorization_url(config, now=fixed_now)

    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    assert parsed.netloc == "accounts.google.com"
    assert params["client_id"] == [config.client_id]
    assert params["redirect_uri"] == [config.redirect_uri]
    assert params["response_type"] == ["code"]
    assert params["scope"] == ["openid email profile"]
    assert "state" in params


def test_handle_google_oauth_callback_builds_authenticated_session(mocker):
    config = _google_config()
    fixed_now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    state = parse_qs(urlparse(build_google_oauth_authorization_url(config, now=fixed_now)).query)["state"][0]
    session = AuthenticatedSession(
        user_id="google-sub-001",
        username="user@gmail.com",
        login_at=datetime(2026, 4, 10, tzinfo=timezone.utc),
        expires_at=datetime(2026, 4, 10, 8, tzinfo=timezone.utc),
        auth_source=AUTH_SOURCE_SSO,
        display_name="Google User",
        email="user@gmail.com",
        permissions=("app_access", "view_dashboard"),
    )

    mocker.patch(
        "src.application.auth.google_oauth_service.get_google_oauth_config",
        return_value=config,
    )
    mocker.patch(
        "src.application.auth.google_oauth_service._exchange_code_for_tokens",
        return_value={"access_token": "token-123"},
    )
    mocker.patch(
        "src.application.auth.google_oauth_service._fetch_userinfo",
        return_value={"sub": "google-sub-001", "email": "user@gmail.com", "name": "Google User"},
    )
    auth_mock = mocker.patch(
        "src.application.auth.google_oauth_service.authenticate_sso_user",
        return_value=session,
    )

    result = handle_google_oauth_callback(
        code="oauth-code",
        state=state,
        now=fixed_now,
    )

    assert result == session
    auth_mock.assert_called_once()
    request = auth_mock.call_args.args[0]
    assert request.principal_id == "google-sub-001"
    assert request.email == "user@gmail.com"
    assert request.display_name == "Google User"
    assert request.provider_id == "google"
