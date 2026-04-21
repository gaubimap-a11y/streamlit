from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.application.auth.sso_auth_service import AuthorizationStore, authenticate_sso_user
from src.core.config import GoogleOAuthConfig, get_google_oauth_config
from src.core.exceptions import AuthError
from src.domain.auth_models import SSO_PROVIDER_GOOGLE, AuthenticatedSession, SsoLoginRequest
from src.domain.auth_validation import AuthenticationValidationError


_STATE_MAX_AGE_SECONDS = 600
_HTTP_TIMEOUT_SECONDS = 15


def build_google_oauth_authorization_url(
    config: GoogleOAuthConfig | None = None,
    *,
    now: datetime | None = None,
) -> str:
    oauth_config = config or get_google_oauth_config()
    if not oauth_config.is_configured():
        raise AuthError("Google OAuth2 is not configured.")

    state = _build_state_token(oauth_config, now=now)
    params: dict[str, str] = {
        "client_id": oauth_config.client_id,
        "redirect_uri": oauth_config.redirect_uri,
        "response_type": "code",
        "scope": " ".join(oauth_config.scopes),
        "state": state,
        "access_type": "online",
        "include_granted_scopes": "true",
        "prompt": oauth_config.prompt or "select_account",
    }
    if oauth_config.hosted_domain.strip():
        params["hd"] = oauth_config.hosted_domain.strip()
    return f"{oauth_config.authorization_endpoint}?{urlencode(params)}"


def handle_google_oauth_callback(
    *,
    code: str,
    state: str,
    error: str = "",
    error_description: str = "",
    now: datetime | None = None,
    authorization_store: AuthorizationStore | None = None,
) -> AuthenticatedSession:
    oauth_config = get_google_oauth_config()
    if not oauth_config.is_configured():
        raise AuthError("Google OAuth2 is not configured.")

    if error.strip():
        message = error_description.strip() or error.strip()
        raise AuthenticationValidationError(f"Google OAuth login failed: {message}")

    normalized_code = code.strip()
    normalized_state = state.strip()
    if not normalized_code:
        raise AuthenticationValidationError("Google OAuth callback is missing code.")
    if not normalized_state:
        raise AuthenticationValidationError("Google OAuth callback is missing state.")

    _verify_state_token(normalized_state, oauth_config, now=now)

    token_payload = _exchange_code_for_tokens(oauth_config, code=normalized_code)
    access_token = str(token_payload.get("access_token", "")).strip()
    if not access_token:
        raise AuthError("Google OAuth token response is missing access_token.")

    userinfo = _fetch_userinfo(oauth_config, access_token=access_token)
    principal_id = _resolve_userinfo_value(userinfo, "sub", "email")
    email = _resolve_userinfo_value(userinfo, "email", "preferred_username", "upn", "username", "login")
    display_name = _resolve_userinfo_value(userinfo, "name", "display_name", "email", "preferred_username", "upn")
    if not principal_id:
        raise AuthenticationValidationError("Google OAuth userinfo is missing subject.")
    if not email:
        raise AuthenticationValidationError("Google OAuth userinfo is missing email.")
    if not display_name:
        display_name = email

    claims = tuple(
        (str(key), _stringify_claim_value(value))
        for key, value in sorted(userinfo.items(), key=lambda item: str(item[0]))
        if _stringify_claim_value(value)
    )
    request = SsoLoginRequest(
        principal_id=principal_id,
        email=email,
        display_name=display_name,
        provider_id=SSO_PROVIDER_GOOGLE,
        claims=claims,
    )
    return authenticate_sso_user(
        request,
        now=now,
        authorization_store=authorization_store,
    )


def _build_state_token(config: GoogleOAuthConfig, *, now: datetime | None = None) -> str:
    issued_at = int((now or datetime.now(tz=timezone.utc)).timestamp())
    payload = {
        "nonce": secrets.token_urlsafe(24),
        "iat": issued_at,
        "redirect_uri": config.redirect_uri,
    }
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    signature = hmac.new(
        config.client_secret.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    raw = f"{payload_json}.{signature}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _verify_state_token(token: str, config: GoogleOAuthConfig, *, now: datetime | None = None) -> dict[str, Any]:
    padded = token + "=" * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        payload_json, signature = raw.rsplit(".", 1)
        expected_signature = hmac.new(
            config.client_secret.encode("utf-8"),
            payload_json.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            raise AuthenticationValidationError("Google OAuth state is invalid.")
        payload = json.loads(payload_json)
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError, binascii.Error) as exc:
        raise AuthenticationValidationError("Google OAuth state is invalid.") from exc

    issued_at = int(payload.get("iat", 0))
    current_timestamp = int((now or datetime.now(tz=timezone.utc)).timestamp())
    if current_timestamp - issued_at > _STATE_MAX_AGE_SECONDS:
        raise AuthenticationValidationError("Google OAuth state has expired.")

    redirect_uri = str(payload.get("redirect_uri", "")).strip()
    if redirect_uri != config.redirect_uri.strip():
        raise AuthenticationValidationError("Google OAuth redirect URI mismatch.")
    return payload


def _exchange_code_for_tokens(config: GoogleOAuthConfig, *, code: str) -> dict[str, Any]:
    payload = {
        "code": code,
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "redirect_uri": config.redirect_uri,
        "grant_type": "authorization_code",
    }
    return _request_json(config.token_endpoint, data=payload, method="POST")


def _fetch_userinfo(config: GoogleOAuthConfig, *, access_token: str) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {access_token}"}
    return _request_json(config.userinfo_endpoint, headers=headers, method="GET")


def _request_json(
    url: str,
    *,
    data: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    method: str = "GET",
) -> dict[str, Any]:
    request_data = urlencode(data).encode("utf-8") if data is not None else None
    request_headers = dict(headers or {})
    if request_data is not None:
        request_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
    request = Request(url, data=request_data, headers=request_headers, method=method)
    try:
        with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        detail = f": {body}" if body else ""
        raise AuthError(f"Google OAuth request failed with HTTP {exc.code}{detail}") from exc
    except URLError as exc:
        raise AuthError("Google OAuth request could not reach the identity provider.") from exc

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise AuthError("Google OAuth response was not valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise AuthError("Google OAuth response must be a JSON object.")
    return parsed


def _resolve_userinfo_value(userinfo: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _stringify_claim_value(userinfo.get(key))
        if value:
            return value
    return ""


def _stringify_claim_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return str(value).strip()
