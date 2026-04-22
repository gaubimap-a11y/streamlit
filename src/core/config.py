from __future__ import annotations

import configparser
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.core.exceptions import ConfigError
from src.core.streamlit_secrets import get_secret_section, load_streamlit_secrets


_WEBAPP_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_LOCAL_CFG = _WEBAPP_ROOT / "databricks.local.cfg"
_DEFAULT_TEMPLATE_CFG = _WEBAPP_ROOT / "databricks.cfg"
_DATABRICKS_SECTION = "databricks"
_AUTH_SECTION = "auth"
_GOOGLE_OAUTH_SECTION = "google_oauth"

_ENV_HOSTNAME = "DATABRICKS_HOSTNAME"
_ENV_HTTP_PATH = "DATABRICKS_HTTP_PATH"
_ENV_TOKEN = "DATABRICKS_TOKEN"
_ENV_CFG_PATH = "DATABRICKS_CFG_PATH"
_ENV_SOCKET_TIMEOUT_SECONDS = "DATABRICKS_SOCKET_TIMEOUT_SECONDS"
_ENV_RETRY_STOP_AFTER_ATTEMPTS_COUNT = "DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_COUNT"
_ENV_RETRY_STOP_AFTER_ATTEMPTS_DURATION_SECONDS = "DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION_SECONDS"


@dataclass(frozen=True)
class DatabricksConfig:
    server_hostname: str
    http_path: str
    access_token: str
    catalog: str = "tmn_kobe"
    socket_timeout_seconds: int = 60
    retry_stop_after_attempts_count: int = 3
    retry_stop_after_attempts_duration_seconds: int = 60


@dataclass(frozen=True)
class AuthConfig:
    enabled_login_modes: tuple[str, ...] = ("internal", "sso")


@dataclass(frozen=True)
class GoogleOAuthConfig:
    client_id: str = ""
    client_secret: str = ""
    redirect_uri: str = ""
    authorization_endpoint: str = "https://accounts.google.com/o/oauth2/v2/auth"
    token_endpoint: str = "https://oauth2.googleapis.com/token"
    userinfo_endpoint: str = "https://openidconnect.googleapis.com/v1/userinfo"
    scopes: tuple[str, ...] = ("openid", "email", "profile")
    hosted_domain: str = ""
    prompt: str = "select_account"

    def is_configured(self) -> bool:
        return bool(self.client_id.strip() and self.client_secret.strip() and self.redirect_uri.strip())


@dataclass(frozen=True)
class Settings:
    databricks: DatabricksConfig
    auth: AuthConfig = field(default_factory=AuthConfig)
    google_oauth: GoogleOAuthConfig = field(default_factory=GoogleOAuthConfig)
    session_timeout_hours: int = 8


def _read_cfg_file(path: Path) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    if path.exists():
        config.read(path, encoding="utf-8")
    return config


def _read_cfg_value(config: configparser.ConfigParser, key: str) -> str:
    try:
        return config[_DATABRICKS_SECTION][key].strip().strip('"').strip("'")
    except KeyError:
        return ""


def _read_auth_cfg_value(config: configparser.ConfigParser, key: str) -> str:
    try:
        return config[_AUTH_SECTION][key].strip().strip('"').strip("'")
    except KeyError:
        return ""


def _read_google_oauth_cfg_value(config: configparser.ConfigParser, key: str) -> str:
    try:
        return config[_GOOGLE_OAUTH_SECTION][key].strip().strip('"').strip("'")
    except KeyError:
        return ""


def _as_clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().strip('"').strip("'")


def _read_secret_value(secrets: dict[str, Any], section: str, key: str) -> str:
    return _as_clean_str(get_secret_section(secrets, section).get(key))


def _read_secret_list(secrets: dict[str, Any], section: str, key: str) -> tuple[str, ...]:
    raw = get_secret_section(secrets, section).get(key)
    if isinstance(raw, (list, tuple)):
        return tuple(_as_clean_str(item) for item in raw if _as_clean_str(item))
    if isinstance(raw, str):
        return tuple(item.strip() for item in raw.split(",") if item.strip())
    return ()


def _read_int_setting(
    config: configparser.ConfigParser,
    secrets: dict[str, Any],
    *,
    section: str,
    env_key: str,
    cfg_key: str,
    default: int,
    min_value: int = 1,
    max_value: int = 3600,
) -> int:
    raw_value = os.environ.get(env_key, "").strip()
    if not raw_value:
        raw_value = _read_secret_value(secrets, section, cfg_key)
    if not raw_value:
        raw_value = _read_cfg_value(config, cfg_key)
    try:
        parsed = int(raw_value)
    except ValueError:
        return default

    if parsed < min_value:
        return min_value
    if parsed > max_value:
        return max_value
    return parsed


def _normalize_csv_values(value: str, *, default: tuple[str, ...]) -> tuple[str, ...]:
    raw_items = [item.strip() for item in value.split(",")]
    normalized = tuple(item for item in raw_items if item)
    return normalized or default


def _load_auth_config(config: configparser.ConfigParser, secrets: dict[str, Any]) -> AuthConfig:
    secret_modes = tuple(mode.lower() for mode in _read_secret_list(secrets, _AUTH_SECTION, "enabled_login_modes"))
    if secret_modes:
        enabled_login_modes = secret_modes
    else:
        enabled_login_modes = _normalize_csv_values(
            _read_auth_cfg_value(config, "enabled_login_modes").lower(),
            default=("internal", "sso"),
        )
    enabled_login_modes = tuple(dict.fromkeys(mode.lower() for mode in enabled_login_modes if mode.strip()))
    if not enabled_login_modes:
        enabled_login_modes = ("internal", "sso")

    return AuthConfig(enabled_login_modes=enabled_login_modes)


def _load_google_oauth_config(config: configparser.ConfigParser, secrets: dict[str, Any]) -> GoogleOAuthConfig:
    secret_scopes = tuple(scope.lower() for scope in _read_secret_list(secrets, _GOOGLE_OAUTH_SECTION, "scopes"))
    if secret_scopes:
        scopes = secret_scopes
    else:
        scopes = _normalize_csv_values(
            _read_google_oauth_cfg_value(config, "scopes").lower(),
            default=("openid", "email", "profile"),
        )
    scopes = tuple(dict.fromkeys(scope.lower() for scope in scopes if scope.strip()))
    if not scopes:
        scopes = ("openid", "email", "profile")

    return GoogleOAuthConfig(
        client_id=_read_secret_value(secrets, _GOOGLE_OAUTH_SECTION, "client_id")
        or _read_google_oauth_cfg_value(config, "client_id"),
        client_secret=_read_secret_value(secrets, _GOOGLE_OAUTH_SECTION, "client_secret")
        or _read_google_oauth_cfg_value(config, "client_secret"),
        redirect_uri=_read_secret_value(secrets, _GOOGLE_OAUTH_SECTION, "redirect_uri")
        or _read_google_oauth_cfg_value(config, "redirect_uri"),
        authorization_endpoint=(
            _read_secret_value(secrets, _GOOGLE_OAUTH_SECTION, "authorization_endpoint")
            or _read_google_oauth_cfg_value(config, "authorization_endpoint")
            or "https://accounts.google.com/o/oauth2/v2/auth"
        ),
        token_endpoint=(
            _read_secret_value(secrets, _GOOGLE_OAUTH_SECTION, "token_endpoint")
            or _read_google_oauth_cfg_value(config, "token_endpoint")
            or "https://oauth2.googleapis.com/token"
        ),
        userinfo_endpoint=(
            _read_secret_value(secrets, _GOOGLE_OAUTH_SECTION, "userinfo_endpoint")
            or _read_google_oauth_cfg_value(config, "userinfo_endpoint")
            or "https://openidconnect.googleapis.com/v1/userinfo"
        ),
        scopes=scopes,
        hosted_domain=_read_secret_value(secrets, _GOOGLE_OAUTH_SECTION, "hosted_domain")
        or _read_google_oauth_cfg_value(config, "hosted_domain"),
        prompt=(
            _read_secret_value(secrets, _GOOGLE_OAUTH_SECTION, "prompt")
            or _read_google_oauth_cfg_value(config, "prompt")
            or "select_account"
        ),
    )


def _build_settings() -> Settings:
    cfg_path_value = os.environ.get(_ENV_CFG_PATH, "").strip()
    cfg_path = Path(cfg_path_value) if cfg_path_value else _DEFAULT_LOCAL_CFG

    secrets = load_streamlit_secrets()
    config = _read_cfg_file(cfg_path)
    if not cfg_path.exists():
        config = _read_cfg_file(_DEFAULT_TEMPLATE_CFG)

    server_hostname = os.environ.get(
        _ENV_HOSTNAME,
        _read_secret_value(secrets, _DATABRICKS_SECTION, "server_hostname")
        or _read_secret_value(secrets, _DATABRICKS_SECTION, "host")
        or _read_cfg_value(config, "server_hostname"),
    ).strip()
    http_path = os.environ.get(
        _ENV_HTTP_PATH,
        _read_secret_value(secrets, _DATABRICKS_SECTION, "http_path")
        or _read_cfg_value(config, "http_path"),
    ).strip()
    access_token = os.environ.get(
        _ENV_TOKEN,
        _read_secret_value(secrets, _DATABRICKS_SECTION, "access_token")
        or _read_secret_value(secrets, _DATABRICKS_SECTION, "token")
        or _read_cfg_value(config, "access_token"),
    ).strip()
    catalog = os.environ.get(
        "DATABRICKS_CATALOG",
        _read_secret_value(secrets, _DATABRICKS_SECTION, "catalog")
        or _read_cfg_value(config, "catalog"),
    ).strip() or "tmn_kobe"

    socket_timeout_seconds = _read_int_setting(
        config,
        secrets,
        section=_DATABRICKS_SECTION,
        env_key=_ENV_SOCKET_TIMEOUT_SECONDS,
        cfg_key="socket_timeout_seconds",
        default=DatabricksConfig.socket_timeout_seconds,
        min_value=1,
        max_value=600,
    )
    retry_stop_after_attempts_count = _read_int_setting(
        config,
        secrets,
        section=_DATABRICKS_SECTION,
        env_key=_ENV_RETRY_STOP_AFTER_ATTEMPTS_COUNT,
        cfg_key="retry_stop_after_attempts_count",
        default=DatabricksConfig.retry_stop_after_attempts_count,
        min_value=1,
        max_value=10,
    )
    retry_stop_after_attempts_duration_seconds = _read_int_setting(
        config,
        secrets,
        section=_DATABRICKS_SECTION,
        env_key=_ENV_RETRY_STOP_AFTER_ATTEMPTS_DURATION_SECONDS,
        cfg_key="retry_stop_after_attempts_duration_seconds",
        default=DatabricksConfig.retry_stop_after_attempts_duration_seconds,
        min_value=1,
        max_value=600,
    )

    missing: list[str] = []
    if not server_hostname:
        missing.append("server_hostname")
    if not http_path:
        missing.append("http_path")
    if not access_token:
        missing.append("access_token")

    if missing:
        if os.environ.get("PYTEST_CURRENT_TEST"):
            return Settings(
                databricks=DatabricksConfig(
                    server_hostname="",
                    http_path="",
                    access_token="",
                ),
            )
        raise ConfigError(
            "Missing Databricks configuration values: "
            + ", ".join(missing)
            + f". Checked env vars, Streamlit secrets, and '{cfg_path.name}'."
        )

    return Settings(
        databricks=DatabricksConfig(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            socket_timeout_seconds=socket_timeout_seconds,
            retry_stop_after_attempts_count=retry_stop_after_attempts_count,
            retry_stop_after_attempts_duration_seconds=retry_stop_after_attempts_duration_seconds,
        ),
        auth=_load_auth_config(config, secrets),
        google_oauth=_load_google_oauth_config(config, secrets),
    )


_settings_cache: Settings | None = None


def get_settings() -> Settings:
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = _build_settings()
    return _settings_cache


def reset_settings_cache() -> None:
    global _settings_cache
    _settings_cache = None


def get_google_oauth_config() -> GoogleOAuthConfig:
    settings = get_settings()
    config = getattr(settings, "google_oauth", None)
    if isinstance(config, GoogleOAuthConfig):
        return config
    return GoogleOAuthConfig()
