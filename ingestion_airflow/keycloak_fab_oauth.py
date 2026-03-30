from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from typing import Any


AIRFLOW_ROLE_NAMES = frozenset({"Viewer", "User", "Op", "Admin", "SuperAdmin"})
DEFAULT_PROVIDER_NAME = "keycloak"
DEFAULT_PUBLIC_ROLE = "Public"
DEFAULT_SCOPE = "openid email profile"


@dataclass(frozen=True)
class KeycloakOAuthSettings:
    client_id: str
    client_secret: str
    issuer_url: str
    internal_issuer_url: str
    provider_name: str = DEFAULT_PROVIDER_NAME
    scope: str = DEFAULT_SCOPE

    @property
    def normalized_issuer_url(self) -> str:
        return self.issuer_url.rstrip("/")

    @property
    def normalized_internal_issuer_url(self) -> str:
        return self.internal_issuer_url.rstrip("/")

    @property
    def oidc_base_url(self) -> str:
        return f"{self.normalized_issuer_url}/protocol/openid-connect"

    @property
    def internal_oidc_base_url(self) -> str:
        return f"{self.normalized_internal_issuer_url}/protocol/openid-connect"

    @property
    def metadata_url(self) -> str:
        return f"{self.normalized_internal_issuer_url}/.well-known/openid-configuration"

    @property
    def auth_url(self) -> str:
        return f"{self.oidc_base_url}/auth"

    @property
    def token_url(self) -> str:
        return f"{self.internal_oidc_base_url}/token"

    @property
    def jwks_url(self) -> str:
        return f"{self.internal_oidc_base_url}/certs"


def _required_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for Airflow FAB OAuth SSO")
    return value


@lru_cache(maxsize=1)
def get_keycloak_oauth_settings() -> KeycloakOAuthSettings:
    return KeycloakOAuthSettings(
        client_id=_required_env("AIRFLOW_OAUTH_CLIENT_ID"),
        client_secret=_required_env("AIRFLOW_OAUTH_CLIENT_SECRET"),
        issuer_url=_required_env("AIRFLOW_OAUTH_ISSUER_URL"),
        internal_issuer_url=(
            os.getenv("AIRFLOW_OAUTH_INTERNAL_ISSUER_URL")
            or _required_env("AIRFLOW_OAUTH_ISSUER_URL")
        ).strip(),
        provider_name=(os.getenv("AIRFLOW_OAUTH_PROVIDER_NAME") or DEFAULT_PROVIDER_NAME).strip()
        or DEFAULT_PROVIDER_NAME,
        scope=(os.getenv("AIRFLOW_OAUTH_SCOPE") or DEFAULT_SCOPE).strip() or DEFAULT_SCOPE,
    )


def build_oauth_provider_config() -> dict[str, Any]:
    settings = get_keycloak_oauth_settings()
    return {
        "name": settings.provider_name,
        "token_key": "access_token",
        "icon": "fa-key",
        "remote_app": {
            "api_base_url": settings.internal_oidc_base_url,
            "access_token_url": settings.token_url,
            "authorize_url": settings.auth_url,
            "request_token_url": None,
            "client_id": settings.client_id,
            "client_secret": settings.client_secret,
            "server_metadata_url": settings.metadata_url,
            "issuer": settings.normalized_issuer_url,
            "jwks_uri": settings.jwks_url,
            "client_kwargs": {
                "scope": settings.scope,
            },
        },
    }


@lru_cache(maxsize=None)
def _jwks_client(jwks_url: str):
    import jwt

    return jwt.PyJWKClient(jwks_url)


def decode_keycloak_access_token(access_token: str) -> dict[str, Any]:
    import jwt

    settings = get_keycloak_oauth_settings()
    signing_key = _jwks_client(settings.jwks_url).get_signing_key_from_jwt(access_token)
    claims = jwt.decode(
        access_token,
        signing_key.key,
        algorithms=["RS256", "RS384", "RS512"],
        audience=settings.client_id,
        issuer=settings.normalized_issuer_url,
    )

    authorized_party = str(claims.get("azp") or "").strip()
    if authorized_party and authorized_party != settings.client_id:
        raise ValueError("Unexpected authorized party in Keycloak access token")

    return claims


def extract_airflow_role_keys(claims: dict[str, Any]) -> list[str]:
    realm_access = claims.get("realm_access")
    if not isinstance(realm_access, dict):
        return [DEFAULT_PUBLIC_ROLE]

    roles = realm_access.get("roles")
    if not isinstance(roles, list):
        return [DEFAULT_PUBLIC_ROLE]

    filtered_roles = [role for role in roles if isinstance(role, str) and role in AIRFLOW_ROLE_NAMES]
    return filtered_roles or [DEFAULT_PUBLIC_ROLE]


def build_userinfo_from_claims(claims: dict[str, Any]) -> dict[str, Any]:
    username = str(claims.get("preferred_username") or claims.get("sub") or "").strip()
    if not username:
        raise ValueError("Keycloak access token does not contain a username")

    return {
        "username": username,
        "email": str(claims.get("email") or "").strip(),
        "first_name": str(claims.get("given_name") or "").strip(),
        "last_name": str(claims.get("family_name") or "").strip(),
        "role_keys": extract_airflow_role_keys(claims),
    }


def build_userinfo_from_access_token(access_token: str) -> dict[str, Any]:
    return build_userinfo_from_claims(decode_keycloak_access_token(access_token))
