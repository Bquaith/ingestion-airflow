from __future__ import annotations

from typing import Any

from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from flask_appbuilder.security.manager import AUTH_OAUTH

from ingestion_airflow.keycloak_fab_oauth import (
    build_oauth_provider_config,
    build_userinfo_from_access_token,
    get_keycloak_oauth_settings,
)


CSRF_ENABLED = True
AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_USER_REGISTRATION_ROLE = "Public"
PERMANENT_SESSION_LIFETIME = 43200

AUTH_ROLES_MAPPING = {
    "Public": ["Public"],
    "Viewer": ["Viewer"],
    "User": ["User"],
    "Op": ["Op"],
    "Admin": ["Admin"],
    "SuperAdmin": ["Admin"],
}

OAUTH_PROVIDERS = [build_oauth_provider_config()]


class KeycloakFabSecurityManager(FabAirflowSecurityManagerOverride):
    def get_oauth_user_info(self, provider: str, response: dict[str, Any]) -> dict[str, Any]:
        settings = get_keycloak_oauth_settings()
        if provider != settings.provider_name:
            return {}

        access_token = str(response.get("access_token") or "").strip()
        if not access_token:
            return {}

        return build_userinfo_from_access_token(access_token)


SECURITY_MANAGER_CLASS = KeycloakFabSecurityManager
