from __future__ import annotations

from ingestion_airflow.keycloak_fab_oauth import (
    DEFAULT_PUBLIC_ROLE,
    build_oauth_provider_config,
    build_userinfo_from_claims,
    extract_airflow_role_keys,
    get_keycloak_oauth_settings,
)


def test_extract_airflow_role_keys_filters_known_roles() -> None:
    claims = {
        "realm_access": {
            "roles": ["offline_access", "Viewer", "User", "uma_authorization", "Op"],
        }
    }

    assert extract_airflow_role_keys(claims) == ["Viewer", "User", "Op"]


def test_extract_airflow_role_keys_falls_back_to_public() -> None:
    assert extract_airflow_role_keys({}) == [DEFAULT_PUBLIC_ROLE]


def test_build_userinfo_from_claims_uses_profile_fields() -> None:
    claims = {
        "preferred_username": "consumer",
        "email": "consumer@local.test",
        "given_name": "Data",
        "family_name": "Consumer",
        "realm_access": {"roles": ["User", "Op"]},
    }

    assert build_userinfo_from_claims(claims) == {
        "username": "consumer",
        "email": "consumer@local.test",
        "first_name": "Data",
        "last_name": "Consumer",
        "role_keys": ["User", "Op"],
    }


def test_build_oauth_provider_config_uses_keycloak_endpoints(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_OAUTH_CLIENT_ID", "airflow-auth")
    monkeypatch.setenv("AIRFLOW_OAUTH_CLIENT_SECRET", "airflow-auth-secret")
    monkeypatch.setenv("AIRFLOW_OAUTH_ISSUER_URL", "http://localhost:8081/realms/vkr")
    monkeypatch.setenv("AIRFLOW_OAUTH_INTERNAL_ISSUER_URL", "http://host.docker.internal:8081/realms/vkr")
    get_keycloak_oauth_settings.cache_clear()

    config = build_oauth_provider_config()

    assert config["name"] == "keycloak"
    remote_app = config["remote_app"]
    assert remote_app["client_id"] == "airflow-auth"
    assert remote_app["client_secret"] == "airflow-auth-secret"
    assert remote_app["authorize_url"] == "http://localhost:8081/realms/vkr/protocol/openid-connect/auth"
    assert remote_app["access_token_url"] == "http://host.docker.internal:8081/realms/vkr/protocol/openid-connect/token"
    assert (
        remote_app["server_metadata_url"]
        == "http://host.docker.internal:8081/realms/vkr/.well-known/openid-configuration"
    )
    assert remote_app["issuer"] == "http://localhost:8081/realms/vkr"
    assert remote_app["jwks_uri"] == "http://host.docker.internal:8081/realms/vkr/protocol/openid-connect/certs"
