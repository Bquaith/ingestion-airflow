from __future__ import annotations

from collections.abc import Callable
import os
from typing import Any

from ingestion_core.adapters.object_store import ObjectStoreConfig
from ingestion_core.adapters.oidc_sts import (
    OIDCClientCredentialsConfig,
    WebIdentitySTSConfig,
    exchange_client_credentials_for_sts,
    request_oidc_access_token,
)


def _parse_bool_env(value: str, field_name: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"{field_name} must be a boolean")


def build_contracts_token_provider() -> Callable[[], str] | None:
    token_url = (os.getenv("CONTRACTS_OIDC_TOKEN_URL") or "").strip()
    client_id = (os.getenv("CONTRACTS_OIDC_CLIENT_ID") or "").strip()
    client_secret = os.getenv("CONTRACTS_OIDC_CLIENT_SECRET") or ""
    scope = (os.getenv("CONTRACTS_OIDC_SCOPE") or "").strip() or None

    if not token_url or not client_id or not client_secret:
        return None

    verify_ssl = _parse_bool_env(
        os.getenv("CONTRACTS_OIDC_VERIFY_SSL", "true"),
        "CONTRACTS_OIDC_VERIFY_SSL",
    )

    def _provider() -> str:
        return request_oidc_access_token(
            OIDCClientCredentialsConfig(
                token_url=token_url,
                client_id=client_id,
                client_secret=client_secret,
                verify_ssl=verify_ssl,
                scope=scope,
            )
        )

    return _provider


def build_object_store_config(config: Any) -> ObjectStoreConfig:
    keycloak_token_url = (os.getenv("KEYCLOAK_TOKEN_URL") or "").strip()
    keycloak_client_id = (os.getenv("KEYCLOAK_CLIENT_ID") or "").strip()
    keycloak_client_secret = os.getenv("KEYCLOAK_CLIENT_SECRET") or ""
    minio_sts_endpoint = (os.getenv("MINIO_STS_ENDPOINT") or "").strip()

    if keycloak_token_url and keycloak_client_id and keycloak_client_secret and minio_sts_endpoint:
        sts_duration_seconds_raw = os.getenv("MINIO_STS_DURATION_SECONDS", "3600")
        try:
            sts_duration_seconds = int(sts_duration_seconds_raw)
        except ValueError as exc:
            raise ValueError("MINIO_STS_DURATION_SECONDS must be an integer") from exc
        if sts_duration_seconds <= 0:
            raise ValueError("MINIO_STS_DURATION_SECONDS must be greater than zero")

        credentials = exchange_client_credentials_for_sts(
            oidc_config=OIDCClientCredentialsConfig(
                token_url=keycloak_token_url,
                client_id=keycloak_client_id,
                client_secret=keycloak_client_secret,
            ),
            sts_config=WebIdentitySTSConfig(
                endpoint_url=minio_sts_endpoint,
                duration_seconds=sts_duration_seconds,
                verify_ssl=config.landing_s3_verify_ssl,
            ),
        )

        return ObjectStoreConfig(
            bucket=config.landing_s3_bucket,
            prefix=config.landing_s3_prefix,
            endpoint_url=config.landing_s3_endpoint_url,
            region_name=config.landing_s3_region,
            verify_ssl=config.landing_s3_verify_ssl,
            access_key_id=credentials.access_key_id,
            secret_access_key=credentials.secret_access_key,
            session_token=credentials.session_token,
        )

    access_key_id = os.getenv("AWS_ACCESS_KEY_ID") or None
    secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY") or None
    session_token = os.getenv("AWS_SESSION_TOKEN") or None

    if not access_key_id or not secret_access_key:
        raise ValueError(
            "Object store credentials are not configured. "
            "Set KEYCLOAK_TOKEN_URL/KEYCLOAK_CLIENT_ID/KEYCLOAK_CLIENT_SECRET/MINIO_STS_ENDPOINT "
            "for STS flow or provide AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY."
        )

    return ObjectStoreConfig(
        bucket=config.landing_s3_bucket,
        prefix=config.landing_s3_prefix,
        endpoint_url=config.landing_s3_endpoint_url,
        region_name=config.landing_s3_region,
        verify_ssl=config.landing_s3_verify_ssl,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
    )


def build_hashdiff_artifacts(namespace: str, name: str, run_id: str) -> dict[str, str]:
    base_key = f"{namespace}/{name}/run_id={run_id}"
    accepted_base_key = f"{base_key}/accepted"
    return {
        "accepted_object_key": f"{accepted_base_key}/accepted_snapshot.ndjson.gz",
        "validation_error_key": f"{accepted_base_key}/errors.ndjson.gz",
        "validation_manifest_key": f"{accepted_base_key}/manifest.json",
    }


def build_incremental_audit_artifacts(namespace: str, name: str, run_id: str) -> dict[str, str]:
    base_key = f"{namespace}/{name}/run_id={run_id}"
    delta_base_key = f"{base_key}/delta"
    return {
        "delta_object_key": f"{delta_base_key}/accepted_delta.ndjson.gz",
        "validation_error_key": f"{delta_base_key}/errors.ndjson.gz",
        "validation_manifest_key": f"{delta_base_key}/manifest.json",
    }
