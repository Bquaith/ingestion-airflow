from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Mapping


_WATERMARK_MODES = {"auto", "commit_timestamp", "recorded_at"}


def _parse_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y"}:
            return True
        if lowered in {"0", "false", "no", "n"}:
            return False
    raise ValueError(f"{field_name} must be a boolean")


def _resolve_required(
    conf: Mapping[str, Any],
    field_name: str,
    env_name: str | None = None,
) -> str:
    value = conf.get(field_name)
    if value is None and env_name:
        value = os.getenv(env_name)
    if value is None or not str(value).strip():
        if env_name is None or env_name == field_name:
            hint = field_name
        else:
            hint = f"{field_name} or {env_name}"
        raise ValueError(f"Missing required configuration values: {hint}")
    return str(value).strip()


def _resolve_optional(
    conf: Mapping[str, Any],
    field_name: str,
    env_name: str | None = None,
) -> str | None:
    value = conf.get(field_name)
    if value is None and env_name:
        value = os.getenv(env_name)
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _require_fields(conf: Mapping[str, Any], required_fields: list[str]) -> None:
    missing = [field for field in required_fields if not conf.get(field)]
    if missing:
        raise ValueError(f"Missing required configuration values: {', '.join(sorted(missing))}")


def _parse_positive_ints(conf: Mapping[str, Any], field_defaults: Mapping[str, int]) -> dict[str, int]:
    parsed: dict[str, int] = {}
    try:
        for field_name, default_value in field_defaults.items():
            parsed[field_name] = int(conf.get(field_name, default_value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{', '.join(field_defaults)} must be integers") from exc

    invalid = [field_name for field_name, value in parsed.items() if value <= 0]
    if invalid:
        raise ValueError(f"{', '.join(invalid)} must be greater than zero")
    return parsed


def _parse_contract_version(conf: Mapping[str, Any]) -> str | None:
    value = conf.get("contract_version")
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _parse_landing_config(conf: Mapping[str, Any]) -> tuple[str, str, str | None, str | None, bool]:
    landing_s3_bucket = _resolve_required(conf, "landing_s3_bucket", "LANDING_S3_BUCKET")
    landing_s3_prefix = _resolve_optional(conf, "landing_s3_prefix", "LANDING_S3_PREFIX") or "accepted"
    landing_s3_endpoint_url = _resolve_optional(conf, "landing_s3_endpoint_url", "LANDING_S3_ENDPOINT_URL")
    landing_s3_region = _resolve_optional(conf, "landing_s3_region", "LANDING_S3_REGION")
    landing_s3_verify_ssl = _parse_bool(
        conf.get("landing_s3_verify_ssl", os.getenv("LANDING_S3_VERIFY_SSL", "true")),
        "landing_s3_verify_ssl",
    )
    return (
        landing_s3_bucket,
        landing_s3_prefix,
        landing_s3_endpoint_url,
        landing_s3_region,
        landing_s3_verify_ssl,
    )


def _parse_replay_input(
    conf: Mapping[str, Any],
    object_key_field_name: str,
) -> tuple[str | None, str | None]:
    object_key = conf.get(object_key_field_name)
    if object_key is not None:
        object_key = str(object_key).strip() or None

    parent_run_id = conf.get("parent_run_id")
    if parent_run_id is not None:
        parent_run_id = str(parent_run_id).strip() or None

    if bool(object_key) == bool(parent_run_id):
        raise ValueError(f"Provide exactly one of {object_key_field_name} or parent_run_id")

    return object_key, parent_run_id


def _parse_watermark_mode(conf: Mapping[str, Any]) -> str:
    value = _resolve_optional(conf, "watermark_mode") or "auto"
    if value not in _WATERMARK_MODES:
        raise ValueError(f"watermark_mode must be one of: {', '.join(sorted(_WATERMARK_MODES))}")
    return value


@dataclass(frozen=True)
class IngestionRunConfig:
    contracts_service_url: str
    audit_dsn: str
    namespace: str
    name: str
    contract_version: str | None
    source_dsn: str
    source_table: str
    target_dsn: str
    target_table_curated: str
    landing_s3_bucket: str
    landing_s3_prefix: str
    landing_s3_endpoint_url: str | None
    landing_s3_region: str | None
    landing_s3_verify_ssl: bool
    source_batch_size: int
    merge_load_batch_size: int
    upsert_batch_size: int

    @property
    def pipeline_id(self) -> str:
        return f"{self.namespace}.{self.name}"

    @classmethod
    def from_dagrun_conf(cls, conf: Mapping[str, Any]) -> "IngestionRunConfig":
        _require_fields(
            conf,
            [
                "namespace",
                "name",
                "source_dsn",
                "source_table",
                "target_dsn",
                "target_table_curated",
            ],
        )
        batch_sizes = _parse_positive_ints(
            conf,
            {
                "source_batch_size": 1000,
                "merge_load_batch_size": 1000,
                "upsert_batch_size": 1000,
            },
        )
        (
            landing_s3_bucket,
            landing_s3_prefix,
            landing_s3_endpoint_url,
            landing_s3_region,
            landing_s3_verify_ssl,
        ) = _parse_landing_config(conf)

        return cls(
            contracts_service_url=_resolve_required(conf, "contracts_service_url", "CONTRACTS_SERVICE_URL"),
            audit_dsn=_resolve_required({}, "AUDIT_DATABASE_DSN", "AUDIT_DATABASE_DSN"),
            namespace=str(conf["namespace"]),
            name=str(conf["name"]),
            contract_version=_parse_contract_version(conf),
            source_dsn=str(conf["source_dsn"]),
            source_table=str(conf["source_table"]),
            target_dsn=str(conf["target_dsn"]),
            target_table_curated=str(conf["target_table_curated"]),
            landing_s3_bucket=landing_s3_bucket,
            landing_s3_prefix=landing_s3_prefix,
            landing_s3_endpoint_url=landing_s3_endpoint_url,
            landing_s3_region=landing_s3_region,
            landing_s3_verify_ssl=landing_s3_verify_ssl,
            source_batch_size=batch_sizes["source_batch_size"],
            merge_load_batch_size=batch_sizes["merge_load_batch_size"],
            upsert_batch_size=batch_sizes["upsert_batch_size"],
        )


@dataclass(frozen=True)
class ReplayRunConfig:
    contracts_service_url: str
    audit_dsn: str
    namespace: str
    name: str
    contract_version: str | None
    accepted_object_key: str | None
    parent_run_id: str | None
    replay_reason: str | None
    target_dsn: str
    target_table_curated: str
    landing_s3_bucket: str
    landing_s3_prefix: str
    landing_s3_endpoint_url: str | None
    landing_s3_region: str | None
    landing_s3_verify_ssl: bool
    merge_load_batch_size: int
    source_batch_size: int
    upsert_batch_size: int

    @property
    def pipeline_id(self) -> str:
        return f"{self.namespace}.{self.name}"

    @classmethod
    def from_dagrun_conf(cls, conf: Mapping[str, Any]) -> "ReplayRunConfig":
        _require_fields(
            conf,
            [
                "namespace",
                "name",
                "target_dsn",
                "target_table_curated",
            ],
        )
        batch_sizes = _parse_positive_ints(
            conf,
            {
                "merge_load_batch_size": 1000,
                "source_batch_size": 1000,
                "upsert_batch_size": 1000,
            },
        )
        (
            landing_s3_bucket,
            landing_s3_prefix,
            landing_s3_endpoint_url,
            landing_s3_region,
            landing_s3_verify_ssl,
        ) = _parse_landing_config(conf)
        accepted_object_key, parent_run_id = _parse_replay_input(conf, "accepted_object_key")
        replay_reason = _resolve_optional(conf, "replay_reason")

        return cls(
            contracts_service_url=_resolve_required(conf, "contracts_service_url", "CONTRACTS_SERVICE_URL"),
            audit_dsn=_resolve_required({}, "AUDIT_DATABASE_DSN", "AUDIT_DATABASE_DSN"),
            namespace=str(conf["namespace"]),
            name=str(conf["name"]),
            contract_version=_parse_contract_version(conf),
            accepted_object_key=accepted_object_key,
            parent_run_id=parent_run_id,
            replay_reason=replay_reason,
            target_dsn=str(conf["target_dsn"]),
            target_table_curated=str(conf["target_table_curated"]),
            landing_s3_bucket=landing_s3_bucket,
            landing_s3_prefix=landing_s3_prefix,
            landing_s3_endpoint_url=landing_s3_endpoint_url,
            landing_s3_region=landing_s3_region,
            landing_s3_verify_ssl=landing_s3_verify_ssl,
            merge_load_batch_size=batch_sizes["merge_load_batch_size"],
            source_batch_size=batch_sizes["source_batch_size"],
            upsert_batch_size=batch_sizes["upsert_batch_size"],
        )


@dataclass(frozen=True)
class IncrementalAuditRunConfig:
    contracts_service_url: str
    audit_dsn: str
    namespace: str
    name: str
    contract_version: str | None
    source_dsn: str
    source_table: str
    source_admin_dsn: str | None
    source_audit_table: str
    target_dsn: str
    target_table_curated: str
    landing_s3_bucket: str
    landing_s3_prefix: str
    landing_s3_endpoint_url: str | None
    landing_s3_region: str | None
    landing_s3_verify_ssl: bool
    extract_batch_size: int
    apply_load_batch_size: int
    upsert_batch_size: int
    auto_setup_audit: bool
    replace_existing_trigger: bool
    watermark_mode: str

    @property
    def pipeline_id(self) -> str:
        return f"{self.namespace}.{self.name}"

    @classmethod
    def from_dagrun_conf(cls, conf: Mapping[str, Any]) -> "IncrementalAuditRunConfig":
        _require_fields(
            conf,
            [
                "namespace",
                "name",
                "source_dsn",
                "source_table",
                "source_audit_table",
                "target_dsn",
                "target_table_curated",
            ],
        )
        batch_sizes = _parse_positive_ints(
            conf,
            {
                "extract_batch_size": 1000,
                "apply_load_batch_size": 1000,
                "upsert_batch_size": 1000,
            },
        )
        (
            landing_s3_bucket,
            landing_s3_prefix,
            landing_s3_endpoint_url,
            landing_s3_region,
            landing_s3_verify_ssl,
        ) = _parse_landing_config(conf)
        auto_setup_audit = _parse_bool(conf.get("auto_setup_audit", "false"), "auto_setup_audit")
        source_admin_dsn = _resolve_optional(conf, "source_admin_dsn", "SOURCE_ADMIN_DSN")
        if auto_setup_audit and not source_admin_dsn:
            raise ValueError("Missing required configuration values: source_admin_dsn or SOURCE_ADMIN_DSN")

        return cls(
            contracts_service_url=_resolve_required(conf, "contracts_service_url", "CONTRACTS_SERVICE_URL"),
            audit_dsn=_resolve_required({}, "AUDIT_DATABASE_DSN", "AUDIT_DATABASE_DSN"),
            namespace=str(conf["namespace"]),
            name=str(conf["name"]),
            contract_version=_parse_contract_version(conf),
            source_dsn=str(conf["source_dsn"]),
            source_table=str(conf["source_table"]),
            source_admin_dsn=source_admin_dsn,
            source_audit_table=str(conf["source_audit_table"]),
            target_dsn=str(conf["target_dsn"]),
            target_table_curated=str(conf["target_table_curated"]),
            landing_s3_bucket=landing_s3_bucket,
            landing_s3_prefix=landing_s3_prefix,
            landing_s3_endpoint_url=landing_s3_endpoint_url,
            landing_s3_region=landing_s3_region,
            landing_s3_verify_ssl=landing_s3_verify_ssl,
            extract_batch_size=batch_sizes["extract_batch_size"],
            apply_load_batch_size=batch_sizes["apply_load_batch_size"],
            upsert_batch_size=batch_sizes["upsert_batch_size"],
            auto_setup_audit=auto_setup_audit,
            replace_existing_trigger=_parse_bool(
                conf.get("replace_existing_trigger", "false"),
                "replace_existing_trigger",
            ),
            watermark_mode=_parse_watermark_mode(conf),
        )


@dataclass(frozen=True)
class IncrementalAuditReplayRunConfig:
    contracts_service_url: str
    audit_dsn: str
    namespace: str
    name: str
    contract_version: str | None
    parent_run_id: str
    replay_reason: str | None
    target_dsn: str
    target_table_curated: str
    landing_s3_bucket: str
    landing_s3_prefix: str
    landing_s3_endpoint_url: str | None
    landing_s3_region: str | None
    landing_s3_verify_ssl: bool
    apply_load_batch_size: int
    upsert_batch_size: int

    @property
    def pipeline_id(self) -> str:
        return f"{self.namespace}.{self.name}"

    @classmethod
    def from_dagrun_conf(cls, conf: Mapping[str, Any]) -> "IncrementalAuditReplayRunConfig":
        _require_fields(
            conf,
            [
                "namespace",
                "name",
                "parent_run_id",
                "target_dsn",
                "target_table_curated",
            ],
        )
        batch_sizes = _parse_positive_ints(
            conf,
            {
                "apply_load_batch_size": 1000,
                "upsert_batch_size": 1000,
            },
        )
        (
            landing_s3_bucket,
            landing_s3_prefix,
            landing_s3_endpoint_url,
            landing_s3_region,
            landing_s3_verify_ssl,
        ) = _parse_landing_config(conf)
        parent_run_id = str(conf["parent_run_id"]).strip()
        if not parent_run_id:
            raise ValueError("Missing required configuration values: parent_run_id")
        replay_reason = _resolve_optional(conf, "replay_reason")

        return cls(
            contracts_service_url=_resolve_required(conf, "contracts_service_url", "CONTRACTS_SERVICE_URL"),
            audit_dsn=_resolve_required({}, "AUDIT_DATABASE_DSN", "AUDIT_DATABASE_DSN"),
            namespace=str(conf["namespace"]),
            name=str(conf["name"]),
            contract_version=_parse_contract_version(conf),
            parent_run_id=parent_run_id,
            replay_reason=replay_reason,
            target_dsn=str(conf["target_dsn"]),
            target_table_curated=str(conf["target_table_curated"]),
            landing_s3_bucket=landing_s3_bucket,
            landing_s3_prefix=landing_s3_prefix,
            landing_s3_endpoint_url=landing_s3_endpoint_url,
            landing_s3_region=landing_s3_region,
            landing_s3_verify_ssl=landing_s3_verify_ssl,
            apply_load_batch_size=batch_sizes["apply_load_batch_size"],
            upsert_batch_size=batch_sizes["upsert_batch_size"],
        )
