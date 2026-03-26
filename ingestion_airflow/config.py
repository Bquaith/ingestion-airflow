from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Mapping


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
    target_table_raw: str
    target_table_curated: str
    landing_s3_bucket: str
    landing_s3_prefix: str
    landing_s3_endpoint_url: str | None
    landing_s3_region: str | None
    landing_s3_verify_ssl: bool
    source_batch_size: int
    raw_load_batch_size: int
    upsert_batch_size: int

    @property
    def pipeline_id(self) -> str:
        return f"{self.namespace}.{self.name}"

    @staticmethod
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

    @classmethod
    def from_dagrun_conf(cls, conf: Mapping[str, Any]) -> "IngestionRunConfig":
        required = [
            "namespace",
            "name",
            "source_dsn",
            "source_table",
            "target_dsn",
            "target_table_curated",
        ]
        missing = [key for key in required if not conf.get(key)]
        contracts_service_url = conf.get("contracts_service_url") or os.getenv("CONTRACTS_SERVICE_URL")
        if not contracts_service_url:
            missing.append("contracts_service_url")
        audit_dsn = os.getenv("AUDIT_DATABASE_DSN")
        if not audit_dsn:
            missing.append("AUDIT_DATABASE_DSN")
        landing_s3_bucket = conf.get("landing_s3_bucket") or os.getenv("LANDING_S3_BUCKET")
        if not landing_s3_bucket:
            missing.append("landing_s3_bucket or LANDING_S3_BUCKET")
        if missing:
            joined = ", ".join(sorted(missing))
            raise ValueError(f"Missing required configuration values: {joined}")

        contract_version = conf.get("contract_version")
        if contract_version is not None:
            contract_version = str(contract_version)

        source_batch_size = conf.get("source_batch_size", 1000)
        raw_load_batch_size = conf.get("raw_load_batch_size", 1000)
        upsert_batch_size = conf.get("upsert_batch_size", 1000)

        try:
            source_batch_size = int(source_batch_size)
            raw_load_batch_size = int(raw_load_batch_size)
            upsert_batch_size = int(upsert_batch_size)
        except (TypeError, ValueError) as exc:
            raise ValueError("source_batch_size, raw_load_batch_size and upsert_batch_size must be integers") from exc
        if source_batch_size <= 0 or raw_load_batch_size <= 0 or upsert_batch_size <= 0:
            raise ValueError("source_batch_size, raw_load_batch_size and upsert_batch_size must be greater than zero")

        landing_s3_prefix = str(conf.get("landing_s3_prefix") or os.getenv("LANDING_S3_PREFIX") or "accepted")
        landing_s3_endpoint_url = conf.get("landing_s3_endpoint_url") or os.getenv("LANDING_S3_ENDPOINT_URL")
        landing_s3_region = conf.get("landing_s3_region") or os.getenv("LANDING_S3_REGION")
        landing_s3_verify_ssl = cls._parse_bool(
            conf.get("landing_s3_verify_ssl", os.getenv("LANDING_S3_VERIFY_SSL", "true")),
            "landing_s3_verify_ssl",
        )

        target_table_raw = conf.get("target_table_raw")
        if target_table_raw:
            target_table_raw = str(target_table_raw)
        else:
            target_table_raw = f'raw.{str(conf["namespace"])}__{str(conf["name"])}'

        return cls(
            contracts_service_url=str(contracts_service_url),
            audit_dsn=str(audit_dsn),
            namespace=str(conf["namespace"]),
            name=str(conf["name"]),
            contract_version=contract_version,
            source_dsn=str(conf["source_dsn"]),
            source_table=str(conf["source_table"]),
            target_dsn=str(conf["target_dsn"]),
            target_table_raw=target_table_raw,
            target_table_curated=str(conf["target_table_curated"]),
            landing_s3_bucket=str(landing_s3_bucket),
            landing_s3_prefix=landing_s3_prefix,
            landing_s3_endpoint_url=str(landing_s3_endpoint_url) if landing_s3_endpoint_url else None,
            landing_s3_region=str(landing_s3_region) if landing_s3_region else None,
            landing_s3_verify_ssl=landing_s3_verify_ssl,
            source_batch_size=source_batch_size,
            raw_load_batch_size=raw_load_batch_size,
            upsert_batch_size=upsert_batch_size,
        )
