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
    target_table_curated: str
    source_batch_size: int
    upsert_batch_size: int

    @property
    def pipeline_id(self) -> str:
        return f"{self.namespace}.{self.name}"

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
        if missing:
            joined = ", ".join(sorted(missing))
            raise ValueError(f"Missing required configuration values: {joined}")

        contract_version = conf.get("contract_version")
        if contract_version is not None:
            contract_version = str(contract_version)

        source_batch_size = conf.get("source_batch_size", 1000)
        upsert_batch_size = conf.get("upsert_batch_size", 1000)

        try:
            source_batch_size = int(source_batch_size)
            upsert_batch_size = int(upsert_batch_size)
        except (TypeError, ValueError) as exc:
            raise ValueError("source_batch_size and upsert_batch_size must be integers") from exc
        if source_batch_size <= 0 or upsert_batch_size <= 0:
            raise ValueError("source_batch_size and upsert_batch_size must be greater than zero")

        return cls(
            contracts_service_url=str(contracts_service_url),
            audit_dsn=str(audit_dsn),
            namespace=str(conf["namespace"]),
            name=str(conf["name"]),
            contract_version=contract_version,
            source_dsn=str(conf["source_dsn"]),
            source_table=str(conf["source_table"]),
            target_dsn=str(conf["target_dsn"]),
            target_table_curated=str(conf["target_table_curated"]),
            source_batch_size=source_batch_size,
            upsert_batch_size=upsert_batch_size,
        )
