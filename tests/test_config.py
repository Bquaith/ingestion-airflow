from __future__ import annotations

import pytest

from ingestion_airflow.config import IngestionRunConfig


@pytest.fixture(autouse=True)
def _set_audit_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "AUDIT_DATABASE_DSN",
        "postgresql+psycopg2://audit_user:audit_pass@postgres_audit:5432/audit_db",
    )


def _base_conf() -> dict[str, str]:
    return {
        "contracts_service_url": "http://contracts.local",
        "namespace": "sales",
        "name": "orders",
        "source_dsn": "postgresql+psycopg2://source_user:source_pass@postgres_source:5432/source_db",
        "source_table": "public.orders",
        "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
        "target_table_curated": "curated.orders",
    }


def test_config_uses_default_batch_sizes() -> None:
    config = IngestionRunConfig.from_dagrun_conf(_base_conf())

    assert config.audit_dsn == "postgresql+psycopg2://audit_user:audit_pass@postgres_audit:5432/audit_db"
    assert config.source_batch_size == 1000
    assert config.upsert_batch_size == 1000


def test_config_validates_positive_batch_sizes() -> None:
    conf = _base_conf()
    conf["source_batch_size"] = "0"

    with pytest.raises(ValueError, match="greater than zero"):
        IngestionRunConfig.from_dagrun_conf(conf)


def test_config_validates_batch_size_type() -> None:
    conf = _base_conf()
    conf["upsert_batch_size"] = "not-int"

    with pytest.raises(ValueError, match="must be integers"):
        IngestionRunConfig.from_dagrun_conf(conf)


def test_config_requires_audit_database_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUDIT_DATABASE_DSN")

    with pytest.raises(ValueError, match="AUDIT_DATABASE_DSN"):
        IngestionRunConfig.from_dagrun_conf(_base_conf())
