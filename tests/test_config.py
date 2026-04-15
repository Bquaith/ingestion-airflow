from __future__ import annotations

import pytest

from ingestion_airflow.config import IngestionRunConfig, ReplayRunConfig
from ingestion_airflow.runtime import build_hashdiff_artifacts


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
        "landing_s3_bucket": "integration-landing",
    }


def test_config_uses_default_batch_sizes() -> None:
    config = IngestionRunConfig.from_dagrun_conf(_base_conf())

    assert config.audit_dsn == "postgresql+psycopg2://audit_user:audit_pass@postgres_audit:5432/audit_db"
    assert config.landing_s3_prefix == "accepted"
    assert config.source_batch_size == 1000
    assert config.merge_load_batch_size == 1000
    assert config.upsert_batch_size == 1000


def test_config_validates_positive_batch_sizes() -> None:
    conf = _base_conf()
    conf["source_batch_size"] = "0"

    with pytest.raises(ValueError, match="greater than zero"):
        IngestionRunConfig.from_dagrun_conf(conf)


def test_config_validates_batch_size_type() -> None:
    conf = _base_conf()
    conf["merge_load_batch_size"] = "not-int"

    with pytest.raises(ValueError, match="must be integers"):
        IngestionRunConfig.from_dagrun_conf(conf)


def test_config_requires_audit_database_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUDIT_DATABASE_DSN")

    with pytest.raises(ValueError, match="AUDIT_DATABASE_DSN"):
        IngestionRunConfig.from_dagrun_conf(_base_conf())


def test_config_requires_landing_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    conf = _base_conf()
    conf.pop("landing_s3_bucket")
    monkeypatch.delenv("LANDING_S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="landing_s3_bucket"):
        IngestionRunConfig.from_dagrun_conf(conf)


def test_config_parses_landing_boolean_override() -> None:
    conf = _base_conf()
    conf["landing_s3_verify_ssl"] = "false"

    config = IngestionRunConfig.from_dagrun_conf(conf)

    assert config.landing_s3_verify_ssl is False


def test_replay_config_requires_exactly_one_replay_input() -> None:
    conf = {
        "contracts_service_url": "http://contracts.local",
        "namespace": "sales",
        "name": "orders",
        "accepted_object_key": "accepted/orders.ndjson.gz",
        "parent_run_id": "run-id",
        "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
        "target_table_curated": "curated.orders",
        "landing_s3_bucket": "integration-landing",
    }

    with pytest.raises(ValueError, match="exactly one"):
        ReplayRunConfig.from_dagrun_conf(conf)


def test_replay_config_uses_default_merge_batch_sizes() -> None:
    conf = {
        "contracts_service_url": "http://contracts.local",
        "namespace": "sales",
        "name": "orders",
        "accepted_object_key": "accepted/orders.ndjson.gz",
        "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
        "target_table_curated": "curated.orders",
        "landing_s3_bucket": "integration-landing",
    }

    config = ReplayRunConfig.from_dagrun_conf(conf)

    assert config.merge_load_batch_size == 1000
    assert config.source_batch_size == 1000
    assert config.upsert_batch_size == 1000


def test_build_hashdiff_artifacts_points_to_accepted_snapshot_layout() -> None:
    artifacts = build_hashdiff_artifacts("sales", "orders", "run-123")

    assert artifacts == {
        "accepted_object_key": "sales/orders/run_id=run-123/accepted/accepted_snapshot.ndjson.gz",
        "validation_error_key": "sales/orders/run_id=run-123/accepted/errors.ndjson.gz",
        "validation_manifest_key": "sales/orders/run_id=run-123/accepted/manifest.json",
    }
