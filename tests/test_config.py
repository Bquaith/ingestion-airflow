from __future__ import annotations

import pytest

from ingestion_airflow.config import (
    IncrementalAuditReplayRunConfig,
    IncrementalAuditRunConfig,
    IngestionRunConfig,
    LogicalCdcReplayRunConfig,
    LogicalCdcRunConfig,
    ReplayRunConfig,
)
from ingestion_airflow.runtime import (
    build_hashdiff_artifacts,
    build_incremental_audit_artifacts,
    build_logical_cdc_artifacts,
)


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


def _incremental_base_conf() -> dict[str, str]:
    conf = _base_conf()
    conf["source_audit_table"] = "ingestion_meta.orders_audit"
    return conf


def _logical_cdc_base_conf() -> dict[str, str]:
    conf = _base_conf()
    conf["source_replication_dsn"] = "postgresql://source_user:source_pass@postgres_source:5432/source_db"
    conf["source_publication_name"] = "pub_ingestion_sales_orders"
    conf["source_slot_name"] = "slot_ingestion_sales_orders"
    return conf


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


def test_incremental_config_uses_default_batch_sizes_and_flags() -> None:
    config = IncrementalAuditRunConfig.from_dagrun_conf(_incremental_base_conf())

    assert config.extract_batch_size == 1000
    assert config.apply_load_batch_size == 1000
    assert config.upsert_batch_size == 1000
    assert config.auto_setup_audit is False
    assert config.replace_existing_trigger is False
    assert config.watermark_mode == "auto"


def test_incremental_config_requires_admin_dsn_for_auto_setup() -> None:
    conf = _incremental_base_conf()
    conf["auto_setup_audit"] = "true"

    with pytest.raises(ValueError, match="source_admin_dsn"):
        IncrementalAuditRunConfig.from_dagrun_conf(conf)


def test_incremental_config_accepts_valid_watermark_mode() -> None:
    conf = _incremental_base_conf()
    conf["watermark_mode"] = "recorded_at"

    config = IncrementalAuditRunConfig.from_dagrun_conf(conf)

    assert config.watermark_mode == "recorded_at"


def test_incremental_replay_config_requires_parent_run_id() -> None:
    conf = {
        "contracts_service_url": "http://contracts.local",
        "namespace": "sales",
        "name": "orders",
        "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
        "target_table_curated": "curated.orders",
        "landing_s3_bucket": "integration-landing",
    }

    with pytest.raises(ValueError, match="parent_run_id"):
        IncrementalAuditReplayRunConfig.from_dagrun_conf(conf)


def test_incremental_replay_config_uses_parent_run_id_only() -> None:
    conf = {
        "contracts_service_url": "http://contracts.local",
        "namespace": "sales",
        "name": "orders",
        "parent_run_id": "run-id",
        "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
        "target_table_curated": "curated.orders",
        "landing_s3_bucket": "integration-landing",
    }

    config = IncrementalAuditReplayRunConfig.from_dagrun_conf(conf)

    assert config.parent_run_id == "run-id"


def test_incremental_replay_config_rejects_delta_object_key() -> None:
    conf = {
        "contracts_service_url": "http://contracts.local",
        "namespace": "sales",
        "name": "orders",
        "parent_run_id": "run-id",
        "delta_object_key": "delta/orders.ndjson.gz",
        "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
        "target_table_curated": "curated.orders",
        "landing_s3_bucket": "integration-landing",
    }

    with pytest.raises(ValueError, match="delta_object_key is not supported"):
        IncrementalAuditReplayRunConfig.from_dagrun_conf(conf)


def test_build_incremental_artifacts_points_to_delta_layout() -> None:
    artifacts = build_incremental_audit_artifacts("sales", "orders", "run-123")

    assert artifacts == {
        "delta_object_key": "sales/orders/run_id=run-123/delta/accepted_delta.ndjson.gz",
        "validation_error_key": "sales/orders/run_id=run-123/delta/errors.ndjson.gz",
        "validation_manifest_key": "sales/orders/run_id=run-123/delta/manifest.json",
    }


def test_logical_cdc_config_uses_pgoutput_defaults() -> None:
    config = LogicalCdcRunConfig.from_dagrun_conf(_logical_cdc_base_conf())

    assert config.output_plugin == "pgoutput"
    assert config.auto_setup_logical_cdc is False
    assert config.auto_configure_wal_settings is False
    assert config.create_slot_if_missing is True
    assert config.replace_existing_publication is False
    assert config.replica_identity_mode == "default"
    assert config.desired_max_replication_slots == 10
    assert config.desired_max_wal_senders == 10
    assert config.max_extract_seconds == 30
    assert config.idle_timeout_seconds is None
    assert config.apply_load_batch_size == 1000
    assert config.upsert_batch_size == 1000


def test_logical_cdc_config_accepts_wal_auto_configure_settings() -> None:
    conf = _logical_cdc_base_conf()
    conf["auto_configure_wal_settings"] = "true"
    conf["desired_max_replication_slots"] = "12"
    conf["desired_max_wal_senders"] = "8"

    config = LogicalCdcRunConfig.from_dagrun_conf(conf)

    assert config.auto_configure_wal_settings is True
    assert config.desired_max_replication_slots == 12
    assert config.desired_max_wal_senders == 8


def test_logical_cdc_config_accepts_idle_timeout() -> None:
    conf = _logical_cdc_base_conf()
    conf["idle_timeout_seconds"] = "3"

    config = LogicalCdcRunConfig.from_dagrun_conf(conf)

    assert config.idle_timeout_seconds == 3


def test_logical_cdc_config_rejects_invalid_idle_timeout() -> None:
    conf = _logical_cdc_base_conf()
    conf["idle_timeout_seconds"] = "0"

    with pytest.raises(ValueError, match="idle_timeout_seconds"):
        LogicalCdcRunConfig.from_dagrun_conf(conf)


def test_logical_cdc_config_rejects_wal2json() -> None:
    conf = _logical_cdc_base_conf()
    conf["output_plugin"] = "wal2json"

    with pytest.raises(ValueError, match="wal2json is not supported"):
        LogicalCdcRunConfig.from_dagrun_conf(conf)


def test_logical_cdc_config_rejects_sqlalchemy_replication_dsn() -> None:
    conf = _logical_cdc_base_conf()
    conf["source_replication_dsn"] = "postgresql+psycopg2://source_user:source_pass@postgres_source:5432/source_db"

    with pytest.raises(ValueError, match="postgresql://"):
        LogicalCdcRunConfig.from_dagrun_conf(conf)


def test_logical_cdc_config_requires_admin_dsn_for_auto_setup() -> None:
    conf = _logical_cdc_base_conf()
    conf["auto_setup_logical_cdc"] = "true"

    with pytest.raises(ValueError, match="source_admin_dsn"):
        LogicalCdcRunConfig.from_dagrun_conf(conf)


def test_logical_cdc_replay_config_uses_parent_run_id_only() -> None:
    conf = {
        "contracts_service_url": "http://contracts.local",
        "namespace": "sales",
        "name": "orders",
        "parent_run_id": "run-id",
        "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
        "target_table_curated": "curated.orders",
        "landing_s3_bucket": "integration-landing",
    }

    config = LogicalCdcReplayRunConfig.from_dagrun_conf(conf)

    assert config.parent_run_id == "run-id"
    assert config.apply_load_batch_size == 1000
    assert config.upsert_batch_size == 1000


def test_logical_cdc_replay_config_rejects_delta_object_key() -> None:
    conf = {
        "contracts_service_url": "http://contracts.local",
        "namespace": "sales",
        "name": "orders",
        "parent_run_id": "run-id",
        "delta_object_key": "wal_delta/orders.ndjson.gz",
        "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
        "target_table_curated": "curated.orders",
        "landing_s3_bucket": "integration-landing",
    }

    with pytest.raises(ValueError, match="delta_object_key is not supported"):
        LogicalCdcReplayRunConfig.from_dagrun_conf(conf)


def test_build_logical_cdc_artifacts_points_to_wal_delta_layout() -> None:
    artifacts = build_logical_cdc_artifacts("sales", "orders", "run-123")

    assert artifacts == {
        "delta_object_key": "sales/orders/run_id=run-123/wal_delta/accepted_delta.ndjson.gz",
        "validation_error_key": "sales/orders/run_id=run-123/wal_delta/errors.ndjson.gz",
        "validation_manifest_key": "sales/orders/run_id=run-123/wal_delta/manifest.json",
    }
