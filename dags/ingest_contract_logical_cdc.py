from __future__ import annotations

from datetime import datetime
import logging
from typing import Any, Callable

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from ingestion_airflow.config import LogicalCdcRunConfig
from ingestion_airflow.db.audit import (
    acquire_pipeline_lock,
    finalize_pipeline_state,
    finish_run_audit,
    finish_stage_audit,
    persist_pipeline_checkpoint,
    read_pipeline_checkpoint,
    release_pipeline_lock,
    start_run_audit,
    start_stage_audit,
)
from ingestion_airflow.runtime import (
    build_contracts_token_provider,
    build_logical_cdc_artifacts,
    build_object_store_config,
)
from ingestion_airflow.task_runtime import get_missing_return_value_tasks
from ingestion_core.adapters.postgres import create_sqlalchemy_engine
from ingestion_core.contracts import ContractDefinition, ContractRegistryClient
from ingestion_core.strategies.logical_cdc import (
    ack_logical_replication_slot,
    apply_wal_delta_to_curated,
    checkpoint_lsn_from_payload,
    ensure_source_logical_cdc_capture,
    extract_validate_land_wal_delta,
)

PIPELINE_TASK_IDS = [
    "fetch_contract",
    "read_checkpoint",
    "start_run",
    "ensure_source_logical_cdc_capture",
    "extract_validate_land_wal_delta",
    "apply_delta",
    "persist_checkpoint_task",
    "ack_replication_slot",
]

logger = logging.getLogger(__name__)


@dag(
    dag_id="ingest_contract_logical_cdc",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "logical-cdc", "pgoutput"],
)
def ingest_contract_logical_cdc() -> None:
    def _load_run_config() -> LogicalCdcRunConfig:
        context = get_current_context()
        dag_run = context["dag_run"]
        return LogicalCdcRunConfig.from_dagrun_conf(dag_run.conf or {})

    def _build_run_context(
        config: LogicalCdcRunConfig,
        run_id: str,
        checkpoint: dict[str, Any] | None,
    ) -> dict[str, Any]:
        cdc_checkpoint = checkpoint_lsn_from_payload(checkpoint)
        return {
            "run_id": run_id,
            "pipeline_id": config.pipeline_id,
            "target_table_curated": config.target_table_curated,
            "previous_checkpoint_run_id": checkpoint.get("run_id") if checkpoint else None,
            "previous_checkpoint_updated_at": checkpoint.get("updated_at") if checkpoint else None,
            "start_lsn": cdc_checkpoint.last_applied_lsn,
            "artifacts": build_logical_cdc_artifacts(config.namespace, config.name, run_id),
        }

    def _execute_stage(
        run_id: str,
        stage_name: str,
        action: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        try:
            logger.info("Stage %s started for run_id=%s pipeline_id=%s", stage_name, run_id, config.pipeline_id)
            start_stage_audit(audit_engine, run_id, stage_name)
            result = action()
            logger.info("Stage %s succeeded for run_id=%s result=%s", stage_name, run_id, result)
            finish_stage_audit(audit_engine, run_id, stage_name, "success", result, None)
            return result
        except Exception as exc:
            logger.exception("Stage %s failed for run_id=%s: %s", stage_name, run_id, exc)
            finish_stage_audit(audit_engine, run_id, stage_name, "failed", None, str(exc))
            raise
        finally:
            audit_engine.dispose()

    @task(multiple_outputs=False)
    def fetch_contract() -> dict[str, Any]:
        config = _load_run_config()
        client = ContractRegistryClient(config.contracts_service_url, token_provider=build_contracts_token_provider())
        return client.fetch_contract(config.namespace, config.name, config.contract_version).to_dict()

    @task(multiple_outputs=False)
    def read_checkpoint() -> dict[str, Any]:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        try:
            return read_pipeline_checkpoint(audit_engine, config.pipeline_id) or {}
        finally:
            audit_engine.dispose()

    @task(multiple_outputs=False)
    def start_run(contract_payload: dict[str, Any], checkpoint: dict[str, Any]) -> dict[str, Any]:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        run_id = start_run_audit(
            audit_engine,
            config.pipeline_id,
            str(contract_payload.get("contract_id", "unknown-contract")),
            str(contract_payload.get("version", "unknown-version")),
            str(contract_payload.get("checksum", "unknown-checksum")),
        )
        lock_acquired = False
        try:
            lock_acquired = acquire_pipeline_lock(audit_engine, config.pipeline_id, run_id)
            if not lock_acquired:
                raise RuntimeError(f"Pipeline lock is already held for {config.pipeline_id}")
            return _build_run_context(config, run_id, checkpoint)
        except Exception as exc:
            finish_run_audit(audit_engine, run_id, "failed", 0, 0, 0, 0, {"failed_stage": "start_run"}, str(exc))
            if lock_acquired:
                release_pipeline_lock(audit_engine, config.pipeline_id, run_id)
            raise
        finally:
            audit_engine.dispose()

    @task(task_id="ensure_source_logical_cdc_capture", multiple_outputs=False)
    def ensure_source_logical_cdc_capture_task(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)
        if not config.auto_setup_logical_cdc:
            return _execute_stage(
                run_context["run_id"],
                "ensure_source_logical_cdc_capture",
                lambda: {"status": "skipped", "auto_setup_logical_cdc": False, "output_plugin": config.output_plugin},
            )

        return _execute_stage(
            run_context["run_id"],
            "ensure_source_logical_cdc_capture",
            lambda: {
                **ensure_source_logical_cdc_capture(
                    source_admin_dsn=str(config.source_admin_dsn),
                    source_table=config.source_table,
                    source_publication_name=config.source_publication_name,
                    source_slot_name=config.source_slot_name,
                    contract=contract,
                    output_plugin=config.output_plugin,
                    replace_existing_publication=config.replace_existing_publication,
                    create_slot_if_missing=config.create_slot_if_missing,
                    replica_identity_mode=config.replica_identity_mode,
                    auto_configure_wal_settings=config.auto_configure_wal_settings,
                    desired_max_replication_slots=config.desired_max_replication_slots,
                    desired_max_wal_senders=config.desired_max_wal_senders,
                ).to_dict(),
                "status": "configured",
            },
        )

    @task(task_id="extract_validate_land_wal_delta", multiple_outputs=False)
    def extract_validate_land_wal_delta_task(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)
        object_store_config = build_object_store_config(config)
        artifacts = run_context["artifacts"]

        return _execute_stage(
            run_context["run_id"],
            "extract_validate_land_wal_delta",
            lambda: extract_validate_land_wal_delta(
                source_dsn=config.source_dsn,
                source_replication_dsn=config.source_replication_dsn,
                source_table=config.source_table,
                source_slot_name=config.source_slot_name,
                source_publication_name=config.source_publication_name,
                contract=contract,
                object_store_config=object_store_config,
                delta_object_key=artifacts["delta_object_key"],
                error_object_key=artifacts["validation_error_key"],
                manifest_key=artifacts["validation_manifest_key"],
                start_lsn=run_context.get("start_lsn"),
                max_extract_seconds=config.max_extract_seconds,
                idle_timeout_seconds=config.idle_timeout_seconds,
                output_plugin=config.output_plugin,
            ).to_dict(),
        )

    @task(task_id="apply_delta", multiple_outputs=False)
    def apply_delta_task(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
        delta_result: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)
        object_store_config = build_object_store_config(config)
        return _execute_stage(
            run_context["run_id"],
            "apply_delta",
            lambda: apply_wal_delta_to_curated(
                target_dsn=config.target_dsn,
                target_table_curated=config.target_table_curated,
                contract=contract,
                object_store_config=object_store_config,
                delta_object_key=str(delta_result["delta_object_key"]),
                load_batch_size=config.apply_load_batch_size,
                upsert_batch_size=config.upsert_batch_size,
            ).to_dict(),
        )

    @task(multiple_outputs=False)
    def persist_checkpoint_task(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
        ensure_result: dict[str, Any],
        delta_result: dict[str, Any],
        apply_result: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)

        def _persist() -> dict[str, Any]:
            last_applied_lsn = (
                apply_result.get("last_applied_lsn")
                or delta_result.get("last_decoded_lsn")
                or delta_result.get("window_end_lsn")
                or run_context.get("start_lsn")
            )
            checkpoint_payload = {
                "status": "success",
                "strategy": "logical_cdc",
                "pipeline_id": config.pipeline_id,
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "checksum": contract.checksum,
                "previous_checkpoint_run_id": run_context.get("previous_checkpoint_run_id"),
                "previous_checkpoint_updated_at": run_context.get("previous_checkpoint_updated_at"),
                "source_table": config.source_table,
                "source_publication_name": config.source_publication_name,
                "source_slot_name": config.source_slot_name,
                "output_plugin": config.output_plugin,
                "target_table_curated": config.target_table_curated,
                "delta_object_key": delta_result.get("delta_object_key"),
                "validation_error_object_key": delta_result.get("error_object_key"),
                "last_applied_lsn": last_applied_lsn,
                "last_flushed_lsn": last_applied_lsn,
                "extract_validate_land_wal_delta": delta_result,
                "ensure_source_logical_cdc_capture": ensure_result,
                "apply_delta": apply_result,
                "last_completed_run_id": run_context["run_id"],
            }
            audit_engine = create_sqlalchemy_engine(config.audit_dsn)
            try:
                persist_pipeline_checkpoint(audit_engine, config.pipeline_id, str(run_context["run_id"]), checkpoint_payload)
            finally:
                audit_engine.dispose()
            return checkpoint_payload

        return _execute_stage(run_context["run_id"], "persist_checkpoint", _persist)

    @task(task_id="ack_replication_slot", multiple_outputs=False)
    def ack_replication_slot_task(run_context: dict[str, Any], checkpoint_result: dict[str, Any]) -> dict[str, Any]:
        config = _load_run_config()
        flush_lsn = checkpoint_result.get("last_flushed_lsn")
        if not flush_lsn:
            return {"status": "skipped", "reason": "no_lsn_to_flush"}
        return _execute_stage(
            run_context["run_id"],
            "ack_replication_slot",
            lambda: ack_logical_replication_slot(
                source_replication_dsn=config.source_replication_dsn,
                source_slot_name=config.source_slot_name,
                source_publication_name=config.source_publication_name,
                flush_lsn=str(flush_lsn),
            ),
        )

    @task(trigger_rule=TriggerRule.ALL_DONE, multiple_outputs=False)
    def finalize_run(run_context: dict[str, Any]) -> dict[str, bool]:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        context = get_current_context()
        task_instance = context["ti"]
        failed_tasks = get_missing_return_value_tasks(task_instance, PIPELINE_TASK_IDS)
        delta_result = task_instance.xcom_pull(task_ids="extract_validate_land_wal_delta") or {}
        apply_result = task_instance.xcom_pull(task_ids="apply_delta") or {}

        try:
            if failed_tasks:
                finish_run_audit(
                    audit_engine,
                    str(run_context["run_id"]),
                    "failed",
                    int(delta_result.get("source_event_count", 0) or 0),
                    int(apply_result.get("insert_count", 0) or 0),
                    int(apply_result.get("update_count", 0) or 0),
                    int(apply_result.get("unchanged_count", 0) or 0),
                    {"failed_tasks": failed_tasks},
                    f"Pipeline failed at stages: {', '.join(failed_tasks)}",
                )
                raise AirflowFailException(f"Pipeline failed at tasks: {', '.join(failed_tasks)}")

            finish_run_audit(
                audit_engine,
                str(run_context["run_id"]),
                "success",
                int(apply_result.get("read_count", 0) or 0),
                int(apply_result.get("insert_count", 0) or 0),
                int(apply_result.get("update_count", 0) or 0),
                int(apply_result.get("unchanged_count", 0) or 0),
                apply_result,
                None,
            )
        finally:
            release_pipeline_lock(audit_engine, config.pipeline_id, str(run_context["run_id"]))
            finalize_pipeline_state(audit_engine, config.pipeline_id)
            audit_engine.dispose()
        return {"finalized": True}

    contract_payload = fetch_contract()
    checkpoint = read_checkpoint()
    run_context = start_run(contract_payload, checkpoint)
    ensure_result = ensure_source_logical_cdc_capture_task(contract_payload, run_context)
    delta_result = extract_validate_land_wal_delta_task(contract_payload, run_context)
    apply_result = apply_delta_task(contract_payload, run_context, delta_result)
    checkpoint_result = persist_checkpoint_task(contract_payload, run_context, ensure_result, delta_result, apply_result)
    ack_result = ack_replication_slot_task(run_context, checkpoint_result)
    finalize_task = finalize_run(run_context)

    ensure_result >> finalize_task
    delta_result >> finalize_task
    apply_result >> finalize_task
    checkpoint_result >> finalize_task
    ack_result >> finalize_task


dag_instance = ingest_contract_logical_cdc()
