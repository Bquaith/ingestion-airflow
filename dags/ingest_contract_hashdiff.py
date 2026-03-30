from __future__ import annotations

from datetime import datetime
import logging
from typing import Any, Callable

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from ingestion_airflow.config import IngestionRunConfig
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
    build_hashdiff_artifacts,
    build_object_store_config,
)
from ingestion_airflow.task_runtime import get_missing_return_value_tasks
from ingestion_core.contracts_client import ContractRegistryClient
from ingestion_core.hash_diff import ContractDefinition
from ingestion_core.hash_diff_pipeline import (
    extract_source_snapshot,
    land_validated_snapshot,
    load_raw_snapshot,
    merge_raw_snapshot_to_curated,
    validate_extracted_snapshot,
)
from ingestion_core.postgres import create_sqlalchemy_engine


PIPELINE_TASK_IDS = [
    "fetch_contract",
    "read_checkpoint",
    "start_run",
    "extract_snapshot",
    "validate_snapshot",
    "land_snapshot",
    "load_raw",
    "merge_curated",
    "persist_checkpoint_task",
]

logger = logging.getLogger(__name__)


@dag(
    dag_id="ingest_contract_hashdiff",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "hash-diff", "staged"],
)
def ingest_contract_hashdiff() -> None:
    def _load_run_config() -> IngestionRunConfig:
        context = get_current_context()
        dag_run = context["dag_run"]
        return IngestionRunConfig.from_dagrun_conf(dag_run.conf or {})

    def _build_run_context(
        config: IngestionRunConfig,
        run_id: str,
        checkpoint: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "pipeline_id": config.pipeline_id,
            "target_table_raw": config.target_table_raw,
            "target_table_curated": config.target_table_curated,
            "previous_checkpoint_run_id": checkpoint.get("run_id") if checkpoint else None,
            "previous_checkpoint_updated_at": checkpoint.get("updated_at") if checkpoint else None,
            "artifacts": build_hashdiff_artifacts(config.namespace, config.name, run_id),
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
            logger.info(
                "Stage %s succeeded for run_id=%s pipeline_id=%s result=%s",
                stage_name,
                run_id,
                config.pipeline_id,
                result,
            )
            finish_stage_audit(
                audit_engine,
                run_id=run_id,
                stage_name=stage_name,
                status="success",
                metrics_json=result,
                error_text=None,
            )
            return result
        except Exception as exc:
            logger.exception(
                "Stage %s failed for run_id=%s pipeline_id=%s: %s",
                stage_name,
                run_id,
                config.pipeline_id,
                exc,
            )
            finish_stage_audit(
                audit_engine,
                run_id=run_id,
                stage_name=stage_name,
                status="failed",
                metrics_json=None,
                error_text=str(exc),
            )
            raise
        finally:
            audit_engine.dispose()

    @task
    def fetch_contract() -> dict[str, Any]:
        config = _load_run_config()
        token_provider = build_contracts_token_provider()

        client = ContractRegistryClient(
            config.contracts_service_url,
            token_provider=token_provider,
        )
        contract_payload = client.fetch_contract(
            namespace=config.namespace,
            name=config.name,
            version=config.contract_version,
        )
        return contract_payload.to_dict()

    @task
    def read_checkpoint() -> dict[str, Any] | None:
        config = _load_run_config()

        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        try:
            return read_pipeline_checkpoint(audit_engine, config.pipeline_id)
        finally:
            audit_engine.dispose()

    @task
    def start_run(
        contract_payload: dict[str, Any],
        checkpoint: dict[str, Any] | None,
    ) -> dict[str, Any]:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)

        run_id = start_run_audit(
            engine=audit_engine,
            pipeline_id=config.pipeline_id,
            contract_id=str(contract_payload.get("contract_id", "unknown-contract")),
            version=str(contract_payload.get("version", "unknown-version")),
            checksum=str(contract_payload.get("checksum", "unknown-checksum")),
        )
        lock_acquired = False

        try:
            lock_acquired = acquire_pipeline_lock(
                engine=audit_engine,
                pipeline_id=config.pipeline_id,
                run_id=run_id,
            )
            if not lock_acquired:
                raise RuntimeError(
                    f"Pipeline lock is already held for {config.pipeline_id}; concurrent run is not allowed"
                )
            return _build_run_context(config, run_id, checkpoint)
        except Exception as exc:
            finish_run_audit(
                engine=audit_engine,
                run_id=run_id,
                status="failed",
                read_count=0,
                insert_count=0,
                update_count=0,
                unchanged_count=0,
                metrics_json={"failed_stage": "start_run"},
                error_text=str(exc),
            )
            if lock_acquired:
                release_pipeline_lock(audit_engine, config.pipeline_id, run_id)
            raise
        finally:
            audit_engine.dispose()

    @task
    def extract_snapshot(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)
        object_store_config = build_object_store_config(config)
        artifacts = run_context["artifacts"]

        return _execute_stage(
            run_context["run_id"],
            "extract",
            lambda: extract_source_snapshot(
                source_dsn=config.source_dsn,
                source_table=config.source_table,
                contract=contract,
                object_store_config=object_store_config,
                extracted_object_key=artifacts["extract_object_key"],
                manifest_key=artifacts["extract_manifest_key"],
                source_batch_size=config.source_batch_size,
            ).to_dict(),
        )

    @task
    def validate_snapshot(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
        extract_result: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)
        object_store_config = build_object_store_config(config)
        artifacts = run_context["artifacts"]

        return _execute_stage(
            run_context["run_id"],
            "validate",
            lambda: validate_extracted_snapshot(
                contract=contract,
                object_store_config=object_store_config,
                extracted_object_key=str(extract_result["object_key"]),
                validated_object_key=artifacts["validated_object_key"],
                error_object_key=artifacts["validation_error_key"],
                manifest_key=artifacts["validation_manifest_key"],
            ).to_dict(),
        )

    @task
    def land_snapshot(run_context: dict[str, Any], validation_result: dict[str, Any]) -> dict[str, Any]:
        config = _load_run_config()
        object_store_config = build_object_store_config(config)
        artifacts = run_context["artifacts"]

        return _execute_stage(
            run_context["run_id"],
            "land",
            lambda: land_validated_snapshot(
                object_store_config=object_store_config,
                staged_validated_object_key=str(validation_result["validated_object_key"]),
                accepted_object_key=artifacts["accepted_object_key"],
                manifest_key=artifacts["landing_manifest_key"],
                row_count=int(validation_result["valid_row_count"]),
            ).to_dict(),
        )

    @task
    def load_raw(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
        landed_result: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)
        object_store_config = build_object_store_config(config)

        return _execute_stage(
            run_context["run_id"],
            "load_raw",
            lambda: load_raw_snapshot(
                target_dsn=config.target_dsn,
                target_table_raw=config.target_table_raw,
                contract=contract,
                object_store_config=object_store_config,
                accepted_object_key=str(landed_result["accepted_object_key"]),
                run_id=str(run_context["run_id"]),
                raw_load_batch_size=config.raw_load_batch_size,
            ).to_dict(),
        )

    @task
    def merge_curated(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
        raw_result: dict[str, Any],
    ) -> dict[str, Any]:
        del raw_result
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)

        def _merge() -> dict[str, Any]:
            result = merge_raw_snapshot_to_curated(
                target_dsn=config.target_dsn,
                target_table_raw=config.target_table_raw,
                target_table_curated=config.target_table_curated,
                contract=contract,
                run_id=str(run_context["run_id"]),
                source_batch_size=config.source_batch_size,
                upsert_batch_size=config.upsert_batch_size,
            )
            return {
                "read_count": result.read_count,
                "insert_count": result.insert_count,
                "update_count": result.update_count,
                "delete_count": result.delete_count,
                "unchanged_count": result.unchanged_count,
                "processed_batches": result.processed_batches,
                "source_read_seconds": result.source_read_seconds,
                "diff_seconds": result.diff_seconds,
                "write_seconds": result.write_seconds,
                "total_seconds": result.total_seconds,
            }

        return _execute_stage(run_context["run_id"], "merge_curated", _merge)

    @task
    def persist_checkpoint_task(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
        extract_result: dict[str, Any],
        validation_result: dict[str, Any],
        landed_result: dict[str, Any],
        raw_result: dict[str, Any],
        merge_result: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)

        def _persist() -> dict[str, Any]:
            checkpoint_payload = {
                "status": "success",
                "strategy": "hash_diff",
                "pipeline_id": config.pipeline_id,
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "checksum": contract.checksum,
                "previous_checkpoint_run_id": run_context.get("previous_checkpoint_run_id"),
                "previous_checkpoint_updated_at": run_context.get("previous_checkpoint_updated_at"),
                "extract_object_key": extract_result.get("object_key"),
                "validated_object_key": validation_result.get("validated_object_key"),
                "validation_error_object_key": validation_result.get("error_object_key"),
                "accepted_object_key": landed_result.get("accepted_object_key"),
                "source_table": config.source_table,
                "target_table_raw": config.target_table_raw,
                "target_table_curated": config.target_table_curated,
                "extract": extract_result,
                "validate": validation_result,
                "land": landed_result,
                "load_raw": raw_result,
                "merge": merge_result,
                "last_completed_run_id": run_context["run_id"],
            }

            audit_engine = create_sqlalchemy_engine(config.audit_dsn)
            try:
                persist_pipeline_checkpoint(
                    engine=audit_engine,
                    pipeline_id=config.pipeline_id,
                    run_id=str(run_context["run_id"]),
                    checkpoint_payload=checkpoint_payload,
                )
            finally:
                audit_engine.dispose()

            return checkpoint_payload

        return _execute_stage(run_context["run_id"], "persist_checkpoint", _persist)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize_run(run_context: dict[str, Any]) -> None:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        context = get_current_context()
        task_instance = context["ti"]

        failed_tasks = get_missing_return_value_tasks(task_instance, PIPELINE_TASK_IDS)

        merge_result = task_instance.xcom_pull(task_ids="merge_curated") or {}
        extract_result = task_instance.xcom_pull(task_ids="extract_snapshot") or {}
        checkpoint_result = task_instance.xcom_pull(task_ids="persist_checkpoint_task") or {}

        try:
            if failed_tasks:
                finish_run_audit(
                    engine=audit_engine,
                    run_id=str(run_context["run_id"]),
                    status="failed",
                    read_count=int(extract_result.get("row_count", 0) or 0),
                    insert_count=int(merge_result.get("insert_count", 0) or 0),
                    update_count=int(merge_result.get("update_count", 0) or 0),
                    unchanged_count=int(merge_result.get("unchanged_count", 0) or 0),
                    metrics_json={
                        "failed_tasks": failed_tasks,
                        "checkpoint_persisted": bool(checkpoint_result),
                    },
                    error_text=f"Pipeline failed at stages: {', '.join(failed_tasks)}",
                )
                raise AirflowFailException(f"Pipeline failed at tasks: {', '.join(failed_tasks)}")
            else:
                finish_run_audit(
                    engine=audit_engine,
                    run_id=str(run_context["run_id"]),
                    status="success",
                    read_count=int(merge_result.get("read_count", 0) or 0),
                    insert_count=int(merge_result.get("insert_count", 0) or 0),
                    update_count=int(merge_result.get("update_count", 0) or 0),
                    unchanged_count=int(merge_result.get("unchanged_count", 0) or 0),
                    metrics_json=merge_result,
                    error_text=None,
                )
        finally:
            release_pipeline_lock(
                engine=audit_engine,
                pipeline_id=config.pipeline_id,
                run_id=str(run_context["run_id"]),
            )
            finalize_pipeline_state(audit_engine, config.pipeline_id)
            audit_engine.dispose()

    contract_payload = fetch_contract()
    checkpoint = read_checkpoint()
    run_context = start_run(contract_payload, checkpoint)
    extract_result = extract_snapshot(contract_payload, run_context)
    validation_result = validate_snapshot(contract_payload, run_context, extract_result)
    landed_result = land_snapshot(run_context, validation_result)
    raw_result = load_raw(contract_payload, run_context, landed_result)
    merge_result = merge_curated(contract_payload, run_context, raw_result)
    checkpoint_result = persist_checkpoint_task(
        contract_payload,
        run_context,
        extract_result,
        validation_result,
        landed_result,
        raw_result,
        merge_result,
    )
    finalize_task = finalize_run(run_context)

    checkpoint_result >> finalize_task
    validation_result >> finalize_task
    landed_result >> finalize_task
    raw_result >> finalize_task
    merge_result >> finalize_task


dag_instance = ingest_contract_hashdiff()
