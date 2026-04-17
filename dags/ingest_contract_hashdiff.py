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
from ingestion_core.adapters.postgres import create_sqlalchemy_engine
from ingestion_core.contracts import ContractDefinition, ContractRegistryClient
from ingestion_core.strategies.hash_diff import (
    extract_validate_land_snapshot,
    merge_accepted_snapshot_to_curated,
)


PIPELINE_TASK_IDS = [
    "fetch_contract",
    "read_checkpoint",
    "start_run",
    "extract_validate_land",
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

    @task(multiple_outputs=False)
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

    @task(multiple_outputs=False)
    def read_checkpoint() -> dict[str, Any]:
        config = _load_run_config()

        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        try:
            return read_pipeline_checkpoint(audit_engine, config.pipeline_id) or {}
        finally:
            audit_engine.dispose()

    @task(multiple_outputs=False)
    def start_run(
        contract_payload: dict[str, Any],
        checkpoint: dict[str, Any],
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

    @task(multiple_outputs=False)
    def extract_validate_land(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)
        object_store_config = build_object_store_config(config)
        artifacts = run_context["artifacts"]

        return _execute_stage(
            run_context["run_id"],
            "extract_validate_land",
            lambda: extract_validate_land_snapshot(
                source_dsn=config.source_dsn,
                source_table=config.source_table,
                contract=contract,
                object_store_config=object_store_config,
                accepted_object_key=artifacts["accepted_object_key"],
                error_object_key=artifacts["validation_error_key"],
                manifest_key=artifacts["validation_manifest_key"],
                source_batch_size=config.source_batch_size,
            ).to_dict(),
        )

    @task(multiple_outputs=False)
    def merge_curated(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
        accepted_result: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)
        object_store_config = build_object_store_config(config)

        def _merge() -> dict[str, Any]:
            result = merge_accepted_snapshot_to_curated(
                target_dsn=config.target_dsn,
                target_table_curated=config.target_table_curated,
                contract=contract,
                object_store_config=object_store_config,
                accepted_object_key=str(accepted_result["accepted_object_key"]),
                merge_load_batch_size=config.merge_load_batch_size,
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

    @task(multiple_outputs=False)
    def persist_checkpoint_task(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
        accepted_result: dict[str, Any],
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
                "accepted_object_key": accepted_result.get("accepted_object_key"),
                "validation_error_object_key": accepted_result.get("error_object_key"),
                "source_table": config.source_table,
                "target_table_curated": config.target_table_curated,
                "extract_validate_land": accepted_result,
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

    @task(trigger_rule=TriggerRule.ALL_DONE, multiple_outputs=False)
    def finalize_run(run_context: dict[str, Any]) -> dict[str, bool]:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        context = get_current_context()
        task_instance = context["ti"]

        failed_tasks = get_missing_return_value_tasks(task_instance, PIPELINE_TASK_IDS)

        merge_result = task_instance.xcom_pull(task_ids="merge_curated") or {}
        accepted_result = task_instance.xcom_pull(task_ids="extract_validate_land") or {}
        checkpoint_result = task_instance.xcom_pull(task_ids="persist_checkpoint_task") or {}

        try:
            if failed_tasks:
                finish_run_audit(
                    engine=audit_engine,
                    run_id=str(run_context["run_id"]),
                    status="failed",
                    read_count=int(accepted_result.get("source_row_count", 0) or 0),
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

        return {"finalized": True}

    contract_payload = fetch_contract()
    checkpoint = read_checkpoint()
    run_context = start_run(contract_payload, checkpoint)
    accepted_result = extract_validate_land(contract_payload, run_context)
    merge_result = merge_curated(contract_payload, run_context, accepted_result)
    checkpoint_result = persist_checkpoint_task(
        contract_payload,
        run_context,
        accepted_result,
        merge_result,
    )
    finalize_task = finalize_run(run_context)

    checkpoint_result >> finalize_task
    accepted_result >> finalize_task
    merge_result >> finalize_task


dag_instance = ingest_contract_hashdiff()
