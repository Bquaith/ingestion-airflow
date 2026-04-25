from __future__ import annotations

from datetime import datetime
import logging
from typing import Any, Callable

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from ingestion_airflow.config import IncrementalAuditRunConfig
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
    build_incremental_audit_artifacts,
    build_object_store_config,
)
from ingestion_airflow.task_runtime import get_missing_return_value_tasks
from ingestion_core.adapters.postgres import create_sqlalchemy_engine
from ingestion_core.contracts import ContractDefinition, ContractRegistryClient
from ingestion_core.strategies.incremental_audit import (
    apply_delta_to_curated,
    checkpoint_watermark_from_payload,
    ensure_source_audit_capture,
    extract_validate_land_delta,
)

PIPELINE_TASK_IDS = [
    "fetch_contract",
    "read_checkpoint",
    "start_run",
    "ensure_source_audit_capture",
    "extract_validate_land_delta",
    "apply_delta",
    "persist_checkpoint_task",
]

logger = logging.getLogger(__name__)


@dag(
    dag_id="ingest_contract_incremental_audit",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "incremental-audit", "cdc"],
)
def ingest_contract_incremental_audit() -> None:
    def _load_run_config() -> IncrementalAuditRunConfig:
        context = get_current_context()
        dag_run = context["dag_run"]
        return IncrementalAuditRunConfig.from_dagrun_conf(dag_run.conf or {})

    def _build_run_context(
        config: IncrementalAuditRunConfig,
        run_id: str,
        checkpoint: dict[str, Any] | None,
    ) -> dict[str, Any]:
        start_watermark = checkpoint_watermark_from_payload(checkpoint)
        return {
            "run_id": run_id,
            "pipeline_id": config.pipeline_id,
            "target_table_curated": config.target_table_curated,
            "previous_checkpoint_run_id": checkpoint.get("run_id") if checkpoint else None,
            "previous_checkpoint_updated_at": checkpoint.get("updated_at") if checkpoint else None,
            "start_watermark": start_watermark.to_dict() if start_watermark else None,
            "artifacts": build_incremental_audit_artifacts(config.namespace, config.name, run_id),
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
        client = ContractRegistryClient(config.contracts_service_url, token_provider=token_provider)
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

    @task(task_id="ensure_source_audit_capture", multiple_outputs=False)
    def ensure_source_audit_capture_task(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)

        if not config.auto_setup_audit:
            return _execute_stage(
                run_context["run_id"],
                "ensure_source_audit_capture",
                lambda: {
                    "status": "skipped",
                    "auto_setup_audit": False,
                    "watermark_mode": config.watermark_mode,
                },
            )

        def _ensure() -> dict[str, Any]:
            result = ensure_source_audit_capture(
                source_admin_dsn=str(config.source_admin_dsn),
                source_table=config.source_table,
                source_audit_table=config.source_audit_table,
                contract=contract,
                watermark_mode=config.watermark_mode,
                replace_existing_trigger=config.replace_existing_trigger,
            )
            payload = result.to_dict()
            payload["status"] = "configured"
            payload["auto_setup_audit"] = True
            return payload

        return _execute_stage(run_context["run_id"], "ensure_source_audit_capture", _ensure)

    @task(task_id="extract_validate_land_delta", multiple_outputs=False)
    def extract_validate_land_delta_task(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        config = _load_run_config()
        contract = ContractDefinition.from_registry_payload(contract_payload)
        object_store_config = build_object_store_config(config)
        artifacts = run_context["artifacts"]
        start_watermark = checkpoint_watermark_from_payload({"last_applied_watermark": run_context.get("start_watermark")})

        return _execute_stage(
            run_context["run_id"],
            "extract_validate_land_delta",
            lambda: extract_validate_land_delta(
                source_dsn=config.source_dsn,
                source_audit_table=config.source_audit_table,
                contract=contract,
                object_store_config=object_store_config,
                delta_object_key=artifacts["delta_object_key"],
                error_object_key=artifacts["validation_error_key"],
                manifest_key=artifacts["validation_manifest_key"],
                extract_batch_size=config.extract_batch_size,
                start_watermark=start_watermark,
                watermark_mode=config.watermark_mode,
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

        def _apply() -> dict[str, Any]:
            result = apply_delta_to_curated(
                target_dsn=config.target_dsn,
                target_table_curated=config.target_table_curated,
                contract=contract,
                object_store_config=object_store_config,
                delta_object_key=str(delta_result["delta_object_key"]),
                load_batch_size=config.apply_load_batch_size,
                upsert_batch_size=config.upsert_batch_size,
            )
            return result.to_dict()

        return _execute_stage(run_context["run_id"], "apply_delta", _apply)

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
            checkpoint_payload = {
                "status": "success",
                "strategy": "incremental_audit",
                "pipeline_id": config.pipeline_id,
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "checksum": contract.checksum,
                "previous_checkpoint_run_id": run_context.get("previous_checkpoint_run_id"),
                "previous_checkpoint_updated_at": run_context.get("previous_checkpoint_updated_at"),
                "source_table": config.source_table,
                "source_audit_table": config.source_audit_table,
                "target_table_curated": config.target_table_curated,
                "delta_object_key": delta_result.get("delta_object_key"),
                "validation_error_object_key": delta_result.get("error_object_key"),
                "invalid_event_count": delta_result.get("invalid_event_count"),
                "window_start": delta_result.get("window_start"),
                "window_end": delta_result.get("window_end"),
                "last_applied_watermark": delta_result.get("window_end") or run_context.get("start_watermark"),
                "watermark_mode": delta_result.get("watermark_mode"),
                "ensure_source_audit_capture": ensure_result,
                "extract_validate_land_delta": delta_result,
                "apply_delta": apply_result,
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
        delta_result = task_instance.xcom_pull(task_ids="extract_validate_land_delta") or {}
        apply_result = task_instance.xcom_pull(task_ids="apply_delta") or {}
        checkpoint_result = task_instance.xcom_pull(task_ids="persist_checkpoint_task") or {}

        try:
            if failed_tasks:
                finish_run_audit(
                    engine=audit_engine,
                    run_id=str(run_context["run_id"]),
                    status="failed",
                    read_count=int(delta_result.get("source_event_count", 0) or 0),
                    insert_count=int(apply_result.get("insert_count", 0) or 0),
                    update_count=int(apply_result.get("update_count", 0) or 0),
                    unchanged_count=int(apply_result.get("unchanged_count", 0) or 0),
                    metrics_json={
                        "failed_tasks": failed_tasks,
                        "checkpoint_persisted": bool(checkpoint_result),
                    },
                    error_text=f"Pipeline failed at stages: {', '.join(failed_tasks)}",
                )
                raise AirflowFailException(f"Pipeline failed at tasks: {', '.join(failed_tasks)}")

            finish_run_audit(
                engine=audit_engine,
                run_id=str(run_context["run_id"]),
                status="success",
                read_count=int(apply_result.get("read_count", 0) or 0),
                insert_count=int(apply_result.get("insert_count", 0) or 0),
                update_count=int(apply_result.get("update_count", 0) or 0),
                unchanged_count=int(apply_result.get("unchanged_count", 0) or 0),
                metrics_json={
                    **apply_result,
                    "source_event_count": delta_result.get("source_event_count"),
                    "invalid_event_count": delta_result.get("invalid_event_count"),
                    "validation_error_object_key": delta_result.get("error_object_key"),
                    "delta_object_key": delta_result.get("delta_object_key"),
                    "window_start": delta_result.get("window_start"),
                    "window_end": delta_result.get("window_end"),
                    "checkpoint_persisted": bool(checkpoint_result),
                },
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
    ensure_result = ensure_source_audit_capture_task(contract_payload, run_context)
    delta_result = extract_validate_land_delta_task(contract_payload, run_context)
    apply_result = apply_delta_task(contract_payload, run_context, delta_result)
    checkpoint_result = persist_checkpoint_task(
        contract_payload,
        run_context,
        ensure_result,
        delta_result,
        apply_result,
    )
    finalize_task = finalize_run(run_context)

    ensure_result >> finalize_task
    delta_result >> finalize_task
    apply_result >> finalize_task
    checkpoint_result >> finalize_task


dag_instance = ingest_contract_incremental_audit()
