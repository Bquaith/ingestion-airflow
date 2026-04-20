from __future__ import annotations

from datetime import datetime
import logging
from typing import Any, Callable

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from ingestion_airflow.config import IncrementalAuditReplayRunConfig
from ingestion_airflow.db.audit import (
    acquire_pipeline_lock,
    finalize_pipeline_state,
    finish_run_audit,
    finish_stage_audit,
    release_pipeline_lock,
    start_run_audit,
    start_stage_audit,
)
from ingestion_airflow.runtime import (
    build_contracts_token_provider,
    build_incremental_audit_artifacts,
    build_object_store_config,
    derive_incremental_audit_manifest_key,
)
from ingestion_airflow.task_runtime import get_missing_return_value_tasks
from ingestion_core.adapters.object_store import ObjectStoreClient
from ingestion_core.adapters.postgres import create_sqlalchemy_engine
from ingestion_core.contracts import ContractDefinition, ContractRegistryClient
from ingestion_core.strategies.incremental_audit import apply_delta_to_curated

REPLAY_STAGE_TASK_IDS = [
    "apply_delta_replay",
]

logger = logging.getLogger(__name__)


@dag(
    dag_id="replay_contract_incremental_audit_from_minio",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "incremental-audit", "replay", "minio"],
)
def replay_contract_incremental_audit_from_minio() -> None:
    def _load_run_config() -> IncrementalAuditReplayRunConfig:
        context = get_current_context()
        dag_run = context["dag_run"]
        return IncrementalAuditReplayRunConfig.from_dagrun_conf(dag_run.conf or {})

    def _execute_stage(
        run_id: str,
        stage_name: str,
        action: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        try:
            logger.info("Replay stage %s started for run_id=%s pipeline_id=%s", stage_name, run_id, config.pipeline_id)
            start_stage_audit(audit_engine, run_id, stage_name)
            result = action()
            logger.info(
                "Replay stage %s succeeded for run_id=%s pipeline_id=%s result=%s",
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
                "Replay stage %s failed for run_id=%s pipeline_id=%s: %s",
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
    def resolve_replay_input() -> dict[str, Any]:
        config = _load_run_config()
        object_store_config = build_object_store_config(config)
        object_store = ObjectStoreClient(object_store_config)

        delta_object_key = config.delta_object_key
        validation_manifest_key: str | None = None

        if config.parent_run_id:
            artifacts = build_incremental_audit_artifacts(config.namespace, config.name, config.parent_run_id)
            delta_object_key = artifacts["delta_object_key"]
            validation_manifest_key = artifacts["validation_manifest_key"]
        elif delta_object_key:
            validation_manifest_key = derive_incremental_audit_manifest_key(delta_object_key)

        if not delta_object_key:
            raise ValueError("Replay delta key could not be resolved")

        contract_version = config.contract_version
        if contract_version is None and validation_manifest_key:
            manifest_payload = object_store.get_json(validation_manifest_key)
            manifest_version = manifest_payload.get("contract_version")
            if isinstance(manifest_version, str) and manifest_version.strip():
                contract_version = manifest_version.strip()

        if contract_version is None:
            raise ValueError("Replay requires contract_version or a resolvable manifest with contract_version")

        return {
            "delta_object_key": delta_object_key,
            "validation_manifest_key": validation_manifest_key,
            "contract_version": contract_version,
            "parent_run_id": config.parent_run_id,
            "replay_reason": config.replay_reason,
        }

    @task(multiple_outputs=False)
    def fetch_contract(replay_input: dict[str, Any]) -> dict[str, Any]:
        config = _load_run_config()
        token_provider = build_contracts_token_provider()
        client = ContractRegistryClient(config.contracts_service_url, token_provider=token_provider)
        contract_payload = client.fetch_contract(
            namespace=config.namespace,
            name=config.name,
            version=str(replay_input["contract_version"]),
        )
        return contract_payload.to_dict()

    @task(multiple_outputs=False)
    def start_replay_run(
        contract_payload: dict[str, Any],
        replay_input: dict[str, Any],
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
            return {
                "run_id": run_id,
                "pipeline_id": config.pipeline_id,
                "target_table_curated": config.target_table_curated,
                "delta_object_key": str(replay_input["delta_object_key"]),
                "validation_manifest_key": replay_input.get("validation_manifest_key"),
                "parent_run_id": replay_input.get("parent_run_id"),
                "replay_reason": replay_input.get("replay_reason"),
                "replayed_contract_version": replay_input.get("contract_version"),
            }
        except Exception as exc:
            finish_run_audit(
                engine=audit_engine,
                run_id=run_id,
                status="failed",
                read_count=0,
                insert_count=0,
                update_count=0,
                unchanged_count=0,
                metrics_json={
                    "failed_stage": "start_replay_run",
                    "mode": "replay",
                    "delta_object_key": replay_input.get("delta_object_key"),
                    "parent_run_id": replay_input.get("parent_run_id"),
                },
                error_text=str(exc),
            )
            if lock_acquired:
                release_pipeline_lock(audit_engine, config.pipeline_id, run_id)
            raise
        finally:
            audit_engine.dispose()

    @task(task_id="apply_delta_replay", multiple_outputs=False)
    def apply_delta_replay_task(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
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
                delta_object_key=str(run_context["delta_object_key"]),
                load_batch_size=config.apply_load_batch_size,
                upsert_batch_size=config.upsert_batch_size,
            )
            return result.to_dict()

        return _execute_stage(run_context["run_id"], "apply_delta", _apply)

    @task(trigger_rule=TriggerRule.ALL_DONE, multiple_outputs=False)
    def finalize_replay(run_context: dict[str, Any]) -> dict[str, bool]:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        context = get_current_context()
        task_instance = context["ti"]

        failed_tasks = get_missing_return_value_tasks(task_instance, REPLAY_STAGE_TASK_IDS)
        apply_result = task_instance.xcom_pull(task_ids="apply_delta_replay") or {}

        try:
            if failed_tasks:
                finish_run_audit(
                    engine=audit_engine,
                    run_id=str(run_context["run_id"]),
                    status="failed",
                    read_count=int(apply_result.get("read_count", 0) or 0),
                    insert_count=int(apply_result.get("insert_count", 0) or 0),
                    update_count=int(apply_result.get("update_count", 0) or 0),
                    unchanged_count=int(apply_result.get("unchanged_count", 0) or 0),
                    metrics_json={
                        "mode": "replay",
                        "failed_tasks": failed_tasks,
                        "delta_object_key": run_context.get("delta_object_key"),
                        "parent_run_id": run_context.get("parent_run_id"),
                    },
                    error_text=f"Replay pipeline failed at stages: {', '.join(failed_tasks)}",
                )
                raise AirflowFailException(f"Replay pipeline failed at tasks: {', '.join(failed_tasks)}")

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
                    "mode": "replay",
                    "delta_object_key": run_context.get("delta_object_key"),
                    "parent_run_id": run_context.get("parent_run_id"),
                    "replayed_contract_version": run_context.get("replayed_contract_version"),
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

    replay_input = resolve_replay_input()
    contract_payload = fetch_contract(replay_input)
    run_context = start_replay_run(contract_payload, replay_input)
    apply_result = apply_delta_replay_task(contract_payload, run_context)
    finalize_task = finalize_replay(run_context)

    apply_result >> finalize_task


dag_instance = replay_contract_incremental_audit_from_minio()
