from __future__ import annotations

from datetime import datetime
import logging
from typing import Any, Callable

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from ingestion_airflow.config import ReplayRunConfig
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
    build_hashdiff_artifacts,
    build_object_store_config,
)
from ingestion_airflow.task_runtime import get_missing_return_value_tasks
from ingestion_core.contracts_client import ContractRegistryClient
from ingestion_core.hash_diff import ContractDefinition
from ingestion_core.hash_diff_pipeline import load_raw_snapshot, merge_raw_snapshot_to_curated
from ingestion_core.object_store import ObjectStoreClient
from ingestion_core.postgres import create_sqlalchemy_engine

REPLAY_STAGE_TASK_IDS = [
    "load_raw_replay",
    "merge_curated_replay",
]

logger = logging.getLogger(__name__)


def _derive_validation_manifest_key(accepted_object_key: str) -> str | None:
    normalized_key = accepted_object_key.strip().strip("/")
    accepted_suffix = "/land/accepted_snapshot.ndjson.gz"
    if not normalized_key.endswith(accepted_suffix):
        return None
    return normalized_key[: -len(accepted_suffix)] + "/validate/manifest.json"


@dag(
    dag_id="replay_contract_hashdiff_from_minio",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "hash-diff", "replay", "minio"],
)
def replay_contract_hashdiff_from_minio() -> None:
    def _load_run_config() -> ReplayRunConfig:
        context = get_current_context()
        dag_run = context["dag_run"]
        return ReplayRunConfig.from_dagrun_conf(dag_run.conf or {})

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

    @task
    def resolve_replay_input() -> dict[str, Any]:
        config = _load_run_config()
        object_store_config = build_object_store_config(config)
        object_store = ObjectStoreClient(object_store_config)

        accepted_object_key = config.accepted_object_key
        validation_manifest_key: str | None = None

        if config.parent_run_id:
            artifacts = build_hashdiff_artifacts(config.namespace, config.name, config.parent_run_id)
            accepted_object_key = artifacts["accepted_object_key"]
            validation_manifest_key = artifacts["validation_manifest_key"]
        elif accepted_object_key:
            validation_manifest_key = _derive_validation_manifest_key(accepted_object_key)

        if not accepted_object_key:
            raise ValueError("Replay accepted snapshot key could not be resolved")

        contract_version = config.contract_version
        if contract_version is None and validation_manifest_key:
            manifest_payload = object_store.get_json(validation_manifest_key)
            manifest_version = manifest_payload.get("contract_version")
            if isinstance(manifest_version, str) and manifest_version.strip():
                contract_version = manifest_version.strip()

        if contract_version is None:
            raise ValueError(
                "Replay requires contract_version or a resolvable validate manifest with contract_version"
            )

        return {
            "accepted_object_key": accepted_object_key,
            "validation_manifest_key": validation_manifest_key,
            "contract_version": contract_version,
            "parent_run_id": config.parent_run_id,
            "replay_reason": config.replay_reason,
        }

    @task
    def fetch_contract(replay_input: dict[str, Any]) -> dict[str, Any]:
        config = _load_run_config()
        token_provider = build_contracts_token_provider()
        client = ContractRegistryClient(
            config.contracts_service_url,
            token_provider=token_provider,
        )
        contract_payload = client.fetch_contract(
            namespace=config.namespace,
            name=config.name,
            version=str(replay_input["contract_version"]),
        )
        return contract_payload.to_dict()

    @task
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
                "target_table_raw": config.target_table_raw,
                "target_table_curated": config.target_table_curated,
                "accepted_object_key": str(replay_input["accepted_object_key"]),
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
                    "accepted_object_key": replay_input.get("accepted_object_key"),
                    "parent_run_id": replay_input.get("parent_run_id"),
                },
                error_text=str(exc),
            )
            if lock_acquired:
                release_pipeline_lock(audit_engine, config.pipeline_id, run_id)
            raise
        finally:
            audit_engine.dispose()

    @task
    def load_raw_replay(
        contract_payload: dict[str, Any],
        run_context: dict[str, Any],
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
                accepted_object_key=str(run_context["accepted_object_key"]),
                run_id=str(run_context["run_id"]),
                raw_load_batch_size=config.raw_load_batch_size,
            ).to_dict(),
        )

    @task
    def merge_curated_replay(
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

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize_replay(run_context: dict[str, Any]) -> None:
        config = _load_run_config()
        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        context = get_current_context()
        task_instance = context["ti"]

        failed_tasks = get_missing_return_value_tasks(task_instance, REPLAY_STAGE_TASK_IDS)

        merge_result = task_instance.xcom_pull(task_ids="merge_curated_replay") or {}

        try:
            if failed_tasks:
                finish_run_audit(
                    engine=audit_engine,
                    run_id=str(run_context["run_id"]),
                    status="failed",
                    read_count=int(merge_result.get("read_count", 0) or 0),
                    insert_count=int(merge_result.get("insert_count", 0) or 0),
                    update_count=int(merge_result.get("update_count", 0) or 0),
                    unchanged_count=int(merge_result.get("unchanged_count", 0) or 0),
                    metrics_json={
                        "mode": "replay",
                        "failed_tasks": failed_tasks,
                        "accepted_object_key": run_context.get("accepted_object_key"),
                        "parent_run_id": run_context.get("parent_run_id"),
                        "replayed_contract_version": run_context.get("replayed_contract_version"),
                        "replay_reason": run_context.get("replay_reason"),
                    },
                    error_text=f"Replay failed at stages: {', '.join(failed_tasks)}",
                )
                raise AirflowFailException(f"Replay failed at tasks: {', '.join(failed_tasks)}")

            finish_run_audit(
                engine=audit_engine,
                run_id=str(run_context["run_id"]),
                status="success",
                read_count=int(merge_result.get("read_count", 0) or 0),
                insert_count=int(merge_result.get("insert_count", 0) or 0),
                update_count=int(merge_result.get("update_count", 0) or 0),
                unchanged_count=int(merge_result.get("unchanged_count", 0) or 0),
                metrics_json={
                    **merge_result,
                    "mode": "replay",
                    "accepted_object_key": run_context.get("accepted_object_key"),
                    "validation_manifest_key": run_context.get("validation_manifest_key"),
                    "parent_run_id": run_context.get("parent_run_id"),
                    "replay_reason": run_context.get("replay_reason"),
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

    replay_input = resolve_replay_input()
    contract_payload = fetch_contract(replay_input)
    run_context = start_replay_run(contract_payload, replay_input)
    raw_result = load_raw_replay(contract_payload, run_context)
    merge_result = merge_curated_replay(contract_payload, run_context, raw_result)
    finalize_task = finalize_replay(run_context)

    raw_result >> finalize_task
    merge_result >> finalize_task


dag_instance = replay_contract_hashdiff_from_minio()
