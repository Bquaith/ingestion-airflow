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
    read_stage_audit_metrics,
    release_pipeline_lock,
    start_run_audit,
    start_stage_audit,
)
from ingestion_airflow.observability import (
    build_application_name,
    enrich_metrics_payload,
    has_artifact_keys,
    with_application_name,
)
from ingestion_airflow.runtime import (
    build_contracts_token_provider,
    build_incremental_audit_artifacts,
    build_object_store_config,
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
STRATEGY = "incremental_audit"
RUN_MODE = "replay"


def _replay_input_error(problem: str, details: list[str]) -> AirflowFailException:
    message = "\n".join(
        [
            "Invalid incremental replay input.",
            f"Problem: {problem}",
            *details,
        ]
    )
    payload = {
        "error": "invalid_incremental_replay_input",
        "problem": problem,
        "details": details,
        "message": message,
    }
    logger.error("INVALID_INCREMENTAL_REPLAY_INPUT\n%s", message)
    try:
        get_current_context()["ti"].xcom_push(key="replay_input_error", value=payload)
    except Exception:
        logger.debug("Could not push replay_input_error XCom", exc_info=True)
    return AirflowFailException(
        "Invalid incremental replay input. "
        "See INVALID_INCREMENTAL_REPLAY_INPUT in task log or XCom key replay_input_error."
    )


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
            metrics_payload = enrich_metrics_payload(
                payload=result,
                strategy=STRATEGY,
                run_mode=RUN_MODE,
                pipeline_id=config.pipeline_id,
                run_id=run_id,
                stage_name=stage_name,
                object_store_config=build_object_store_config(config) if has_artifact_keys(result) else None,
            )
            logger.info(
                "Replay stage %s succeeded for run_id=%s pipeline_id=%s result=%s",
                stage_name,
                run_id,
                config.pipeline_id,
                metrics_payload,
            )
            finish_stage_audit(
                audit_engine,
                run_id=run_id,
                stage_name=stage_name,
                status="success",
                metrics_json=metrics_payload,
                error_text=None,
            )
            return metrics_payload
        except Exception as exc:
            logger.exception(
                "Replay stage %s failed for run_id=%s pipeline_id=%s: %s",
                stage_name,
                run_id,
                config.pipeline_id,
                exc,
            )
            failure_payload = enrich_metrics_payload(
                payload={
                    "status": "failed",
                    "exception_class": exc.__class__.__name__,
                    "error_message": str(exc),
                },
                strategy=STRATEGY,
                run_mode=RUN_MODE,
                pipeline_id=config.pipeline_id,
                run_id=run_id,
                stage_name=stage_name,
            )
            finish_stage_audit(
                audit_engine,
                run_id=run_id,
                stage_name=stage_name,
                status="failed",
                metrics_json=failure_payload,
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

        delta_object_key: str | None = None
        validation_manifest_key: str | None = None
        parent_stage_metrics_found = False

        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        try:
            stage_metrics = read_stage_audit_metrics(
                audit_engine,
                config.parent_run_id,
                "extract_validate_land_delta",
            )
        finally:
            audit_engine.dispose()

        if stage_metrics:
            parent_stage_metrics_found = True
            delta_object_key = str(stage_metrics.get("delta_object_key") or "").strip() or None
            validation_manifest_key = str(stage_metrics.get("manifest_key") or "").strip() or None
            if not delta_object_key:
                raise _replay_input_error(
                    "Parent incremental run has extract metadata without delta_object_key.",
                    [
                        f"parent_run_id: {config.parent_run_id}",
                        "Expected: stage_audit.metrics_json.delta_object_key must point to accepted_delta.",
                        "Fix: use a successful ingest_contract_incremental_audit run_id as parent_run_id.",
                    ],
                ) from None
        else:
            artifacts = build_incremental_audit_artifacts(config.namespace, config.name, config.parent_run_id)
            delta_object_key = artifacts["delta_object_key"]
            validation_manifest_key = artifacts["validation_manifest_key"]

        if not delta_object_key:
            raise _replay_input_error(
                "Replay delta key could not be resolved.",
                [
                    "Expected: parent_run_id references a successful ingest_contract_incremental_audit run.",
                    "Fix: use the ingestion audit run_id from ingestion_meta.run_audit or start_run XCom.",
                ],
            ) from None

        contract_version = config.contract_version
        if contract_version is None and validation_manifest_key:
            if not object_store.object_exists(validation_manifest_key):
                normalized_manifest_key = object_store_config.normalize_key(validation_manifest_key)
                if config.parent_run_id:
                    expected_artifacts = build_incremental_audit_artifacts(
                        config.namespace,
                        config.name,
                        config.parent_run_id,
                    )
                    expected_delta_key = object_store_config.normalize_key(expected_artifacts["delta_object_key"])
                    raise _replay_input_error(
                        "Incremental replay manifest was not found in object store.",
                        [
                            f"parent_run_id: {config.parent_run_id}",
                            "Expected: parent_run_id must be a successful ingest_contract_incremental_audit "
                            "run_id from ingestion_meta.run_audit, not an Airflow DagRun id and not a hash-diff run.",
                            f"Successful extract stage metadata found in audit DB: {parent_stage_metrics_found}",
                            f"Expected manifest key: {normalized_manifest_key}",
                            f"Expected delta key: {expected_delta_key}",
                            "Fix: use the ingestion audit run_id from ingestion_meta.run_audit or start_run XCom.",
                        ],
                    ) from None
                raise _replay_input_error(
                    "Incremental replay could not derive contract_version because manifest was not found.",
                    [
                        f"Expected manifest key: {normalized_manifest_key}",
                        "Expected: either manifest exists or contract_version is provided explicitly.",
                        "Fix: use a parent_run_id from a successful ingest_contract_incremental_audit run.",
                    ],
                ) from None
            manifest_payload = object_store.get_json(validation_manifest_key)
            manifest_version = manifest_payload.get("contract_version")
            if isinstance(manifest_version, str) and manifest_version.strip():
                contract_version = manifest_version.strip()

        if contract_version is None:
            raise _replay_input_error(
                "Replay requires contract_version or a resolvable manifest with contract_version.",
                    [
                        "Expected: contract_version is present in dag_run.conf or manifest.json.",
                        "Fix: provide contract_version explicitly or use a parent_run_id whose manifest exists.",
                    ],
                ) from None

        if not object_store.object_exists(delta_object_key):
            normalized_delta_key = object_store_config.normalize_key(delta_object_key)
            raise _replay_input_error(
                "Incremental replay delta artifact was not found in object store.",
                [
                    f"Expected delta key: {normalized_delta_key}",
                    "Expected: parent_run_id references a successful ingest_contract_incremental_audit run "
                    "with an existing accepted_delta object.",
                    "Fix: check parent_run_id, namespace, name and landing_s3_prefix.",
                ],
            ) from None

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
            strategy=STRATEGY,
            run_mode=RUN_MODE,
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
                delete_count=0,
                unchanged_count=0,
                metrics_json=enrich_metrics_payload(
                    payload={
                        "failed_stage": "start_replay_run",
                        "delta_object_key": replay_input.get("delta_object_key"),
                        "parent_run_id": replay_input.get("parent_run_id"),
                    },
                    strategy=STRATEGY,
                    run_mode=RUN_MODE,
                    pipeline_id=config.pipeline_id,
                    run_id=run_id,
                    object_store_config=build_object_store_config(config),
                ),
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
                target_dsn=with_application_name(
                    config.target_dsn,
                    build_application_name(
                        strategy=STRATEGY,
                        run_mode=RUN_MODE,
                        stage_name="apply_delta",
                        role="lake",
                    ),
                ),
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
                    delete_count=int(apply_result.get("delete_count", 0) or 0),
                    unchanged_count=int(apply_result.get("unchanged_count", 0) or 0),
                    metrics_json=enrich_metrics_payload(
                        payload={
                            "failed_tasks": failed_tasks,
                            "delta_object_key": run_context.get("delta_object_key"),
                            "validation_manifest_key": run_context.get("validation_manifest_key"),
                            "parent_run_id": run_context.get("parent_run_id"),
                        },
                        strategy=STRATEGY,
                        run_mode=RUN_MODE,
                        pipeline_id=config.pipeline_id,
                        run_id=str(run_context["run_id"]),
                        object_store_config=build_object_store_config(config),
                    ),
                    error_text=f"Replay pipeline failed at stages: {', '.join(failed_tasks)}",
                )
                raise AirflowFailException(f"Replay pipeline failed at tasks: {', '.join(failed_tasks)}")

            run_metrics = enrich_metrics_payload(
                payload={
                    **apply_result,
                    "delta_object_key": run_context.get("delta_object_key"),
                    "validation_manifest_key": run_context.get("validation_manifest_key"),
                    "parent_run_id": run_context.get("parent_run_id"),
                    "replayed_contract_version": run_context.get("replayed_contract_version"),
                },
                strategy=STRATEGY,
                run_mode=RUN_MODE,
                pipeline_id=config.pipeline_id,
                run_id=str(run_context["run_id"]),
                object_store_config=build_object_store_config(config),
            )
            finish_run_audit(
                engine=audit_engine,
                run_id=str(run_context["run_id"]),
                status="success",
                read_count=int(apply_result.get("read_count", 0) or 0),
                insert_count=int(apply_result.get("insert_count", 0) or 0),
                update_count=int(apply_result.get("update_count", 0) or 0),
                delete_count=int(apply_result.get("delete_count", 0) or 0),
                unchanged_count=int(apply_result.get("unchanged_count", 0) or 0),
                metrics_json=run_metrics,
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
