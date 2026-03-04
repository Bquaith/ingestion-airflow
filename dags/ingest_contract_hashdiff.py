from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from ingestion_airflow.config import IngestionRunConfig
from ingestion_airflow.db.audit import (
    acquire_pipeline_lock,
    finalize_pipeline_state,
    finish_run_audit,
    persist_pipeline_checkpoint,
    release_pipeline_lock,
    start_run_audit,
)
from ingestion_core.contracts_client import ContractRegistryClient
from ingestion_core.hash_diff import ContractDefinition, run_hash_diff
from ingestion_core.postgres import create_sqlalchemy_engine


@dag(
    dag_id="ingest_contract_hashdiff",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "hash-diff"],
)
def ingest_contract_hashdiff() -> None:
    def _load_run_config() -> IngestionRunConfig:
        context = get_current_context()
        dag_run = context["dag_run"]
        return IngestionRunConfig.from_dagrun_conf(dag_run.conf or {})

    @task
    def fetch_contract() -> dict:
        config = _load_run_config()

        client = ContractRegistryClient(config.contracts_service_url)
        contract_payload = client.fetch_contract(
            namespace=config.namespace,
            name=config.name,
            version=config.contract_version,
        )
        return contract_payload.to_dict()

    @task
    def run_hashdiff(contract_payload: dict) -> dict:
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
                    f"Pipeline lock is already held for {config.pipeline_id}; "
                    "concurrent run is not allowed"
                )

            contract = ContractDefinition.from_registry_payload(contract_payload)
            result = run_hash_diff(
                source_dsn=config.source_dsn,
                source_table=config.source_table,
                target_dsn=config.target_dsn,
                target_table_curated=config.target_table_curated,
                contract=contract,
                source_batch_size=config.source_batch_size,
                upsert_batch_size=config.upsert_batch_size,
            )

            finish_run_audit(
                engine=audit_engine,
                run_id=run_id,
                status="success",
                read_count=result.read_count,
                insert_count=result.insert_count,
                update_count=result.update_count,
                unchanged_count=result.unchanged_count,
                metrics_json=result.metrics_dict(),
                error_text=None,
            )
            persist_pipeline_checkpoint(
                engine=audit_engine,
                pipeline_id=config.pipeline_id,
                run_id=run_id,
                checkpoint_payload={
                    "status": "success",
                    "read_count": result.read_count,
                    "insert_count": result.insert_count,
                    "update_count": result.update_count,
                    "delete_count": result.delete_count,
                    "unchanged_count": result.unchanged_count,
                    "contract_id": contract.contract_id,
                    "contract_version": contract.version,
                    "checksum": contract.checksum,
                    "target_table_curated": config.target_table_curated,
                    "source_batch_size": config.source_batch_size,
                    "upsert_batch_size": config.upsert_batch_size,
                    "metrics": result.metrics_dict(),
                },
            )

            return {
                "run_id": run_id,
                "status": "success",
                "read_count": result.read_count,
                "insert_count": result.insert_count,
                "update_count": result.update_count,
                "delete_count": result.delete_count,
                "unchanged_count": result.unchanged_count,
                "metrics": result.metrics_dict(),
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
                metrics_json=None,
                error_text=str(exc),
            )
            raise
        finally:
            if lock_acquired:
                release_pipeline_lock(
                    engine=audit_engine,
                    pipeline_id=config.pipeline_id,
                    run_id=run_id,
                )
            audit_engine.dispose()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize_audit() -> None:
        config = _load_run_config()

        audit_engine = create_sqlalchemy_engine(config.audit_dsn)
        try:
            finalize_pipeline_state(audit_engine, config.pipeline_id)
        finally:
            audit_engine.dispose()

    contract_payload = fetch_contract()
    hashdiff_task = run_hashdiff(contract_payload)
    finalize_task = finalize_audit()

    hashdiff_task >> finalize_task


dag_instance = ingest_contract_hashdiff()
