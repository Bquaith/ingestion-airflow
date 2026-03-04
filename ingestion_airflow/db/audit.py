from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from sqlalchemy import case, delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from ingestion_airflow.db.audit_tables import (
    pipeline_checkpoint_table,
    pipeline_lock_table,
    pipeline_state_table,
    run_audit_table,
)


def _parse_run_id(run_id: str) -> uuid.UUID:
    return uuid.UUID(run_id)


def start_run_audit(
    engine: Engine,
    pipeline_id: str,
    contract_id: str,
    version: str,
    checksum: str,
) -> str:
    run_id = uuid.uuid4()

    statement = insert(run_audit_table).values(
        run_id=run_id,
        pipeline_id=pipeline_id,
        contract_id=contract_id,
        version=version,
        checksum=checksum,
        started_at=func.now(),
        status="running",
    )

    with engine.begin() as conn:
        conn.execute(statement)

    return str(run_id)


def acquire_pipeline_lock(
    engine: Engine,
    pipeline_id: str,
    run_id: str,
    ttl_seconds: int = 7200,
) -> bool:
    run_uuid = _parse_run_id(run_id)
    locked_at = datetime.now(timezone.utc)
    lock_until = locked_at + timedelta(seconds=ttl_seconds)

    statement = (
        pg_insert(pipeline_lock_table)
        .values(
            pipeline_id=pipeline_id,
            run_id=run_uuid,
            locked_at=locked_at,
            lock_until=lock_until,
        )
        .on_conflict_do_update(
            index_elements=[pipeline_lock_table.c.pipeline_id],
            set_={
                "run_id": run_uuid,
                "locked_at": locked_at,
                "lock_until": lock_until,
            },
            where=pipeline_lock_table.c.lock_until <= func.now(),
        )
        .returning(pipeline_lock_table.c.run_id)
    )

    with engine.begin() as conn:
        lock_row = conn.execute(statement).scalar_one_or_none()

    return lock_row is not None


def release_pipeline_lock(engine: Engine, pipeline_id: str, run_id: str) -> None:
    statement = delete(pipeline_lock_table).where(
        pipeline_lock_table.c.pipeline_id == pipeline_id,
        pipeline_lock_table.c.run_id == _parse_run_id(run_id),
    )

    with engine.begin() as conn:
        conn.execute(statement)


def persist_pipeline_checkpoint(
    engine: Engine,
    pipeline_id: str,
    run_id: str,
    checkpoint_payload: Mapping[str, Any],
) -> None:
    statement = (
        pg_insert(pipeline_checkpoint_table)
        .values(
            pipeline_id=pipeline_id,
            run_id=_parse_run_id(run_id),
            checkpoint_json=dict(checkpoint_payload),
            updated_at=func.now(),
        )
        .on_conflict_do_update(
            index_elements=[pipeline_checkpoint_table.c.pipeline_id],
            set_={
                "run_id": _parse_run_id(run_id),
                "checkpoint_json": dict(checkpoint_payload),
                "updated_at": func.now(),
            },
        )
    )

    with engine.begin() as conn:
        conn.execute(statement)


def finish_run_audit(
    engine: Engine,
    run_id: str,
    status: str,
    read_count: int,
    insert_count: int,
    update_count: int,
    unchanged_count: int,
    metrics_json: Mapping[str, Any] | None = None,
    error_text: str | None = None,
) -> None:
    statement = (
        update(run_audit_table)
        .where(run_audit_table.c.run_id == _parse_run_id(run_id))
        .values(
            finished_at=func.now(),
            read_count=read_count,
            insert_count=insert_count,
            update_count=update_count,
            unchanged_count=unchanged_count,
            status=status,
            metrics_json=dict(metrics_json or {}),
            error_text=error_text,
        )
    )

    with engine.begin() as conn:
        conn.execute(statement)


def finalize_pipeline_state(engine: Engine, pipeline_id: str) -> None:
    latest_statement = (
        select(
            run_audit_table.c.started_at,
            run_audit_table.c.finished_at,
            run_audit_table.c.status,
            run_audit_table.c.error_text,
        )
        .where(run_audit_table.c.pipeline_id == pipeline_id)
        .order_by(run_audit_table.c.started_at.desc())
        .limit(1)
    )

    with engine.begin() as conn:
        latest = conn.execute(latest_statement).mappings().first()
        if latest is None:
            return

        last_run_at = latest["finished_at"] or latest["started_at"]
        last_success_at = last_run_at if latest["status"] == "success" else None
        last_error = None if latest["status"] == "success" else latest["error_text"]

        upsert_statement = pg_insert(pipeline_state_table).values(
            pipeline_id=pipeline_id,
            last_run_at=last_run_at,
            last_success_at=last_success_at,
            last_status=latest["status"],
            last_error=last_error,
            updated_at=func.now(),
        )
        upsert_statement = upsert_statement.on_conflict_do_update(
            index_elements=[pipeline_state_table.c.pipeline_id],
            set_={
                "last_run_at": upsert_statement.excluded.last_run_at,
                "last_success_at": case(
                    (upsert_statement.excluded.last_status == "success", upsert_statement.excluded.last_run_at),
                    else_=pipeline_state_table.c.last_success_at,
                ),
                "last_status": upsert_statement.excluded.last_status,
                "last_error": upsert_statement.excluded.last_error,
                "updated_at": func.now(),
            },
        )
        conn.execute(upsert_statement)
