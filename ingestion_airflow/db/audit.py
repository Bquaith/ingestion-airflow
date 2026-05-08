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
    stage_audit_table,
    validation_error_audit_table,
)


def _parse_run_id(run_id: str) -> uuid.UUID:
    return uuid.UUID(run_id)


def start_run_audit(
    engine: Engine,
    pipeline_id: str,
    strategy: str,
    run_mode: str,
    contract_id: str,
    version: str,
    checksum: str,
) -> str:
    run_id = uuid.uuid4()

    statement = insert(run_audit_table).values(
        run_id=run_id,
        pipeline_id=pipeline_id,
        strategy=strategy,
        run_mode=run_mode,
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


def read_pipeline_checkpoint(engine: Engine, pipeline_id: str) -> dict[str, Any] | None:
    statement = select(
        pipeline_checkpoint_table.c.run_id,
        pipeline_checkpoint_table.c.checkpoint_json,
        pipeline_checkpoint_table.c.updated_at,
    ).where(pipeline_checkpoint_table.c.pipeline_id == pipeline_id)

    with engine.begin() as conn:
        row = conn.execute(statement).mappings().first()
        if row is None:
            return None

    checkpoint = dict(row["checkpoint_json"] or {})
    checkpoint["run_id"] = str(row["run_id"])
    checkpoint["updated_at"] = row["updated_at"].isoformat() if row["updated_at"] else None
    return checkpoint


def read_stage_audit_metrics(engine: Engine, run_id: str, stage_name: str) -> dict[str, Any] | None:
    statement = select(stage_audit_table.c.metrics_json).where(
        stage_audit_table.c.run_id == _parse_run_id(run_id),
        stage_audit_table.c.stage_name == stage_name,
        stage_audit_table.c.status == "success",
    )

    with engine.begin() as conn:
        row = conn.execute(statement).mappings().first()
        if row is None:
            return None

    return dict(row["metrics_json"] or {})


def read_stage_audit_record(engine: Engine, run_id: str, stage_name: str) -> dict[str, Any] | None:
    statement = select(
        stage_audit_table.c.status,
        stage_audit_table.c.metrics_json,
        stage_audit_table.c.error_text,
        stage_audit_table.c.started_at,
        stage_audit_table.c.finished_at,
    ).where(
        stage_audit_table.c.run_id == _parse_run_id(run_id),
        stage_audit_table.c.stage_name == stage_name,
    )

    with engine.begin() as conn:
        row = conn.execute(statement).mappings().first()
        if row is None:
            return None

    return {
        "status": row["status"],
        "metrics_json": dict(row["metrics_json"] or {}),
        "error_text": row["error_text"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
    }


def start_stage_audit(engine: Engine, run_id: str, stage_name: str) -> None:
    now = datetime.now(timezone.utc)
    statement = (
        pg_insert(stage_audit_table)
        .values(
            run_id=_parse_run_id(run_id),
            stage_name=stage_name,
            status="running",
            started_at=now,
            finished_at=None,
            metrics_json={},
            error_text=None,
        )
        .on_conflict_do_update(
            index_elements=[stage_audit_table.c.run_id, stage_audit_table.c.stage_name],
            set_={
                "status": "running",
                "started_at": now,
                "finished_at": None,
                "metrics_json": {},
                "error_text": None,
            },
        )
    )

    with engine.begin() as conn:
        conn.execute(statement)


def finish_stage_audit(
    engine: Engine,
    run_id: str,
    stage_name: str,
    status: str,
    metrics_json: Mapping[str, Any] | None = None,
    error_text: str | None = None,
) -> None:
    statement = (
        update(stage_audit_table)
        .where(
            stage_audit_table.c.run_id == _parse_run_id(run_id),
            stage_audit_table.c.stage_name == stage_name,
        )
        .values(
            status=status,
            finished_at=func.now(),
            metrics_json=dict(metrics_json or {}),
            error_text=error_text,
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
    delete_count: int,
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
            delete_count=delete_count,
            unchanged_count=unchanged_count,
            status=status,
            metrics_json=dict(metrics_json or {}),
            error_text=error_text,
        )
    )

    with engine.begin() as conn:
        conn.execute(statement)


def replace_validation_errors_for_stage(
    engine: Engine,
    run_id: str,
    pipeline_id: str,
    strategy: str,
    run_mode: str,
    stage_name: str,
    error_object_key: str | None,
    error_rows: list[Mapping[str, Any]],
) -> int:
    run_uuid = _parse_run_id(run_id)

    normalized_rows: list[dict[str, Any]] = []
    for error in error_rows:
        details_json = dict(error)
        normalized_rows.append(
            {
                "error_id": uuid.uuid4(),
                "run_id": run_uuid,
                "pipeline_id": pipeline_id,
                "strategy": strategy,
                "run_mode": run_mode,
                "stage_name": stage_name,
                "row_number": _coerce_optional_int(error.get("row_number")),
                "field": str(error.get("field") or "$"),
                "code": str(error.get("code") or "validation_error"),
                "message": str(error.get("message") or "validation error"),
                "constraint": _coerce_optional_text(error.get("constraint")),
                "actual_value": _coerce_optional_text(error.get("actual_value")),
                "error_object_key": _coerce_optional_text(error_object_key),
                "details_json": details_json,
            }
        )

    delete_statement = delete(validation_error_audit_table).where(
        validation_error_audit_table.c.run_id == run_uuid,
        validation_error_audit_table.c.stage_name == stage_name,
    )

    with engine.begin() as conn:
        conn.execute(delete_statement)
        if normalized_rows:
            conn.execute(insert(validation_error_audit_table), normalized_rows)

    return len(normalized_rows)


def finalize_pipeline_state(engine: Engine, pipeline_id: str) -> None:
    latest_statement = (
        select(
            run_audit_table.c.started_at,
            run_audit_table.c.finished_at,
            run_audit_table.c.strategy,
            run_audit_table.c.run_mode,
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


def _coerce_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _coerce_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
