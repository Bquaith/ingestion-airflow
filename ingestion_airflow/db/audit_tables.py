from __future__ import annotations

from sqlalchemy import Column, DateTime, Integer, MetaData, Table, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.engine import Engine

AUDIT_SCHEMA = "ingestion_meta"

audit_metadata = MetaData()

pipeline_state_table = Table(
    "pipeline_state",
    audit_metadata,
    Column("pipeline_id", Text, primary_key=True),
    Column("last_run_at", DateTime(timezone=True)),
    Column("last_success_at", DateTime(timezone=True)),
    Column("last_status", Text),
    Column("last_error", Text),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    schema=AUDIT_SCHEMA,
)

run_audit_table = Table(
    "run_audit",
    audit_metadata,
    Column("run_id", PGUUID(as_uuid=True), primary_key=True),
    Column("pipeline_id", Text, nullable=False),
    Column("contract_id", Text, nullable=False),
    Column("version", Text, nullable=False),
    Column("checksum", Text, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("read_count", Integer, nullable=False, server_default=text("0")),
    Column("insert_count", Integer, nullable=False, server_default=text("0")),
    Column("update_count", Integer, nullable=False, server_default=text("0")),
    Column("unchanged_count", Integer, nullable=False, server_default=text("0")),
    Column("status", Text, nullable=False),
    Column("metrics_json", JSONB),
    Column("error_text", Text),
    schema=AUDIT_SCHEMA,
)

stage_audit_table = Table(
    "stage_audit",
    audit_metadata,
    Column("run_id", PGUUID(as_uuid=True), primary_key=True),
    Column("stage_name", Text, primary_key=True),
    Column("status", Text, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("metrics_json", JSONB),
    Column("error_text", Text),
    schema=AUDIT_SCHEMA,
)

pipeline_lock_table = Table(
    "pipeline_lock",
    audit_metadata,
    Column("pipeline_id", Text, primary_key=True),
    Column("run_id", PGUUID(as_uuid=True), nullable=False),
    Column("locked_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("lock_until", DateTime(timezone=True), nullable=False),
    schema=AUDIT_SCHEMA,
)

pipeline_checkpoint_table = Table(
    "pipeline_checkpoint",
    audit_metadata,
    Column("pipeline_id", Text, primary_key=True),
    Column("run_id", PGUUID(as_uuid=True), nullable=False),
    Column("checkpoint_json", JSONB, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    schema=AUDIT_SCHEMA,
)


def ensure_audit_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{AUDIT_SCHEMA}"'))
        audit_metadata.create_all(bind=conn, checkfirst=True)
