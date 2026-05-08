from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from ingestion_airflow.db.audit_tables import AUDIT_SCHEMA

revision = "202605080004"
down_revision = "202605040003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "validation_error_audit",
        sa.Column("error_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("pipeline_id", sa.Text(), nullable=False),
        sa.Column("strategy", sa.Text(), nullable=False),
        sa.Column("run_mode", sa.Text(), nullable=False, server_default=sa.text("'ingest'")),
        sa.Column("stage_name", sa.Text(), nullable=False),
        sa.Column("row_number", sa.Integer(), nullable=True),
        sa.Column("field", sa.Text(), nullable=False),
        sa.Column("code", sa.Text(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("constraint", sa.Text(), nullable=True),
        sa.Column("actual_value", sa.Text(), nullable=True),
        sa.Column("error_object_key", sa.Text(), nullable=True),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("error_id"),
        schema=AUDIT_SCHEMA,
    )
    op.create_index(
        op.f("ix_validation_error_audit_run_id"),
        "validation_error_audit",
        ["run_id"],
        unique=False,
        schema=AUDIT_SCHEMA,
    )
    op.create_index(
        op.f("ix_validation_error_audit_pipeline_id"),
        "validation_error_audit",
        ["pipeline_id"],
        unique=False,
        schema=AUDIT_SCHEMA,
    )
    op.create_index(
        op.f("ix_validation_error_audit_stage_name"),
        "validation_error_audit",
        ["stage_name"],
        unique=False,
        schema=AUDIT_SCHEMA,
    )

    op.execute(
        f"""
        CREATE OR REPLACE VIEW "{AUDIT_SCHEMA}"."v_validation_errors" AS
        SELECT
            error_id::text AS error_id,
            run_id::text AS run_id,
            pipeline_id,
            strategy,
            run_mode,
            stage_name,
            row_number,
            field,
            code,
            message,
            "constraint",
            actual_value,
            error_object_key,
            details_json,
            created_at AS observed_at
        FROM "{AUDIT_SCHEMA}"."validation_error_audit"
        """
    )


def downgrade() -> None:
    op.execute(f'DROP VIEW IF EXISTS "{AUDIT_SCHEMA}"."v_validation_errors"')
    op.drop_index(op.f("ix_validation_error_audit_stage_name"), table_name="validation_error_audit", schema=AUDIT_SCHEMA)
    op.drop_index(op.f("ix_validation_error_audit_pipeline_id"), table_name="validation_error_audit", schema=AUDIT_SCHEMA)
    op.drop_index(op.f("ix_validation_error_audit_run_id"), table_name="validation_error_audit", schema=AUDIT_SCHEMA)
    op.drop_table("validation_error_audit", schema=AUDIT_SCHEMA)
