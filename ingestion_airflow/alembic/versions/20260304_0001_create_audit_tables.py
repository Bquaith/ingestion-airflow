from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from ingestion_airflow.db.audit_tables import AUDIT_SCHEMA

revision = "202603040001"
down_revision = "5f2621c13b39"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f'CREATE SCHEMA IF NOT EXISTS "{AUDIT_SCHEMA}"')
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("pipeline_state", schema=AUDIT_SCHEMA):
        op.create_table(
            "pipeline_state",
            sa.Column("pipeline_id", sa.Text(), primary_key=True),
            sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_status", sa.Text(), nullable=True),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            schema=AUDIT_SCHEMA,
        )

    if not inspector.has_table("run_audit", schema=AUDIT_SCHEMA):
        op.create_table(
            "run_audit",
            sa.Column("run_id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("pipeline_id", sa.Text(), nullable=False),
            sa.Column("contract_id", sa.Text(), nullable=False),
            sa.Column("version", sa.Text(), nullable=False),
            sa.Column("checksum", sa.Text(), nullable=False),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("read_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("insert_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("update_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("unchanged_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("status", sa.Text(), nullable=False),
            sa.Column("metrics_json", postgresql.JSONB(), nullable=True),
            sa.Column("error_text", sa.Text(), nullable=True),
            schema=AUDIT_SCHEMA,
        )

    if not inspector.has_table("pipeline_lock", schema=AUDIT_SCHEMA):
        op.create_table(
            "pipeline_lock",
            sa.Column("pipeline_id", sa.Text(), primary_key=True),
            sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("locked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("lock_until", sa.DateTime(timezone=True), nullable=False),
            schema=AUDIT_SCHEMA,
        )

    if not inspector.has_table("pipeline_checkpoint", schema=AUDIT_SCHEMA):
        op.create_table(
            "pipeline_checkpoint",
            sa.Column("pipeline_id", sa.Text(), primary_key=True),
            sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("checkpoint_json", postgresql.JSONB(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            schema=AUDIT_SCHEMA,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if inspector.has_table("pipeline_checkpoint", schema=AUDIT_SCHEMA):
        op.drop_table("pipeline_checkpoint", schema=AUDIT_SCHEMA)
    if inspector.has_table("pipeline_lock", schema=AUDIT_SCHEMA):
        op.drop_table("pipeline_lock", schema=AUDIT_SCHEMA)
    if inspector.has_table("run_audit", schema=AUDIT_SCHEMA):
        op.drop_table("run_audit", schema=AUDIT_SCHEMA)
    if inspector.has_table("pipeline_state", schema=AUDIT_SCHEMA):
        op.drop_table("pipeline_state", schema=AUDIT_SCHEMA)

    op.execute(f'DROP SCHEMA IF EXISTS "{AUDIT_SCHEMA}"')
