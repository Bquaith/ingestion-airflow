from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from ingestion_airflow.db.audit_tables import AUDIT_SCHEMA

revision = "202603260002"
down_revision = "202603040001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("stage_audit", schema=AUDIT_SCHEMA):
        op.create_table(
            "stage_audit",
            sa.Column("run_id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("stage_name", sa.Text(), primary_key=True),
            sa.Column("status", sa.Text(), nullable=False),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("metrics_json", postgresql.JSONB(), nullable=True),
            sa.Column("error_text", sa.Text(), nullable=True),
            schema=AUDIT_SCHEMA,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if inspector.has_table("stage_audit", schema=AUDIT_SCHEMA):
        op.drop_table("stage_audit", schema=AUDIT_SCHEMA)
