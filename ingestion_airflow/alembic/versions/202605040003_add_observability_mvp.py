from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from ingestion_airflow.db.audit_tables import AUDIT_SCHEMA

revision = "202605040003"
down_revision = "202603260002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("run_audit", schema=AUDIT_SCHEMA)}

    if "strategy" not in columns:
        op.add_column(
            "run_audit",
            sa.Column("strategy", sa.Text(), nullable=True),
            schema=AUDIT_SCHEMA,
        )
    if "run_mode" not in columns:
        op.add_column(
            "run_audit",
            sa.Column("run_mode", sa.Text(), nullable=True, server_default=sa.text("'ingest'")),
            schema=AUDIT_SCHEMA,
        )
    if "delete_count" not in columns:
        op.add_column(
            "run_audit",
            sa.Column("delete_count", sa.Integer(), nullable=True, server_default=sa.text("0")),
            schema=AUDIT_SCHEMA,
        )

    op.execute(
        sa.text(
            f"""
            UPDATE "{AUDIT_SCHEMA}"."run_audit"
            SET
                strategy = CASE
                    WHEN pipeline_id LIKE '%%.hash_diff' THEN 'hash_diff'
                    WHEN pipeline_id LIKE '%%.incremental_audit' THEN 'incremental_audit'
                    WHEN pipeline_id LIKE '%%.logical_cdc' THEN 'logical_cdc'
                    ELSE COALESCE(NULLIF(metrics_json->>'strategy', ''), 'unknown')
                END,
                run_mode = CASE
                    WHEN COALESCE(metrics_json->>'run_mode', metrics_json->>'mode', '') LIKE '%%replay%%' THEN 'replay'
                    ELSE 'ingest'
                END,
                delete_count = COALESCE((metrics_json->>'delete_count')::int, 0)
            """
        )
    )
    op.alter_column("run_audit", "strategy", nullable=False, schema=AUDIT_SCHEMA)
    op.alter_column("run_audit", "run_mode", nullable=False, schema=AUDIT_SCHEMA, server_default=sa.text("'ingest'"))
    op.alter_column("run_audit", "delete_count", nullable=False, schema=AUDIT_SCHEMA, server_default=sa.text("0"))

    op.execute(
        f"""
        CREATE OR REPLACE VIEW "{AUDIT_SCHEMA}"."v_run_metrics" AS
        SELECT
            run_id::text AS run_id,
            pipeline_id,
            strategy,
            run_mode,
            contract_id,
            version AS contract_version,
            checksum,
            started_at,
            finished_at,
            EXTRACT(EPOCH FROM (finished_at - started_at)) AS run_duration_seconds,
            read_count,
            insert_count,
            update_count,
            delete_count,
            unchanged_count,
            status,
            metrics_json,
            error_text,
            COALESCE(
                (metrics_json->>'source_row_count')::bigint,
                (metrics_json->>'source_event_count')::bigint,
                0
            ) AS source_input_count,
            COALESCE(
                (metrics_json->'problem_summary'->>'invalid_record_count')::bigint,
                (metrics_json->>'invalid_row_count')::bigint,
                (metrics_json->>'invalid_event_count')::bigint,
                0
            ) AS invalid_record_count,
            COALESCE(
                (metrics_json->'problem_summary'->>'invalid_transaction_count')::bigint,
                (metrics_json->>'invalid_transaction_count')::bigint,
                0
            ) AS invalid_transaction_count,
            COALESCE(
                (metrics_json->'problem_summary'->>'quarantined_event_count')::bigint,
                (metrics_json->>'quarantined_event_count')::bigint,
                0
            ) AS quarantined_event_count,
            COALESCE(
                (metrics_json->'problem_summary'->>'quarantined_transaction_count')::bigint,
                (metrics_json->>'quarantined_transaction_count')::bigint,
                0
            ) AS quarantined_transaction_count,
            COALESCE((metrics_json->>'artifact_total_bytes')::bigint, 0) AS artifact_total_bytes,
            COALESCE(
                metrics_json->>'validation_error_object_key',
                metrics_json->>'error_object_key'
            ) AS error_artifact_key
        FROM "{AUDIT_SCHEMA}"."run_audit"
        """
    )

    op.execute(
        f"""
        CREATE OR REPLACE VIEW "{AUDIT_SCHEMA}"."v_stage_metrics" AS
        SELECT
            sa.run_id::text AS run_id,
            ra.pipeline_id,
            ra.strategy,
            ra.run_mode,
            ra.contract_id,
            ra.version AS contract_version,
            sa.stage_name,
            sa.status,
            sa.started_at,
            sa.finished_at,
            EXTRACT(EPOCH FROM (sa.finished_at - sa.started_at)) AS stage_duration_seconds,
            sa.metrics_json,
            sa.error_text,
            COALESCE((sa.metrics_json->>'artifact_total_bytes')::bigint, 0) AS artifact_total_bytes,
            COALESCE(
                (sa.metrics_json->'problem_summary'->>'invalid_record_count')::bigint,
                (sa.metrics_json->>'invalid_row_count')::bigint,
                (sa.metrics_json->>'invalid_event_count')::bigint,
                0
            ) AS invalid_record_count,
            COALESCE(
                (sa.metrics_json->'problem_summary'->>'invalid_transaction_count')::bigint,
                (sa.metrics_json->>'invalid_transaction_count')::bigint,
                0
            ) AS invalid_transaction_count,
            COALESCE(
                (sa.metrics_json->'problem_summary'->>'quarantined_event_count')::bigint,
                (sa.metrics_json->>'quarantined_event_count')::bigint,
                0
            ) AS quarantined_event_count,
            COALESCE(
                (sa.metrics_json->'problem_summary'->>'quarantined_transaction_count')::bigint,
                (sa.metrics_json->>'quarantined_transaction_count')::bigint,
                0
            ) AS quarantined_transaction_count,
            COALESCE(
                sa.metrics_json->>'validation_error_object_key',
                sa.metrics_json->>'error_object_key'
            ) AS error_artifact_key
        FROM "{AUDIT_SCHEMA}"."stage_audit" sa
        JOIN "{AUDIT_SCHEMA}"."run_audit" ra ON ra.run_id = sa.run_id
        """
    )

    op.execute(
        f"""
        CREATE OR REPLACE VIEW "{AUDIT_SCHEMA}"."v_pipeline_health" AS
        SELECT
            ps.pipeline_id,
            latest.run_id::text AS last_run_id,
            latest.strategy,
            latest.run_mode,
            ps.last_run_at,
            ps.last_success_at,
            ps.last_status,
            ps.last_error,
            EXTRACT(EPOCH FROM (now() - ps.last_run_at)) AS seconds_since_last_run,
            EXTRACT(EPOCH FROM (now() - ps.last_success_at)) AS seconds_since_last_success
        FROM "{AUDIT_SCHEMA}"."pipeline_state" ps
        LEFT JOIN LATERAL (
            SELECT run_id, strategy, run_mode
            FROM "{AUDIT_SCHEMA}"."run_audit" ra
            WHERE ra.pipeline_id = ps.pipeline_id
            ORDER BY ra.started_at DESC
            LIMIT 1
        ) latest ON TRUE
        """
    )

    op.execute(
        f"""
        CREATE OR REPLACE VIEW "{AUDIT_SCHEMA}"."v_problem_summary" AS
        SELECT
            vm.run_id,
            vm.pipeline_id,
            vm.strategy,
            vm.run_mode,
            NULL::text AS stage_name,
            vm.started_at AS observed_at,
            'run'::text AS scope,
            CASE
                WHEN vm.status = 'failed' THEN 'run_failure'
                WHEN vm.invalid_transaction_count > 0 THEN 'invalid_transactions'
                WHEN vm.quarantined_transaction_count > 0 THEN 'quarantined_transactions'
                WHEN vm.invalid_record_count > 0 THEN 'invalid_records'
                WHEN vm.quarantined_event_count > 0 THEN 'quarantined_events'
                ELSE 'none'
            END AS problem_type,
            GREATEST(
                CASE WHEN vm.status = 'failed' THEN 1 ELSE 0 END,
                vm.invalid_record_count,
                vm.invalid_transaction_count,
                vm.quarantined_event_count,
                vm.quarantined_transaction_count
            ) AS problem_count,
            vm.error_artifact_key,
            vm.error_text
        FROM "{AUDIT_SCHEMA}"."v_run_metrics" vm
        WHERE vm.status = 'failed'
           OR vm.invalid_record_count > 0
           OR vm.invalid_transaction_count > 0
           OR vm.quarantined_event_count > 0
           OR vm.quarantined_transaction_count > 0

        UNION ALL

        SELECT
            sm.run_id,
            sm.pipeline_id,
            sm.strategy,
            sm.run_mode,
            sm.stage_name,
            COALESCE(sm.finished_at, sm.started_at) AS observed_at,
            'stage'::text AS scope,
            CASE
                WHEN sm.status = 'failed' THEN 'stage_failure'
                WHEN sm.invalid_transaction_count > 0 THEN 'invalid_transactions'
                WHEN sm.quarantined_transaction_count > 0 THEN 'quarantined_transactions'
                WHEN sm.invalid_record_count > 0 THEN 'invalid_records'
                WHEN sm.quarantined_event_count > 0 THEN 'quarantined_events'
                ELSE 'none'
            END AS problem_type,
            GREATEST(
                CASE WHEN sm.status = 'failed' THEN 1 ELSE 0 END,
                sm.invalid_record_count,
                sm.invalid_transaction_count,
                sm.quarantined_event_count,
                sm.quarantined_transaction_count
            ) AS problem_count,
            sm.error_artifact_key,
            sm.error_text
        FROM "{AUDIT_SCHEMA}"."v_stage_metrics" sm
        WHERE sm.status = 'failed'
           OR sm.invalid_record_count > 0
           OR sm.invalid_transaction_count > 0
           OR sm.quarantined_event_count > 0
           OR sm.quarantined_transaction_count > 0
        """
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    op.execute(f'DROP VIEW IF EXISTS "{AUDIT_SCHEMA}"."v_problem_summary"')
    op.execute(f'DROP VIEW IF EXISTS "{AUDIT_SCHEMA}"."v_pipeline_health"')
    op.execute(f'DROP VIEW IF EXISTS "{AUDIT_SCHEMA}"."v_stage_metrics"')
    op.execute(f'DROP VIEW IF EXISTS "{AUDIT_SCHEMA}"."v_run_metrics"')

    columns = {column["name"] for column in inspector.get_columns("run_audit", schema=AUDIT_SCHEMA)}
    if "delete_count" in columns:
        op.drop_column("run_audit", "delete_count", schema=AUDIT_SCHEMA)
    if "run_mode" in columns:
        op.drop_column("run_audit", "run_mode", schema=AUDIT_SCHEMA)
    if "strategy" in columns:
        op.drop_column("run_audit", "strategy", schema=AUDIT_SCHEMA)
