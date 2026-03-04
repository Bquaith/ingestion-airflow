from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from ingestion_airflow.db.audit_tables import audit_metadata
from ingestion_airflow.db.migrations import ALEMBIC_VERSION_TABLE

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = audit_metadata


def get_database_url() -> str:
    database_url = config.attributes.get("database_url")
    if database_url:
        return str(database_url)

    database_url = os.getenv("AUDIT_DATABASE_DSN") or os.getenv("ALEMBIC_DATABASE_URL") or config.get_main_option("sqlalchemy.url")
    if database_url:
        return database_url

    raise RuntimeError("Database URL is not configured for Alembic")


def run_migrations_offline() -> None:
    context.configure(
        url=get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        include_schemas=True,
        version_table=ALEMBIC_VERSION_TABLE,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_database_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_schemas=True,
            version_table=ALEMBIC_VERSION_TABLE,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
