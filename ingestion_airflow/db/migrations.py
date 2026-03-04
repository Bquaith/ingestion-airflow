from __future__ import annotations

import argparse
import os
from typing import Sequence

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

ALEMBIC_SCRIPT_LOCATION = "ingestion_airflow:alembic"
ALEMBIC_VERSION_TABLE = "ingestion_airflow_alembic_version"


def build_alembic_config(database_url: str) -> Config:
    if not database_url:
        raise ValueError("database_url is required")

    config = Config()
    config.set_main_option("script_location", ALEMBIC_SCRIPT_LOCATION)
    config.set_main_option("sqlalchemy.url", database_url)
    config.attributes["database_url"] = database_url
    return config


def upgrade_audit_schema(database_url: str, revision: str = "head") -> None:
    command.upgrade(build_alembic_config(database_url), revision)


def downgrade_audit_schema(database_url: str, revision: str) -> None:
    command.downgrade(build_alembic_config(database_url), revision)


def show_current_audit_revision(database_url: str, verbose: bool = False) -> None:
    command.current(build_alembic_config(database_url), verbose=verbose)


def _resolve_database_url(database_url: str | None) -> str:
    resolved = database_url or os.getenv("AUDIT_DATABASE_DSN") or os.getenv("ALEMBIC_DATABASE_URL")
    if not resolved:
        raise SystemExit("Set --database-url or AUDIT_DATABASE_DSN")
    return resolved


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage ingestion audit schema migrations")
    parser.add_argument(
        "--database-url",
        help="Target database URL. Falls back to AUDIT_DATABASE_DSN.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    upgrade_parser = subparsers.add_parser("upgrade", help="Upgrade audit schema")
    upgrade_parser.add_argument("revision", nargs="?", default="head")

    downgrade_parser = subparsers.add_parser("downgrade", help="Downgrade audit schema")
    downgrade_parser.add_argument("revision")

    current_parser = subparsers.add_parser("current", help="Show current audit revision")
    current_parser.add_argument("--verbose", action="store_true")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    database_url = _resolve_database_url(args.database_url)

    if args.command == "upgrade":
        upgrade_audit_schema(database_url, args.revision)
        return 0

    if args.command == "downgrade":
        downgrade_audit_schema(database_url, args.revision)
        return 0

    if args.command == "current":
        show_current_audit_revision(database_url, verbose=args.verbose)
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
