PYTHON ?= python3

.PHONY: install-dev build migrate migrate-down migrate-current migrate-repair

install-dev:
	$(PYTHON) -m pip install -e '.[dev]'

build:
	$(PYTHON) -m build

migrate:
	$(PYTHON) -m ingestion_airflow.db.migrations upgrade

migrate-down:
	$(PYTHON) -m ingestion_airflow.db.migrations downgrade -1

migrate-current:
	$(PYTHON) -m ingestion_airflow.db.migrations current --verbose
