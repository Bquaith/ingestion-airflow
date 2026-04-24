PYTHON ?= python3
DOCKER_COMPOSE_DIR ?= docker

.PHONY: install-dev build migrate migrate-down migrate-current migrate-repair docker-recreate

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

docker-recreate:
	cd $(DOCKER_COMPOSE_DIR) && docker compose down
	cd $(DOCKER_COMPOSE_DIR) && docker compose build --no-cache
	cd $(DOCKER_COMPOSE_DIR) && docker compose up -d --force-recreate
