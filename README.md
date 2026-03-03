# ingestion-airflow

Airflow runtime-репозиторий для запуска DAG `ingest_contract_hashdiff` поверх пакета `ingestion-core`.

Репозиторий содержит:
- универсальный DAG `dags/ingest_contract_hashdiff.py`
- runtime-конфиг для чтения `dagrun.conf`
- Docker Compose для локального запуска Airflow

Бизнес-логика hash-diff, клиент contract registry и работа с PostgreSQL вынесены в отдельный репозиторий `ingestion-core`.

## Локальная структура

Для текущей сборки репозитории должны лежать рядом:

```text
integration-platform/
  ingestion-core/
  ingestion-airflow/
```

Docker image для Airflow собирается из `ingestion-airflow`, но во время build копирует sibling-репозиторий `ingestion-core` и устанавливает его как Python package.

## Структура

```text
ingestion-airflow/
  dags/
    ingest_contract_hashdiff.py
  docker/
    Dockerfile.airflow
    docker-compose.yml
    requirements.txt
  ingestion_airflow/
    __init__.py
    config.py
  tests/
    test_config.py
  pyproject.toml
  requirements-airflow.txt
  requirements-test.txt
  tox.ini
```

## Запуск Docker Compose

```bash
cd ingestion-airflow/docker
docker compose up --build -d
```

Сервисы:
- `airflow-init`
- `airflow-webserver` (`http://localhost:8088`, `airflow/airflow`)
- `airflow-scheduler`

Переменные окружения:
- `CONTRACTS_SERVICE_URL`
- `AIRFLOW_METADATA_DSN`

## Trigger DAG

Пример запуска через Airflow REST API:

```bash
curl -u airflow:airflow -X POST "http://localhost:8088/api/v1/dags/ingest_contract_hashdiff/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_run_id": "manual-orders-1",
    "conf": {
      "contracts_service_url": "http://host.docker.internal:8081",
      "namespace": "sales",
      "name": "orders",
      "contract_version": "1",
      "source_dsn": "postgresql+psycopg2://source_user:source_pass@postgres_source:5432/source_db",
      "source_table": "public.orders",
      "target_dsn": "postgresql+psycopg2://target_user:target_pass@postgres_target:5432/target_db",
      "target_table_curated": "curated.orders",
      "source_batch_size": 1000,
      "upsert_batch_size": 1000
    }
  }'
```

## Тесты

```bash
tox
```
