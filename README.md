# ingestion-airflow

Airflow runtime-репозиторий для запуска DAG `ingest_contract_hashdiff` поверх пакета `ingestion-core`.

Репозиторий содержит:
- универсальный DAG `dags/ingest_contract_hashdiff.py`
- runtime-конфиг для чтения `dagrun.conf`
- Docker Compose для локального запуска Airflow

DAG `ingest_contract_hashdiff` реализует staged hash-diff pipeline:

```text
fetch_contract
  -> read_checkpoint
  -> start_run
  -> extract_snapshot
  -> validate_snapshot
  -> land_snapshot
  -> load_raw
  -> merge_curated
  -> persist_checkpoint
  -> finalize_run
```

Бизнес-логика hash-diff, клиент contract registry и работа с PostgreSQL вынесены в отдельный репозиторий `ingestion-core`.

## Локальная структура

Для текущей сборки репозитории должны лежать рядом:

```text
integration-platform/
  ingestion-core/
  ingestion-airflow/
```

Docker image для Airflow собирается из `ingestion-airflow`, но во время build копирует sibling-репозиторий `ingestion-core` и устанавливает его как Python package.

## Запуск Docker Compose

```bash
cd ingestion-airflow/docker
cp .env.example .env
docker compose up --build -d
```

Сервисы:
- `airflow-init`
- `airflow-webserver` (`http://localhost:8088`, `airflow/airflow`)
- `airflow-scheduler`

Переменные окружения:
- `CONTRACTS_SERVICE_URL`
- `AIRFLOW_METADATA_DSN`
- `AUDIT_DATABASE_DSN`
- `LANDING_S3_BUCKET`
- `LANDING_S3_PREFIX`
- `LANDING_S3_ENDPOINT_URL`
- `LANDING_S3_REGION`
- `LANDING_S3_VERIFY_SSL`
- `KEYCLOAK_TOKEN_URL`
- `KEYCLOAK_CLIENT_ID`
- `KEYCLOAK_CLIENT_SECRET`
- `MINIO_STS_ENDPOINT`
- `MINIO_STS_DURATION_SECONDS`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`

Готовый шаблон окружения для локального стенда лежит в `docker/.env.example`.
Готовый пример `dag_run.conf` лежит в `docker/dag_run.hashdiff.orders.example.json`.

По умолчанию DAG использует flow `Keycloak client_credentials -> MinIO AssumeRoleWithWebIdentity -> temporary S3 credentials`.
Статические `AWS_*` переменные оставлены только как fallback.

## Trigger DAG

Пример запуска через Airflow REST API:

```bash
curl -u airflow:airflow -X POST "http://localhost:8088/api/v1/dags/ingest_contract_hashdiff/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_run_id": "manual-orders-1",
    "conf": {
      "contracts_service_url": "http://host.docker.internal:8000",
      "namespace": "sales",
      "name": "orders",
      "contract_version": "1.0.0",
      "source_dsn": "postgresql+psycopg2://postgres:postgres@host.docker.internal:5432/test_data_set",
      "source_table": "public.orders",
      "target_dsn": "postgresql+psycopg2://postgres:postgres@host.docker.internal:5432/data_lake",
      "target_table_raw": "raw.sales__orders",
      "target_table_curated": "curated.orders",
      "landing_s3_bucket": "ingestion-landing",
      "landing_s3_prefix": "accepted",
      "landing_s3_endpoint_url": "http://host.docker.internal:9000",
      "landing_s3_region": "us-east-1",
      "landing_s3_verify_ssl": false,
      "source_batch_size": 1000,
      "raw_load_batch_size": 1000,
      "upsert_batch_size": 1000
    }
  }'
```

## Тесты

```bash
tox
```
