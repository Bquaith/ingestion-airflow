# ingestion-airflow

Airflow runtime-репозиторий для запуска DAG-ов для инкрементальной закрзки данных в озеро данных.

Репозиторий содержит:
- основной DAG `dags/ingest_contract_hashdiff.py`
- replay DAG `dags/replay_contract_hashdiff_from_minio.py`
- runtime-конфиг для чтения `dagrun.conf`
- Docker Compose для локального запуска Airflow

DAG `ingest_contract_hashdiff` реализует hash-diff pipeline:

```text
fetch_contract
  -> read_checkpoint
  -> start_run
  -> extract_validate_land
  -> merge_curated
  -> persist_checkpoint
  -> finalize_run
```

DAG `replay_contract_hashdiff_from_minio` переигрывает загрузку от уже сохраненного
`accepted_snapshot` в MinIO/S3:

```text
resolve_replay_input
  -> fetch_contract
  -> start_replay_run
  -> merge_curated_replay
  -> finalize_replay
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
docker compose up --build -d --remove-orphans
```

Сервисы:
- `airflow-init`
- `airflow-api-server` (`http://localhost:8088`)
- `airflow-scheduler`
- `airflow-dag-processor`

Переменные окружения:
- `AIRFLOW__API__BASE_URL`
- `AIRFLOW__API__WORKERS`
- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`
- `AIRFLOW__API_AUTH__JWT_SECRET`
- `AIRFLOW__FAB__CONFIG_FILE`
- `AIRFLOW_OAUTH_CLIENT_ID`
- `AIRFLOW_OAUTH_CLIENT_SECRET`
- `AIRFLOW_OAUTH_ISSUER_URL`
- `AIRFLOW_OAUTH_INTERNAL_ISSUER_URL`
- `AIRFLOW_OAUTH_PROVIDER_NAME`
- `AIRFLOW_OAUTH_SCOPE`
- `CONTRACTS_SERVICE_URL`
- `CONTRACTS_OIDC_TOKEN_URL`
- `CONTRACTS_OIDC_CLIENT_ID`
- `CONTRACTS_OIDC_CLIENT_SECRET`
- `CONTRACTS_OIDC_SCOPE`
- `CONTRACTS_OIDC_VERIFY_SSL`
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

Шаблон окружения для локального стенда лежит в `docker/.env.example`.
Пример `dag_run.conf` для основного DAG лежит в `docker/dag_run.hashdiff.orders.example.json`.
Пример `dag_run.conf` для replay DAG лежит в `docker/dag_run.hashdiff.replay.example.json`.

Для Airflow `3.x` с `LocalExecutor` внешний URL и внутренний execution API должны быть разведены:
- `AIRFLOW__API__BASE_URL=http://localhost:8088`
- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/`
- `AIRFLOW__API_AUTH__JWT_SECRET=<одинаковое значение для всех Airflow контейнеров>`

Статические `AWS_*` переменные оставлены только как fallback.

## Airflow доступ через Keycloak

Airflow запускается на версии `3.0.6` и использует стандартный
`FabAuthManager` с OAuth SSO через Keycloak.

Для Docker-контура issuer разделён на два URL:

- `AIRFLOW_OAUTH_ISSUER_URL=http://localhost:8081/realms/vkr` для browser redirect и проверки `iss`
- `AIRFLOW_OAUTH_INTERNAL_ISSUER_URL=http://host.docker.internal:8081/realms/vkr` для server-side token exchange и JWKS fetch из контейнера

Role mapping строится по `realm_access.roles` access token-а Keycloak:

- `Viewer` -> `Viewer`
- `User` -> `User`
- `Op` -> `Op`
- `Admin` -> `Admin`
- `SuperAdmin` -> `Admin`

Для стенда bootstrap поднимает:

- Keycloak client `airflow-auth`
- realm roles `Viewer`, `User`, `Op`, `Admin`, `SuperAdmin`
- mappings:
  - `admin / admin` -> `SuperAdmin`
  - `producer / producer` -> `User`
  - `consumer / consumer` -> `User + Op`

При `docker compose up --build -d` init-контейнер:

1. выполняет `airflow db migrate`
2. применяет audit migrations

Для входа в UI:

1. открой `http://localhost:8088`
2. нажми login через Keycloak
3. авторизуйся пользователем (базовые пользователи: `admin`, `producer` или `consumer`)
