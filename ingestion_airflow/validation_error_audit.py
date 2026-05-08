from __future__ import annotations

import json
import logging
from typing import Any, Mapping

from sqlalchemy.engine import Engine

from ingestion_airflow.db.audit import replace_validation_errors_for_stage
from ingestion_core.adapters.object_store import ObjectStoreClient, ObjectStoreConfig

logger = logging.getLogger(__name__)

_VALIDATION_ERROR_BATCH_LIMIT = 10_000


def extract_validation_error_context(exc: BaseException) -> dict[str, Any]:
    payload: dict[str, Any] = {}

    error_object_key = _text_or_none(getattr(exc, "error_object_key", None))
    manifest_key = _text_or_none(getattr(exc, "manifest_key", None))
    accepted_object_key = _text_or_none(getattr(exc, "accepted_object_key", None))
    delta_object_key = _text_or_none(getattr(exc, "delta_object_key", None))

    if error_object_key is not None:
        payload["validation_error_object_key"] = error_object_key
        payload["error_object_key"] = error_object_key
    if manifest_key is not None:
        payload["validation_manifest_key"] = manifest_key
        payload["manifest_key"] = manifest_key
    if accepted_object_key is not None:
        payload["accepted_object_key"] = accepted_object_key
    if delta_object_key is not None:
        payload["delta_object_key"] = delta_object_key

    for key in (
        "invalid_row_count",
        "invalid_event_count",
        "invalid_transaction_count",
        "quarantined_event_count",
        "quarantined_transaction_count",
    ):
        value = getattr(exc, key, None)
        if value is not None:
            payload[key] = value

    return payload


def persist_validation_errors_from_artifact(
    engine: Engine,
    object_store_config: ObjectStoreConfig,
    *,
    run_id: str,
    pipeline_id: str,
    strategy: str,
    run_mode: str,
    stage_name: str,
    stage_payload: Mapping[str, Any] | None,
) -> int:
    payload = dict(stage_payload or {})
    error_object_key = _text_or_none(payload.get("validation_error_object_key")) or _text_or_none(
        payload.get("error_object_key")
    )
    if error_object_key is None:
        return 0

    object_store = ObjectStoreClient(object_store_config)
    error_rows: list[dict[str, Any]] = []
    try:
        with object_store.open_gzip_text_reader(error_object_key) as reader:
            for raw_line in reader:
                line = raw_line.strip()
                if not line:
                    continue
                parsed = json.loads(line)
                if not isinstance(parsed, dict):
                    logger.warning(
                        "Skipping non-object validation error payload for run_id=%s stage_name=%s key=%s",
                        run_id,
                        stage_name,
                        error_object_key,
                    )
                    continue
                error_rows.append(dict(parsed))
                if len(error_rows) >= _VALIDATION_ERROR_BATCH_LIMIT:
                    logger.warning(
                        "Validation error ingestion truncated at %s rows for run_id=%s stage_name=%s key=%s",
                        _VALIDATION_ERROR_BATCH_LIMIT,
                        run_id,
                        stage_name,
                        error_object_key,
                    )
                    break
    except Exception:
        logger.warning(
            "Failed to ingest validation errors from object store for run_id=%s stage_name=%s key=%s",
            run_id,
            stage_name,
            error_object_key,
            exc_info=True,
        )
        return 0

    return replace_validation_errors_for_stage(
        engine=engine,
        run_id=run_id,
        pipeline_id=pipeline_id,
        strategy=strategy,
        run_mode=run_mode,
        stage_name=stage_name,
        error_object_key=error_object_key,
        error_rows=error_rows,
    )


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None
