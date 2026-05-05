from __future__ import annotations

from collections.abc import Mapping
import logging
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ingestion_core.adapters.object_store import ObjectStoreClient, ObjectStoreConfig

logger = logging.getLogger(__name__)


def with_application_name(dsn: str, application_name: str) -> str:
    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["application_name"] = application_name
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def build_application_name(
    *,
    strategy: str,
    run_mode: str,
    stage_name: str,
    role: str,
) -> str:
    compact_strategy = strategy.replace("_", "-")
    compact_stage = stage_name.replace("_", "-")
    compact_role = role.replace("_", "-")
    compact_run_mode = run_mode.replace("_", "-")
    return f"ingestion/{compact_strategy}/{compact_run_mode}/{compact_stage}/{compact_role}"[:63]


def has_artifact_keys(payload: Mapping[str, Any]) -> bool:
    return any(
        isinstance(key, str)
        and key.endswith(("_object_key", "_manifest_key"))
        and _text_or_none(value) is not None
        for key, value in payload.items()
    )


def enrich_metrics_payload(
    *,
    payload: Mapping[str, Any],
    strategy: str,
    run_mode: str,
    pipeline_id: str,
    run_id: str,
    stage_name: str | None = None,
    object_store_config: ObjectStoreConfig | None = None,
) -> dict[str, Any]:
    metrics = dict(payload)
    metrics["strategy"] = strategy
    metrics["run_mode"] = run_mode
    metrics["pipeline_id"] = pipeline_id
    metrics["run_id"] = run_id
    if stage_name is not None:
        metrics["stage_name"] = stage_name

    metrics.update(_derive_rate_metrics(metrics))
    metrics.update(_collect_artifact_metrics(metrics, object_store_config))
    metrics["problem_summary"] = _build_problem_summary(metrics)
    return metrics


def _derive_rate_metrics(payload: Mapping[str, Any]) -> dict[str, Any]:
    derived: dict[str, Any] = {}

    source_count = _first_int(payload, "source_row_count", "source_event_count")
    source_read_seconds = _number_or_none(payload.get("source_read_seconds"))
    if source_count is not None and source_read_seconds is not None and source_read_seconds > 0:
        derived["source_records_per_second"] = round(source_count / source_read_seconds, 3)

    read_count = _first_int(payload, "read_count")
    total_seconds = _number_or_none(payload.get("total_seconds"))
    if read_count is not None and total_seconds is not None and total_seconds > 0:
        derived["processed_records_per_second"] = round(read_count / total_seconds, 3)

    if read_count is not None and read_count > 0:
        change_count = sum(
            value or 0
            for value in (
                _first_int(payload, "insert_count"),
                _first_int(payload, "update_count"),
                _first_int(payload, "delete_count"),
            )
        )
        derived["change_ratio"] = round(change_count / read_count, 6)

    invalid_count = _first_int(payload, "invalid_row_count", "invalid_event_count")
    if source_count is not None and source_count > 0 and invalid_count is not None:
        derived["invalid_ratio"] = round(invalid_count / source_count, 6)

    return derived


def _collect_artifact_metrics(
    payload: Mapping[str, Any],
    object_store_config: ObjectStoreConfig | None,
) -> dict[str, Any]:
    if object_store_config is None:
        return {}

    artifact_keys = {
        key: normalized
        for key, value in payload.items()
        if isinstance(key, str)
        and key.endswith(("_object_key", "_manifest_key"))
        and (normalized := _text_or_none(value)) is not None
    }
    if not artifact_keys:
        return {}

    object_store = ObjectStoreClient(object_store_config)
    artifact_sizes: dict[str, int] = {}
    for metric_key, object_key in artifact_keys.items():
        try:
            artifact_sizes[metric_key] = object_store.get_object_size(object_key)
        except Exception:
            logger.warning("Could not resolve artifact size for key=%s", object_key, exc_info=True)

    if not artifact_sizes:
        return {}

    return {
        "artifact_sizes_bytes": artifact_sizes,
        "artifact_total_bytes": sum(artifact_sizes.values()),
    }


def _build_problem_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    invalid_record_count = _first_int(payload, "invalid_row_count", "invalid_event_count") or 0
    invalid_transaction_count = _first_int(payload, "invalid_transaction_count") or 0
    quarantined_event_count = _first_int(payload, "quarantined_event_count") or 0
    quarantined_transaction_count = _first_int(payload, "quarantined_transaction_count") or 0
    failed_task_count = len(payload.get("failed_tasks") or [])

    has_problems = any(
        (
            invalid_record_count,
            invalid_transaction_count,
            quarantined_event_count,
            quarantined_transaction_count,
            failed_task_count,
        )
    )

    return {
        "has_problems": has_problems,
        "invalid_record_count": invalid_record_count,
        "invalid_transaction_count": invalid_transaction_count,
        "quarantined_event_count": quarantined_event_count,
        "quarantined_transaction_count": quarantined_transaction_count,
        "failed_task_count": failed_task_count,
        "error_artifact_key": (
            _text_or_none(payload.get("validation_error_object_key"))
            or _text_or_none(payload.get("error_object_key"))
        ),
    }


def _first_int(payload: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = payload.get(key)
        if value is None or value == "":
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _number_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None
