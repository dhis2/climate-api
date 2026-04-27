"""Sync engine for managed datasets.

This module owns the two sync responsibilities:

- `plan_sync(...)` decides what EO API should do for one managed dataset
- `run_sync(...)` applies that plan through the existing artifact creation flow

Keeping both operations here gives the route/service layer a stable entry point
and keeps future scheduler-driven sync jobs on the same code path.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

from climate_api.ingestions.schemas import ArtifactRecord, SyncAction, SyncDetail, SyncKind, SyncResponse
from climate_api.providers import availability as provider_availability
from climate_api.publications.services import managed_dataset_id_for
from climate_api.shared.time import (
    datetime_to_period_string,
    normalize_period_string,
    parse_hourly_period_string,
    utc_now,
    utc_today,
)

logger = logging.getLogger(__name__)


class SyncConfigurationError(RuntimeError):
    """Raised when server-side sync configuration is invalid."""


def plan_sync(
    *,
    source_dataset: dict[str, Any],
    latest_artifact: ArtifactRecord,
    requested_end: str | None,
) -> SyncDetail:
    """Return the sync decision for one managed dataset without changing local state.

    The planner reads the latest materialized coverage plus the dataset template's
    declared `sync_kind` and produces the action EO API should take next.

    Current first-pass behavior:

    - temporal datasets compare the latest local period against the requested end
    - release datasets compare the current materialized release against the requested end
    - static datasets are marked as not syncable

    This planner deliberately does not download data or persist artifacts.
    """
    sync_kind_value = source_dataset.get("sync_kind")
    if not isinstance(sync_kind_value, str) or not sync_kind_value:
        raise ValueError("source_dataset must define sync_kind for sync planning")
    sync_kind = SyncKind(sync_kind_value)
    current_start = latest_artifact.request_scope.start
    current_end = latest_artifact.coverage.temporal.end

    if sync_kind == SyncKind.STATIC:
        return SyncDetail(
            source_dataset_id=latest_artifact.dataset_id,
            extent_id=latest_artifact.request_scope.extent_id,
            sync_kind=sync_kind,
            action=SyncAction.NOT_SYNCABLE,
            reason="static_dataset",
            message="This dataset is static and is not syncable.",
            current_start=current_start,
            current_end=current_end,
            target_end=current_end,
            target_end_source="current_coverage",
        )

    period_type = str(source_dataset["period_type"])
    normalized_requested_end = requested_end.strip() if isinstance(requested_end, str) else None
    normalized_requested_end = normalized_requested_end or None
    requested_target_end_source = "request" if normalized_requested_end is not None else "default_today"
    if normalized_requested_end is not None:
        resolved_end = normalize_period_string(normalized_requested_end, period_type)
    else:
        resolved_end = _default_target_end(period_type=period_type)
    latest_available_end = _latest_available_end(source_dataset=source_dataset, requested_end=resolved_end)
    target_end_source = (
        requested_target_end_source
        if latest_available_end == resolved_end
        else f"{requested_target_end_source}_clamped_by_availability"
    )

    if sync_kind == SyncKind.TEMPORAL:
        next_period_start = _next_period_start(current_end, period_type=period_type)
        if next_period_start > latest_available_end:
            return SyncDetail(
                source_dataset_id=latest_artifact.dataset_id,
                extent_id=latest_artifact.request_scope.extent_id,
                sync_kind=sync_kind,
                action=SyncAction.NO_OP,
                reason="no_new_period",
                message=(
                    f"Data already exists through {current_end}; target {latest_available_end} "
                    "does not require a new download."
                ),
                current_start=current_start,
                current_end=current_end,
                target_end=latest_available_end,
                target_end_source=target_end_source,
            )
        action = SyncAction.APPEND if _supports_append(source_dataset) else SyncAction.REMATERIALIZE
        reason = "new_periods_available_for_append" if action == SyncAction.APPEND else "new_periods_available"
        return SyncDetail(
            source_dataset_id=latest_artifact.dataset_id,
            extent_id=latest_artifact.request_scope.extent_id,
            sync_kind=sync_kind,
            action=action,
            reason=reason,
            message=_sync_plan_message(
                action=action,
                current_end=current_end,
                target_end=latest_available_end,
                delta_start=next_period_start,
                delta_end=latest_available_end,
            ),
            current_start=current_start,
            current_end=current_end,
            target_end=latest_available_end,
            target_end_source=target_end_source,
            delta_start=next_period_start,
            delta_end=latest_available_end,
        )

    if current_end >= latest_available_end:
        return SyncDetail(
            source_dataset_id=latest_artifact.dataset_id,
            extent_id=latest_artifact.request_scope.extent_id,
            sync_kind=sync_kind,
            action=SyncAction.NO_OP,
            reason="no_new_release",
            message=(
                f"Release {current_end} is already available locally; target {latest_available_end} "
                "does not require a new download."
            ),
            current_start=current_start,
            current_end=current_end,
            target_end=latest_available_end,
            target_end_source=target_end_source,
        )

    return SyncDetail(
        source_dataset_id=latest_artifact.dataset_id,
        extent_id=latest_artifact.request_scope.extent_id,
        sync_kind=sync_kind,
        action=SyncAction.REMATERIALIZE,
        reason="new_release_available",
        message=f"A newer release is available: {latest_available_end}. Sync will rematerialize the dataset.",
        current_start=current_start,
        current_end=current_end,
        target_end=latest_available_end,
        target_end_source=target_end_source,
    )


def run_sync(
    *,
    latest_artifact: ArtifactRecord,
    source_dataset: dict[str, Any],
    requested_end: str | None,
    prefer_zarr: bool,
    publish: bool,
    create_artifact_fn: Callable[..., ArtifactRecord],
    get_dataset_fn: Callable[[str], Any],
) -> SyncResponse:
    """Plan and execute one sync operation for a managed dataset.

    `run_sync(...)` is the engine entry point that non-HTTP callers can reuse
    later, for example scheduled jobs. It keeps all sync outcomes on the same
    response model:

    - `up_to_date` when no new upstream state is planned
    - `not_syncable` for templates that should not be synced
    - `completed` when EO API rematerializes a fresh backing artifact
    """
    sync_detail = plan_sync(
        source_dataset=source_dataset,
        latest_artifact=latest_artifact,
        requested_end=requested_end,
    )
    dataset_id = managed_dataset_id_for(latest_artifact)
    logger.info(
        "Sync plan for dataset '%s': action=%s reason=%s current=%s..%s target=%s delta=%s..%s",
        dataset_id,
        sync_detail.action,
        sync_detail.reason,
        sync_detail.current_start,
        sync_detail.current_end,
        sync_detail.target_end,
        sync_detail.delta_start,
        sync_detail.delta_end,
    )

    if sync_detail.action == SyncAction.NO_OP:
        logger.info("Sync skipped for dataset '%s': already current", dataset_id)
        return SyncResponse(
            sync_id=None,
            status="up_to_date",
            message="Managed dataset is already current with upstream source state.",
            dataset=get_dataset_fn(dataset_id),
            sync_detail=sync_detail,
        )

    if sync_detail.action == SyncAction.NOT_SYNCABLE:
        logger.info("Sync skipped for dataset '%s': dataset is not syncable", dataset_id)
        return SyncResponse(
            sync_id=None,
            status="not_syncable",
            message="Managed dataset is not syncable under its configured sync policy.",
            dataset=get_dataset_fn(dataset_id),
            sync_detail=sync_detail,
        )

    # Execution always goes through the ingestion materialization path so sync
    # does not grow a second downloader/storage implementation. APPEND is a V1
    # delta-download plus canonical rebuild, not in-place Zarr mutation.
    if sync_detail.current_start is None:
        raise ValueError("Sync execution requires current_start for rematerialize or append actions")
    if sync_detail.target_end is None:
        raise ValueError("Sync execution requires target_end for rematerialize or append actions")
    download_start = sync_detail.delta_start if sync_detail.action == SyncAction.APPEND else None
    logger.info(
        "Sync executing for dataset '%s': action=%s artifact_scope=%s..%s download_scope=%s..%s publish=%s",
        dataset_id,
        sync_detail.action,
        sync_detail.current_start,
        sync_detail.target_end,
        download_start or sync_detail.current_start,
        sync_detail.delta_end if download_start is not None else sync_detail.target_end,
        publish,
    )
    artifact = create_artifact_fn(
        dataset=source_dataset,
        start=sync_detail.current_start,
        end=sync_detail.target_end,
        download_start=download_start,
        download_end=sync_detail.delta_end if download_start is not None else None,
        extent_id=latest_artifact.request_scope.extent_id,
        bbox=list(latest_artifact.request_scope.bbox) if latest_artifact.request_scope.bbox is not None else None,
        country_code=None,
        overwrite=False,
        prefer_zarr=prefer_zarr,
        publish=publish,
    )
    logger.info(
        "Sync completed for dataset '%s': artifact_id=%s action=%s",
        dataset_id,
        artifact.artifact_id,
        sync_detail.action,
    )
    return SyncResponse(
        sync_id=artifact.artifact_id,
        status="completed",
        message=_sync_completed_message(sync_detail.action),
        dataset=get_dataset_fn(managed_dataset_id_for(artifact)),
        sync_detail=sync_detail,
    )


def _sync_completed_message(action: SyncAction) -> str:
    """Return a user-facing completion message for the executed sync action."""
    if action == SyncAction.APPEND:
        return "Managed dataset was synced by downloading the missing period range and rebuilding the artifact."
    return "Managed dataset was rematerialized against the latest planned upstream state."


def _sync_plan_message(
    *,
    action: SyncAction,
    current_end: str,
    target_end: str,
    delta_start: str,
    delta_end: str,
) -> str:
    """Return a human-readable sync plan summary."""
    if action == SyncAction.APPEND:
        return (
            f"Data exists through {current_end}. Sync will download missing periods "
            f"{delta_start} through {delta_end} and rebuild coverage through {target_end}."
        )
    return f"Data exists through {current_end}. Sync will rematerialize the dataset through {target_end}."


def _next_period_start(latest_period_end: str, *, period_type: str) -> str:
    """Return the next dataset-native period after a covered temporal end value.

    This helper is part of sync planning because temporal datasets need to know
    whether another period could exist beyond the current materialized coverage.
    """
    if period_type == "hourly":
        timestamp = parse_hourly_period_string(latest_period_end)
        return datetime_to_period_string(timestamp + timedelta(hours=1), period_type)
    if period_type == "daily":
        current = date.fromisoformat(latest_period_end)
        return (current + timedelta(days=1)).isoformat()
    if period_type == "monthly":
        current = date.fromisoformat(f"{latest_period_end}-01")
        month = current.month + 1
        year = current.year + (1 if month == 13 else 0)
        month = 1 if month == 13 else month
        return f"{year:04d}-{month:02d}"
    if period_type == "yearly":
        return str(int(latest_period_end) + 1)
    raise ValueError(f"Unsupported period_type '{period_type}' for sync")


def _default_target_end(*, period_type: str) -> str:
    """Return the default sync target in the dataset-native period format."""
    today = utc_today()
    if period_type == "hourly":
        return datetime_to_period_string(utc_now(), period_type)
    if period_type == "daily":
        return today.isoformat()
    if period_type == "monthly":
        return f"{today.year:04d}-{today.month:02d}"
    if period_type == "yearly":
        return str(today.year)
    raise ValueError(f"Unsupported period_type '{period_type}' for sync")


def _latest_available_end(*, source_dataset: dict[str, Any], requested_end: str) -> str:
    """Clamp requested sync end to the latest upstream state declared by template metadata.

    The current engine does not query upstream providers directly. Instead it can
    apply conservative template metadata so sync planning does not overshoot known
    provider lag or release cadence.
    """
    availability = source_dataset.get("sync_availability")
    if not isinstance(availability, dict):
        return requested_end

    provider_latest = _provider_latest_available_end(
        source_dataset=source_dataset,
        availability=availability,
        requested_end=requested_end,
    )
    if provider_latest is not None:
        return min(requested_end, provider_latest)
    # Keep the legacy metadata-only lag fallback for templates that do not yet
    # declare a latest_available_function, but delegate to the provider helper
    # so lag logic lives in one place.
    return min(
        requested_end,
        provider_availability.lagged_latest_available(
            dataset=source_dataset,
            requested_end=requested_end,
        ),
    )


def _supports_append(source_dataset: dict[str, Any]) -> bool:
    """Return whether this template opts into V1 delta-download sync execution."""
    if source_dataset.get("sync_execution") != SyncAction.APPEND.value:
        return False
    cache_info = source_dataset.get("cache_info")
    if not isinstance(cache_info, dict):
        return False
    # Multiscale stores are rebuilt as pyramids today; keep append opt-in to flat
    # canonical stores until pyramid delta behavior is explicitly designed.
    if cache_info.get("multiscales"):
        logger.warning(
            "Sync append execution is not supported for multiscale dataset '%s'; falling back to rematerialize",
            source_dataset.get("id", "<unknown>"),
        )
        return False
    return True


def _provider_latest_available_end(
    *,
    source_dataset: dict[str, Any],
    availability: dict[str, Any],
    requested_end: str,
) -> str | None:
    """Call an optional provider-specific latest-availability function."""
    function_path = availability.get("latest_available_function")
    if not isinstance(function_path, str) or not function_path:
        return None

    try:
        latest_available_fn = _get_dynamic_function(function_path)
        params: dict[str, Any] = {}
        signature = inspect.signature(latest_available_fn)
        if "dataset" in signature.parameters:
            params["dataset"] = source_dataset
        if "requested_end" in signature.parameters:
            params["requested_end"] = requested_end
        result = latest_available_fn(**params)
    except (AttributeError, ImportError, TypeError) as exc:
        raise SyncConfigurationError(f"Latest availability function '{function_path}' failed: {exc}") from exc
    if not isinstance(result, str):
        raise SyncConfigurationError(f"Latest availability function '{function_path}' must return a period string")
    try:
        return normalize_period_string(result, period_type=str(source_dataset["period_type"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise SyncConfigurationError(
            f"Latest availability function '{function_path}' returned invalid period "
            f"'{result}' for dataset period_type '{source_dataset.get('period_type')}'"
        ) from exc


def _get_dynamic_function(full_path: str) -> Callable[..., Any]:
    """Import and return a function given its dotted module path."""
    parts = full_path.split(".")
    module_path = ".".join(parts[:-1])
    function_name = parts[-1]
    module = importlib.import_module(module_path)
    return getattr(module, function_name)  # type: ignore[no-any-return]
