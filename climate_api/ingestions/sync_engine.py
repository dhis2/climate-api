"""Sync engine for managed datasets.

This module owns the two sync responsibilities:

- `plan_sync(...)` decides what EO API should do for one managed dataset
- `run_sync(...)` applies that plan through the existing artifact creation flow

Keeping both operations here gives the route/service layer a stable entry point
and keeps future scheduler-driven sync jobs on the same code path.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from typing import Any

from climate_api.ingestions.schemas import ArtifactRecord, SyncAction, SyncDetail, SyncKind, SyncResponse
from climate_api.publications.services import managed_dataset_id_for
from climate_api.shared.time import (
    datetime_to_period_string,
    normalize_period_string,
    parse_hourly_period_string,
    parse_period_string_to_datetime,
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
    current_end: str | None = None,
) -> SyncDetail:
    """Return the sync decision for one managed dataset without changing local state.

    The planner reads the latest materialized coverage plus the dataset template's
    declared `sync_kind` and produces the action EO API should take next.

    Current first-pass behavior:

    - temporal datasets compare the latest local period against the requested end
    - release datasets compare the current materialized release against the requested end
    - static datasets are marked as not syncable

    `current_end` overrides `latest_artifact.coverage.temporal.end` when provided.
    Callers pass the store-authoritative value for formats (e.g. Icechunk) where the
    artifact metadata record may lag behind what is actually committed on disk.

    This planner deliberately does not download data or persist artifacts.
    """
    sync_kind_value = source_dataset.get("sync", {}).get("kind")
    if not isinstance(sync_kind_value, str) or not sync_kind_value:
        raise ValueError("source_dataset must define sync.kind for sync planning")
    sync_kind = SyncKind(sync_kind_value)
    current_start = latest_artifact.request_scope.start
    current_end = current_end if current_end is not None else latest_artifact.coverage.temporal.end

    if sync_kind == SyncKind.STATIC:
        return SyncDetail(
            source_dataset_id=latest_artifact.dataset_id,
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
    latest_available_end = _latest_available_end(
        source_dataset=source_dataset, requested_end=resolved_end, current_end=current_end
    )
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
        action = SyncAction.APPEND if _supports_append(source_dataset, latest_artifact) else SyncAction.REMATERIALIZE
        reason = "new_periods_available_for_append" if action == SyncAction.APPEND else "new_periods_available"
        return SyncDetail(
            source_dataset_id=latest_artifact.dataset_id,
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
    current_end: str | None = None,
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
        current_end=current_end,
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
        bbox=list(latest_artifact.request_scope.bbox) if latest_artifact.request_scope.bbox is not None else None,
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
    if period_type == "weekly":
        current = parse_period_string_to_datetime(latest_period_end).date()
        next_week = datetime.combine(current + timedelta(days=7), time(0))
        return datetime_to_period_string(next_week, period_type)
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
    if period_type == "weekly":
        return datetime_to_period_string(utc_now(), period_type)
    if period_type == "monthly":
        return f"{today.year:04d}-{today.month:02d}"
    if period_type == "yearly":
        return str(today.year)
    raise ValueError(f"Unsupported period_type '{period_type}' for sync")


def _latest_available_end(*, source_dataset: dict[str, Any], requested_end: str, current_end: str | None = None) -> str:
    """Clamp requested sync end to the latest upstream state via the plugin's periods() method.

    current_end must be provided so the function can return it when periods() reports nothing
    new (empty list → NOOP detected by caller).
    """
    period_type = source_dataset.get("period_type")
    if current_end is not None and isinstance(period_type, str):
        ingestion = source_dataset.get("ingestion")
        if isinstance(ingestion, dict) and isinstance(ingestion.get("plugin"), str):
            next_start = _next_period_start(current_end, period_type=period_type)
            plugin_latest = _plugin_latest_available_period(
                source_dataset=source_dataset,
                next_period_start=next_start,
                requested_end=requested_end,
                current_end=current_end,
            )
            if plugin_latest is not None:
                return min(requested_end, plugin_latest)

    return requested_end


def _plugin_latest_available_period(
    *,
    source_dataset: dict[str, Any],
    next_period_start: str,
    requested_end: str,
    current_end: str,
) -> str | None:
    """Return the last period available from next_period_start..requested_end via the plugin.

    Returns:
    - str: the last available period in the range (may equal current_end when nothing new)
    - None: plugin could not be instantiated (caller falls back to legacy availability logic)

    Calls asyncio.run() which requires no running event loop — plan_sync is synchronous
    and FastAPI runs sync handlers in a thread pool, so this is safe.
    """
    ingestion = source_dataset.get("ingestion")
    if not isinstance(ingestion, dict):
        return None
    plugin_path = ingestion.get("plugin")
    if not isinstance(plugin_path, str):
        return None

    _raw_params = ingestion.get("params")
    params: dict[str, Any] = dict(_raw_params) if isinstance(_raw_params, dict) else {}

    try:
        from climate_api.ingest.orchestrator import load_plugin

        plugin = load_plugin(plugin_path, params)
    except (TypeError, ValueError, ImportError, AttributeError) as exc:
        logger.debug(
            "Plugin '%s' cannot be instantiated for availability check (needs extra_params?): %s",
            plugin_path,
            exc,
        )
        return None

    try:
        periods = asyncio.run(plugin.periods(next_period_start, requested_end))
    except Exception as exc:
        logger.debug("plugin.periods() failed during availability check for '%s': %s", plugin_path, exc)
        return None

    return periods[-1] if periods else current_end


def _supports_append(source_dataset: dict[str, Any], latest_artifact: ArtifactRecord) -> bool:
    """Return whether this artifact supports incremental append sync execution.

    Icechunk stores always support append: the orchestrator uses read_committed_period_ids
    to determine exactly which periods are missing and commits only those. No YAML
    sync.execution flag is required.

    For all other formats the YAML must opt in with sync.execution: append, and
    pyramid zarr stores (identified by a "0/" subdirectory) are excluded because
    they must be rebuilt in full.
    """
    from climate_api.ingestions.schemas import ArtifactFormat

    if latest_artifact.format == ArtifactFormat.ICECHUNK:
        # Pyramid Icechunk stores have data under group "0"; appending to root
        # would create a second flat dataset instead of extending the pyramid.
        # Fall back to rematerialize so the full pyramid is rebuilt.
        artifact_path = latest_artifact.path
        if artifact_path:
            from pathlib import Path

            from climate_api.ingest.store import open_or_create_repo

            try:
                import zarr

                repo = open_or_create_repo(Path(artifact_path))
                session = repo.readonly_session("main")
                root = zarr.open_group(session.store, mode="r")
                if "multiscales" in root.attrs:
                    logger.warning(
                        "Sync append is not supported for pyramid Icechunk dataset '%s'; falling back to rematerialize",
                        source_dataset.get("id", "<unknown>"),
                    )
                    return False
            except Exception:
                pass  # store missing or unreadable — let the ingest path handle it
        return True

    if source_dataset.get("sync", {}).get("execution") != SyncAction.APPEND.value:
        return False
    # Pyramid zarr stores cannot be appended to — they must be rebuilt in full.
    # Detect this from the existing artifact's on-disk structure rather than YAML.
    from pathlib import Path

    artifact_path = latest_artifact.path
    if artifact_path and "://" not in artifact_path and (Path(artifact_path) / "0").is_dir():
        logger.warning(
            "Sync append execution is not supported for pyramid zarr dataset '%s'; falling back to rematerialize",
            source_dataset.get("id", "<unknown>"),
        )
        return False
    return True
