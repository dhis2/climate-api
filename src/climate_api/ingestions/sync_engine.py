"""Sync engine for managed datasets.

This module owns the two sync responsibilities:

- `plan_sync(...)` decides what EO API should do for one managed dataset
- `run_sync(...)` applies that plan through the existing artifact creation flow

Keeping both operations here gives the route/service layer a stable entry point
and keeps future scheduler-driven sync jobs on the same code path.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any

from climate_api.ingestions.schemas import ArtifactRecord, SyncAction, SyncDetail, SyncKind, SyncResponse
from climate_api.publications.services import managed_dataset_id_for


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
    sync_kind = SyncKind(str(source_dataset.get("sync_kind", SyncKind.TEMPORAL.value)))
    resolved_end = requested_end or date.today().isoformat()
    current_start = latest_artifact.request_scope.start
    current_end = latest_artifact.coverage.temporal.end
    latest_available_end = _latest_available_end(source_dataset=source_dataset, requested_end=resolved_end)
    next_period_start = _next_period_start(current_end, period_type=str(source_dataset["period_type"]))

    if sync_kind == SyncKind.STATIC:
        return SyncDetail(
            source_dataset_id=latest_artifact.dataset_id,
            extent_id=latest_artifact.request_scope.extent_id,
            sync_kind=sync_kind,
            action=SyncAction.NOT_SYNCABLE,
            reason="static_dataset",
            requested_start=current_start,
            requested_end=current_end,
            latest_available_end=current_end,
        )

    if sync_kind == SyncKind.TEMPORAL:
        # V1 rematerializes from the original request scope even when only a delta exists.
        if next_period_start > latest_available_end:
            return SyncDetail(
                source_dataset_id=latest_artifact.dataset_id,
                extent_id=latest_artifact.request_scope.extent_id,
                sync_kind=sync_kind,
                action=SyncAction.NO_OP,
                reason="no_new_period",
                requested_start=current_start,
                requested_end=current_end,
                latest_available_end=latest_available_end,
            )
        return SyncDetail(
            source_dataset_id=latest_artifact.dataset_id,
            extent_id=latest_artifact.request_scope.extent_id,
            sync_kind=sync_kind,
            action=SyncAction.REMATERIALIZE,
            reason="new_periods_available",
            requested_start=current_start,
            requested_end=latest_available_end,
            latest_available_start=next_period_start,
            latest_available_end=latest_available_end,
        )

    if current_end >= latest_available_end:
        return SyncDetail(
            source_dataset_id=latest_artifact.dataset_id,
            extent_id=latest_artifact.request_scope.extent_id,
            sync_kind=sync_kind,
            action=SyncAction.NO_OP,
            reason="no_new_release",
            requested_start=current_start,
            requested_end=current_end,
            latest_available_end=latest_available_end,
        )

    return SyncDetail(
        source_dataset_id=latest_artifact.dataset_id,
        extent_id=latest_artifact.request_scope.extent_id,
        sync_kind=sync_kind,
        action=SyncAction.REMATERIALIZE,
        reason="new_release_available",
        requested_start=current_start,
        requested_end=latest_available_end,
        latest_available_end=latest_available_end,
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

    if sync_detail.action == SyncAction.NO_OP:
        return SyncResponse(
            sync_id=None,
            status="up_to_date",
            message="Managed dataset is already current with upstream source state.",
            dataset=get_dataset_fn(dataset_id),
            sync_detail=sync_detail,
        )

    if sync_detail.action == SyncAction.NOT_SYNCABLE:
        return SyncResponse(
            sync_id=None,
            status="not_syncable",
            message="Managed dataset is not syncable under its configured sync policy.",
            dataset=get_dataset_fn(dataset_id),
            sync_detail=sync_detail,
        )

    # Execution always goes through the ingestion materialization path so sync
    # does not grow a second downloader/storage implementation.
    artifact = create_artifact_fn(
        dataset=source_dataset,
        start=sync_detail.requested_start or latest_artifact.request_scope.start,
        end=sync_detail.requested_end,
        extent_id=latest_artifact.request_scope.extent_id,
        bbox=list(latest_artifact.request_scope.bbox) if latest_artifact.request_scope.bbox is not None else None,
        country_code=None,
        overwrite=False,
        prefer_zarr=prefer_zarr,
        publish=publish,
    )
    return SyncResponse(
        sync_id=artifact.artifact_id,
        status="completed",
        message="Managed dataset was rematerialized against the latest planned upstream state.",
        dataset=get_dataset_fn(managed_dataset_id_for(artifact)),
        sync_detail=sync_detail,
    )


def _next_period_start(latest_period_end: str, *, period_type: str) -> str:
    """Return the next dataset-native period after a covered temporal end value.

    This helper is part of sync planning because temporal datasets need to know
    whether another period could exist beyond the current materialized coverage.
    """
    if period_type == "hourly":
        timestamp = datetime.fromisoformat(latest_period_end)
        return (timestamp + timedelta(hours=1)).isoformat()
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


def _latest_available_end(*, source_dataset: dict[str, Any], requested_end: str) -> str:
    """Clamp requested sync end to the latest upstream state declared by template metadata.

    The current engine does not query upstream providers directly. Instead it can
    apply conservative template metadata so sync planning does not overshoot known
    provider lag or release cadence.
    """
    availability = source_dataset.get("sync_availability")
    if not isinstance(availability, dict):
        return requested_end

    period_type = str(source_dataset["period_type"])
    if period_type in {"hourly", "daily", "monthly"}:
        lag_days = availability.get("lag_days")
        if isinstance(lag_days, int) and lag_days > 0:
            latest_by_lag = (date.today() - timedelta(days=lag_days)).isoformat()
            return min(requested_end, latest_by_lag)
        return requested_end

    if period_type == "yearly":
        latest_year_offset = availability.get("latest_year_offset")
        if isinstance(latest_year_offset, int) and latest_year_offset >= 0:
            latest_year = date.today().year - latest_year_offset
            return str(min(int(requested_end), latest_year))
        return requested_end

    return requested_end
