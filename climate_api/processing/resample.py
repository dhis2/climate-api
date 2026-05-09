"""Temporal resampling for derived managed datasets."""

from __future__ import annotations

import logging
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import numpy as np
import xarray as xr
from fastapi import HTTPException

from climate_api.data_accessor.services.accessor import open_zarr_dataset
from climate_api.data_manager.services.utils import get_time_dim
from climate_api.data_registry.services import datasets as registry_datasets
from climate_api.ingestions import services as ingestion_services
from climate_api.ingestions import sync_engine
from climate_api.ingestions.schemas import ArtifactFormat, ArtifactRecord, ArtifactRequestScope, PublicationStatus
from climate_api.publications.services import managed_dataset_id_for_scope
from climate_api.shared.time import _coerce_numpy_datetime, datetime_to_period_string, parse_period_string_to_datetime

logger = logging.getLogger(__name__)


def _derived_data_dir() -> Path:
    """Return the directory used to store derived Zarr artifacts."""
    from climate_api import config as api_config

    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "derived"
    return Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share")) / "climate-api" / "derived"


DERIVED_DATA_DIR: Path = _derived_data_dir()
_PERIOD_ORDER = {"hourly": 0, "daily": 1, "weekly": 2, "monthly": 3, "yearly": 4}


def derived_dataset_id(*, source_dataset_id: str, period_type: str, method: str) -> str:
    """Return the stable derived dataset id auto-generated from source + parameters."""
    return f"{source_dataset_id}_{period_type}_{method}"


def materialize_resampled_artifact(
    *,
    source_dataset_id: str,
    period_type: str,
    method: str,
    week_start: str = "monday",
    start: str,
    end: str | None,
    extent_id: str | None,
    bbox: list[float] | None,
    overwrite: bool,
    publish: bool,
) -> ArtifactRecord:
    """Materialize a derived dataset by resampling an existing managed source dataset."""
    target_dataset_id = derived_dataset_id(source_dataset_id=source_dataset_id, period_type=period_type, method=method)
    resolved_end = end or ingestion_services._default_request_end(period_type)
    normalized_start = ingestion_services._normalize_request_period(start, period_type=period_type, field_name="start")
    normalized_end = ingestion_services._normalize_request_period(
        resolved_end, period_type=period_type, field_name="end"
    )

    existing = ingestion_services._find_existing_artifact(
        dataset_id=target_dataset_id,
        request_scope=ArtifactRequestScope(
            start=normalized_start,
            end=normalized_end,
            extent_id=extent_id,
            bbox=(bbox[0], bbox[1], bbox[2], bbox[3]) if bbox is not None and extent_id is None else None,
        ),
        prefer_zarr=True,
    )
    if existing is not None and not overwrite:
        if publish and existing.publication.status != PublicationStatus.PUBLISHED:
            return ingestion_services.publish_artifact_record(existing.artifact_id)
        return existing

    source_dataset = registry_datasets.get_dataset(source_dataset_id)
    if source_dataset is None:
        raise HTTPException(status_code=404, detail=f"Source dataset template '{source_dataset_id}' not found")

    _validate_period_hierarchy(source_period_type=str(source_dataset["period_type"]), target_period_type=period_type)
    source_artifact = _resolve_source_artifact(source_dataset_id=source_dataset_id, extent_id=extent_id, bbox=bbox)
    if source_artifact.format != ArtifactFormat.ZARR:
        raise HTTPException(status_code=409, detail="Resampling currently requires a Zarr-backed source artifact")

    source_bbox = bbox
    if source_bbox is None and source_artifact.request_scope.bbox is not None:
        source_bbox = list(source_artifact.request_scope.bbox)
    target_managed_dataset_id = managed_dataset_id_for_scope(
        target_dataset_id,
        extent_id=extent_id,
        bbox=source_bbox if extent_id is None else None,
    )
    zarr_path = DERIVED_DATA_DIR / f"{target_managed_dataset_id}.zarr"

    source_ds = open_zarr_dataset(source_artifact.path or source_artifact.asset_paths[0])
    try:
        resampled = _resample_dataset(
            source_ds=source_ds,
            source_period_type=str(source_dataset["period_type"]),
            method=method,
            week_start=week_start,
            target_period_type=period_type,
            start=normalized_start,
            end=normalized_end,
        )
        if resampled.sizes.get(get_time_dim(resampled), 0) == 0:
            raise HTTPException(status_code=409, detail="Source artifact does not contain any complete target periods")
        existing_realized = _find_existing_resampled_artifact(
            target_dataset_id=target_dataset_id,
            period_type=period_type,
            extent_id=extent_id,
            bbox=source_bbox if extent_id is None else None,
            start=normalized_start,
            resampled=resampled,
        )
        if existing_realized is not None and not overwrite:
            if publish and existing_realized.publication.status != PublicationStatus.PUBLISHED:
                return ingestion_services.publish_artifact_record(existing_realized.artifact_id)
            return existing_realized
        _write_resampled_zarr(resampled, zarr_path)
    finally:
        source_ds.close()

    target_dataset: dict[str, object] = {
        "id": target_dataset_id,
        "name": target_dataset_id,
        "variable": source_dataset.get("variable", "value"),
        "period_type": period_type,
    }
    return ingestion_services.store_materialized_zarr_artifact(
        dataset=target_dataset,
        start=normalized_start,
        end=normalized_end,
        extent_id=extent_id,
        bbox=source_bbox if extent_id is None else None,
        zarr_path=zarr_path,
        overwrite=overwrite,
        publish=publish,
    )


def _validate_period_hierarchy(*, source_period_type: str, target_period_type: str) -> None:
    if source_period_type not in _PERIOD_ORDER or target_period_type not in _PERIOD_ORDER:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported source/target period types: {source_period_type} -> {target_period_type}",
        )
    if _PERIOD_ORDER[source_period_type] >= _PERIOD_ORDER[target_period_type]:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Resampling requires a coarser target period than source: {source_period_type} -> {target_period_type}"
            ),
        )


def _resolve_source_artifact(
    *,
    source_dataset_id: str,
    extent_id: str | None,
    bbox: list[float] | None,
) -> ArtifactRecord:
    managed_dataset_id = managed_dataset_id_for_scope(source_dataset_id, extent_id=extent_id, bbox=bbox)
    return ingestion_services.get_latest_artifact_for_dataset_or_404(managed_dataset_id)


def _resample_dataset(
    *,
    source_ds: xr.Dataset,
    source_period_type: str,
    method: str,
    week_start: str,
    target_period_type: str,
    start: str,
    end: str,
) -> xr.Dataset:
    time_dim = get_time_dim(source_ds)
    target_end_exclusive = parse_period_string_to_datetime(
        sync_engine._next_period_start(end, period_type=target_period_type)
    ).replace(tzinfo=None)
    target_start = parse_period_string_to_datetime(start).replace(tzinfo=None)
    subset = source_ds.where(source_ds[time_dim] >= np.datetime64(target_start), drop=True)
    subset = subset.where(subset[time_dim] < np.datetime64(target_end_exclusive), drop=True)
    if subset.sizes.get(time_dim, 0) == 0:
        raise HTTPException(status_code=409, detail="Source artifact contains no data for the requested resample range")
    source_start = _coerce_numpy_datetime(subset[time_dim].values[0])
    source_end = _coerce_numpy_datetime(subset[time_dim].values[-1])

    with xr.set_options(keep_attrs=True):
        resampler = subset.resample(
            {time_dim: _resample_frequency(target_period_type=target_period_type, week_start=week_start)},
            label="left",
            closed="left",
        )
        result = cast(xr.Dataset, getattr(resampler, method)())
    result = _drop_incomplete_edge_periods(
        result=result,
        source_start=source_start,
        source_end=source_end,
        source_period_type=source_period_type,
        target_period_type=target_period_type,
    )
    return result


def _resample_frequency(*, target_period_type: str, week_start: str) -> str:
    if target_period_type == "daily":
        return "1D"
    if target_period_type == "weekly":
        return "W-MON" if week_start == "monday" else "W-SUN"
    if target_period_type == "monthly":
        return "MS"
    if target_period_type == "yearly":
        return "YS"
    raise HTTPException(status_code=400, detail=f"Unsupported target period_type '{target_period_type}' for resampling")


def _drop_incomplete_edge_periods(
    *,
    result: xr.Dataset,
    source_start: datetime,
    source_end: datetime,
    source_period_type: str,
    target_period_type: str,
) -> xr.Dataset:
    time_dim = get_time_dim(result)
    if result.sizes.get(time_dim, 0) == 0:
        return result

    first_output_start = _coerce_numpy_datetime(result[time_dim].values[0])
    if source_start > first_output_start:
        result = result.isel({time_dim: slice(1, None)})
        if result.sizes.get(time_dim, 0) == 0:
            return result

    last_output_start = _coerce_numpy_datetime(result[time_dim].values[-1])
    next_target_start = parse_period_string_to_datetime(
        sync_engine._next_period_start(
            datetime_to_period_string(last_output_start.replace(tzinfo=UTC), target_period_type),
            period_type=target_period_type,
        )
    ).replace(tzinfo=None)
    required_source_end = _previous_source_period_start(next_target_start, source_period_type=source_period_type)
    if source_end < required_source_end:
        return result.isel({time_dim: slice(0, -1)})
    return result


def _find_existing_resampled_artifact(
    *,
    target_dataset_id: str,
    period_type: str,
    extent_id: str | None,
    bbox: list[float] | None,
    start: str,
    resampled: xr.Dataset,
) -> ArtifactRecord | None:
    time_dim = get_time_dim(resampled)
    realized_end = datetime_to_period_string(
        _coerce_numpy_datetime(resampled[time_dim].values[-1]).replace(tzinfo=UTC),
        period_type,
    )
    return ingestion_services._find_existing_artifact(
        dataset_id=target_dataset_id,
        request_scope=ArtifactRequestScope(
            start=start,
            end=realized_end,
            extent_id=extent_id,
            bbox=(bbox[0], bbox[1], bbox[2], bbox[3]) if bbox is not None else None,
        ),
        prefer_zarr=True,
    )


def _previous_source_period_start(boundary: datetime, *, source_period_type: str) -> datetime:
    if source_period_type == "hourly":
        return boundary - timedelta(hours=1)
    if source_period_type == "daily":
        return boundary - timedelta(days=1)
    if source_period_type == "weekly":
        return boundary - timedelta(days=7)
    if source_period_type == "monthly":
        previous_month_last_day = boundary.replace(day=1) - timedelta(days=1)
        return previous_month_last_day.replace(day=1)
    if source_period_type == "yearly":
        return boundary.replace(year=boundary.year - 1, month=1, day=1)
    raise HTTPException(status_code=400, detail=f"Unsupported source period_type '{source_period_type}' for resampling")


def _write_resampled_zarr(ds: xr.Dataset, zarr_path: Path) -> None:
    zarr_path.parent.mkdir(parents=True, exist_ok=True)
    if zarr_path.exists():
        shutil.rmtree(zarr_path)
    ds_chunked = ds.chunk("auto").unify_chunks()
    try:
        ds_chunked.to_zarr(zarr_path, mode="w", consolidated=True)
    finally:
        ds_chunked.close()
        ds.close()
