"""Temporal resampling for derived managed datasets."""

from __future__ import annotations

import logging
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import numpy as np
import xarray as xr
from fastapi import HTTPException

from climate_api.data_accessor.services.accessor import open_zarr_dataset
from climate_api.data_manager.services.utils import get_time_dim
from climate_api.data_registry.services import datasets as registry_datasets
from climate_api.ingestions import services as ingestion_services
from climate_api.ingestions import sync_engine
from climate_api.ingestions.schemas import ArtifactFormat, ArtifactRecord, ArtifactRequestScope
from climate_api.publications.services import managed_dataset_id_for_scope
from climate_api.shared.time import datetime_to_period_string, parse_period_string_to_datetime

logger = logging.getLogger(__name__)

DERIVED_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "derived"
_PERIOD_ORDER = {"hourly": 0, "daily": 1, "weekly": 2, "monthly": 3, "yearly": 4}


def materialize_resampled_artifact(
    *,
    target_dataset: dict[str, Any],
    start: str,
    end: str | None,
    extent_id: str | None,
    bbox: list[float] | None,
    overwrite: bool,
    publish: bool,
) -> ArtifactRecord:
    """Materialize a derived dataset by resampling an existing managed source dataset."""
    resample = _require_resample_config(target_dataset)
    resolved_end = end or ingestion_services._default_request_end(str(target_dataset["period_type"]))
    normalized_start = ingestion_services._normalize_request_period(
        start,
        period_type=str(target_dataset["period_type"]),
        field_name="start",
    )
    normalized_end = ingestion_services._normalize_request_period(
        resolved_end,
        period_type=str(target_dataset["period_type"]),
        field_name="end",
    )

    existing = ingestion_services._find_existing_artifact(
        dataset_id=str(target_dataset["id"]),
        request_scope=ArtifactRequestScope(
            start=normalized_start,
            end=normalized_end,
            extent_id=extent_id,
            bbox=(bbox[0], bbox[1], bbox[2], bbox[3]) if bbox is not None and extent_id is None else None,
        ),
        prefer_zarr=True,
    )
    if existing is not None and not overwrite:
        return existing

    source_dataset_id = str(resample["source_dataset_id"])
    source_dataset = registry_datasets.get_dataset(source_dataset_id)
    if source_dataset is None:
        raise HTTPException(status_code=404, detail=f"Source dataset template '{source_dataset_id}' not found")

    _validate_period_hierarchy(source_period_type=str(source_dataset["period_type"]), target_dataset=target_dataset)
    source_artifact = _resolve_source_artifact(
        source_dataset_id=source_dataset_id,
        extent_id=extent_id,
        bbox=bbox,
    )
    if source_artifact.format != ArtifactFormat.ZARR:
        raise HTTPException(status_code=409, detail="Resampling currently requires a Zarr-backed source artifact")

    source_bbox = bbox
    if source_bbox is None and source_artifact.request_scope.bbox is not None:
        source_bbox = list(source_artifact.request_scope.bbox)
    target_managed_dataset_id = managed_dataset_id_for_scope(
        str(target_dataset["id"]),
        extent_id=extent_id,
        bbox=source_bbox if extent_id is None else None,
    )
    zarr_path = DERIVED_DATA_DIR / f"{target_managed_dataset_id}.zarr"

    source_ds = open_zarr_dataset(source_artifact.path or source_artifact.asset_paths[0])
    try:
        resampled = _resample_dataset(
            source_ds=source_ds,
            source_period_type=str(source_dataset["period_type"]),
            resample_config=resample,
            target_period_type=str(target_dataset["period_type"]),
            start=normalized_start,
            end=normalized_end,
        )
        if resampled.sizes.get(get_time_dim(resampled), 0) == 0:
            raise HTTPException(status_code=409, detail="Source artifact does not contain any complete target periods")
        _write_resampled_zarr(resampled, zarr_path)
    finally:
        source_ds.close()

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


def _require_resample_config(target_dataset: dict[str, Any]) -> dict[str, Any]:
    resample = target_dataset.get("resample")
    if not isinstance(resample, dict):
        raise HTTPException(
            status_code=400,
            detail=f"Dataset '{target_dataset.get('id')}' is not configured for resampling",
        )
    return resample


def _validate_period_hierarchy(*, source_period_type: str, target_dataset: dict[str, Any]) -> None:
    target_period_type = str(target_dataset["period_type"])
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
    resample_config: dict[str, Any],
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
    source_end = _coord_to_datetime(subset[time_dim].values[-1])

    with xr.set_options(keep_attrs=True):
        resampler = subset.resample(
            {
                time_dim: _resample_frequency(
                    target_period_type=target_period_type,
                    week_start=str(resample_config.get("week_start", "monday")),
                )
            },
            label="left",
            closed="left",
        )
        result = cast(xr.Dataset, getattr(resampler, str(resample_config["method"]))())
    result = _drop_incomplete_trailing_period(
        result=result,
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


def _drop_incomplete_trailing_period(
    *,
    result: xr.Dataset,
    source_end: datetime,
    source_period_type: str,
    target_period_type: str,
) -> xr.Dataset:
    time_dim = get_time_dim(result)
    if result.sizes.get(time_dim, 0) == 0:
        return result

    last_output_start = _coord_to_datetime(result[time_dim].values[-1])
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
    DERIVED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    if zarr_path.exists():
        shutil.rmtree(zarr_path)
    ds_chunked = ds.chunk("auto").unify_chunks()
    try:
        ds_chunked.to_zarr(zarr_path, mode="w", consolidated=True)
    finally:
        ds_chunked.close()
        ds.close()


def _coord_to_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    np_value = np.datetime64(cast(Any, value))
    return datetime.fromisoformat(np.datetime_as_string(np_value, unit="s"))
