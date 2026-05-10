"""Temporal resampling for derived managed datasets."""

from __future__ import annotations

import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import xarray as xr
from fastapi import HTTPException

from climate_api.data_accessor.services.accessor import open_zarr_dataset
from climate_api.data_manager.services.utils import get_time_dim
from climate_api.data_registry.services import datasets as registry_datasets
from climate_api.data_registry.services.datasets import get_period_type
from climate_api.ingestions import services as ingestion_services
from climate_api.ingestions.schemas import ArtifactFormat, ArtifactRecord, ArtifactRequestScope, PublicationStatus
from climate_api.publications.services import managed_dataset_id_for_scope
from climate_api.shared.time import (
    _coerce_numpy_datetime,
    _iso_duration_to_offset,
    _iso_resolution_to_frequency,
    utc_today,
)

logger = logging.getLogger(__name__)


def _derived_data_dir() -> Path:
    """Return the directory used to store derived Zarr artifacts."""
    from climate_api import config as api_config

    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "derived"
    return Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share")) / "climate-api" / "derived"


DERIVED_DATA_DIR: Path = _derived_data_dir()


def derived_dataset_id(*, source_dataset_id: str, resolution: str, method: str) -> str:
    """Return the stable derived dataset id auto-generated from source + parameters."""
    resolution_slug = re.sub(r"[^a-z0-9]", "", resolution.lower())
    return f"{source_dataset_id}_{resolution_slug}_{method}"


def materialize_resampled_artifact(
    *,
    source_dataset_id: str,
    resolution: str,
    method: str,
    start: str,
    end: str | None,
    overwrite: bool,
    publish: bool,
) -> ArtifactRecord:
    """Materialize a derived dataset by resampling an existing managed source dataset."""
    target_dataset_id = derived_dataset_id(source_dataset_id=source_dataset_id, resolution=resolution, method=method)
    resolved_end = end or utc_today().isoformat()

    existing = ingestion_services._find_existing_artifact(
        dataset_id=target_dataset_id,
        request_scope=ArtifactRequestScope(start=start, end=resolved_end),
        prefer_zarr=True,
    )
    if existing is not None and not overwrite:
        if publish and existing.publication.status != PublicationStatus.PUBLISHED:
            return ingestion_services.publish_artifact_record(existing.artifact_id)
        return existing

    source_dataset = registry_datasets.get_dataset(source_dataset_id)
    if source_dataset is None:
        raise HTTPException(status_code=404, detail=f"Source dataset template '{source_dataset_id}' not found")

    source_artifact = _resolve_source_artifact(source_dataset_id=source_dataset_id)
    if source_artifact.format != ArtifactFormat.ZARR:
        raise HTTPException(status_code=409, detail="Resampling currently requires a Zarr-backed source artifact")

    target_managed_dataset_id = managed_dataset_id_for_scope(target_dataset_id)
    zarr_path = DERIVED_DATA_DIR / f"{target_managed_dataset_id}.zarr"

    pandas_frequency = _iso_resolution_to_frequency(resolution)
    source_ds = open_zarr_dataset(source_artifact.path or source_artifact.asset_paths[0])
    try:
        resampled = _resample_dataset(
            source_ds=source_ds,
            source_period_type=get_period_type(source_dataset),
            pandas_frequency=pandas_frequency,
            method=method,
            start=start,
            end=resolved_end,
        )
        if resampled.sizes.get(get_time_dim(resampled), 0) == 0:
            raise HTTPException(status_code=409, detail="Source artifact does not contain any complete target periods")
        existing_realized = _find_existing_resampled_artifact(
            target_dataset_id=target_dataset_id,
            start=start,
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
        "extents": {
            "temporal": {
                "resolution": resolution,
            }
        },
    }
    return ingestion_services.store_materialized_zarr_artifact(
        dataset=target_dataset,
        start=start,
        end=resolved_end,
        bbox=None,
        zarr_path=zarr_path,
        overwrite=overwrite,
        publish=publish,
    )


def _resolve_source_artifact(*, source_dataset_id: str) -> ArtifactRecord:
    managed_dataset_id = managed_dataset_id_for_scope(source_dataset_id)
    return ingestion_services.get_latest_artifact_for_dataset_or_404(managed_dataset_id)


def _resample_dataset(
    *,
    source_ds: xr.Dataset,
    source_period_type: str,
    pandas_frequency: str,
    method: str,
    start: str,
    end: str,
) -> xr.Dataset:
    time_dim = get_time_dim(source_ds)
    offset = pd.tseries.frequencies.to_offset(pandas_frequency)
    target_start = pd.Timestamp(start).to_pydatetime().replace(tzinfo=None)
    target_end_exclusive = (pd.Timestamp(end) + offset).to_pydatetime().replace(tzinfo=None)
    subset = source_ds.where(source_ds[time_dim] >= np.datetime64(target_start), drop=True)
    subset = subset.where(subset[time_dim] < np.datetime64(target_end_exclusive), drop=True)
    if subset.sizes.get(time_dim, 0) == 0:
        raise HTTPException(status_code=409, detail="Source artifact contains no data for the requested resample range")
    source_start = _coerce_numpy_datetime(subset[time_dim].values[0])
    source_end = _coerce_numpy_datetime(subset[time_dim].values[-1])

    with xr.set_options(keep_attrs=True):
        resampler = subset.resample(
            {time_dim: pandas_frequency},
            label="left",
            closed="left",
        )
        result = cast(xr.Dataset, getattr(resampler, method)())
    result = _drop_incomplete_edge_periods(
        result=result,
        source_start=source_start,
        source_end=source_end,
        source_period_type=source_period_type,
        pandas_frequency=pandas_frequency,
    )
    return result


def _drop_incomplete_edge_periods(
    *,
    result: xr.Dataset,
    source_start: datetime,
    source_end: datetime,
    source_period_type: str,
    pandas_frequency: str,
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
    offset = pd.tseries.frequencies.to_offset(pandas_frequency)
    next_target_start = (pd.Timestamp(last_output_start) + offset).to_pydatetime().replace(tzinfo=None)
    required_source_end = _previous_source_period_start(next_target_start, source_period_type=source_period_type)
    if source_end < required_source_end:
        return result.isel({time_dim: slice(0, -1)})
    return result


def _find_existing_resampled_artifact(
    *,
    target_dataset_id: str,
    start: str,
    resampled: xr.Dataset,
) -> ArtifactRecord | None:
    time_dim = get_time_dim(resampled)
    realized_end = _coerce_numpy_datetime(resampled[time_dim].values[-1]).strftime("%Y-%m-%d")
    return ingestion_services._find_existing_artifact(
        dataset_id=target_dataset_id,
        request_scope=ArtifactRequestScope(start=start, end=realized_end),
        prefer_zarr=True,
    )


def _previous_source_period_start(boundary: datetime, *, source_period_type: str) -> datetime:
    return (pd.Timestamp(boundary) - _iso_duration_to_offset(source_period_type)).to_pydatetime()


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
