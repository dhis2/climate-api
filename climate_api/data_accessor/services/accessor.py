"""Loading raster data from downloaded files into xarray."""

import logging
import os
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
from pyproj import Transformer

from ...data_manager.services.downloader import get_icechunk_path
from ...data_manager.services.utils import get_time_dim, get_x_y_dims
from ...shared.time import numpy_datetime_to_period_string

logger = logging.getLogger(__name__)


def get_data(
    dataset: dict[str, Any],
    start: str | None = None,
    end: str | None = None,
    bbox: list[float] | None = None,
) -> xr.Dataset:
    """Load an xarray raster dataset for a given time range and bbox."""
    logger.info("Opening dataset")
    store_path = get_icechunk_path(dataset)
    ds = open_icechunk_dataset(store_path)

    if start and end:
        logger.info(f"Subsetting time to {start} and {end}")
        time_dim = get_time_dim(ds)
        ds = ds.sel(**{time_dim: slice(start, end)})  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    if bbox is not None:
        logger.info(f"Subsetting xy to {bbox}")
        xmin, ymin, xmax, ymax = list(map(float, bbox))
        x_dim, y_dim = get_x_y_dims(ds)
        # TODO: this assumes y axis increases towards north and is not very stable
        # ...and also does not consider partial pixels at the edges
        # ...should probably switch to rioxarray.clip instead
        ds = ds.sel(**{x_dim: slice(xmin, xmax), y_dim: slice(ymax, ymin)})  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    return ds


def get_data_coverage(dataset: dict[str, Any]) -> dict[str, Any]:
    """Return temporal and spatial coverage metadata for downloaded data."""
    from climate_api import config as api_config

    native_crs = api_config.get_crs() or "EPSG:4326"
    ds = get_data(dataset)
    try:
        return _coverage_from_dataset(ds=ds, period_type=str(dataset["period_type"]), native_crs=native_crs)
    finally:
        ds.close()


def get_data_coverage_for_paths(
    dataset: dict[str, Any],
    *,
    zarr_path: str,
) -> dict[str, Any]:
    """Return coverage metadata for a materialized flat-zarr artifact."""
    ds = open_zarr_dataset(zarr_path)

    from climate_api import config as api_config

    native_crs = api_config.get_crs() or "EPSG:4326"
    try:
        return _coverage_from_dataset(ds=ds, period_type=str(dataset["period_type"]), native_crs=native_crs)
    finally:
        ds.close()


def open_zarr_dataset(zarr_path: str) -> xr.Dataset:
    """Open a zarr store, handling pyramid stores by opening the base resolution level.

    When the root group has no data variables (as in a multiscale pyramid store),
    attempts to open level 0 instead. This works for any store backend — local
    paths, S3 URIs, GCS URIs, fsspec mappers — without filesystem-specific checks.
    """
    ds = _open_zarr(zarr_path)
    if not ds.data_vars:
        level0_path = zarr_path.rstrip("/") + "/0"
        try:
            level0 = _open_zarr(level0_path)
        except Exception as exc:
            ds.close()
            raise ValueError(
                f"Zarr store at {zarr_path!r} has no data variables at the root "
                f"and base pyramid level {level0_path!r} could not be opened"
            ) from exc
        ds.close()
        ds = level0
    return ds


def open_icechunk_dataset(store_path: str | Path) -> xr.Dataset:
    """Open an Icechunk store as an xarray Dataset via a readonly MVCC session.

    Detects multiscale pyramid stores (root group has ``multiscales`` in attrs)
    and opens group ``0`` (full resolution) in that case.
    """
    import icechunk
    import zarr

    path = Path(store_path)
    if not path.exists():
        raise FileNotFoundError(f"Icechunk store not found: {path}")
    storage = icechunk.local_filesystem_storage(str(path))
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    root = zarr.open_group(session.store, mode="r")
    group: str | None = "0" if "multiscales" in root.attrs else None
    return xr.open_zarr(session.store, group=group)  # type: ignore[no-any-return]


def _open_zarr(zarr_path: str) -> xr.Dataset:
    """Open a zarr store with automatic consolidated metadata detection."""
    return xr.open_zarr(zarr_path, consolidated=None)  # type: ignore[no-any-return]


def coverage_from_open_dataset(ds: xr.Dataset, *, period_type: str, native_crs: str = "EPSG:4326") -> dict[str, Any]:
    """Summarize temporal and spatial coverage for a caller-managed open dataset.

    Unlike get_data_coverage_for_paths, this function does not close the dataset.
    Use when the caller already holds a store handle (e.g. an Icechunk session store).
    """
    return _coverage_from_dataset(ds=ds, period_type=period_type, native_crs=native_crs)


def _coverage_from_dataset(*, ds: xr.Dataset, period_type: str, native_crs: str = "EPSG:4326") -> dict[str, Any]:
    """Summarize temporal and spatial coverage for an already opened dataset."""
    if any(size == 0 for size in ds.sizes.values()):
        return {
            "has_data": False,
            "coverage": {
                "temporal": {"start": None, "end": None},
                "spatial": {"xmin": None, "ymin": None, "xmax": None, "ymax": None},
                "spatial_wgs84": None,
            },
        }

    x_dim, y_dim = get_x_y_dims(ds)

    try:
        time_dim = get_time_dim(ds)
        start = _period_string_scalar(numpy_datetime_to_period_string(ds[time_dim].min(), period_type))  # type: ignore[arg-type]
        end = _period_string_scalar(numpy_datetime_to_period_string(ds[time_dim].max(), period_type))  # type: ignore[arg-type]
    except ValueError:
        start = end = None

    xmin, xmax = ds[x_dim].min().item(), ds[x_dim].max().item()
    ymin, ymax = ds[y_dim].min().item(), ds[y_dim].max().item()

    spatial_wgs84 = None
    wgs84 = "EPSG:4326"
    if native_crs.upper() != wgs84:
        transformer = Transformer.from_crs(native_crs, wgs84, always_xy=True)
        lon_min, lat_min, lon_max, lat_max = transformer.transform_bounds(xmin, ymin, xmax, ymax)
        spatial_wgs84 = {"xmin": lon_min, "ymin": lat_min, "xmax": lon_max, "ymax": lat_max}

    return {
        "has_data": True,
        "coverage": {
            "temporal": {"start": start, "end": end},
            "spatial": {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax},
            "spatial_wgs84": spatial_wgs84,
        },
    }


def _period_string_scalar(value: Any) -> str:
    """Normalize a numpy scalar or 0-d array period string to plain Python str."""
    if isinstance(value, np.ndarray):
        return str(value.item())
    return str(value)


def xarray_to_temporary_netcdf(ds: xr.Dataset) -> str:
    """Write a dataset to a temporary NetCDF file and return the path."""
    fd = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
    path = fd.name
    fd.close()
    ds.to_netcdf(path)
    return path


def cleanup_file(path: str) -> None:
    """Remove a file from disk."""
    os.remove(path)
