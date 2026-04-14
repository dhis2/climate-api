"""Dataset cache: download, store, and optimize raster data as local files."""

import datetime
import importlib
import inspect
import logging
import os
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import xarray as xr
import xproj # pyright: ignore[reportUnusedImport]
from fastapi import BackgroundTasks, HTTPException
from geozarr_toolkit import create_geozarr_attrs, MultiscalesConventionMetadata
from topozarr.coarsen import create_pyramid

from ...shared.dhis2_adapter import create_client, get_org_units_geojson
from .utils import get_lon_lat_dims, get_time_dim

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
_download_dir = SCRIPT_DIR.parent.parent.parent.parent / "data" / "downloads"
CACHE_OVERRIDE = os.getenv("CACHE_OVERRIDE")
if CACHE_OVERRIDE:
    _download_dir = Path(CACHE_OVERRIDE)
DOWNLOAD_DIR = _download_dir

CRS ="EPSG:4326"

def download_dataset(
    dataset: dict[str, Any],
    start: str,
    end: str | None,
    bbox: list[float] | None,
    country_code: str | None,
    overwrite: bool,
    background_tasks: BackgroundTasks | None,
) -> list[Path]:
    """Download dataset from source and store as local NetCDF cache files."""
    cache_info = dataset["cache_info"]
    eo_download_func_path = cache_info["eo_function"]
    eo_download_func = _get_dynamic_function(eo_download_func_path)
    before_files = {path.resolve(): path.stat().st_mtime_ns for path in get_cache_files(dataset)}

    params = dict(cache_info.get("default_params", {}))
    params.update(
        {
            "start": start,
            "end": end or datetime.date.today().isoformat(),
            "dirname": DOWNLOAD_DIR,
            "prefix": _get_cache_prefix(dataset),
            "overwrite": overwrite,
        }
    )

    sig = inspect.signature(eo_download_func)
    try:
        if "bbox" in sig.parameters:
            params["bbox"] = _resolve_bbox(bbox=bbox)
        if "country_code" in sig.parameters:
            resolved_country_code = country_code or os.getenv("COUNTRY_CODE")
            if resolved_country_code:
                params["country_code"] = resolved_country_code
            else:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Downloading this dataset requires a country code. "
                        "Provide it through the resolved extent configuration or set COUNTRY_CODE in the environment."
                    ),
                )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if background_tasks is not None:
        background_tasks.add_task(eo_download_func, **params)
        return []

    try:
        eo_download_func(**params)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        message = str(exc).strip() or "Unexpected error from upstream data provider"
        raise HTTPException(status_code=502, detail=f"Upstream dataset download failed: {message}") from exc

    after_files = [path.resolve() for path in get_cache_files(dataset)]
    changed_files = [
        path for path in after_files if path not in before_files or path.stat().st_mtime_ns != before_files[path]
    ]
    return changed_files


def build_dataset_zarr(dataset: dict[str, Any]) -> None:
    """Collect all dataset files into a single optimised zarr archive."""
    logger.info(f"Optimizing cache for dataset {dataset['id']}")

    print(json.dumps(dataset, indent=2))

    cache_info = dataset["cache_info"]
    files = get_cache_files(dataset)
    logger.info(f"Opening {len(files)} files from cache")
    ds = xr.open_mfdataset(files)
    ds.load()

    lon_dim, lat_dim = get_lon_lat_dims(ds)
    dims = [lon_dim, lat_dim]

    # trim to only minimal vars and coords
    logger.info("Trimming unnecessary variables and coordinates")
    varname = dataset["variable"]
    ds = ds[[varname]]
    keep_coords = [get_time_dim(ds)] + dims
    drop_coords = [c for c in ds.coords if c not in keep_coords]
    ds = ds.drop_vars(drop_coords)

    xmin = ds[lon_dim].min().item() 
    xmax = ds[lon_dim].max().item()
    ymin = ds[lat_dim].min().item() 
    ymax = ds[lat_dim].max().item()
    bbox = [xmin, ymin, xmax, ymax]
    shape = (ds.sizes[lon_dim], ds.sizes[lat_dim]) 

    # https://github.com/zarr-developers/geozarr-toolkit/issues/15
    geozarr_attrs =  create_geozarr_attrs(
        dimensions=dims,
        crs=CRS,
        bbox=bbox,
        shape=shape,
    )

    # save as zarr
    logger.info("Saving to optimized zarr file")
    zarr_path = DOWNLOAD_DIR / f"{_get_cache_prefix(dataset)}.zarr"

    multiscales = dict(cache_info.get("multiscales", {}))

    if (multiscales):
        levels = multiscales.get("levels", 4)
        method = multiscales.get("method", "mean")

        # Add multiscales convention metadata to the zarr attributes
        zarr_conventions = geozarr_attrs.get("zarr_conventions", [])
        zarr_conventions.append(MultiscalesConventionMetadata().model_dump())
        geozarr_attrs["zarr_conventions"] = zarr_conventions

        ds = ds.proj.assign_crs(spatial_ref=CRS)

        # https://github.com/carbonplan/topozarr/issues/13
        pyramid = create_pyramid(
            ds,
            levels=levels,
            x_dim=lon_dim,
            y_dim=lat_dim,
            method=method
        )

        pyramid.dt.attrs.update(geozarr_attrs)
        pyramid.dt.to_zarr(zarr_path, mode="w", encoding=pyramid.encoding, zarr_format=3)

        # zarr-layer looks for the time coordinate at the root of the store, not inside each level.
        # Copy it from level 0 so browser clients can discover it without knowing the level structure.
        time_src = zarr_path / "0" / "time"
        time_dst = zarr_path / "time"
        if time_src.exists():
            if time_dst.exists():
                shutil.rmtree(time_dst)
            shutil.copytree(time_src, time_dst)

        pyramid.dt.close()

    else:    
        # determine optimal chunk sizes
        logger.info("Determining optimal chunk size for zarr archive")
        ds_autochunk = ds.chunk("auto").unify_chunks()
        uniform_chunks: dict[str, Any] = {str(dim): ds_autochunk.chunks[dim][0] for dim in ds_autochunk.dims}
        time_space_chunks = _compute_time_space_chunks(ds, dataset)
        uniform_chunks.update(time_space_chunks)
        logging.info(f"--> {uniform_chunks}")

        ds.attrs.update(geozarr_attrs)
        ds_chunked = ds.chunk(uniform_chunks)
        ds_chunked.to_zarr(zarr_path, mode="w")
        ds_chunked.close()

    ds.close()
    logger.info("Finished cache optimization")


def _compute_time_space_chunks(
    ds: xr.Dataset,
    dataset: dict[str, Any],
    max_spatial_chunk: int = 256,
) -> dict[str, int]:
    """Compute chunk sizes tuned for common temporal access patterns."""
    chunks: dict[str, int] = {}

    dim = get_time_dim(ds)
    period_type = dataset["period_type"]
    if period_type == "hourly":
        chunks[dim] = 24 * 7
    elif period_type == "daily":
        chunks[dim] = 30
    elif period_type == "monthly":
        chunks[dim] = 12
    elif period_type == "yearly":
        chunks[dim] = 1

    lon_dim, lat_dim = get_lon_lat_dims(ds)
    chunks[lon_dim] = min(ds.sizes[lon_dim], max_spatial_chunk)
    chunks[lat_dim] = min(ds.sizes[lat_dim], max_spatial_chunk)

    return chunks


def _get_cache_prefix(dataset: dict[str, Any]) -> str:
    return str(dataset["id"])


def get_cache_files(dataset: dict[str, Any]) -> list[Path]:
    """Return all NetCDF cache files matching this dataset's prefix."""
    # TODO: not bulletproof -- e.g. 2m_temperature matches 2m_temperature_modified
    prefix = _get_cache_prefix(dataset)
    return list(DOWNLOAD_DIR.glob(f"{prefix}*.nc"))


def get_zarr_path(dataset: dict[str, Any]) -> Path | None:
    """Return the optimised zarr archive path if it exists."""
    prefix = _get_cache_prefix(dataset)
    optimized = DOWNLOAD_DIR / f"{prefix}.zarr"
    if optimized.exists():
        return optimized
    return None


def _get_dynamic_function(full_path: str) -> Callable[..., Any]:
    """Import and return a function given its dotted module path."""
    parts = full_path.split(".")
    module_path = ".".join(parts[:-1])
    function_name = parts[-1]
    module = importlib.import_module(module_path)
    return getattr(module, function_name)  # type: ignore[no-any-return]


def _get_default_bbox() -> list[float]:
    """Compute the default download bbox from DHIS2 org units when needed."""
    import geopandas as gpd

    client = create_client()
    org_units_geojson = get_org_units_geojson(client, level=2)
    gdf = gpd.GeoDataFrame.from_features(org_units_geojson.get("features", []))
    return list(map(float, gdf.total_bounds))


def _resolve_bbox(*, bbox: list[float] | None) -> list[float]:
    """Resolve bbox from request, env, or DHIS2-derived defaults."""
    if bbox is not None:
        return bbox

    env_bbox = _bbox_from_env()
    if env_bbox is not None:
        return env_bbox

    try:
        return _get_default_bbox()
    except Exception as exc:
        raise ValueError(
            "A bbox is required for this dataset. Provide it in the request or set DOWNLOAD_BBOX in the environment."
        ) from exc


def _bbox_from_env() -> list[float] | None:
    """Parse a default bbox from environment if configured."""
    raw_bbox = os.getenv("DOWNLOAD_BBOX") or os.getenv("DEFAULT_DOWNLOAD_BBOX")
    if not raw_bbox:
        return None

    parts = [part.strip() for part in raw_bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("DOWNLOAD_BBOX must contain four comma-separated numbers: xmin,ymin,xmax,ymax")
    return [float(part) for part in parts]
