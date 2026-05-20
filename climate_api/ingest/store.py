"""Icechunk store lifecycle helpers."""

from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import icechunk

_PYRAMID_PIXEL_THRESHOLD = 2048 * 2048
_PYRAMID_TARGET_TILE_SIZE = 512
_PYRAMID_MAX_LEVELS = 8

logger = logging.getLogger(__name__)


def open_or_create_repo(store_path: Path) -> "icechunk.Repository":
    """Open an existing Icechunk repository or create one at store_path."""
    import icechunk

    storage = icechunk.local_filesystem_storage(str(store_path))
    if store_path.exists():
        return icechunk.Repository.open(storage)
    return icechunk.Repository.create(storage)


def rechunk_store(store_path: Path, *, time_chunk: int) -> None:
    """Rewrite the committed Icechunk store with a coarser time chunk size.

    Opens the latest committed snapshot for reading and a new writable session
    for writing, lazily rechunks the time dimension via dask, then commits the
    result as a new snapshot. Icechunk's MVCC ensures the previous snapshot is
    preserved — if the rechunk fails the store rolls back to its original state.

    A no-op when the store does not exist or has no time dimension.
    """
    import xarray as xr

    if not store_path.exists():
        return

    repo = open_or_create_repo(store_path)
    read_session = repo.readonly_session("main")
    ds = xr.open_zarr(read_session.store)
    try:
        n_times = ds.sizes.get("time", 0)
        if n_times == 0:
            return

        effective_chunk = min(time_chunk, n_times)
        # Keys that are CF conventions attrs, not valid zarr encoding parameters.
        # xarray copies them into .encoding when reading from zarr; strip them
        # before passing back to to_zarr() to avoid ValueError.
        _INVALID_ZARR_KEYS = frozenset({"scale_factor", "add_offset", "missing_value", "_FillValue", "coordinates"})
        encoding: dict[str, dict] = {}
        for name in list(ds.data_vars) + list(ds.coords):
            da = ds[name]
            existing = {k: v for k, v in da.encoding.items() if k not in _INVALID_ZARR_KEYS}
            if "time" in da.dims:
                current = existing.get("chunks")
                if isinstance(current, (list, tuple)):
                    new_chunks = list(current)
                    new_chunks[list(da.dims).index("time")] = effective_chunk
                else:
                    new_chunks = [effective_chunk if dim == "time" else da.sizes[dim] for dim in da.dims]
                existing["chunks"] = new_chunks
            encoding[name] = existing  # pyright: ignore[reportArgumentType]

        write_session = repo.writable_session("main")
        ds.chunk({"time": effective_chunk}).to_zarr(write_session.store, mode="w", encoding=encoding)
        write_session.commit(f"rechunk: time={effective_chunk}")
        logger.info("Rechunked %s: time chunk → %d (%d periods)", store_path, effective_chunk, n_times)
    finally:
        ds.close()


def build_pyramid_store(store_path: Path, *, x_dim: str = "x", y_dim: str = "y") -> None:
    """Rewrite the committed Icechunk store as a multiscale pyramid.

    Level count is derived from the actual spatial dimensions using the same
    512-pixel tile target as the legacy downloader. A no-op when the store does
    not exist or its spatial extent is below the 2048×2048 threshold.

    The pyramid commit replaces the flat root structure: data moves from root
    to ``0/`` and coarsened overviews are written to ``1/``, ``2/``, etc.
    Intermediate ingest snapshots are left for the orchestrator's
    expire_snapshots call to prune.
    """
    import xarray as xr
    from topozarr import create_pyramid

    if not store_path.exists():
        return

    import zarr

    repo = open_or_create_repo(store_path)
    read_session = repo.readonly_session("main")
    ds = xr.open_zarr(read_session.store)
    try:
        nx = ds.sizes.get(x_dim, 0)
        ny = ds.sizes.get(y_dim, 0)
        if nx * ny <= _PYRAMID_PIXEL_THRESHOLD:
            logger.info("Skipping pyramid for %s: %dx%d below threshold", store_path, nx, ny)
            return
        levels = min(math.ceil(math.log2(max(nx, ny) / _PYRAMID_TARGET_TILE_SIZE)), _PYRAMID_MAX_LEVELS)
        ds_loaded = ds.load()
    finally:
        ds.close()

    # topozarr requires xproj CRS on the dataset. Read it from the GeoZarr
    # root attribute written by the orchestrator (proj:code = "EPSG:<n>").
    try:
        import xproj  # noqa: F401 — registers .proj accessor

        root = zarr.open_group(read_session.store, mode="r")
        proj_code = root.attrs.get("proj:code", "EPSG:4326")
        epsg = int(proj_code.split(":")[1]) if ":" in proj_code else 4326
        ds_loaded = ds_loaded.proj.assign_crs({"EPSG": epsg})
    except Exception:
        logger.warning("Could not assign CRS for pyramid build on %s; proceeding without xproj", store_path)

    pyramid = create_pyramid(ds_loaded, levels=levels, x_dim=x_dim, y_dim=y_dim)
    # Strip "shards" from topozarr encoding: sharding_indexed codec isn't supported
    # by zarr-layer (JS) client, causing a render loop in the map viewer.
    no_shard_encoding = {
        level: {var: {k: v for k, v in enc.items() if k != "shards"} for var, enc in vars_.items()}
        for level, vars_ in pyramid.encoding.items()
    }
    write_session = repo.writable_session("main")
    pyramid.dt.to_zarr(write_session.store, mode="w", encoding=no_shard_encoding, zarr_format=3)
    write_session.commit(f"pyramid: {levels} levels")
    logger.info("Built %d-level pyramid for %s (%dx%d)", levels, store_path, nx, ny)


def read_committed_period_ids(store_path: Path, period_type: str) -> set[str]:
    """Return the set of period IDs already committed to the Icechunk store.

    Reads the time coordinate from the last committed snapshot and converts
    each timestamp back to a period string using the dataset's period_type.
    Returns an empty set when the store does not yet exist or has no time dim.
    """
    import xarray as xr

    from climate_api.shared.time import datetime_to_period_string

    if not store_path.exists():
        return set()

    try:
        repo = open_or_create_repo(store_path)
        session = repo.readonly_session("main")
        ds = xr.open_zarr(session.store)
        try:
            if "time" not in ds.coords:
                # Pyramid store: time lives in level "0", not at root.
                if "multiscales" not in ds.attrs:
                    return set()
                ds.close()
                ds = xr.open_zarr(session.store, group="0")
            if "time" not in ds.coords:
                return set()
            import pandas as pd

            return {datetime_to_period_string(pd.Timestamp(t.item()).to_pydatetime(), period_type) for t in ds.time}
        finally:
            ds.close()
    except Exception:
        logger.debug("Could not read committed periods from %s", store_path, exc_info=True)
        return set()
