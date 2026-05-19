"""Icechunk store lifecycle helpers."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import icechunk

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

    n_times = ds.sizes.get("time", 0)
    if n_times == 0:
        return

    effective_chunk = min(time_chunk, n_times)
    encoding: dict[str, dict] = {}
    for name in list(ds.data_vars) + list(ds.coords):
        da = ds[name]
        existing = dict(da.encoding)
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
        if "time" not in ds.coords:
            return set()
        import pandas as pd

        return {datetime_to_period_string(pd.Timestamp(t.item()).to_pydatetime(), period_type) for t in ds.time}
    except Exception:
        logger.debug("Could not read committed periods from %s", store_path, exc_info=True)
        return set()
