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
