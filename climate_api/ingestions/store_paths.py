"""Path resolution for managed dataset stores.

All on-disk stores live under STORE_DIR (api_config.get_data_dir() / "downloads"
or ~/.local/share/climate-api/downloads when no data dir is configured).
"""

from __future__ import annotations

import os
from pathlib import Path

from climate_api import config as api_config


def _resolve_store_dir() -> Path:
    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "downloads"
    xdg_data = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return xdg_data / "climate-api" / "downloads"


STORE_DIR = _resolve_store_dir()


def get_icechunk_path(dataset_id: str) -> Path:
    """Return the Icechunk store path for a dataset id."""
    return STORE_DIR / f"{dataset_id}.icechunk"


def get_zarr_path(dataset_id: str) -> Path | None:
    """Return the legacy Zarr archive path if it exists on disk, else None."""
    path = STORE_DIR / f"{dataset_id}.zarr"
    return path if path.exists() else None


def get_cache_files(dataset_id: str) -> list[Path]:
    """Return all NetCDF cache files matching this dataset id prefix."""
    return list(STORE_DIR.glob(f"{dataset_id}*.nc"))
