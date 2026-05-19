"""Dataset cache: utilities for locating and reading downloaded raster files."""

import importlib
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import xarray as xr

from climate_api import config as api_config

logger = logging.getLogger(__name__)


def _resolve_download_dir() -> Path:
    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "downloads"
    xdg_data = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return xdg_data / "climate-api" / "downloads"


DOWNLOAD_DIR = _resolve_download_dir()


def _run_transforms(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    dataset_id = dataset.get("id", "?")
    for entry in dataset.get("transforms", []):
        if isinstance(entry, str):
            func_path, params = entry, {}
        elif isinstance(entry, dict):
            if "function" not in entry:
                raise ValueError(
                    f"Transform entry in dataset '{dataset_id}' is missing required key 'function': {entry!r}"
                )
            func_path = entry["function"]
            params = entry.get("params", {})
        else:
            raise ValueError(
                f"Transform entry in dataset '{dataset_id}' must be a string or dict,"
                f" got {type(entry).__name__!r}: {entry!r}"
            )
        func = _get_dynamic_function(func_path)
        logger.info("Applying transform %s to dataset %s", func_path, dataset_id)
        ds = func(ds, dataset, **params)
    return ds


def _get_cache_prefix(dataset: dict[str, Any]) -> str:
    return str(dataset["id"])


def get_icechunk_path(dataset: dict[str, Any]) -> Path:
    """Return the Icechunk store path for a dataset (may not exist yet)."""
    return DOWNLOAD_DIR / f"{_get_cache_prefix(dataset)}.icechunk"


def _get_dynamic_function(full_path: str) -> Callable[..., Any]:
    """Import and return a function given its dotted module path."""
    parts = full_path.split(".")
    module_path = ".".join(parts[:-1])
    function_name = parts[-1]
    module = importlib.import_module(module_path)
    return getattr(module, function_name)  # type: ignore[no-any-return]
