"""Lightweight client for discovering and opening published Climate API datasets."""

import httpx
import xarray as xr

DEFAULT_BASE_URL = "http://127.0.0.1:8000"


def list_datasets(base_url: str = DEFAULT_BASE_URL) -> list[dict]:
    """Return all published datasets from the STAC catalog.

    Each entry is a STAC child link dict with at least ``title`` and ``href``.
    """
    response = httpx.get(f"{base_url}/stac/catalog.json")
    response.raise_for_status()
    catalog = response.json()
    return [link for link in catalog["links"] if link["rel"] == "child"]


def open_dataset(dataset_id: str, *, base_url: str = DEFAULT_BASE_URL) -> xr.Dataset:
    """Open a published dataset as an xarray Dataset.

    Fetches the STAC collection for ``dataset_id``, reads the Zarr asset
    metadata, and returns the opened dataset. Coordinates are always
    ``time``, ``latitude``, and ``longitude``.
    """
    response = httpx.get(f"{base_url}/stac/collections/{dataset_id}")
    response.raise_for_status()
    collection = response.json()
    asset = collection["assets"]["zarr"]
    return xr.open_zarr(  # type: ignore[no-any-return]
        asset["href"],
        consolidated=asset["xarray:open_kwargs"]["consolidated"],
    )
