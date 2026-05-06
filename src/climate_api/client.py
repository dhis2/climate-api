"""Lightweight client for discovering and opening published Climate API datasets."""

import os

import httpx
import xarray as xr

_FALLBACK_BASE_URL = "http://127.0.0.1:8000"


def _default_base_url() -> str:
    return os.environ.get("CLIMATE_API_BASE_URL", _FALLBACK_BASE_URL)


class Client:
    """Client for a Climate API instance.

    Args:
        base_url: Base URL of the running Climate API (e.g. ``http://localhost:8000``).
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def catalog(self) -> list[dict]:
        """Return all published datasets from the STAC catalog.

        Each entry is a STAC child link dict with at least ``id``, ``title``, and ``href``.
        """
        return list_datasets(self.base_url)

    def open(self, dataset_id: str) -> xr.Dataset:
        """Open a published dataset as an xarray Dataset.

        Fetches the STAC collection for ``dataset_id``, reads the Zarr asset
        metadata, and returns the opened dataset. Coordinates are always
        ``time``, ``latitude``, and ``longitude``.
        """
        return open_dataset(dataset_id, base_url=self.base_url)


def list_datasets(base_url: str | None = None) -> list[dict]:
    """Return all published datasets from the STAC catalog.

    Each entry is a STAC child link dict with at least ``id``, ``title``, and ``href``.
    ``base_url`` defaults to the ``CLIMATE_API_BASE_URL`` environment variable,
    falling back to ``http://127.0.0.1:8000``.
    """
    url = (base_url or _default_base_url()).rstrip("/")
    response = httpx.get(f"{url}/stac/catalog.json")
    response.raise_for_status()
    catalog = response.json()
    raw_links = catalog.get("links")
    if not isinstance(raw_links, list):
        raise ValueError(f"Invalid STAC catalog response from {url}: missing or non-list 'links' field")
    links = []
    for link in raw_links:
        if isinstance(link, dict) and link.get("rel") == "child":
            link["id"] = link["href"].rstrip("/").rsplit("/", 1)[-1]
            links.append(link)
    return links


def open_dataset(dataset_id: str, *, base_url: str | None = None) -> xr.Dataset:
    """Open a published dataset as an xarray Dataset.

    Fetches the STAC collection for ``dataset_id``, reads the Zarr asset
    metadata, and returns the opened dataset. Coordinates are always
    ``time``, ``latitude``, and ``longitude``.
    ``base_url`` defaults to the ``CLIMATE_API_BASE_URL`` environment variable,
    falling back to ``http://127.0.0.1:8000``.
    """
    url = (base_url or _default_base_url()).rstrip("/")
    response = httpx.get(f"{url}/stac/collections/{dataset_id}")
    response.raise_for_status()
    collection = response.json()
    asset = collection.get("assets", {}).get("zarr")
    if asset is None:
        raise KeyError(f"Dataset '{dataset_id}' has no Zarr asset in the STAC collection")
    open_kwargs = asset.get("xarray:open_kwargs", {})
    return xr.open_zarr(asset["href"], **open_kwargs)  # type: ignore[no-any-return]
