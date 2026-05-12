"""Remote Zarr/Icechunk store access for externally hosted datasets."""

from typing import Any

import xarray as xr


def is_remote_source(dataset: dict[str, Any]) -> bool:
    """Return True when a dataset template declares a remote zarr store."""
    store = dataset.get("store")
    return isinstance(store, dict) and store.get("kind") == "remote_zarr"


def open_remote_dataset(source: dict[str, Any]) -> xr.Dataset:
    """Open a remote dataset from a template source block.

    Dispatches on source.store_format: 'icechunk' uses the icechunk library;
    anything else falls back to xr.open_zarr with fsspec.
    """
    store_format = source.get("store_format", "zarr")
    if store_format == "icechunk":
        return _open_icechunk(source)
    return _open_zarr_url(source)


def _open_icechunk(source: dict[str, Any]) -> xr.Dataset:
    """Open an Icechunk store via the icechunk library (v2 API)."""
    try:
        import icechunk
    except ImportError as exc:
        raise RuntimeError(
            "The 'icechunk' package is required to open Icechunk stores. Install it with: uv add icechunk"
        ) from exc

    store_url: str = source["store_url"]
    storage_options: dict[str, Any] = source.get("storage_options", {})
    anon: bool = storage_options.get("anon", False)
    client_kwargs: dict[str, Any] = storage_options.get("client_kwargs", {})
    region: str | None = client_kwargs.get("region_name")

    s3_path = store_url.removeprefix("s3://")
    bucket, _, prefix = s3_path.partition("/")
    prefix = prefix.rstrip("/")

    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region=region, anonymous=anon)
    repo = icechunk.Repository.open(storage)
    store = repo.readonly_session(branch="main").store
    return xr.open_zarr(store, consolidated=False)  # type: ignore[no-any-return]


def _open_zarr_url(source: dict[str, Any]) -> xr.Dataset:
    """Open a plain Zarr store (v2/v3) via xarray with optional fsspec storage options."""
    store_url: str = source["store_url"]
    storage_options: dict[str, Any] = source.get("storage_options", {})
    if storage_options:
        return xr.open_zarr(store_url, storage_options=storage_options, consolidated=None)  # type: ignore[no-any-return]
    return xr.open_zarr(store_url, consolidated=None)  # type: ignore[no-any-return]
