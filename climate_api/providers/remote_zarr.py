"""Remote Zarr/Icechunk store access for externally hosted datasets."""

from typing import Any

import xarray as xr

# Module-level caches keyed by store_url so each remote store is opened once.
_store_cache: dict[str, Any] = {}
_store_size_cache: dict[tuple[str, str], int] = {}


def is_remote_source(dataset: dict[str, Any]) -> bool:
    """Return True when a dataset template declares a remote zarr store."""
    store = dataset.get("store")
    return isinstance(store, dict) and store.get("kind") == "remote_zarr"


def open_remote_dataset(source: dict[str, Any]) -> xr.Dataset:
    """Open a remote dataset from a template source block.

    Dispatches on source.store_format: 'icechunk' uses the icechunk library;
    anything else falls back to xr.open_zarr with fsspec.

    The underlying store is cached per store_url (see open_icechunk_store), so
    reconnecting to S3 happens only once per process.  A fresh xr.Dataset is
    returned on each call so callers may close it without poisoning a shared
    dataset instance.
    """
    store_format = source.get("store_format", "zarr")
    return _open_icechunk(source) if store_format == "icechunk" else _open_zarr_url(source)


def open_icechunk_store(source: dict[str, Any]) -> Any:
    """Open an Icechunk store and return the raw zarr v3 Store object for direct key access.

    The store is cached per store_url so the repository is opened only once per process.
    """
    try:
        import icechunk
    except ImportError as exc:
        raise RuntimeError(
            "The 'icechunk' package is required to open Icechunk stores. Install it with: uv add icechunk"
        ) from exc

    store_url: str = source["store_url"]
    if store_url in _store_cache:
        return _store_cache[store_url]

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
    _store_cache[store_url] = store
    return store


async def get_icechunk_key_size(store: Any, key: str, store_url: str) -> int:
    """Return the byte size of a key in the store, using a per-process cache."""
    cache_key = (store_url, key)
    if cache_key in _store_size_cache:
        return _store_size_cache[cache_key]
    size: int = await store.getsize(key)
    _store_size_cache[cache_key] = size
    return size


def warmup_remote_store(source: dict[str, Any]) -> None:
    """Open and cache the store and dataset for a remote source block.

    Intended to be called at server startup (in a background thread) so the
    first real request hits warm caches instead of cold S3 connections.
    """
    import logging

    logger = logging.getLogger(__name__)
    store_url: str = source.get("store_url", "")
    try:
        open_remote_dataset(source)
        logger.info("Warmed up remote zarr store: %s", store_url)
    except Exception as exc:
        logger.warning("Failed to warm up remote zarr store '%s': %s", store_url, exc)


def _open_icechunk(source: dict[str, Any]) -> xr.Dataset:
    """Open an Icechunk store via the icechunk library (v2 API)."""
    store = open_icechunk_store(source)
    return xr.open_zarr(store, consolidated=False)  # type: ignore[no-any-return]


def _open_zarr_url(source: dict[str, Any]) -> xr.Dataset:
    """Open a plain Zarr store (v2/v3) via xarray with optional fsspec storage options."""
    store_url: str = source["store_url"]
    storage_options: dict[str, Any] = source.get("storage_options", {})
    if storage_options:
        return xr.open_zarr(store_url, storage_options=storage_options, consolidated=None)  # type: ignore[no-any-return]
    return xr.open_zarr(store_url, consolidated=None)  # type: ignore[no-any-return]
