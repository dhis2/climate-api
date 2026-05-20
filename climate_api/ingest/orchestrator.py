"""Per-period Icechunk ingest orchestrator.

The orchestrator is the only place that writes to the Icechunk store.
Plugins implement three focused async methods (probe / periods / fetch_period)
and never touch zarr directly.

Crash recovery: every period is committed individually. The cursor is saved
every commit_batch_size periods so that a restart resumes from the last
cursor checkpoint. A crash loses at most commit_batch_size periods of
re-fetch work (the store itself is always in a valid committed state).
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import xarray as xr

from climate_api.ingest.protocol import GridSpec, IngestionPlugin
from climate_api.ingest.store import build_pyramid_store, open_or_create_repo, read_committed_period_ids, rechunk_store

logger = logging.getLogger(__name__)

_CF_ENCODING_KEYS = frozenset({"scale_factor", "add_offset", "missing_value", "_FillValue", "coordinates"})


def _strip_cf_encoding(ds: xr.Dataset, period_type: str) -> None:
    """Strip CF attrs and clear encoding to prevent zarr append conflicts.

    GeoTIFF-sourced arrays carry scale_factor/add_offset/_FillValue in both
    .encoding and .attrs. xarray raises ValueError when appending to zarr if
    those keys collide with the stored array metadata from a prior write.
    """
    for name in list(ds.data_vars) + list(ds.coords):
        ds[name].encoding.clear()
        ds[name].attrs = {k: v for k, v in ds[name].attrs.items() if k not in _CF_ENCODING_KEYS}
    if "time" in ds.coords:
        units = "hours since 1970-01-01" if period_type == "hourly" else "days since 1970-01-01"
        ds["time"].encoding.update({"units": units, "dtype": "int32"})


def _write_geozarr_attrs(store: Any, *, spec: GridSpec, bbox: list[float]) -> None:
    """Write GeoZarr root-level attributes to the store after the first mode='w' write."""
    import zarr

    root = zarr.open_group(store, mode="r+")
    attrs: dict[str, Any] = {
        "proj:code": f"EPSG:{spec.crs}",
        "spatial:bbox": bbox,
    }
    attrs.update(spec.attrs)
    root.attrs.update(attrs)


def load_plugin(
    dotted_path: str,
    params: dict[str, Any],
    extra_params: dict[str, Any] | None = None,
) -> IngestionPlugin:
    """Instantiate an IngestionPlugin from a dotted import path and YAML params.

    The class is imported from dotted_path and called with **params. Built-in
    plugins accept variable and other source-specific kwargs; custom plugins
    define their own __init__ signature.

    extra_params are merged into params only for keys that the constructor
    declares and that are not already present in params. This is used to inject
    instance-level config (e.g. country_code from the extent) without requiring
    every plugin to accept it.
    """
    module_path, _, class_name = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"Invalid plugin path '{dotted_path}': must be 'module.ClassName'")
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    merged = dict(params)
    if extra_params:
        sig = inspect.signature(cls.__init__)
        for key, value in extra_params.items():
            if key not in merged and key in sig.parameters:
                merged[key] = value
    plugin = cls(**merged)
    if not isinstance(plugin, IngestionPlugin):
        raise TypeError(f"{dotted_path} does not implement IngestionPlugin")
    return plugin


async def run_ingest(
    *,
    plugin: IngestionPlugin,
    params: dict[str, Any],
    bbox: list[float],
    start: str,
    end: str,
    store_path: Path,
    period_type: str,
    on_progress: Callable[..., None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    save_cursor: Callable[[dict[str, Any]], None] | None = None,
    load_cursor: Callable[[], dict[str, Any] | None] | None = None,
    rechunk_time: int | None = None,
    apply_transforms: Callable[[xr.Dataset], xr.Dataset] | None = None,
    pyramid: bool = False,
) -> None:
    """Probe the source then stream per-period data into an Icechunk store.

    On the first run creates the store. On resume continues from the last
    committed period recorded in the job cursor (falling back to reading the
    store's committed time coordinates when no cursor is present).

    Memory usage is bounded by plugin.max_concurrency datasets held in flight
    concurrently. Writes are always sequential: tasks are awaited in
    chronological order so the time axis stays sorted.
    """
    spec: GridSpec = await asyncio.to_thread(plugin.probe, bbox, **params)
    logger.info("Probe: shape=%s crs=EPSG:%d time_dim=%s", spec.shape, spec.crs, spec.time_dim)

    all_periods = plugin.periods(start, end)
    if not all_periods:
        logger.info("No periods available for range %s..%s", start, end)
        return

    # Determine pending periods: prefer cursor (fast) then fall back to store read.
    cursor = load_cursor() if load_cursor else None
    last_committed: str | None = cursor.get("last_committed") if cursor else None

    if last_committed and last_committed in all_periods and store_path.exists():
        idx = all_periods.index(last_committed) + 1
        pending = all_periods[idx:]
        logger.info("Resuming after %s: %d/%d periods remain", last_committed, len(pending), len(all_periods))
    else:
        present = read_committed_period_ids(store_path, period_type)
        pending = [p for p in all_periods if p not in present]
        already_done = len(all_periods) - len(pending)
        logger.info("Periods: %d already committed, %d pending", already_done, len(pending))

    if not pending:
        logger.info("Store is current — nothing to ingest")
        return

    done_offset = len(all_periods) - len(pending)
    if on_progress:
        on_progress(done=done_offset, total=len(all_periods), message=f"{len(pending)} periods pending")

    # True when no periods have been committed yet — handles both a brand-new
    # store and a store directory that exists as an empty skeleton from a
    # previous failed initialisation (where append_dim would fail on an empty store).
    is_first_write = done_offset == 0
    # Capture before any commits so expire_snapshots only marks snapshots that
    # were created during this run, not the pre-existing HEAD.
    ingest_started_at = datetime.now(tz=timezone.utc)
    repo = open_or_create_repo(store_path)

    semaphore = asyncio.Semaphore(plugin.max_concurrency)

    async def _fetch(period_id: str) -> xr.Dataset:
        async with semaphore:
            return await asyncio.to_thread(plugin.fetch_period, period_id, bbox, **params)

    # Create all tasks upfront so up to max_concurrency fetches start immediately.
    # Await in chronological order so writes are always sequential.
    tasks = [asyncio.create_task(_fetch(p)) for p in pending]

    try:
        for i, task in enumerate(tasks):
            if is_cancel_requested and is_cancel_requested():
                for t in tasks[i:]:
                    t.cancel()
                from climate_api.jobs.models import JobCancelledError

                raise JobCancelledError("Ingest cancelled between periods")

            ds = await task
            period_id = pending[i]
            if apply_transforms is not None:
                ds = apply_transforms(ds)
            _strip_cf_encoding(ds, period_type=period_type)

            # Each period uses its own writable session so that to_zarr(append_dim=)
            # on the next period reads the committed store and finds the time axis.
            # Icechunk 2.x sessions do not expose uncommitted writes to subsequent
            # zarr.open_group calls, so batching writes within one session breaks the
            # append — committing per period is the correct pattern.
            session = repo.writable_session("main")

            is_first_period_write = not spec.time_dim or (i == 0 and is_first_write)
            if is_first_period_write:
                ds.to_zarr(session.store, mode="w")
                _write_geozarr_attrs(session.store, spec=spec, bbox=bbox)
            else:
                ds.to_zarr(session.store, append_dim="time")

            session.commit(f"ingest: {period_id}")

            # Save cursor at commit_batch_size intervals and at the end.
            # commit_batch_size controls resume granularity (cursor save frequency),
            # not commit frequency — every period is committed for correctness.
            if save_cursor and ((i + 1) % plugin.commit_batch_size == 0 or (i + 1) == len(pending)):
                save_cursor({"last_committed": period_id})
                logger.info("Cursor saved: up to %s (%d/%d)", period_id, i + 1, len(pending))

            logger.debug("Committed: %s (%d/%d)", period_id, i + 1, len(pending))

            if on_progress:
                on_progress(done=done_offset + i + 1, total=len(all_periods), message=f"Wrote {period_id}")

            if not spec.time_dim:
                for t in tasks[i + 1 :]:
                    t.cancel()
                break
    except BaseException:
        for t in tasks:
            if not t.done():
                t.cancel()
        raise

    if rechunk_time is not None and spec.time_dim:
        logger.info("Rechunking %s after ingest: time chunk → %d", store_path, rechunk_time)
        rechunk_store(store_path, time_chunk=rechunk_time)
        repo = open_or_create_repo(store_path)

    if pyramid:
        build_pyramid_store(store_path, x_dim=spec.x_dim, y_dim=spec.y_dim)
        # Reopen repo so expire_snapshots sees the post-pyramid HEAD.
        repo = open_or_create_repo(store_path)

    # Prune intermediate ingest snapshots: each period commit created one
    # snapshot; only the final state (HEAD of "main") needs to be retained.
    # expire_snapshots marks older snapshots as expired without deleting chunk
    # data — garbage_collect would be needed to reclaim manifest storage.
    # The "main" branch ref preserves HEAD even when it appears in the expired set.
    try:
        expired = repo.expire_snapshots(older_than=ingest_started_at)
        if expired:
            logger.info("Expired %d intermediate snapshots from %s", len(expired), store_path)
    except Exception:
        logger.warning("expire_snapshots failed for %s — store remains valid", store_path, exc_info=True)


def run_ingest_sync(
    *,
    plugin: IngestionPlugin,
    params: dict[str, Any],
    bbox: list[float],
    start: str,
    end: str,
    store_path: Path,
    period_type: str,
    on_progress: Callable[..., None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    save_cursor: Callable[[dict[str, Any]], None] | None = None,
    load_cursor: Callable[[], dict[str, Any] | None] | None = None,
    rechunk_time: int | None = None,
    apply_transforms: Callable[[xr.Dataset], xr.Dataset] | None = None,
    pyramid: bool = False,
) -> None:
    """Synchronous wrapper around run_ingest for use in threaded job workers."""
    asyncio.run(
        run_ingest(
            plugin=plugin,
            params=params,
            bbox=bbox,
            start=start,
            end=end,
            store_path=store_path,
            period_type=period_type,
            on_progress=on_progress,
            is_cancel_requested=is_cancel_requested,
            save_cursor=save_cursor,
            load_cursor=load_cursor,
            rechunk_time=rechunk_time,
            apply_transforms=apply_transforms,
            pyramid=pyramid,
        )
    )
