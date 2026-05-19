"""Per-period Icechunk ingest orchestrator.

The orchestrator is the only place that writes to the Icechunk store.
Plugins implement three focused async methods (probe / periods / fetch_period)
and never touch zarr directly.

Crash recovery: each commit advances the job cursor. On restart the
orchestrator reads the cursor, skips already-committed periods, and continues
from where it stopped. A crash loses at most one uncommitted batch.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import xarray as xr

from climate_api.ingest.protocol import GridSpec, IngestionPlugin
from climate_api.ingest.store import open_or_create_repo, read_committed_period_ids

logger = logging.getLogger(__name__)


def load_plugin(dotted_path: str, params: dict[str, Any]) -> IngestionPlugin:
    """Instantiate an IngestionPlugin from a dotted import path and YAML params.

    The class is imported from dotted_path and called with **params. Built-in
    plugins accept variable and other source-specific kwargs; custom plugins
    define their own __init__ signature.
    """
    module_path, _, class_name = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"Invalid plugin path '{dotted_path}': must be 'module.ClassName'")
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    plugin = cls(**params)
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
) -> None:
    """Probe the source then stream per-period data into an Icechunk store.

    On the first run creates the store. On resume continues from the last
    committed period recorded in the job cursor (falling back to reading the
    store's committed time coordinates when no cursor is present).

    Memory usage is bounded by plugin.max_concurrency datasets held in flight
    concurrently. Writes are always sequential: tasks are awaited in
    chronological order so the time axis stays sorted.
    """
    spec: GridSpec = await plugin.probe(bbox, **params)
    logger.info("Probe: shape=%s crs=EPSG:%d time_dim=%s", spec.shape, spec.crs, spec.time_dim)

    all_periods = await plugin.periods(start, end)
    if not all_periods:
        logger.info("No periods available for range %s..%s", start, end)
        return

    # Determine pending periods: prefer cursor (fast) then fall back to store read.
    cursor = load_cursor() if load_cursor else None
    last_committed: str | None = cursor.get("last_committed") if cursor else None

    if last_committed and last_committed in all_periods:
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

    is_first_write = not store_path.exists()
    repo = open_or_create_repo(store_path)

    semaphore = asyncio.Semaphore(plugin.max_concurrency)

    async def _fetch(period_id: str) -> xr.Dataset:
        async with semaphore:
            return await plugin.fetch_period(period_id, bbox, **params)

    # Create all tasks upfront so up to max_concurrency fetches start immediately.
    # Await in chronological order so writes are always sequential.
    tasks = [asyncio.create_task(_fetch(p)) for p in pending]

    session = repo.writable_session("main")

    for i, task in enumerate(tasks):
        if is_cancel_requested and is_cancel_requested():
            for t in tasks[i:]:
                t.cancel()
            from climate_api.jobs.models import JobCancelledError

            raise JobCancelledError("Ingest cancelled between periods")

        ds = await task
        period_id = pending[i]

        if not spec.time_dim:
            # Static dataset: single write, no append dimension.
            ds.to_zarr(session.store, mode="w")
        elif i == 0 and is_first_write:
            ds.to_zarr(session.store, mode="w")
        else:
            ds.to_zarr(session.store, append_dim="time")

        should_commit = (i + 1) % plugin.commit_batch_size == 0 or (i + 1) == len(pending)
        if should_commit:
            session.commit(f"ingest up to {period_id}")
            logger.info("Committed: up to %s (%d/%d)", period_id, i + 1, len(pending))
            if save_cursor:
                save_cursor({"last_committed": period_id})
            if (i + 1) < len(pending):
                # Fresh writable session for the next batch.
                session = repo.writable_session("main")

        if on_progress:
            on_progress(done=done_offset + i + 1, total=len(all_periods), message=f"Wrote {period_id}")

        if not spec.time_dim:
            break


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
        )
    )
