"""openEO job persistence and execution service."""

from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any, TypeVar
from uuid import uuid4

import portalocker
from fastapi import HTTPException

from climate_api import config as api_config
from climate_api.openeo.schemas import (
    OpenEOJobCreate,
    OpenEOJobListResponse,
    OpenEOJobRecord,
    OpenEOJobResults,
    OpenEOJobStatus,
    OpenEOJobUpdate,
)
from climate_api.shared.time import utc_now

_T = TypeVar("_T")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


def _resolve_openeo_jobs_dir() -> Path:
    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "openeo_jobs"
    xdg_data = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return xdg_data / "climate-api" / "openeo_jobs"


_JOBS_DIR = _resolve_openeo_jobs_dir()
_JOBS_INDEX = _JOBS_DIR / "jobs.json"


def _ensure_store() -> None:
    _JOBS_DIR.mkdir(parents=True, exist_ok=True)
    if not _JOBS_INDEX.exists():
        _JOBS_INDEX.write_text("[]\n", encoding="utf-8")


def _load_raw_records() -> list[dict[str, object]]:
    _ensure_store()
    with open(_JOBS_INDEX, encoding="utf-8") as fh:
        portalocker.lock(fh, portalocker.LOCK_SH)
        try:
            payload = json.load(fh)
        finally:
            portalocker.unlock(fh)
    if not isinstance(payload, list):
        raise ValueError("openeo jobs.json must contain a list")
    return payload


def _mutate_store(mutation: Callable[[list[dict[str, object]]], _T]) -> _T:
    _ensure_store()
    with open(_JOBS_INDEX, "r+", encoding="utf-8") as fh:
        portalocker.lock(fh, portalocker.LOCK_EX)
        try:
            payload = json.load(fh)
            records: list[dict[str, object]] = payload if isinstance(payload, list) else []
            result = mutation(records)
            fh.seek(0)
            json.dump(records, fh, indent=2, default=str)
            fh.write("\n")
            fh.truncate()
            return result
        finally:
            portalocker.unlock(fh)


def store_list_jobs() -> list[OpenEOJobRecord]:
    """Return all persisted openEO job records."""
    return [OpenEOJobRecord.model_validate(r) for r in _load_raw_records()]


def store_get_job(job_id: str) -> OpenEOJobRecord | None:
    """Return one job record, or None if not found."""
    for raw in _load_raw_records():
        if raw.get("id") == job_id:
            return OpenEOJobRecord.model_validate(raw)
    return None


def store_create_job(record: OpenEOJobRecord) -> OpenEOJobRecord:
    """Persist a newly created job; raises ValueError if id already exists."""

    def _mutation(records: list[dict[str, object]]) -> OpenEOJobRecord:
        if any(r.get("id") == record.id for r in records):
            raise ValueError(f"Job '{record.id}' already exists")
        records.append(_serialize(record))
        return record

    return _mutate_store(_mutation)


def store_update_job(job_id: str, mutation: Callable[[OpenEOJobRecord], OpenEOJobRecord]) -> OpenEOJobRecord:
    """Load, mutate, and persist one existing job record."""

    def _apply(records: list[dict[str, object]]) -> OpenEOJobRecord:
        for idx, raw in enumerate(records):
            if raw.get("id") != job_id:
                continue
            updated = mutation(OpenEOJobRecord.model_validate(raw))
            records[idx] = _serialize(updated)
            return updated
        raise KeyError(job_id)

    return _mutate_store(_apply)


def store_delete_job(job_id: str) -> bool:
    """Delete a job; returns True if it existed."""

    def _mutation(records: list[dict[str, object]]) -> bool:
        for idx, raw in enumerate(records):
            if raw.get("id") == job_id:
                records.pop(idx)
                return True
        return False

    return _mutate_store(_mutation)


def _serialize(record: OpenEOJobRecord) -> dict[str, object]:
    # model_dump() respects Field(exclude=True) on error_message and cancel_requested,
    # which is correct for HTTP responses but wrong for disk persistence.
    # Explicitly re-add those fields so they survive a server restart.
    data: dict[str, object] = record.model_dump(mode="json", exclude_none=False)
    data["error_message"] = record.error_message
    data["cancel_requested"] = record.cancel_requested
    return data


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class OpenEOJobService:
    """Manages openEO job lifecycle and asynchronous execution."""

    def __init__(self, *, max_workers: int = 4) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="openeo-job")
        self._futures: dict[str, Future[None]] = {}
        self._lock = threading.Lock()

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    def recover_pending_jobs(self) -> None:
        """Recover jobs left in a non-terminal state from a previous server run.

        QUEUED jobs are re-enqueued.  RUNNING jobs are marked ERROR because their
        executor thread no longer exists after the restart.
        """
        for record in store_list_jobs():
            if record.status == OpenEOJobStatus.RUNNING:
                logger.warning("openEO job %s was RUNNING at restart — marking as error", record.id)
                try:
                    store_update_job(
                        record.id,
                        lambda r: r.model_copy(
                            update={
                                "status": OpenEOJobStatus.ERROR,
                                "error_message": "Interrupted by server restart",
                                "updated": utc_now(),
                            }
                        ),
                    )
                except KeyError:
                    pass
            elif record.status == OpenEOJobStatus.QUEUED:
                logger.info("openEO job %s was QUEUED at restart — re-enqueueing", record.id)
                try:
                    self._enqueue(record.id)
                except Exception:
                    logger.exception("Failed to re-enqueue openEO job %s", record.id)

    # ------------------------------------------------------------------
    # HTTP-layer helpers
    # ------------------------------------------------------------------

    def list_jobs(self) -> OpenEOJobListResponse:
        records = sorted(store_list_jobs(), key=lambda r: r.created, reverse=True)
        return OpenEOJobListResponse(
            jobs=records,
            links=[{"rel": "self", "href": "/jobs", "type": "application/json"}],
        )

    def create_job(self, body: OpenEOJobCreate) -> OpenEOJobRecord:
        if not isinstance(body.process.get("process_graph"), dict):
            raise HTTPException(
                status_code=422,
                detail="process.process_graph must be an object",
            )
        job_id = str(uuid4())
        now = utc_now()
        record = OpenEOJobRecord(
            id=job_id,
            title=body.title,
            description=body.description,
            process=body.process,
            status=OpenEOJobStatus.CREATED,
            created=now,
            updated=now,
            plan=body.plan,
            budget=body.budget,
            links=[
                {"rel": "self", "href": f"/jobs/{job_id}", "type": "application/json"},
                {"rel": "results", "href": f"/jobs/{job_id}/results", "type": "application/json"},
                {"rel": "logs", "href": f"/jobs/{job_id}/logs", "type": "application/json"},
            ],
        )
        return store_create_job(record)

    def get_job_or_404(self, job_id: str) -> OpenEOJobRecord:
        record = store_get_job(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        return record

    def update_job(self, job_id: str, body: OpenEOJobUpdate) -> OpenEOJobRecord:
        record = self.get_job_or_404(job_id)
        if record.status in {OpenEOJobStatus.QUEUED, OpenEOJobStatus.RUNNING}:
            raise HTTPException(status_code=400, detail="Cannot update a job that is queued or running")
        updates: dict[str, Any] = {}
        if body.title is not None:
            updates["title"] = body.title
        if body.description is not None:
            updates["description"] = body.description
        if body.process is not None:
            if not isinstance(body.process.get("process_graph"), dict):
                raise HTTPException(status_code=422, detail="process.process_graph must be an object")
            updates["process"] = body.process
        if body.plan is not None:
            updates["plan"] = body.plan
        if body.budget is not None:
            updates["budget"] = body.budget
        if updates:
            updates["updated"] = utc_now()
            return store_update_job(job_id, lambda r: r.model_copy(update=updates))
        return record

    def delete_job(self, job_id: str) -> None:
        record = self.get_job_or_404(job_id)
        if record.status in {OpenEOJobStatus.QUEUED, OpenEOJobStatus.RUNNING}:
            raise HTTPException(status_code=400, detail="Cannot delete a running job; cancel it first")
        store_delete_job(job_id)
        import shutil

        job_dir = _JOBS_DIR / job_id
        if job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)

    def start_job(self, job_id: str) -> None:
        """Queue a job for processing (POST /jobs/{id}/results)."""
        record = self.get_job_or_404(job_id)
        if record.status == OpenEOJobStatus.RUNNING:
            raise HTTPException(status_code=400, detail="Job is already running")
        if record.status == OpenEOJobStatus.QUEUED:
            return
        store_update_job(
            job_id,
            lambda r: r.model_copy(update={"status": OpenEOJobStatus.QUEUED, "updated": utc_now()}),
        )
        self._enqueue(job_id)

    def cancel_job(self, job_id: str) -> None:
        """Request cancellation (DELETE /jobs/{id}/results)."""
        record = self.get_job_or_404(job_id)
        if record.status not in {OpenEOJobStatus.QUEUED, OpenEOJobStatus.RUNNING}:
            raise HTTPException(status_code=400, detail="Job is not running or queued")
        with self._lock:
            future = self._futures.get(job_id)
        if record.status == OpenEOJobStatus.QUEUED and (future is None or future.cancel()):
            # Job hasn't started yet and was successfully cancelled before it could run.
            store_update_job(
                job_id,
                lambda r: r.model_copy(update={"status": OpenEOJobStatus.CANCELED, "updated": utc_now()}),
            )
        else:
            # Job is running (or cancel() returned False) — set flag for cooperative cancellation.
            store_update_job(
                job_id,
                lambda r: r.model_copy(update={"cancel_requested": True, "updated": utc_now()}),
            )
            if future is not None:
                future.cancel()

    def get_results(self, job_id: str) -> OpenEOJobResults:
        """Return result asset links for a finished job."""
        record = self.get_job_or_404(job_id)
        if record.status == OpenEOJobStatus.ERROR:
            raise HTTPException(
                status_code=424,
                detail=record.error_message or "Job finished with an error",
            )
        if record.status != OpenEOJobStatus.FINISHED:
            raise HTTPException(
                status_code=400,
                detail=f"Results not available yet; job status is '{record.status}'",
            )
        assets = _result_assets(record)
        return OpenEOJobResults(
            stac_version="1.1.0",
            id=job_id,
            assets=assets,
            links=[{"rel": "self", "href": f"/jobs/{job_id}/results", "type": "application/json"}],
        )

    # ------------------------------------------------------------------
    # Internal execution
    # ------------------------------------------------------------------

    def _enqueue(self, job_id: str) -> None:
        with self._lock:
            existing = self._futures.get(job_id)
            if existing is not None and not existing.done():
                return
            future = self._pool.submit(self._run_job, job_id)
            self._futures[job_id] = future

    def _run_job(self, job_id: str) -> None:
        try:
            self._execute(job_id)
        finally:
            with self._lock:
                self._futures.pop(job_id, None)

    def _execute(self, job_id: str) -> None:
        from climate_api.openeo.execution import run_process_graph

        record = store_get_job(job_id)
        if record is None:
            return
        if record.cancel_requested:
            store_update_job(
                job_id,
                lambda r: r.model_copy(update={"status": OpenEOJobStatus.CANCELED, "updated": utc_now()}),
            )
            return

        store_update_job(
            job_id,
            lambda r: r.model_copy(update={"status": OpenEOJobStatus.RUNNING, "updated": utc_now()}),
        )

        try:
            result = run_process_graph(record.process)
            # Re-read record — cancellation may have been requested while running.
            current = store_get_job(job_id)
            if current is not None and current.cancel_requested:
                store_update_job(
                    job_id,
                    lambda r: r.model_copy(update={"status": OpenEOJobStatus.CANCELED, "updated": utc_now()}),
                )
                return
            output_path = self._persist_result(job_id, result)
            store_update_job(
                job_id,
                lambda r: r.model_copy(
                    update={
                        "status": OpenEOJobStatus.FINISHED,
                        "updated": utc_now(),
                        "usage": {"output_path": output_path} if output_path else {},
                    }
                ),
            )
        except Exception as job_exc:
            logger.exception("openEO job %s failed", job_id)
            error_msg = f"{type(job_exc).__name__}: {job_exc}"
            store_update_job(
                job_id,
                lambda r: r.model_copy(
                    update={
                        "status": OpenEOJobStatus.ERROR,
                        "error_message": error_msg,
                        "updated": utc_now(),
                    }
                ),
            )

    def _persist_result(self, job_id: str, result: Any) -> str | None:
        import xarray as xr

        from climate_api.openeo.execution import SaveResultEnvelope

        results_dir = _JOBS_DIR / job_id / "results"
        results_dir.mkdir(parents=True, exist_ok=True)

        # Unwrap format envelope from save_result
        fmt = "ZARR"
        if isinstance(result, SaveResultEnvelope):
            fmt = result.format
            result = result.data

        # Resolve DataArray → Dataset for raster formats
        if isinstance(result, xr.DataArray):
            result = result.to_dataset(name=result.name or "result")

        if isinstance(result, xr.Dataset):
            return _write_raster(result, results_dir, fmt)

        # Tabular: resolve dask_geopandas → GeoDataFrame
        try:
            import dask_geopandas

            if isinstance(result, dask_geopandas.GeoDataFrame):
                result = result.compute()
        except ImportError:
            pass

        try:
            import geopandas as gpd

            if isinstance(result, gpd.GeoDataFrame):
                return _write_vector(result, results_dir, fmt)
        except ImportError:
            pass

        return None


def _result_assets(record: OpenEOJobRecord) -> dict[str, Any]:
    usage = record.usage or {}
    output_path = usage.get("output_path")
    if not output_path or not isinstance(output_path, str):
        return {}
    if output_path.endswith(".zarr"):
        return {
            "result": {
                # Trailing slash signals a directory root; Zarr HTTP clients
                # append chunk paths (e.g. .zmetadata, t/0.0) to this href.
                "href": f"/jobs/{record.id}/results/result.zarr/",
                "type": "application/vnd+zarr",
                "title": "Zarr result store",
                "roles": ["data"],
            }
        }
    if output_path.endswith(".geojson"):
        return {
            "result": {
                "href": f"/jobs/{record.id}/results/result.geojson",
                "type": "application/geo+json",
                "title": "GeoJSON result",
                "roles": ["data"],
            }
        }
    ext_map = {
        ".nc": ("application/netcdf", "NetCDF result"),
        ".tif": ("image/tiff; subtype=geotiff", "GeoTIFF result"),
        ".png": ("image/png", "PNG result"),
        ".csv": ("text/csv", "CSV result"),
        ".parquet": ("application/vnd.apache.parquet", "GeoParquet result"),
    }
    for ext, (mime, title) in ext_map.items():
        if output_path.endswith(ext):
            fname = output_path.rsplit("/", 1)[-1]
            return {
                "result": {
                    "href": f"/jobs/{record.id}/results/{fname}",
                    "type": mime,
                    "title": title,
                    "roles": ["data"],
                }
            }
    return {}


# ---------------------------------------------------------------------------
# Format writers
# ---------------------------------------------------------------------------

_RASTER_FORMATS: dict[str, tuple[str, str]] = {
    "ZARR": (".zarr", "application/vnd+zarr"),
    "NETCDF": (".nc", "application/netcdf"),
    "NC": (".nc", "application/netcdf"),
    "GTIFF": (".tif", "image/tiff; subtype=geotiff"),
    "GEOTIFF": (".tif", "image/tiff; subtype=geotiff"),
    "PNG": (".png", "image/png"),
    "CSV": (".csv", "text/csv"),
}

_VECTOR_FORMATS: dict[str, tuple[str, str]] = {
    "GEOJSON": (".geojson", "application/geo+json"),
    "CSV": (".csv", "text/csv"),
    "PARQUET": (".parquet", "application/vnd.apache.parquet"),
}


def _write_raster(ds: Any, results_dir: Any, fmt: str) -> str | None:
    """Write an xr.Dataset to disk in the requested format. Returns the output path."""
    ext, _ = _RASTER_FORMATS.get(fmt, (".zarr", "application/vnd+zarr"))

    if ext == ".zarr":
        path = str(results_dir / "result.zarr")
        ds.to_zarr(path, mode="w")
        return path

    if ext == ".nc":
        path = str(results_dir / "result.nc")
        ds.to_netcdf(path)
        return path

    if ext == ".tif":
        import rioxarray  # noqa: F401 — activates .rio accessor

        path = str(results_dir / "result.tif")
        # GeoTIFF requires a 2-D or 3-D array; use the first variable
        var = list(ds.data_vars)[0]
        da = ds[var]
        if "spatial_ref" in da.coords:
            da = da.drop_vars("spatial_ref")
        da.rio.to_raster(path)
        return path

    if ext == ".png":
        return _write_png(ds, results_dir)


    if ext == ".csv":
        path = str(results_dir / "result.csv")
        ds.to_dataframe().to_csv(path)
        return path

    # Fallback to Zarr
    path = str(results_dir / "result.zarr")
    ds.to_zarr(path, mode="w")
    return path


def _write_vector(gdf: Any, results_dir: Any, fmt: str) -> str | None:
    """Write a GeoDataFrame to disk in the requested format. Returns the output path."""
    ext, _ = _VECTOR_FORMATS.get(fmt, (".geojson", "application/geo+json"))

    if ext == ".geojson":
        path = str(results_dir / "result.geojson")
        gdf.to_file(path, driver="GeoJSON")
        return path

    if ext == ".parquet":
        path = str(results_dir / "result.parquet")
        gdf.to_parquet(path)
        return path

    if ext == ".csv":
        path = str(results_dir / "result.csv")
        gdf.drop(columns="geometry", errors="ignore").to_csv(path, index=False)
        return path

    # Fallback to GeoJSON
    path = str(results_dir / "result.geojson")
    gdf.to_file(path, driver="GeoJSON")
    return path


def _write_png(ds: Any, results_dir: Any) -> str | None:
    """Render an xr.Dataset as a styled PNG using the collection's render settings.

    Applies the same colormap, rescale range, and NaN transparency as the /map
    viewer.  Squeezes to a 2-D slice (first time step if temporal).
    """
    import matplotlib
    import numpy as np

    matplotlib.use("agg")  # non-interactive backend — safe on worker threads
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize

    var = list(ds.data_vars)[0]
    arr = ds[var]

    # Squeeze to 2-D (first step of each leading dim)
    while arr.ndim > 2:
        arr = arr.isel({arr.dims[0]: 0})

    data = arr.values.astype(float)
    data[data == 0] = np.nan  # treat exact-zero as nodata like the map viewer

    # Look up render settings from the published collection via the dataset registry
    colormap_name = "viridis"
    vmin, vmax = float(np.nanmin(data)), float(np.nanmax(data))
    try:
        from climate_api.data_registry.services import datasets as reg

        for ds in reg.list_datasets():
            display = ds.get("display", {})
            ds_var = ds.get("variable", "")
            if ds_var == var or ds.get("id", "").endswith(var):
                colormap_name = display.get("colormap", colormap_name)
                rng = display.get("range")
                if isinstance(rng, list) and len(rng) == 2:
                    vmin, vmax = float(rng[0]), float(rng[1])
                break
    except Exception:
        pass

    cmap = plt.get_cmap(colormap_name.title()).copy()
    cmap.set_bad(alpha=0)  # NaN → transparent

    norm = Normalize(vmin=vmin, vmax=vmax, clip=False)

    # Render at the natural aspect ratio of the data
    height, width = data.shape
    dpi = 150
    fig_w = max(4, width / dpi)
    fig_h = max(3, height / dpi)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=dpi)
    fig.patch.set_alpha(0)
    ax.imshow(data, origin="upper", cmap=cmap, norm=norm, interpolation="nearest")
    ax.axis("off")
    fig.tight_layout(pad=0)

    path = str(results_dir / "result.png")
    fig.savefig(path, bbox_inches="tight", dpi=dpi, transparent=True, pad_inches=0)
    plt.close(fig)
    return path


_service: OpenEOJobService | None = None


def get_openeo_job_service() -> OpenEOJobService:
    """Return the singleton openEO job service."""
    global _service
    if _service is None:
        _service = OpenEOJobService()
    return _service


def reset_openeo_job_service() -> None:
    """Reset singleton for tests."""
    global _service
    if _service is not None:
        _service.shutdown()
    _service = None
