"""openEO HTTP routes."""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse

from climate_api.openeo import collections as collections_service
from climate_api.openeo import processes as processes_service
from climate_api.openeo import udps as udp_store
from climate_api.openeo.capabilities import build_capabilities
from climate_api.openeo.jobs import get_openeo_job_service
from climate_api.openeo.schemas import (
    OpenEOJobCreate,
    OpenEOJobListResponse,
    OpenEOJobRecord,
    OpenEOJobResults,
    OpenEOJobUpdate,
    UDPListResponse,
    UDPRecord,
)

capabilities_router = APIRouter(tags=["openEO"])
collections_router = APIRouter(tags=["openEO"])
processes_router = APIRouter(tags=["openEO"])
jobs_router = APIRouter(tags=["openEO"])
udp_router = APIRouter(tags=["openEO"])
result_router = APIRouter(tags=["openEO"])


# ---------------------------------------------------------------------------
# Capabilities  GET /
# (registered without prefix in main.py so it overlays the system route)
# ---------------------------------------------------------------------------


def get_openeo_capabilities(request: Request) -> JSONResponse:
    """Return openEO capabilities when the client requests JSON."""
    base_url = _abs_base(request)
    caps = build_capabilities(base_url)
    return JSONResponse(caps.model_dump())


@capabilities_router.get("/.well-known/openeo")
def well_known_openeo(request: Request) -> dict[str, Any]:
    """OpenEO service discovery — lets clients find the versioned API URL."""
    base_url = _abs_base(request)
    return {
        "versions": [
            {
                "url": base_url,
                "api_version": "1.2.0",
                "production": False,
            }
        ]
    }


@capabilities_router.get("/credentials/oidc")
def credentials_oidc() -> dict[str, Any]:
    """Return OIDC authentication providers (empty = open/anonymous access)."""
    return {"providers": []}


@capabilities_router.get("/file_formats")
def file_formats() -> dict[str, Any]:
    """Return supported input and output file formats."""
    zarr_format = {
        "title": "Zarr",
        "description": "Zarr v3 chunked array store",
        "gis_data_types": ["raster"],
        "parameters": {},
        "links": [],
    }
    return {
        "input": {},
        "output": {"Zarr": zarr_format},
    }


@capabilities_router.get("/service_types")
def service_types() -> dict[str, Any]:
    """Return supported secondary web service types (none currently)."""
    return {}


@capabilities_router.get("/me")
def me() -> dict[str, Any]:
    """Return basic account info for this open/anonymous backend."""
    return {"user_id": "anonymous", "links": []}


# ---------------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------------


@collections_router.get("")
def list_collections(request: Request) -> dict[str, Any]:
    """Return all published collections (openEO + STAC compatible)."""
    return collections_service.list_collections(request)


@collections_router.get("/{collection_id}")
def get_collection(collection_id: str, request: Request) -> dict[str, Any]:
    """Return one published collection."""
    return collections_service.get_collection(collection_id, request)


# ---------------------------------------------------------------------------
# Processes
# ---------------------------------------------------------------------------


@processes_router.get("")
def list_processes(request: Request) -> dict[str, Any]:
    """Return all available openEO processes."""
    procs = processes_service.list_openeo_processes()
    base_url = _abs_base(request)
    return {
        "processes": procs,
        "links": [{"rel": "self", "href": f"{base_url}/processes", "type": "application/json"}],
    }


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


@jobs_router.get("", response_model=OpenEOJobListResponse)
def list_jobs() -> OpenEOJobListResponse:
    """Return all openEO jobs."""
    return get_openeo_job_service().list_jobs()


@jobs_router.post("", status_code=201)
def create_job(body: OpenEOJobCreate, response: Response) -> OpenEOJobRecord:
    """Create a new openEO job."""
    record = get_openeo_job_service().create_job(body)
    response.headers["Location"] = f"/jobs/{record.id}"
    response.headers["OpenEO-Identifier"] = record.id
    return record


@jobs_router.get("/{job_id}", response_model=OpenEOJobRecord)
def get_job(job_id: str) -> OpenEOJobRecord:
    """Return one openEO job."""
    return get_openeo_job_service().get_job_or_404(job_id)


@jobs_router.patch("/{job_id}", status_code=204)
def update_job(job_id: str, body: OpenEOJobUpdate) -> Response:
    """Update job metadata (title, description, process, etc.)."""
    get_openeo_job_service().update_job(job_id, body)
    return Response(status_code=204)


@jobs_router.delete("/{job_id}", status_code=204)
def delete_job(job_id: str) -> Response:
    """Delete a job and its results."""
    get_openeo_job_service().delete_job(job_id)
    return Response(status_code=204)


@jobs_router.post("/{job_id}/results", status_code=202)
def start_job(job_id: str) -> Response:
    """Queue a job for processing."""
    get_openeo_job_service().start_job(job_id)
    return Response(status_code=202)


@jobs_router.get("/{job_id}/results", response_model=OpenEOJobResults)
def get_results(job_id: str) -> OpenEOJobResults:
    """Return result links for a finished job."""
    return get_openeo_job_service().get_results(job_id)


@jobs_router.delete("/{job_id}/results", status_code=204)
def cancel_job(job_id: str) -> Response:
    """Cancel a running or queued job."""
    get_openeo_job_service().cancel_job(job_id)
    return Response(status_code=204)


@jobs_router.get("/{job_id}/results/result.geojson")
def download_geojson_result(job_id: str) -> FileResponse:
    """Serve the GeoJSON result file for a finished batch job."""
    from climate_api.openeo.jobs import _JOBS_DIR

    path = _JOBS_DIR / job_id / "results" / "result.geojson"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Result file not found")
    return FileResponse(str(path), media_type="application/geo+json", filename="result.geojson")


@jobs_router.get("/{job_id}/results/result.zarr/{zarr_path:path}")
def download_zarr_chunk(job_id: str, zarr_path: str) -> FileResponse:
    """Serve one file from within the Zarr result store (chunk or metadata)."""
    from climate_api.openeo.jobs import _JOBS_DIR

    store_dir = _JOBS_DIR / job_id / "results" / "result.zarr"
    if not store_dir.is_dir():
        raise HTTPException(status_code=404, detail="Result Zarr store not found")

    # Reject path traversal attempts.
    try:
        chunk_path = (store_dir / zarr_path).resolve()
        chunk_path.relative_to(store_dir.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Zarr path")

    if not chunk_path.exists() or not chunk_path.is_file():
        raise HTTPException(status_code=404, detail="Zarr chunk not found")

    return FileResponse(str(chunk_path))


# ---------------------------------------------------------------------------
# Synchronous result  POST /result
# ---------------------------------------------------------------------------


@result_router.post("")
def execute_synchronous(
    body: dict[str, Any] = Body(...),
    request: Request = None,  # type: ignore[assignment]
) -> Any:
    """Execute a process graph synchronously and return the result.

    Returns JSON for scalar/dict results, or a 200 response with Zarr
    metadata for dataset results.
    """
    from climate_api.openeo.execution import run_process_graph

    # Accept both { "process": { "process_graph": ... } } (spec)
    # and   { "process_graph": ... } (editor direct export)
    if "process" in body:
        process = body["process"]
    elif "process_graph" in body:
        process = body
    else:
        raise HTTPException(status_code=422, detail="Body must contain a 'process' object or 'process_graph' directly")
    if not isinstance(process, dict):
        raise HTTPException(status_code=422, detail="Body must contain a 'process' object")
    result = run_process_graph(process, request)

    import xarray as xr

    if isinstance(result, xr.DataArray):
        info: dict[str, Any] = {
            "type": "datacube",
            "name": result.name,
            "dims": dict(zip(result.dims, result.shape)),
            "dtype": str(result.dtype),
            "coords": {k: _coord_summary(v) for k, v in result.coords.items()},
        }
        return JSONResponse(info)
    if isinstance(result, xr.Dataset):
        info = {
            "type": "datacube",
            "dims": dict(result.dims),
            "variables": list(result.data_vars),
            "coords": {k: _coord_summary(v) for k, v in result.coords.items()},
        }
        return JSONResponse(info)
    return result


def _coord_summary(coord: Any) -> Any:
    """Compact coordinate summary safe for JSON serialization."""
    import numpy as np

    vals = coord.values
    if vals.ndim == 0:
        v = vals.item()
        return str(v) if not isinstance(v, (int, float, bool)) else v
    if vals.size == 0:
        return []

    def _fmt(v: Any) -> Any:
        return v.item() if isinstance(v, np.generic) else str(v)

    if vals.size <= 4:
        return [_fmt(v) for v in vals]
    return {"first": _fmt(vals[0]), "last": _fmt(vals[-1]), "size": int(vals.size)}


# ---------------------------------------------------------------------------
# User-defined processes  /process_graphs
# ---------------------------------------------------------------------------


@udp_router.get("", response_model=UDPListResponse)
def list_udps() -> UDPListResponse:
    """Return all stored user-defined processes."""
    return udp_store.list_udps()


@udp_router.get("/{process_graph_id}", response_model=UDPRecord)
def get_udp(process_graph_id: str) -> UDPRecord:
    """Return one user-defined process."""
    record = udp_store.get_udp(process_graph_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Process graph '{process_graph_id}' not found")
    return record


@udp_router.put("/{process_graph_id}", status_code=200)
def put_udp(process_graph_id: str, body: dict[str, Any] = Body(...)) -> UDPRecord:
    """Store or replace a user-defined process."""
    return udp_store.put_udp(process_graph_id, body)


@udp_router.delete("/{process_graph_id}", status_code=204)
def delete_udp(process_graph_id: str) -> Response:
    """Delete a user-defined process."""
    found = udp_store.delete_udp(process_graph_id)
    if not found:
        raise HTTPException(status_code=404, detail=f"Process graph '{process_graph_id}' not found")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _abs_base(request: Request) -> str:
    base_url = os.getenv("CLIMATE_API_BASE_URL")
    if base_url:
        return base_url.rstrip("/")
    return str(request.base_url).rstrip("/")
