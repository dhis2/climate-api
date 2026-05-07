"""Root API endpoints."""

import sys
from importlib.metadata import version as _pkg_version

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from .schemas import AppInfo, HealthStatus, Status
from .templates import ROOT_RESPONSES, app_version, render_landing, render_maps, root_json, wants_json

router = APIRouter()


@router.get("/", response_class=Response, responses=ROOT_RESPONSES)
def read_index(request: Request) -> Response:
    """Return the landing page (HTML) or a navigation object (JSON with ?f=json)."""
    base = str(request.base_url).rstrip("/")
    if wants_json(request):
        return JSONResponse(root_json(base).model_dump())
    return HTMLResponse(render_landing(app_version, base))


@router.get("/map", response_class=HTMLResponse, include_in_schema=False)
def maps(request: Request) -> HTMLResponse:
    """Return the interactive map viewer."""
    base = str(request.base_url).rstrip("/")
    return HTMLResponse(render_maps(base))


@router.get("/health")
def health() -> HealthStatus:
    """Return health status for container health checks."""
    return HealthStatus(status=Status.HEALTHY)


@router.get("/info")
def info() -> AppInfo:
    """Return application version and environment info."""
    return AppInfo(
        app_version=_pkg_version("climate-api"),
        python_version=sys.version,
        pygeoapi_version=_pkg_version("pygeoapi"),
        uvicorn_version=_pkg_version("uvicorn"),
    )
