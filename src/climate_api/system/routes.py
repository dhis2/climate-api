"""Root API endpoints."""

import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from .schemas import AppInfo, HealthStatus, Link, RootResponse, Status
from .templates import render_landing, wants_json

router = APIRouter()

try:
    _app_version = _pkg_version("climate-api")
except PackageNotFoundError:
    _app_version = "unknown"


def _root_json(base: str) -> RootResponse:
    return RootResponse(
        message="Welcome to DHIS2 Climate API",
        links=[
            Link(href=f"{base}/ogcapi/", rel="ogcapi", title="OGC API"),
            Link(href=f"{base}/stac/catalog.json", rel="stac", title="STAC Catalog"),
            Link(href=f"{base}/extent", rel="extent", title="Extent"),
            Link(href=f"{base}/ingestions", rel="ingestions", title="Ingestions"),
            Link(href=f"{base}/datasets", rel="datasets", title="Datasets"),
            Link(href=f"{base}/docs", rel="docs", title="API Docs"),
        ],
    )


@router.get(
    "/",
    response_class=Response,
    responses={
        200: {
            "description": "Landing page (HTML) or navigation document (JSON)",
            "content": {
                "text/html": {"schema": {"type": "string"}},
                "application/json": {"model": RootResponse},
            },
        }
    },
)
def read_index(request: Request) -> Response:
    """Return the landing page (HTML) or a navigation object (JSON with ?f=json)."""
    base = str(request.base_url).rstrip("/")
    if wants_json(request):
        return JSONResponse(_root_json(base).model_dump())
    return HTMLResponse(render_landing(_app_version, base))


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
