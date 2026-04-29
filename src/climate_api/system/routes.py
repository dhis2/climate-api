"""Root API endpoints."""

import sys
from importlib.metadata import version

from fastapi import APIRouter, Request

from .schemas import AppInfo, HealthStatus, Link, RootResponse, Status

router = APIRouter()


@router.get("/")
def read_index(request: Request) -> RootResponse:
    """Return a welcome message with navigation links."""
    base = str(request.base_url).rstrip("/")
    return RootResponse(
        message="Welcome to DHIS2 Climate API",
        links=[
            Link(href=f"{base}/ogcapi/", rel="ogcapi", title="OGC API"),
            Link(href=f"{base}/stac/catalog.json", rel="stac", title="STAC Catalog"),
            Link(href=f"{base}/extents", rel="extents", title="Extents"),
            Link(href=f"{base}/ingestions", rel="ingestions", title="Ingestions"),
            Link(href=f"{base}/datasets", rel="datasets", title="Datasets"),
            Link(href=f"{base}/docs", rel="docs", title="API Docs"),
        ],
    )


@router.get("/health")
def health() -> HealthStatus:
    """Return health status for container health checks."""
    return HealthStatus(status=Status.HEALTHY)


@router.get("/info")
def info() -> AppInfo:
    """Return application version and environment info."""
    return AppInfo(
        app_version=version("climate-api"),
        python_version=sys.version,
        pygeoapi_version=version("pygeoapi"),
        uvicorn_version=version("uvicorn"),
    )
