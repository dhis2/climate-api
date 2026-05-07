"""Root API endpoints."""

import importlib.resources
import sys
from importlib.metadata import version
from typing import Any

import jinja2
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from climate_api.extents.services import get_extent
from climate_api.ingestions.services import list_datasets

from .schemas import AppInfo, HealthStatus, Link, RootResponse, Status

router = APIRouter()

_env = jinja2.Environment(
    loader=jinja2.BaseLoader(),
    autoescape=True,
)

_template_text: str | None = None


def _get_template() -> jinja2.Template:
    global _template_text
    if _template_text is None:
        resource = importlib.resources.files("climate_api") / "data" / "templates" / "index.html"
        _template_text = resource.read_text(encoding="utf-8")
    return _env.from_string(_template_text)


def _wants_json(request: Request) -> bool:
    if request.query_params.get("f") == "json":
        return True
    accept = request.headers.get("accept", "")
    return "application/json" in accept and "text/html" not in accept


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


def _root_html() -> str:
    try:
        extent: dict[str, Any] | None = get_extent()
    except Exception:
        extent = None
    try:
        datasets = list_datasets().items
    except Exception:
        datasets = []
    return _get_template().render(
        version=version("climate-api"),
        extent=extent,
        datasets=datasets,
    )


@router.get("/", response_class=Response)
def read_index(request: Request) -> Response:
    """Return the landing page (HTML) or a navigation object (JSON with ?f=json)."""
    base = str(request.base_url).rstrip("/")
    if _wants_json(request):
        return JSONResponse(_root_json(base).model_dump())
    return HTMLResponse(_root_html())


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
