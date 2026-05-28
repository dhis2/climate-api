"""Root API endpoints."""

import asyncio
import sys
import urllib.parse
from importlib.metadata import version as _pkg_version

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from starlette.responses import RedirectResponse

from climate_api import config as api_config

from .schemas import AppInfo, HealthStatus, Status
from .templates import (
    ROOT_RESPONSES,
    app_version,
    render_landing,
    render_manage,
    render_maps,
    wants_json,
)

router = APIRouter()


@router.get("/", response_class=Response, responses=ROOT_RESPONSES)
def read_index(request: Request) -> Response:
    """Return openEO capabilities (JSON) or the landing page (HTML)."""
    base = str(request.base_url).rstrip("/")
    if wants_json(request):
        from climate_api.openeo.capabilities import build_capabilities

        caps = build_capabilities(base)
        return JSONResponse(caps.model_dump())
    return HTMLResponse(render_landing(app_version, base))


@router.get("/map", response_class=HTMLResponse, include_in_schema=False)
def maps(request: Request) -> HTMLResponse:
    """Return the interactive map viewer."""
    base = str(request.base_url).rstrip("/")
    return HTMLResponse(render_maps(base))


@router.get("/openeo", response_class=HTMLResponse, include_in_schema=False)
def openeo_editor(request: Request) -> RedirectResponse:
    """Redirect to the openEO Web Editor pre-connected to this backend."""
    base = str(request.base_url).rstrip("/")
    params = urllib.parse.urlencode({"server": base, "server-title": api_config.get_name()})
    return RedirectResponse(f"https://editor.openeo.org/?{params}", status_code=302)


@router.get("/manage", response_class=HTMLResponse, include_in_schema=False)
def manage(
    request: Request,
    message: str | None = None,
    error: str | None = None,
) -> HTMLResponse:
    """Return the management interface for ingestion and sync operations."""
    base = str(request.base_url).rstrip("/")
    return HTMLResponse(render_manage(app_version, base, message=message, error=error))


@router.post("/manage/ingest", include_in_schema=False)
async def manage_ingest(request: Request) -> RedirectResponse:
    """Handle ingest form submission and redirect to the management page."""
    from fastapi import HTTPException

    from climate_api.data_registry.services.datasets import get_dataset
    from climate_api.extents.services import get_extent_or_404
    from climate_api.ingestions.services import create_artifact

    base = str(request.base_url).rstrip("/")
    try:
        form = await request.form()
        dataset_id = str(form.get("dataset_id", ""))
        start = str(form.get("start", ""))
        end = str(form.get("end", "")) or None
        publish = "publish" in form
        overwrite = "overwrite" in form

        template = get_dataset(dataset_id)
        if template is None:
            msg = urllib.parse.quote(f"Dataset template '{dataset_id}' not found")
            return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)

        extent = get_extent_or_404()
        resolved_bbox = list(extent["bbox"])
        country_code = extent.get("country_code")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: create_artifact(
                dataset=template,
                start=start,
                end=end,
                bbox=resolved_bbox,
                country_code=country_code,
                overwrite=overwrite,
                publish=publish,
            ),
        )
        name = urllib.parse.quote(template.get("name", dataset_id))
        return RedirectResponse(f"{base}/manage?message=Ingested+{name}", status_code=303)
    except HTTPException as exc:
        msg = urllib.parse.quote(str(exc.detail))
        return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)
    except Exception as exc:
        msg = urllib.parse.quote(str(exc))
        return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)


@router.post("/manage/sync", include_in_schema=False)
async def manage_sync(request: Request) -> RedirectResponse:
    """Handle sync form submission and redirect to the management page."""
    from fastapi import HTTPException

    from climate_api.ingestions.services import sync_dataset

    base = str(request.base_url).rstrip("/")
    try:
        form = await request.form()
        dataset_id = str(form.get("dataset_id", ""))
        publish = "publish" in form

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: sync_dataset(dataset_id=dataset_id, end=None, publish=publish))
        return RedirectResponse(f"{base}/manage?message=Sync+completed", status_code=303)
    except HTTPException as exc:
        msg = urllib.parse.quote(str(exc.detail))
        return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)
    except Exception as exc:
        msg = urllib.parse.quote(str(exc))
        return RedirectResponse(f"{base}/manage?error={msg}", status_code=303)


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
