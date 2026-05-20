"""Root API endpoints."""

import asyncio
import json
import sys
import urllib.parse
from importlib.metadata import version as _pkg_version
from typing import Any, AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from starlette.responses import RedirectResponse

from .schemas import AppInfo, HealthStatus, Status
from .templates import ROOT_RESPONSES, app_version, render_landing, render_manage, render_maps, root_json, wants_json

router = APIRouter()


async def _sse_stream(
    task: asyncio.Task[None],
    queue: asyncio.Queue[dict[str, Any] | None],
) -> AsyncGenerator[str, None]:
    """Yield SSE events from queue until the task sentinel (None) arrives."""
    while True:
        event = await queue.get()
        if event is None:
            break
        yield f"data: {json.dumps(event)}\n\n"


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
async def manage_ingest(request: Request) -> Response:
    """Stream ingest progress as SSE, then signal redirect on completion."""
    from climate_api.data_registry.services.datasets import get_dataset
    from climate_api.extents.services import get_extent_or_404
    from climate_api.ingestions.services import create_artifact

    base = str(request.base_url).rstrip("/")
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

    queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    loop = asyncio.get_event_loop()
    error_holder: list[str] = []

    def on_progress(done: int, total: int, message: str = "") -> None:
        loop.call_soon_threadsafe(queue.put_nowait, {"done": done, "total": total, "message": message})

    async def run() -> None:
        try:
            await asyncio.to_thread(
                create_artifact,
                dataset=template,
                start=start,
                end=end,
                bbox=resolved_bbox,
                overwrite=overwrite,
                publish=publish,
                on_progress=on_progress,
            )
        except Exception as exc:
            error_holder.append(str(exc))
        finally:
            await queue.put(None)

    task = asyncio.create_task(run())

    async def event_stream() -> AsyncGenerator[str, None]:
        async for chunk in _sse_stream(task, queue):
            yield chunk
        if error_holder:
            yield f"data: {json.dumps({'error': error_holder[0]})}\n\n"
        else:
            name = urllib.parse.quote(str(template.get("name", dataset_id)))
            yield f"data: {json.dumps({'redirect': f'{base}/manage?message=Ingested+{name}'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/manage/sync", include_in_schema=False)
async def manage_sync(request: Request) -> Response:
    """Stream sync progress as SSE, then signal redirect on completion."""
    from climate_api.ingestions.services import sync_dataset

    base = str(request.base_url).rstrip("/")
    form = await request.form()
    dataset_id = str(form.get("dataset_id", ""))
    publish = "publish" in form

    queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    loop = asyncio.get_event_loop()
    error_holder: list[str] = []

    def on_progress(done: int, total: int, message: str = "") -> None:
        loop.call_soon_threadsafe(queue.put_nowait, {"done": done, "total": total, "message": message})

    async def run() -> None:
        try:
            await asyncio.to_thread(
                sync_dataset, dataset_id=dataset_id, end=None, publish=publish, on_progress=on_progress
            )
        except Exception as exc:
            error_holder.append(str(exc))
        finally:
            await queue.put(None)

    task = asyncio.create_task(run())

    async def event_stream() -> AsyncGenerator[str, None]:
        async for chunk in _sse_stream(task, queue):
            yield chunk
        if error_holder:
            yield f"data: {json.dumps({'error': error_holder[0]})}\n\n"
        else:
            yield f"data: {json.dumps({'redirect': f'{base}/manage?message=Sync+completed'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


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
