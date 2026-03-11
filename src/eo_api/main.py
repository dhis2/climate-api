"""DHIS2 EO API - Earth observation data API for DHIS2.

load_dotenv() is called before pygeoapi import because pygeoapi
reads PYGEOAPI_CONFIG and PYGEOAPI_OPENAPI at import time.

Prefect UI env vars are set before any imports because Prefect
caches its settings on first import.
"""

import json
import logging
import os
import warnings
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from html import escape
from importlib.util import find_spec
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

os.environ.setdefault("PREFECT_UI_SERVE_BASE", "/prefect/")
os.environ.setdefault("PREFECT_UI_API_URL", "/prefect/api")
os.environ.setdefault("PREFECT_SERVER_API_BASE_PATH", "/prefect/api")
os.environ.setdefault("PREFECT_API_URL", "http://localhost:8000/prefect/api")
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
os.environ.setdefault("PREFECT_SERVER_UI_SHOW_PROMOTIONAL_CONTENT", "false")


def _configure_proj_data() -> None:
    """Point PROJ to rasterio bundled data to avoid mixed-install conflicts."""
    spec = find_spec("rasterio")
    if spec is None or spec.origin is None:
        return

    proj_data = Path(spec.origin).parent / "proj_data"
    if not proj_data.is_dir():
        return

    proj_data_path = str(proj_data)
    os.environ["PROJ_DATA"] = proj_data_path
    os.environ["PROJ_LIB"] = proj_data_path


_configure_proj_data()

warnings.filterwarnings("ignore", message="ecCodes .* or higher is recommended")
warnings.filterwarnings("ignore", message=r"Engine 'cfgrib' loading failed:[\s\S]*", category=RuntimeWarning)

logging.getLogger("pygeoapi.api.processes").setLevel(logging.ERROR)
logging.getLogger("pygeoapi.l10n").setLevel(logging.ERROR)

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

openapi_path = os.getenv("PYGEOAPI_OPENAPI")
config_path = os.getenv("PYGEOAPI_CONFIG")
if openapi_path and config_path and not Path(openapi_path).exists():
    from pygeoapi.openapi import generate_openapi_document  # noqa: E402

    with Path(config_path).open(encoding="utf-8") as config_file:
        openapi_doc = generate_openapi_document(
            config_file,
            output_format=cast(Any, "yaml"),
            fail_on_invalid_collection=False,
        )
    Path(openapi_path).write_text(openapi_doc, encoding="utf-8")
    warnings.warn(f"Generated missing OpenAPI document at '{openapi_path}'.", RuntimeWarning)

from fastapi import FastAPI, Request  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import HTMLResponse, RedirectResponse  # noqa: E402
from starlette.responses import JSONResponse, Response  # noqa: E402

from eo_api.integrations.orchestration import preview_store  # noqa: E402
from eo_api.integrations.orchestration.output_collections import ensure_output_collections_seeded  # noqa: E402
from eo_api.ogc_async import enrich_async_job_payload  # noqa: E402
from eo_api.ogc_jobs import install_processes_jobs_filter_patch  # noqa: E402
from eo_api.routers import cog, ogcapi, pipelines, prefect, root, workflow_capabilities  # noqa: E402

ensure_output_collections_seeded()
install_processes_jobs_filter_patch()


def _safe_forward_headers(headers: dict[str, str]) -> dict[str, str]:
    """Drop hop-by-hop/body-size headers before building a new response."""
    blocked = {"content-length", "transfer-encoding", "connection"}
    return {key: value for key, value in headers.items() if key.lower() not in blocked}


# Keep app progress logs visible while muting noisy third-party info logs.
eo_logger = logging.getLogger("eo_api")
eo_logger.setLevel(logging.INFO)
if not eo_logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    eo_logger.addHandler(handler)
eo_logger.propagate = False

logging.getLogger("dhis2eo").setLevel(logging.WARNING)
logging.getLogger("xarray").setLevel(logging.WARNING)


async def _serve_flows() -> None:
    """Register Prefect deployments and start a runner to execute them."""
    from prefect.runner import Runner

    from eo_api.prefect_flows.flows import ALL_FLOWS

    runner = Runner()
    for fl in ALL_FLOWS:
        await runner.aadd_flow(fl, name=fl.name)
    await runner.start()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start Prefect server, then register and serve pipeline deployments."""
    import asyncio

    # Mounted sub-apps don't get their lifespans called automatically,
    # so we trigger the Prefect server's lifespan here to initialize
    # the database, docket, and background workers.
    prefect_app = prefect.app
    async with prefect_app.router.lifespan_context(prefect_app):
        task = asyncio.create_task(_serve_flows())
        yield
        task.cancel()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(root.router)
app.include_router(cog.router, prefix="/cog", tags=["Cloud Optimized GeoTIFF"])
app.include_router(pipelines.router, prefix="/pipelines", tags=["Pipelines"])
app.include_router(workflow_capabilities.router)

_PREVIEW_COLLECTION_ID = "generic-dhis2-datavalue-preview"
_PREVIEW_ITEMS_PATH = f"/ogcapi/collections/{_PREVIEW_COLLECTION_ID}/items"
_PREVIEW_JOBS_PICKER_PATH = f"/ogcapi/collections/{_PREVIEW_COLLECTION_ID}/jobs"


@app.middleware("http")
async def enrich_ogc_async_execution_response(request, call_next):  # type: ignore[no-untyped-def]
    """Add absolute job URLs into OGC async accepted response bodies."""
    response = await call_next(request)

    is_ogc_execution = request.url.path.startswith("/ogcapi/processes/") and request.url.path.endswith("/execution")
    if not is_ogc_execution or response.status_code != 201:
        return response

    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type:
        return response

    body = b""
    async for chunk in response.body_iterator:
        body += chunk

    try:
        import json

        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return Response(
            content=body,
            status_code=response.status_code,
            headers=_safe_forward_headers(dict(response.headers)),
            media_type=response.media_type,
        )

    if not isinstance(payload, dict):
        return Response(
            content=body,
            status_code=response.status_code,
            headers=_safe_forward_headers(dict(response.headers)),
            media_type=response.media_type,
        )

    location = response.headers.get("Location")
    base_url = f"{request.url.scheme}://{request.url.netloc}/"
    enriched = enrich_async_job_payload(payload, location=location, base_url=base_url)
    return JSONResponse(
        content=enriched,
        status_code=response.status_code,
        headers=_safe_forward_headers(dict(response.headers)),
    )


@app.middleware("http")
async def scope_ogc_jobs_process_filter(request, call_next):  # type: ignore[no-untyped-def]
    """Expose active jobs process filter in response headers for diagnostics."""
    if request.url.path.rstrip("/") != "/ogcapi/jobs":
        return await call_next(request)

    process_id = request.query_params.get("process_id") or request.query_params.get("processID")
    response = await call_next(request)
    if process_id:
        response.headers["X-EO-Jobs-Process-Filter"] = process_id
    return response


@app.get(_PREVIEW_JOBS_PICKER_PATH, include_in_schema=False)
async def preview_collection_jobs_picker(request: Request) -> Response:
    """Render/select recent preview jobs for map browsing."""
    all_jobs = preview_store.list_preview_jobs(limit=500)
    if "process_id" in request.query_params:
        process_filter = (request.query_params.get("process_id") or "").strip() or "all"
    else:
        process_filter = (request.cookies.get("eo_preview_process_id") or "all").strip() or "all"
    if "dataset_type" in request.query_params:
        # Explicit empty value means "All datasets"; do not fall back to cookie.
        dataset_filter = (request.query_params.get("dataset_type") or "").strip().lower()
    else:
        dataset_filter = (request.cookies.get("eo_preview_dataset_type") or "").strip().lower()
    jobs = all_jobs
    if process_filter not in {"all", "generic-dhis2-workflow"}:
        jobs = []
    if dataset_filter:
        jobs = [j for j in jobs if str(j.get("dataset_type") or "").strip().lower() == dataset_filter]
    if request.query_params.get("f") == "json":
        return JSONResponse(
            {
                "collection_id": _PREVIEW_COLLECTION_ID,
                "process_id": process_filter,
                "dataset_type": dataset_filter or None,
                "jobs": jobs,
            }
        )

    if not jobs:
        html = (
            "<html><body><h1>Preview jobs</h1>"
            "<p>No preview jobs available yet. Run generic workflow first.</p>"
            f'<p><a href="/ogcapi/collections/{_PREVIEW_COLLECTION_ID}">Back to collection</a></p>'
            "</body></html>"
        )
        return HTMLResponse(html)

    rows = []
    for item in jobs:
        job_id = item["job_id"]
        href = f"{_PREVIEW_ITEMS_PATH}?job_id={job_id}"
        rows.append(
            "<tr>"
            f'<td><a href="{href}">{job_id}</a></td>'
            f"<td>{item.get('dataset_type') or ''}</td>"
            f"<td>{item.get('published_at') or ''}</td>"
            f"<td>{item.get('row_count') or 0}</td>"
            "</tr>"
        )
    table_rows = "".join(rows)
    datasets = sorted({str(item.get("dataset_type")) for item in all_jobs if item.get("dataset_type")})
    dataset_options = ["<option value=''>All datasets</option>"]
    for ds in datasets:
        selected = " selected" if dataset_filter == ds else ""
        dataset_options.append(f"<option value='{escape(ds)}'{selected}>{escape(ds)}</option>")
    dataset_options_html = "".join(dataset_options)
    process_generic_selected = " selected" if process_filter == "generic-dhis2-workflow" else ""
    process_all_selected = " selected" if process_filter == "all" else ""
    html = (
        "<html><body>"
        "<h1>Pick a preview job</h1>"
        f"<p>Collection: <code>{_PREVIEW_COLLECTION_ID}</code></p>"
        "<form method='get' style='margin:8px 0 14px;'>"
        "<label for='process_id'>Process:</label> "
        "<select id='process_id' name='process_id'>"
        f"<option value='generic-dhis2-workflow'{process_generic_selected}>generic-dhis2-workflow</option>"
        f"<option value='all'{process_all_selected}>all</option>"
        "</select> "
        "<label for='dataset_type'>Dataset:</label> "
        f"<select id='dataset_type' name='dataset_type'>{dataset_options_html}</select> "
        "<button type='submit'>Apply</button>"
        "</form>"
        f'<p><a href="/ogcapi/collections/{_PREVIEW_COLLECTION_ID}">Back to collection</a></p>'
        "<table border='1' cellpadding='6' cellspacing='0'>"
        "<thead><tr><th>Job ID</th><th>Dataset</th><th>Published</th><th>Rows</th></tr></thead>"
        f"<tbody>{table_rows}</tbody></table>"
        "</body></html>"
    )
    response = HTMLResponse(html)
    response.set_cookie("eo_preview_process_id", process_filter, max_age=60 * 60 * 24 * 30, samesite="lax")
    response.set_cookie("eo_preview_dataset_type", dataset_filter, max_age=60 * 60 * 24 * 30, samesite="lax")
    return response


@app.middleware("http")
async def redirect_preview_items_to_explicit_job(request, call_next):  # type: ignore[no-untyped-def]
    """Render in-flow job picker for HTML browsing unless explicit job scope exists."""
    if request.method != "GET" or request.url.path.rstrip("/") != _PREVIEW_ITEMS_PATH:
        return await call_next(request)

    params = dict(request.query_params)
    has_job_scope = bool(params.get("job_id")) or bool(params.get("all_jobs"))
    if has_job_scope:
        # For selected-job browsing, request enough rows so map coverage is not limited
        # by the default page size (many rows can share the same orgUnit geometry).
        if params.get("job_id") and not params.get("period"):
            periods = preview_store.list_preview_periods(job_id=str(params["job_id"]))
            if periods:
                params["period"] = periods[0]
                return RedirectResponse(url=f"{_PREVIEW_ITEMS_PATH}?{urlencode(params)}", status_code=307)
        if params.get("job_id") and not params.get("limit"):
            params["limit"] = "5000"
            return RedirectResponse(url=f"{_PREVIEW_ITEMS_PATH}?{urlencode(params)}", status_code=307)
        return await call_next(request)

    wants_json = params.get("f") == "json" or "application/json" in request.headers.get("accept", "")
    if wants_json:
        return RedirectResponse(url=f"{_PREVIEW_JOBS_PICKER_PATH}?f=json", status_code=307)

    all_jobs = preview_store.list_preview_jobs(limit=500)
    if "process_id" in request.query_params:
        process_filter = (params.get("process_id") or "").strip() or "all"
    else:
        process_filter = (request.cookies.get("eo_preview_process_id") or "all").strip() or "all"
    if "dataset_type" in request.query_params:
        # Explicit empty value means "All datasets"; do not fall back to cookie.
        dataset_filter = (params.get("dataset_type") or "").strip().lower()
    else:
        dataset_filter = (request.cookies.get("eo_preview_dataset_type") or "").strip().lower()
    jobs = all_jobs
    if process_filter not in {"all", "generic-dhis2-workflow"}:
        jobs = []
    if dataset_filter:
        jobs = [j for j in jobs if str(j.get("dataset_type") or "").strip().lower() == dataset_filter]
    if not jobs:
        return HTMLResponse(
            "<html><body><h1>No preview jobs yet</h1>"
            "<p>Run generic workflow first, then return here.</p>"
            f"<p><a href='/ogcapi/collections/{_PREVIEW_COLLECTION_ID}'>Back to collection</a></p>"
            "</body></html>"
        )

    row_items = []
    for item in jobs:
        job_id = str(item["job_id"])
        href = f"{_PREVIEW_ITEMS_PATH}?job_id={job_id}"
        row_items.append(
            "<tr>"
            f"<td><a href='{escape(href)}'>{escape(job_id)}</a></td>"
            f"<td>{escape(str(item.get('dataset_type') or ''))}</td>"
            f"<td>{escape(str(item.get('published_at') or ''))}</td>"
            f"<td>{int(item.get('row_count') or 0)}</td>"
            "</tr>"
        )
    rows_html = "".join(row_items)
    datasets = sorted({str(item.get("dataset_type")) for item in all_jobs if item.get("dataset_type")})
    dataset_options = ["<option value=''>All datasets</option>"]
    for ds in datasets:
        selected = " selected" if dataset_filter == ds else ""
        dataset_options.append(f"<option value='{escape(ds)}'{selected}>{escape(ds)}</option>")
    dataset_options_html = "".join(dataset_options)
    process_generic_selected = " selected" if process_filter == "generic-dhis2-workflow" else ""
    process_all_selected = " selected" if process_filter == "all" else ""
    html = (
        "<!doctype html><html><head><meta charset='utf-8'><title>Pick preview job</title>"
        "<style>"
        "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#f4f6f8;}"
        ".frame{min-height:100vh;display:grid;place-items:center;padding:24px;}"
        "dialog{width:min(980px,96vw);border:0;border-radius:12px;box-shadow:0 16px 50px rgba(0,0,0,.25);padding:0;}"
        ".head{padding:16px 20px;border-bottom:1px solid #e5e7eb;background:#fff;"
        "display:flex;justify-content:space-between;align-items:center;}"
        ".title{font-size:20px;font-weight:700;margin:0;}"
        ".meta{font-size:13px;color:#6b7280;}"
        ".body{padding:0 16px 16px;background:#fff;max-height:70vh;overflow:auto;}"
        "table{width:100%;border-collapse:collapse;font-size:14px;}"
        "th,td{border-bottom:1px solid #edf0f2;padding:8px;text-align:left;vertical-align:top;}"
        "th{position:sticky;top:0;background:#f9fafb;z-index:1;}"
        "a{color:#1d4ed8;text-decoration:none;}a:hover{text-decoration:underline;}"
        ".foot{padding:12px 20px;border-top:1px solid #e5e7eb;background:#fff;}"
        "</style></head><body>"
        "<div class='frame'><dialog open>"
        "<div class='head'>"
        "<h1 class='title'>Pick a preview job</h1>"
        f"<div class='meta'>Collection: {escape(_PREVIEW_COLLECTION_ID)}</div>"
        "</div>"
        "<div class='body'>"
        "<form method='get' style='margin:12px 0;'>"
        "<label for='process_id'><strong>Process:</strong></label> "
        "<select id='process_id' name='process_id'>"
        f"<option value='generic-dhis2-workflow'{process_generic_selected}>generic-dhis2-workflow</option>"
        f"<option value='all'{process_all_selected}>all</option>"
        "</select> "
        "<label for='dataset_type'><strong>Dataset:</strong></label> "
        f"<select id='dataset_type' name='dataset_type'>{dataset_options_html}</select> "
        "<button type='submit'>Apply</button>"
        "</form>"
        "<table>"
        "<thead><tr><th>Job ID</th><th>Dataset</th><th>Published</th><th>Rows</th></tr></thead>"
        f"<tbody>{rows_html}</tbody></table></div>"
        "<div class='foot'>"
        f"<a href='/ogcapi/collections/{_PREVIEW_COLLECTION_ID}'>Back to collection</a>"
        "</div>"
        "</dialog></div></body></html>"
    )
    response = HTMLResponse(html)
    response.set_cookie("eo_preview_process_id", process_filter, max_age=60 * 60 * 24 * 30, samesite="lax")
    response.set_cookie("eo_preview_dataset_type", dataset_filter, max_age=60 * 60 * 24 * 30, samesite="lax")
    return response


@app.middleware("http")
async def customize_preview_items_header_with_dataset(request, call_next):  # type: ignore[no-untyped-def]
    """Set preview items HTML heading to include selected job dataset."""
    if request.method != "GET" or request.url.path.rstrip("/") != _PREVIEW_ITEMS_PATH:
        return await call_next(request)

    job_id = request.query_params.get("job_id")
    if not job_id:
        return await call_next(request)

    if request.query_params.get("f") == "json" or "application/json" in request.headers.get("accept", ""):
        return await call_next(request)

    response = await call_next(request)
    content_type = response.headers.get("content-type", "")
    if "text/html" not in content_type:
        return response

    page = b""
    async for chunk in response.body_iterator:
        page += chunk

    dataset = ""
    current_period = (request.query_params.get("period") or "").strip()
    periods = preview_store.list_preview_periods(job_id=job_id)
    preview = preview_store.query_preview_features(job_id=job_id, offset=0, limit=1)
    features = preview.get("features", [])
    if isinstance(features, list) and features:
        first = features[0] if isinstance(features[0], dict) else {}
        props = first.get("properties", {})
        if isinstance(props, dict):
            value = props.get("dataset_type")
            if isinstance(value, str):
                dataset = value.strip()

    period_options = ["<option value=''>All periods</option>"]
    for period in periods:
        selected = " selected" if current_period == period else ""
        period_options.append(f"<option value='{escape(period)}'{selected}>{escape(period)}</option>")
    period_options_html = "".join(period_options)

    scroll_style = (
        "<style>"
        ".eo-filters{display:flex;gap:10px;align-items:center;margin:8px 0 10px;}"
        ".eo-table-scroll{max-height:62vh;overflow:auto;border:1px solid #e5e7eb;border-radius:8px;}"
        ".eo-table-scroll table{margin:0;}"
        ".eo-table-scroll th{position:sticky;top:0;background:#f9fafb;z-index:2;}"
        "</style>"
        "<script>"
        "document.addEventListener('DOMContentLoaded',function(){"
        "var mapNode=document.querySelector('.leaflet-container');"
        "var host=mapNode?mapNode.closest('div'):null;"
        "var search=new URLSearchParams(window.location.search);"
        "if(host&&!document.querySelector('.eo-filters')){"
        "var bar=document.createElement('form');"
        "bar.className='eo-filters';bar.method='GET';"
        "var hiddenJob=document.createElement('input');hiddenJob.type='hidden';hiddenJob.name='job_id';"
        "hiddenJob.value=search.get('job_id')||'';bar.appendChild(hiddenJob);"
        "var hiddenLimit=document.createElement('input');hiddenLimit.type='hidden';hiddenLimit.name='limit';"
        "hiddenLimit.value=search.get('limit')||'5000';bar.appendChild(hiddenLimit);"
        "var label=document.createElement('label');label.textContent='Period:';bar.appendChild(label);"
        "var select=document.createElement('select');select.name='period';"
        f"select.innerHTML={json.dumps(period_options_html)};"
        "bar.appendChild(select);"
        "var btn=document.createElement('button');btn.type='submit';btn.textContent='Apply';bar.appendChild(btn);"
        "host.parentNode.insertBefore(bar,host.nextSibling);"
        "}"
        "var table=document.querySelector('table');"
        "if(!table||table.closest('.eo-table-scroll')) return;"
        "var headers=Array.from(table.querySelectorAll('thead th'));"
        "var idxOrg=headers.findIndex(function(th){return th.textContent.trim()==='orgUnit';});"
        "var idxName=headers.findIndex(function(th){return th.textContent.trim()==='orgUnitName';});"
        "if(idxOrg>=0&&idxName>=0&&idxName!==idxOrg+1){"
        "var moveCell=function(row){var cells=row.children;if(idxName<cells.length&&idxOrg<cells.length){"
        "var cell=cells[idxName];"
        "row.removeChild(cell);"
        "row.insertBefore(cell,cells[idxOrg+1]||null);}};"
        "moveCell(table.querySelector('thead tr'));"
        "Array.from(table.querySelectorAll('tbody tr')).forEach(moveCell);"
        "}"
        "var wrap=document.createElement('div');"
        "wrap.className='eo-table-scroll';"
        "table.parentNode.insertBefore(wrap,table);"
        "wrap.appendChild(table);"
        "});"
        "</script>"
    )
    text = page.decode("utf-8", errors="replace")
    text = text.replace("</head>", f"{scroll_style}</head>")

    if dataset:
        original = "Generic DHIS2 dataValue preview"
        customized = f"{original} - {dataset}"
        text = text.replace(original, customized)
        return Response(
            content=text.encode("utf-8"),
            status_code=response.status_code,
            headers=_safe_forward_headers(dict(response.headers)),
            media_type=response.media_type,
        )
    return Response(
        content=text.encode("utf-8"),
        status_code=response.status_code,
        headers=_safe_forward_headers(dict(response.headers)),
        media_type=response.media_type,
    )


@app.get("/ogcapi", include_in_schema=False)
async def ogcapi_redirect() -> RedirectResponse:
    """Redirect /ogcapi to /ogcapi/ for trailing-slash consistency."""
    return RedirectResponse(url="/ogcapi/")


app.mount(path="/ogcapi", app=ogcapi.app)
app.mount(path="/", app=prefect.app)
