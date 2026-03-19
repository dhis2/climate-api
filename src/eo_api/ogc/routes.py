"""Thin OGC API adapter routes over the native workflow engine."""

from __future__ import annotations

import uuid
from html import escape
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request, Response
from fastapi.responses import HTMLResponse

from ..publications.schemas import PublishedResourceExposure
from ..publications.services import collection_id_for_resource, get_published_resource
from ..shared.api_errors import api_error
from ..workflows.schemas import WorkflowExecuteEnvelopeRequest, WorkflowJobStatus
from ..workflows.services.definitions import load_workflow_definition
from ..workflows.services.engine import execute_workflow
from ..workflows.services.job_store import get_job, get_job_result, initialize_job, list_jobs
from ..workflows.services.simple_mapper import normalize_simple_request

router = APIRouter()

_PROCESS_ID = "generic-dhis2-workflow"
_PROCESS_TITLE = "Generic DHIS2 workflow"


@router.get("", response_model=None)
def get_ogc_root(request: Request, f: str | None = None) -> dict[str, Any] | HTMLResponse:
    """Return a native OGC landing page for processes and jobs."""
    base_url = str(request.base_url).rstrip("/")
    body = {
        "title": "DHIS2 EO API",
        "description": (
            "Native OGC API landing page for workflow processes and jobs. "
            "Collections and items are served by the mounted geospatial publication layer."
        ),
        "links": [
            {"rel": "self", "type": "application/json", "href": _request_href(request, f="json")},
            {"rel": "alternate", "type": "text/html", "href": _request_href(request, f="html")},
            {"rel": "service-desc", "type": "application/vnd.oai.openapi+json;version=3.0", "href": "/ogcapi/openapi"},
            {"rel": "conformance", "type": "application/json", "href": f"{base_url}/ogcapi/conformance"},
            {"rel": "data", "type": "application/json", "href": f"{base_url}/ogcapi/collections"},
            {"rel": "processes", "type": "application/json", "href": f"{base_url}/ogcapi/processes"},
            {"rel": "jobs", "type": "application/json", "href": f"{base_url}/ogcapi/jobs"},
        ],
        "navigation": [
            {
                "title": "Browse Collections",
                "description": "Open the OGC publication surface for collections and items.",
                "href": f"{base_url}/ogcapi/collections?f=html",
            },
            {
                "title": "List Processes",
                "description": "View the exposed OGC process catalog backed by the native workflow engine.",
                "href": f"{base_url}/ogcapi/processes",
            },
            {
                "title": "List Jobs",
                "description": "Inspect OGC job records backed by the native job store.",
                "href": f"{base_url}/ogcapi/jobs",
            },
            {
                "title": "Conformance",
                "description": "See the standards conformance declarations for the mounted OGC publication layer.",
                "href": f"{base_url}/ogcapi/conformance",
            },
        ],
    }
    if _wants_html(request, f):
        return HTMLResponse(_render_ogc_root_html(body))
    return body


@router.get("/processes")
def list_processes(request: Request) -> dict[str, Any]:
    """List exposed OGC processes."""
    return {
        "processes": [
            {
                "id": _PROCESS_ID,
                "title": _PROCESS_TITLE,
                "description": "Execute the generic DHIS2 EO workflow and persist a native job record.",
                "jobControlOptions": ["sync-execute", "async-execute"],
                "outputTransmission": ["value", "reference"],
                "links": [
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": str(request.url_for("describe_ogc_process", process_id=_PROCESS_ID)),
                    }
                ],
            }
        ]
    }


@router.get("/processes/{process_id}", name="describe_ogc_process")
def describe_process(process_id: str, request: Request) -> dict[str, Any]:
    """Describe the single exposed generic workflow process."""
    _require_process(process_id)
    return {
        "id": _PROCESS_ID,
        "title": _PROCESS_TITLE,
        "description": "OGC-facing adapter over the reusable native workflow engine.",
        "jobControlOptions": ["sync-execute", "async-execute"],
        "outputTransmission": ["value", "reference"],
        "links": [
            {
                "rel": "execute",
                "type": "application/json",
                "href": str(request.url_for("execute_ogc_process", process_id=_PROCESS_ID)),
            }
        ],
    }


@router.post("/processes/{process_id}/execution", name="execute_ogc_process")
def execute_process(
    process_id: str,
    payload: WorkflowExecuteEnvelopeRequest,
    request: Request,
    response: Response,
    background_tasks: BackgroundTasks,
    prefer: str | None = Header(default=None),
) -> dict[str, Any]:
    """Execute the generic workflow synchronously or submit it asynchronously."""
    _require_process(process_id)
    normalized, _warnings = normalize_simple_request(payload.request)

    if prefer is not None and "respond-async" in prefer.lower():
        job_id = str(uuid.uuid4())
        workflow = load_workflow_definition(payload.request.workflow_id)
        initialize_job(
            job_id=job_id,
            request=normalized,
            request_payload=payload.request.model_dump(exclude_none=True),
            workflow=workflow,
            workflow_definition_source="catalog",
            workflow_id=payload.request.workflow_id,
            workflow_version=workflow.version,
            status=WorkflowJobStatus.ACCEPTED,
            process_id=_PROCESS_ID,
        )
        background_tasks.add_task(
            _run_async_workflow_job,
            job_id,
            normalized,
            payload.request.workflow_id,
            payload.request.model_dump(exclude_none=True),
            payload.request.include_component_run_details,
        )
        job_url = str(request.url_for("get_ogc_job", job_id=job_id))
        results_url = str(request.url_for("get_ogc_job_results", job_id=job_id))
        response.status_code = 202
        response.headers["Location"] = job_url
        return {
            "jobID": job_id,
            "status": WorkflowJobStatus.ACCEPTED,
            "location": job_url,
            "jobUrl": job_url,
            "resultsUrl": results_url,
        }

    result = execute_workflow(
        normalized,
        workflow_id=payload.request.workflow_id,
        request_params=payload.request.model_dump(exclude_none=True),
        include_component_run_details=payload.request.include_component_run_details,
        workflow_definition_source="catalog",
    )
    job_url = str(request.url_for("get_ogc_job", job_id=result.run_id))
    results_url = str(request.url_for("get_ogc_job_results", job_id=result.run_id))
    publication = get_published_resource(f"workflow-output-{result.run_id}")
    links: list[dict[str, Any]] = [
        {"rel": "monitor", "type": "application/json", "href": job_url},
        {"rel": "results", "type": "application/json", "href": results_url},
    ]
    if publication is not None and publication.exposure == PublishedResourceExposure.OGC:
        links.append(
            {
                "rel": "collection",
                "type": "application/json",
                "href": _collection_href(request, collection_id_for_resource(publication)),
            }
        )
    return {
        "jobID": result.run_id,
        "processID": _PROCESS_ID,
        "status": WorkflowJobStatus.SUCCESSFUL,
        "outputs": result.model_dump(mode="json"),
        "links": links,
    }


@router.get("/jobs")
def list_ogc_jobs(process_id: str | None = None) -> dict[str, Any]:
    """List OGC-visible jobs backed by the native job store."""
    jobs = list_jobs(process_id=process_id, status=None)
    return {"jobs": [job.model_dump(mode="json") for job in jobs]}


@router.get("/jobs/{job_id}", name="get_ogc_job")
def get_ogc_job(job_id: str, request: Request) -> dict[str, Any]:
    """Fetch one OGC job view from the native job store."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="job_not_found",
                error_code="JOB_NOT_FOUND",
                message=f"Unknown job_id '{job_id}'",
                job_id=job_id,
            ),
        )
    publication = get_published_resource(f"workflow-output-{job.job_id}")
    links: list[dict[str, Any]] = [
        {
            "rel": "self",
            "type": "application/json",
            "href": str(request.url_for("get_ogc_job", job_id=job.job_id)),
        },
        {
            "rel": "results",
            "type": "application/json",
            "href": str(request.url_for("get_ogc_job_results", job_id=job.job_id)),
        },
    ]
    if publication is not None and publication.exposure == PublishedResourceExposure.OGC:
        links.append(
            {
                "rel": "collection",
                "type": "application/json",
                "href": _collection_href(request, collection_id_for_resource(publication)),
            }
        )
    return {
        "jobID": job.job_id,
        "processID": job.process_id,
        "status": job.status,
        "created": job.created_at,
        "updated": job.updated_at,
        "links": links,
    }


@router.get("/jobs/{job_id}/results", name="get_ogc_job_results")
def get_ogc_job_results(job_id: str) -> dict[str, Any]:
    """Return persisted results for a completed OGC job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="job_not_found",
                error_code="JOB_NOT_FOUND",
                message=f"Unknown job_id '{job_id}'",
                job_id=job_id,
            ),
        )
    result = get_job_result(job_id)
    if result is None:
        raise HTTPException(
            status_code=409,
            detail=api_error(
                error="job_result_unavailable",
                error_code="JOB_RESULT_UNAVAILABLE",
                message=f"Result is not available for job '{job_id}'",
                job_id=job_id,
                status=str(job.status),
            ),
        )
    return result


def _require_process(process_id: str) -> None:
    if process_id != _PROCESS_ID:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="process_not_found",
                error_code="PROCESS_NOT_FOUND",
                message=f"Unknown process_id '{process_id}'",
                process_id=process_id,
            ),
        )


def _run_async_workflow_job(
    job_id: str,
    normalized_request: Any,
    workflow_id: str,
    request_params: dict[str, Any],
    include_component_run_details: bool,
) -> None:
    try:
        execute_workflow(
            normalized_request,
            workflow_id=workflow_id,
            request_params=request_params,
            include_component_run_details=include_component_run_details,
            run_id=job_id,
        )
    except HTTPException:
        return


def _collection_href(request: Request, collection_id: str) -> str:
    return str(request.base_url).rstrip("/") + f"/ogcapi/collections/{collection_id}"


def _request_href(request: Request, **updates: Any) -> str:
    params = dict(request.query_params)
    for key, value in updates.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = str(value)
    query = "&".join(f"{key}={value}" for key, value in params.items())
    suffix = f"?{query}" if query else ""
    return f"{request.url.path}{suffix}"


def _wants_html(request: Request, f: str | None) -> bool:
    if f is not None:
        return f.lower() == "html"
    accept = request.headers.get("accept", "")
    return "text/html" in accept and "application/json" not in accept


def _render_ogc_root_html(body: dict[str, Any]) -> str:
    nav_cards = "".join(
        (
            '<a class="nav-card" href="{href}">'
            '<div class="nav-card__kicker">Open</div>'
            '<strong>{title}</strong>'
            '<span>{description}</span>'
            '<div class="nav-card__cta">Go</div>'
            "</a>"
        ).format(
            href=escape(item["href"]),
            title=escape(item["title"]),
            description=escape(item["description"]),
        )
        for item in body.get("navigation", [])
    )
    link_items = "".join(
        (
            '<a class="link-row" href="{href}">'
            '<strong>{rel}</strong>'
            '<span>{type}</span>'
            "</a>"
        ).format(
            href=escape(link["href"]),
            rel=escape(link["rel"]),
            type=escape(link["type"]),
        )
        for link in body["links"]
    )
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(body["title"])}</title>
    <style>
      :root {{
        --bg: #f6f2e9;
        --panel: rgba(255, 255, 255, 0.88);
        --ink: #142033;
        --muted: #5d6a7a;
        --accent: #9f4d1d;
        --line: rgba(20, 32, 51, 0.12);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(200, 120, 58, 0.18), transparent 28%),
          linear-gradient(180deg, #fbf8f2 0%, var(--bg) 100%);
      }}
      main {{
        max-width: 1040px;
        margin: 0 auto;
        padding: 36px 22px 56px;
      }}
      .eyebrow {{
        display: inline-block;
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(159, 77, 29, 0.08);
        color: var(--accent);
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}
      h1 {{
        margin: 16px 0 8px;
        font-size: clamp(2.2rem, 5vw, 4.4rem);
        line-height: 0.98;
        letter-spacing: -0.05em;
      }}
      p {{
        max-width: 760px;
        color: var(--muted);
        font-size: 1.05rem;
      }}
      .panel {{
        margin-top: 28px;
        padding: 22px 24px;
        border: 1px solid var(--line);
        border-radius: 24px;
        background: var(--panel);
        box-shadow: 0 22px 60px rgba(20, 32, 51, 0.08);
      }}
      .nav-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 14px;
        margin-top: 28px;
      }}
      .nav-card {{
        display: block;
        min-height: 132px;
        padding: 18px;
        border: 1px solid var(--line);
        border-radius: 20px;
        background:
          linear-gradient(180deg, rgba(255, 255, 255, 0.95) 0%, rgba(246, 242, 233, 0.9) 100%);
        box-shadow: 0 16px 34px rgba(20, 32, 51, 0.05);
        transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease;
      }}
      .nav-card:hover {{
        transform: translateY(-1px);
        border-color: rgba(159, 77, 29, 0.28);
        box-shadow: 0 20px 40px rgba(20, 32, 51, 0.08);
      }}
      .nav-card__kicker {{
        color: var(--accent);
        font-size: 0.74rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 10px;
      }}
      .nav-card strong {{
        display: block;
        margin-bottom: 8px;
        font-size: 1.08rem;
        line-height: 1.15;
      }}
      .nav-card span {{
        display: block;
        padding-right: 18px;
      }}
      .nav-card__cta {{
        margin-top: 18px;
        color: var(--accent);
        font-weight: 700;
      }}
      .section-title {{
        margin: 0 0 14px;
        font-size: 0.95rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        color: var(--muted);
      }}
      .link-list {{
        display: grid;
        gap: 10px;
      }}
      a {{
        color: var(--ink);
        font-weight: 700;
        text-decoration: none;
      }}
      span {{
        color: var(--muted);
        font-size: 0.95rem;
      }}
      .link-row {{
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 16px;
        padding: 12px 14px;
        border-radius: 14px;
        background: rgba(255, 255, 255, 0.68);
        border: 1px solid rgba(20, 32, 51, 0.08);
      }}
      .link-row strong {{
        font-size: 0.98rem;
      }}
      .link-row span {{
        white-space: nowrap;
        text-align: right;
      }}
      @media (max-width: 720px) {{
        .link-row {{
          flex-direction: column;
          align-items: flex-start;
        }}
        .link-row span {{
          white-space: normal;
          text-align: left;
        }}
      }}
    </style>
  </head>
  <body>
    <main>
      <div class="eyebrow">OGC API</div>
      <h1>{escape(body["title"])}</h1>
      <p>{escape(body["description"])}</p>
      <section class="nav-grid">{nav_cards}</section>
      <section class="panel">
        <h2 class="section-title">API Links</h2>
        <div class="link-list">{link_items}</div>
      </section>
    </main>
  </body>
</html>"""
