"""Routes for derived processing operations."""

from typing import Any

from fastapi import APIRouter, Body, HTTPException

from climate_api.data_registry.services import processes as process_registry
from climate_api.processing.schemas import ProcessDetail, ProcessField, ProcessLink, ProcessListResponse, ProcessSummary

router = APIRouter()


def _process_links(process_id: str) -> list[ProcessLink]:
    return [
        ProcessLink(href=f"/processes/{process_id}", rel="self", title="Process detail"),
        ProcessLink(href=f"/processes/{process_id}/execution", rel="execute", title="Execute process"),
    ]


def _catalog_links() -> list[ProcessLink]:
    return [ProcessLink(href="/processes", rel="self", title="Processes")]


def _field_map(raw: object) -> dict[str, ProcessField]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, ProcessField] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, dict):
            raw_enum = value.get("enum")
            result[key] = ProcessField(
                type=value.get("type") if isinstance(value.get("type"), str) else None,
                required=value.get("required") if isinstance(value.get("required"), bool) else None,
                description=value.get("description") if isinstance(value.get("description"), str) else None,
                enum=[item for item in raw_enum if isinstance(item, str)] if isinstance(raw_enum, list) else None,
            )
        else:
            result[key] = ProcessField()
    return result


def _public_process_summary(process: dict[str, Any]) -> ProcessSummary:
    process_id = str(process["id"])
    keywords = process.get("keywords")
    job_control_options = process.get("jobControlOptions")
    return ProcessSummary(
        id=process_id,
        title=str(process["title"]),
        description=str(process["description"]) if isinstance(process.get("description"), str) else None,
        version=str(process["version"]) if isinstance(process.get("version"), str) else None,
        keywords=[k for k in keywords if isinstance(k, str)] if isinstance(keywords, list) else [],
        jobControlOptions=[value for value in job_control_options if isinstance(value, str)]
        if isinstance(job_control_options, list)
        else [],
        links=_process_links(process_id),
    )


def _public_process_detail(process: dict[str, Any]) -> ProcessDetail:
    summary = _public_process_summary(process)
    return ProcessDetail(
        **summary.model_dump(),
        inputs=_field_map(process.get("inputs")),
        outputs=_field_map(process.get("outputs")),
    )


@router.get("", response_model=ProcessListResponse)
def list_processes() -> ProcessListResponse:
    """Return the registered native process catalog."""
    visible = [process for process in process_registry.list_processes() if process.get("expose", True) is not False]
    return ProcessListResponse(
        processes=[_public_process_summary(process) for process in visible],
        links=_catalog_links(),
    )


@router.get("/{process_id}", response_model=ProcessDetail)
def get_process(process_id: str) -> ProcessDetail:
    """Return one registered process description."""
    process = process_registry.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail=f"Unknown process '{process_id}'")
    return _public_process_detail(process)


@router.post("/{process_id}/execution")
def run_process_execution(
    process_id: str,
    request: dict[str, Any] = Body(...),
) -> Any:
    """Dispatch to a registered process execution function by process id."""
    process = process_registry.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail=f"Unknown process '{process_id}'")
    func = process_registry._get_dynamic_function(process["execution"]["function"])
    try:
        return func(**request)
    except TypeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
