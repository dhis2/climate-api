"""Routes for derived processing operations."""

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Response

from climate_api.data_registry.services import processes as process_registry
def _validate_process_request(process: dict[str, Any], request: dict[str, Any]) -> None:
    _validate_required_process_inputs(process, request)
    if process.get("id") == "resample":
        processing_services.validate_resample_request(**request)


def _supports_async_execution(process: dict[str, Any]) -> bool:
    job_control_options = process.get("jobControlOptions")
    return isinstance(job_control_options, list) and "async-execute" in job_control_options


@router.get("", response_model=ProcessListResponse)
def get_processes_catalog() -> ProcessListResponse:
    """Return the registered native process catalog."""
    visible = process_registry.list_processes()
    return ProcessListResponse(
        processes=[_public_process_summary(process) for process in visible],
        links=_catalog_links(),
    )


@router.get("/{process_id}", response_model=ProcessDetail)
def get_process(process_id: str) -> ProcessDetail:
    """Return one registered process description."""
    process = _get_public_process_or_404(process_id)
    return _public_process_detail(process)


@router.post("/{process_id}/execution")
def run_process_execution(
    process_id: str,
    response: Response,
    request: dict[str, Any] = Body(...),
) -> Any:
    """Dispatch to a registered process execution function synchronously.

    For async execution of custom plugins, use the openEO POST /jobs endpoint
    with a process graph that calls the plugin by its registered process id.
    """
    process = _get_public_process_or_404(process_id)
    _validate_process_request(process, request)
    if _prefer_respond_async(prefer):
        if not _supports_async_execution(process):
            raise HTTPException(
                status_code=409,
                detail=f"Process '{process_id}' does not support async execution",
            )
        job = get_job_service().submit_process_job(process_id=process_id, request=request)
        response.status_code = 202
        response.headers["Location"] = f"/internal/jobs/{job.job_id}"
        return job

    try:
        func = process_registry.get_process_function(process_id)
    except (ImportError, AttributeError, ValueError) as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load execution.function for process '{process_id}': {exc}",
        ) from exc

    try:
        return func(**request)
    except TypeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
