"""Routes for derived processing operations."""

from fastapi import APIRouter, HTTPException

from climate_api.processing import services
from climate_api.processing.schemas import (
    _SUPPORTED_PERIOD_TYPES,
    _SUPPORTED_RESAMPLE_METHODS,
    _WEEK_STARTS,
    ResampleProcessRequest,
    ResampleProcessResponse,
)

router = APIRouter()


@router.post("/{process_id}/execution", response_model=ResampleProcessResponse)
def run_process_execution(process_id: str, request: ResampleProcessRequest) -> ResampleProcessResponse:
    """Materialize a derived dataset by executing the named process."""
    if process_id != "resample":
        raise HTTPException(status_code=404, detail=f"Unknown process '{process_id}'")
    if request.period_type not in _SUPPORTED_PERIOD_TYPES:
        supported = ", ".join(sorted(_SUPPORTED_PERIOD_TYPES))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported period_type '{request.period_type}'. Supported: {supported}",
        )
    if request.method not in _SUPPORTED_RESAMPLE_METHODS:
        supported = ", ".join(sorted(_SUPPORTED_RESAMPLE_METHODS))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported method '{request.method}'. Supported: {supported}",
        )
    if request.week_start not in _WEEK_STARTS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported week_start '{request.week_start}'. Supported: {', '.join(sorted(_WEEK_STARTS))}",
        )
    artifact_id, dataset_summary = services.run_resample_process(
        source_dataset_id=request.source_dataset_id,
        period_type=request.period_type,
        method=request.method,
        week_start=request.week_start,
        start=request.start,
        end=request.end,
        extent_id=request.extent_id,
        overwrite=request.overwrite,
        publish=request.publish,
    )
    return ResampleProcessResponse(artifact_id=artifact_id, status="completed", dataset=dataset_summary)
