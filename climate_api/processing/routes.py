"""Routes for derived processing operations."""

import pandas as pd
from fastapi import APIRouter, HTTPException

from climate_api.processing import services
from climate_api.processing.schemas import (
    _SUPPORTED_RESAMPLE_METHODS,
    ResampleProcessRequest,
    ResampleProcessResponse,
)

router = APIRouter()


@router.post("/{process_id}/execution", response_model=ResampleProcessResponse)
def run_process_execution(process_id: str, request: ResampleProcessRequest) -> ResampleProcessResponse:
    """Materialize a derived dataset by executing the named process."""
    if process_id != "resample":
        raise HTTPException(status_code=404, detail=f"Unknown process '{process_id}'")
    if request.method not in _SUPPORTED_RESAMPLE_METHODS:
        supported = ", ".join(sorted(_SUPPORTED_RESAMPLE_METHODS))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported method '{request.method}'. Supported: {supported}",
        )
    try:
        _offset = pd.tseries.frequencies.to_offset(request.frequency)
    except ValueError:
        _offset = None
    if _offset is None:
        raise HTTPException(status_code=400, detail=f"Invalid frequency '{request.frequency}'")
    artifact_id, dataset_summary = services.run_resample_process(
        source_dataset_id=request.source_dataset_id,
        frequency=request.frequency,
        method=request.method,
        start=request.start,
        end=request.end,
        extent_id=request.extent_id,
        overwrite=request.overwrite,
        publish=request.publish,
    )
    return ResampleProcessResponse(artifact_id=artifact_id, status="completed", dataset=dataset_summary)
