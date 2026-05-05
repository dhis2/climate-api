"""Routes for derived processing operations."""

from fastapi import APIRouter

from climate_api.data_registry.routes import _get_dataset_or_404
from climate_api.processing import services
from climate_api.processing.schemas import ResampleProcessRequest, ResampleProcessResponse

router = APIRouter()


@router.post("/resample", response_model=ResampleProcessResponse)
def run_resample_process(request: ResampleProcessRequest) -> ResampleProcessResponse:
    """Materialize a derived dataset by resampling an existing managed source dataset."""
    dataset = _get_dataset_or_404(request.dataset_id)
    artifact_id, dataset_summary = services.run_resample_process(
        dataset=dataset,
        start=request.start,
        end=request.end,
        extent_id=request.extent_id,
        overwrite=request.overwrite,
        publish=request.publish,
    )
    return ResampleProcessResponse(artifact_id=artifact_id, status="completed", dataset=dataset_summary)
