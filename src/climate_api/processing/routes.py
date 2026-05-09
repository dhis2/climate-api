"""Routes for derived processing operations."""

from fastapi import APIRouter, HTTPException

from climate_api.data_registry.services import datasets as registry_datasets
from climate_api.processing import services
from climate_api.processing.schemas import ResampleProcessRequest, ResampleProcessResponse

router = APIRouter()


@router.post("/{process_id}/execution", response_model=ResampleProcessResponse)
def run_process_execution(process_id: str, request: ResampleProcessRequest) -> ResampleProcessResponse:
    """Materialize a derived dataset by executing the named process."""
    dataset = registry_datasets.get_dataset(request.dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset template '{request.dataset_id}' not found")
    if dataset.get("sync_kind") != "derived":
        raise HTTPException(
            status_code=400,
            detail=f"Dataset '{request.dataset_id}' is not a derived dataset and cannot be processed here",
        )
    if process_id != "resample":
        raise HTTPException(status_code=404, detail=f"Unknown process '{process_id}'")
    artifact_id, dataset_summary = services.run_resample_process(
        dataset=dataset,
        start=request.start,
        end=request.end,
        extent_id=request.extent_id,
        overwrite=request.overwrite,
        publish=request.publish,
    )
    return ResampleProcessResponse(artifact_id=artifact_id, status="completed", dataset=dataset_summary)
