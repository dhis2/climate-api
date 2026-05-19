"""FastAPI router exposing dataset endpoints."""

from fastapi import APIRouter, BackgroundTasks

from ..data_registry.routes import _get_dataset_or_404
from .services import downloader

router = APIRouter()


@router.get(
    "/{dataset_id}/build_zarr",
    response_model=dict,
    summary="Internal dataset Zarr build",
    include_in_schema=False,
)
def build_dataset_zarr(
    dataset_id: str,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    """Internal low-level cache optimization route kept for compatibility."""
    dataset = _get_dataset_or_404(dataset_id)
    background_tasks.add_task(downloader.build_dataset_zarr, dataset)
    return {"status": "Building zarr file from dataset downloads"}
