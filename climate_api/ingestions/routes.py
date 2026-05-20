"""Routes for EO ingestion, datasets, and sync operations."""

from typing import Any

from fastapi import APIRouter, Header
from starlette.responses import Response

from climate_api.data_registry.routes import _get_dataset_or_404
from climate_api.extents.services import get_extent_or_404
from climate_api.ingestions import services
from climate_api.ingestions.schemas import (
    CreateIngestionRequest,
    DatasetDetailRecord,
    DatasetListResponse,
    IngestionListResponse,
    IngestionResponse,
    SyncDatasetRequest,
    SyncDetail,
    SyncResponse,
)

ingestions_router = APIRouter()
datasets_router = APIRouter()
zarr_router = APIRouter()
sync_router = APIRouter()


def _prefer_respond_async(prefer: str | None) -> bool:
    if prefer is None:
        return False
    directives = [item.strip().split(";", 1)[0].strip().lower() for item in prefer.split(",")]
    return "respond-async" in directives


@ingestions_router.post("", response_model=None)
def create_ingestion(
    request: CreateIngestionRequest,
    response: Response,
    prefer: str | None = Header(default=None),
) -> Any:
    """Create or update a managed dataset from a dataset template and configured extent.

    Send ``Prefer: respond-async`` to run the ingest as a background job and
    receive HTTP 202 with a ``Location: /jobs/{job_id}`` header immediately.
    Poll ``GET /jobs/{job_id}`` for progress and completion status.
    """
    if _prefer_respond_async(prefer):
        from climate_api.jobs.service import get_job_service

        job = get_job_service().submit_process_job(
            process_id="ingest",
            request={
                "dataset_id": request.dataset_id,
                "start": request.start,
                "end": request.end,
                "overwrite": request.overwrite,
                "publish": request.publish,
            },
        )
        response.status_code = 202
        response.headers["Location"] = f"/jobs/{job.job_id}"
        return job

    dataset = _get_dataset_or_404(request.dataset_id)
    extent = get_extent_or_404()
    resolved_bbox = list(extent["bbox"])
    artifact = services.create_artifact(
        dataset=dataset,
        start=request.start,
        end=request.end,
        bbox=resolved_bbox,
        overwrite=request.overwrite,
        publish=request.publish,
    )
    return IngestionResponse(
        ingestion_id=artifact.artifact_id,
        status="completed",
        dataset=services.get_dataset_summary_for_artifact_or_404(artifact.artifact_id),
    )


@ingestions_router.get("", response_model=IngestionListResponse)
def list_ingestions() -> IngestionListResponse:
    """List ingestion run records for operational and admin use."""
    return services.list_ingestions()


@ingestions_router.get("/{ingestion_id}", response_model=IngestionResponse)
def get_ingestion(ingestion_id: str) -> IngestionResponse:
    """Return the managed dataset view created for a given ingestion."""
    return services.get_ingestion_or_404(ingestion_id)


@datasets_router.get("", response_model=DatasetListResponse)
def list_datasets() -> DatasetListResponse:
    """List managed datasets."""
    return services.list_datasets()


@datasets_router.get("/{dataset_id}", response_model=DatasetDetailRecord)
def get_dataset(dataset_id: str) -> DatasetDetailRecord:
    """Get managed dataset metadata and available versions."""
    return services.get_dataset_or_404(dataset_id)



@zarr_router.api_route("/{dataset_id}", methods=["GET", "HEAD"])
def get_canonical_zarr_store_info(dataset_id: str) -> dict[str, object]:
    """Return canonical Zarr store listing for a managed dataset."""
    return services.get_dataset_zarr_store_info_or_404(dataset_id)


@zarr_router.api_route("/{dataset_id}/{relative_path:path}", methods=["GET", "HEAD"], response_model=None)
def get_canonical_zarr_store_file(dataset_id: str, relative_path: str) -> Response | dict[str, object]:
    """Serve canonical Zarr store content for a managed dataset."""
    return services.get_dataset_zarr_store_file_or_404(dataset_id, relative_path)


@sync_router.post("/{dataset_id}", response_model=SyncResponse)
def sync_dataset(dataset_id: str, request: SyncDatasetRequest) -> SyncResponse:
    """Sync a managed dataset forward from its latest available time step."""
    return services.sync_dataset(
        dataset_id=dataset_id,
        end=request.end,
        publish=request.publish,
    )


@sync_router.get("/{dataset_id}/plan", response_model=SyncDetail)
def plan_sync_dataset(dataset_id: str, end: str | None = None) -> SyncDetail:
    """Return the sync plan for a managed dataset without starting a download."""
    return services.plan_sync_dataset(dataset_id=dataset_id, end=end)
