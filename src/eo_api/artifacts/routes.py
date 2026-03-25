"""Routes for EO artifact creation and publication."""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from eo_api.artifacts import services
from eo_api.artifacts.schemas import ArtifactListResponse, ArtifactRecord, CreateDownloadRequest, DownloadResponse
from eo_api.data_registry.routes import _get_dataset_or_404

router = APIRouter()


@router.post("/downloads", response_model=DownloadResponse)
def create_download(request: CreateDownloadRequest) -> DownloadResponse:
    """Create a new downloaded artifact from a dataset request."""
    dataset = _get_dataset_or_404(request.dataset_id)
    artifact = services.create_artifact(
        dataset=dataset,
        start=request.start,
        end=request.end,
        bbox=list(request.bbox) if request.bbox is not None else None,
        country_code=request.country_code,
        overwrite=request.overwrite,
        prefer_zarr=request.prefer_zarr,
        publish=request.publish,
    )
    return DownloadResponse(download_id=artifact.artifact_id, status="completed", artifact=artifact)


@router.get("/downloads/{download_id}", response_model=ArtifactRecord)
def get_download(download_id: str) -> ArtifactRecord:
    """Return the artifact record created for a given download."""
    return services.get_artifact_or_404(download_id)


@router.get("/artifacts", response_model=ArtifactListResponse)
def list_artifacts() -> ArtifactListResponse:
    """List stored artifacts."""
    return services.list_artifacts()


@router.get("/artifacts/{artifact_id}", response_model=ArtifactRecord)
def get_artifact(artifact_id: str) -> ArtifactRecord:
    """Get stored artifact metadata."""
    return services.get_artifact_or_404(artifact_id)


@router.get("/artifacts/{artifact_id}/download")
def download_artifact_file(artifact_id: str) -> FileResponse:
    """Download the primary saved file for an artifact when available."""
    artifact = services.get_artifact_or_404(artifact_id)
    if artifact.path is None or artifact.format.value == "zarr":
        raise HTTPException(
            status_code=409,
            detail="Artifact is not a single downloadable file; use metadata and asset_paths instead",
        )

    media_type = "application/x-netcdf"
    filename = f"{artifact.dataset_id}.nc"
    return FileResponse(artifact.path, media_type=media_type, filename=filename)


@router.post("/artifacts/{artifact_id}/publish", response_model=ArtifactRecord)
def publish_artifact(artifact_id: str) -> ArtifactRecord:
    """Publish an artifact as a pygeoapi collection."""
    return services.publish_artifact_record(artifact_id)


@router.get("/artifacts/{artifact_id}/publication", response_model=ArtifactRecord)
def get_artifact_publication(artifact_id: str) -> ArtifactRecord:
    """Return artifact metadata including publication state."""
    return services.get_artifact_or_404(artifact_id)
