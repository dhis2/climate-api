"""Pydantic schemas for artifact and publication APIs."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class ArtifactFormat(StrEnum):
    """Supported stored artifact formats."""

    ZARR = "zarr"
    NETCDF = "netcdf"


class PublicationStatus(StrEnum):
    """Publication lifecycle states."""

    UNPUBLISHED = "unpublished"
    PUBLISHED = "published"


class CoverageSpatial(BaseModel):
    """Spatial extent summary."""

    xmin: float
    ymin: float
    xmax: float
    ymax: float


class CoverageTemporal(BaseModel):
    """Temporal extent summary."""

    start: str
    end: str


class ArtifactCoverage(BaseModel):
    """Artifact coverage metadata."""

    spatial: CoverageSpatial
    temporal: CoverageTemporal


class ArtifactPublication(BaseModel):
    """Publication metadata for an artifact."""

    status: PublicationStatus = PublicationStatus.UNPUBLISHED
    collection_id: str | None = None
    published_at: datetime | None = None
    pygeoapi_path: str | None = None


class ArtifactRecord(BaseModel):
    """Stored artifact metadata."""

    artifact_id: str
    dataset_id: str
    dataset_name: str
    variable: str
    format: ArtifactFormat
    path: str | None = None
    asset_paths: list[str] = Field(default_factory=list)
    variables: list[str] = Field(default_factory=list)
    coverage: ArtifactCoverage
    created_at: datetime
    publication: ArtifactPublication = Field(default_factory=ArtifactPublication)


class CreateDownloadRequest(BaseModel):
    """Request payload for creating a new EO artifact."""

    dataset_id: str
    start: str
    end: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    country_code: str | None = None
    overwrite: bool = False
    prefer_zarr: bool = True
    publish: bool = True


class DownloadResponse(BaseModel):
    """Response returned after creating a new artifact."""

    download_id: str
    status: str
    artifact: ArtifactRecord


class ArtifactListResponse(BaseModel):
    """Collection response for artifacts."""

    items: list[ArtifactRecord]
