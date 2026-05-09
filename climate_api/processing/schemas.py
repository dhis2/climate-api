"""Pydantic schemas for derived data processing routes."""

from typing import Literal

from pydantic import BaseModel, Field

from climate_api.ingestions.schemas import DatasetRecord


class ResampleProcessRequest(BaseModel):
    """Request payload for materializing a derived resampled dataset."""

    dataset_id: str = Field(description="Target derived dataset template id from the Climate API registry.")
    start: str = Field(description="Start period to materialize in the target dataset period format.")
    end: str | None = Field(default=None, description="Optional end period to materialize.")
    extent_id: str | None = Field(
        default=None,
        description="Configured Climate API extent identifier used to resolve spatial scope for the source dataset.",
    )
    overwrite: bool = Field(
        default=False,
        description="Whether to force rematerialization of an existing matching derived artifact.",
    )
    publish: bool = Field(
        default=True,
        description="Whether to publish the resulting derived dataset through pygeoapi.",
    )


class ResampleProcessResponse(BaseModel):
    """Synchronous response returned after materializing a derived resampled dataset."""

    artifact_id: str = Field(description="Identifier of the materialized derived artifact.")
    status: Literal["completed"] = Field(description="Execution status of the resample request.")
    dataset: DatasetRecord = Field(description="Managed dataset summary produced or resolved by the resample request.")
