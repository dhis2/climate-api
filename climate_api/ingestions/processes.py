"""Process execution wrappers for ingestion operations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import HTTPException

from climate_api.data_registry.services import datasets as registry_datasets
from climate_api.extents.services import get_extent_or_404
from climate_api.ingestions import services
from climate_api.ingestions.schemas import IngestionResponse


def execute_ingestion(
    *,
    dataset_id: str,
    start: str,
    end: str | None = None,
    overwrite: bool = False,
    prefer_zarr: bool = True,
    publish: bool = True,
    on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    save_cursor: Callable[[dict[str, Any]], None] | None = None,
    load_cursor: Callable[[], dict[str, Any] | None] | None = None,
) -> IngestionResponse:
    """Execute one managed-dataset ingestion using the configured extent."""
    dataset = registry_datasets.get_dataset(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    extent = get_extent_or_404()
    artifact = services.create_artifact(
        dataset=dataset,
        start=start,
        end=end,
        bbox=list(extent["bbox"]),
        country_code=extent.get("country_code"),
        overwrite=overwrite,
        prefer_zarr=prefer_zarr,
        publish=publish,
        on_progress=on_progress,
        is_cancel_requested=is_cancel_requested,
        save_cursor=save_cursor,
    )
    return services.get_ingestion_or_404(artifact.artifact_id)
