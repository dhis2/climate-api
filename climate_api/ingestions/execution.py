"""Execution function for the ingest process (used by the async jobs framework)."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from climate_api.data_registry.services import datasets as registry_datasets
from climate_api.extents.services import get_extent_or_404
from climate_api.ingestions import services


def execute_ingest(
    *,
    dataset_id: str,
    start: str,
    end: str | None = None,
    overwrite: bool = False,
    publish: bool = True,
    on_progress: Any | None = None,
    is_cancel_requested: Any | None = None,
    save_cursor: Any | None = None,
    load_cursor: Any | None = None,
) -> dict[str, Any]:
    """Ingest one dataset for the configured extent and return a result summary.

    Accepts optional job-framework callbacks (on_progress, is_cancel_requested,
    save_cursor, load_cursor) so that progress is visible when run as an async job.
    """
    dataset = registry_datasets.get_dataset(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    extent = get_extent_or_404()
    resolved_bbox = list(extent["bbox"])
    artifact = services.create_artifact(
        dataset=dataset,
        start=start,
        end=end,
        bbox=resolved_bbox,
        overwrite=overwrite,
        publish=publish,
        on_progress=on_progress,
        is_cancel_requested=is_cancel_requested,
        save_cursor=save_cursor,
        load_cursor=load_cursor,
    )
    return {
        "status": "completed",
        "ingestion_id": artifact.artifact_id,
    }
