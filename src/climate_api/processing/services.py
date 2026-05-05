"""Services for derived processing workflows."""

from __future__ import annotations

from climate_api.extents.services import get_extent_or_404
from climate_api.ingestions import services as ingestion_services
from climate_api.ingestions.schemas import DatasetRecord
from climate_api.processing.resample import materialize_resampled_artifact


def run_resample_process(
    *,
    dataset: dict[str, object],
    start: str,
    end: str | None,
    extent_id: str | None,
    overwrite: bool,
    publish: bool,
) -> tuple[str, DatasetRecord]:
    """Materialize one derived resampled dataset and return its artifact id plus dataset summary."""
    resolved_bbox: list[float] | None = None
    if extent_id is not None:
        extent = get_extent_or_404(extent_id)
        bbox = extent.get("bbox")
        resolved_bbox = list(bbox) if isinstance(bbox, list) else None

    artifact = materialize_resampled_artifact(
        target_dataset=dataset,
        start=start,
        end=end,
        extent_id=extent_id,
        bbox=resolved_bbox,
        overwrite=overwrite,
        publish=publish,
    )
    return artifact.artifact_id, ingestion_services.get_dataset_summary_for_artifact_or_404(artifact.artifact_id)
