"""Services for derived processing workflows."""

from __future__ import annotations

from climate_api.ingestions import services as ingestion_services
from climate_api.ingestions.schemas import DatasetRecord
from climate_api.processing.resample import materialize_resampled_artifact


def run_resample_process(
    *,
    source_dataset_id: str,
    frequency: str,
    method: str,
    start: str,
    end: str | None,
    extent_id: str | None,
    overwrite: bool,
    publish: bool,
) -> tuple[str, DatasetRecord]:
    """Materialize one derived resampled dataset and return its artifact id plus dataset summary."""
    artifact = materialize_resampled_artifact(
        source_dataset_id=source_dataset_id,
        frequency=frequency,
        method=method,
        start=start,
        end=end,
        extent_id=extent_id,
        bbox=None,
        overwrite=overwrite,
        publish=publish,
    )
    return artifact.artifact_id, ingestion_services.get_dataset_summary_for_artifact_or_404(artifact.artifact_id)
