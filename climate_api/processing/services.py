"""Services for derived processing workflows."""

from __future__ import annotations

from typing import Any

import pandas as pd
from fastapi import HTTPException

from climate_api.ingestions import services as ingestion_services
from climate_api.ingestions.schemas import DatasetRecord
from climate_api.processing.resample import materialize_resampled_artifact

_SUPPORTED_RESAMPLE_METHODS: frozenset[str] = frozenset({"mean", "sum", "min", "max"})


def execute_resample(
    *,
    source_dataset_id: str,
    frequency: str,
    method: str,
    start: str,
    end: str | None = None,
    overwrite: bool = False,
    publish: bool = True,
    **_ignored: Any,
) -> dict[str, Any]:
    """Validate and execute the resample process; return a JSON-serializable response dict.

    This is the execution_function registered in the process YAML. The route dispatches
    here by calling it with the raw request body as keyword arguments.
    """
    if method not in _SUPPORTED_RESAMPLE_METHODS:
        supported = ", ".join(sorted(_SUPPORTED_RESAMPLE_METHODS))
        raise HTTPException(status_code=400, detail=f"Unsupported method '{method}'. Supported: {supported}")
    try:
        _offset = pd.tseries.frequencies.to_offset(frequency)
    except ValueError:
        _offset = None
    if _offset is None:
        raise HTTPException(status_code=400, detail=f"Invalid frequency '{frequency}'")

    artifact_id, dataset_summary = run_resample_process(
        source_dataset_id=source_dataset_id,
        frequency=frequency,
        method=method,
        start=start,
        end=end,
        overwrite=overwrite,
        publish=publish,
    )
    return {
        "artifact_id": artifact_id,
        "status": "completed",
        "dataset": dataset_summary.model_dump() if hasattr(dataset_summary, "model_dump") else dataset_summary,
    }


def run_resample_process(
    *,
    source_dataset_id: str,
    frequency: str,
    method: str,
    start: str,
    end: str | None,
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
        overwrite=overwrite,
        publish=publish,
    )
    return artifact.artifact_id, ingestion_services.get_dataset_summary_for_artifact_or_404(artifact.artifact_id)
