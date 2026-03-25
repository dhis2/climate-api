"""Services for artifact creation, persistence, and publication metadata."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import HTTPException

from eo_api.artifacts.schemas import (
    ArtifactCoverage,
    ArtifactFormat,
    ArtifactListResponse,
    ArtifactPublication,
    ArtifactRecord,
    CoverageSpatial,
    CoverageTemporal,
)
from eo_api.data_accessor.services.accessor import get_data_coverage
from eo_api.data_manager.services import downloader
from eo_api.publications.services import publish_artifact

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"
ARTIFACTS_DIR = DATA_DIR / "artifacts"
ARTIFACTS_INDEX_PATH = ARTIFACTS_DIR / "records.json"


def ensure_store() -> None:
    """Create the artifact metadata store if it does not exist."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    if not ARTIFACTS_INDEX_PATH.exists():
        ARTIFACTS_INDEX_PATH.write_text("[]\n", encoding="utf-8")


def list_artifacts() -> ArtifactListResponse:
    """Return all stored artifacts."""
    return ArtifactListResponse(items=_load_records())


def get_artifact_or_404(artifact_id: str) -> ArtifactRecord:
    """Return a single artifact or raise 404."""
    for record in _load_records():
        if record.artifact_id == artifact_id:
            return record
    raise HTTPException(status_code=404, detail=f"Artifact '{artifact_id}' not found")


def create_artifact(
    *,
    dataset: dict[str, object],
    start: str,
    end: str | None,
    bbox: list[float] | None,
    country_code: str | None,
    overwrite: bool,
    prefer_zarr: bool,
    publish: bool,
) -> ArtifactRecord:
    """Download a dataset, persist it locally, and store artifact metadata."""
    downloader.download_dataset(
        dataset,
        start=start,
        end=end,
        bbox=bbox,
        country_code=country_code,
        overwrite=overwrite,
        background_tasks=None,
    )

    if prefer_zarr:
        try:
            downloader.build_dataset_zarr(dataset)
        except Exception:
            # Fall back to NetCDF when Zarr materialization is not viable.
            pass

    zarr_path = downloader.get_zarr_path(dataset)
    cache_files = downloader.get_cache_files(dataset)
    primary_path: str | None

    if zarr_path is not None:
        artifact_format = ArtifactFormat.ZARR
        primary_path = str(zarr_path.resolve())
        asset_paths = [primary_path]
    elif cache_files:
        artifact_format = ArtifactFormat.NETCDF
        asset_paths = [str(path.resolve()) for path in cache_files]
        primary_path = asset_paths[0] if len(asset_paths) == 1 else None
    else:
        raise HTTPException(status_code=500, detail="Download finished without any saved artifact files")

    coverage_data = get_data_coverage(dataset)
    coverage = ArtifactCoverage(
        temporal=CoverageTemporal(**coverage_data["coverage"]["temporal"]),
        spatial=CoverageSpatial(**coverage_data["coverage"]["spatial"]),
    )

    record = ArtifactRecord(
        artifact_id=str(uuid4()),
        dataset_id=str(dataset["id"]),
        dataset_name=str(dataset["name"]),
        variable=str(dataset["variable"]),
        format=artifact_format,
        path=primary_path,
        asset_paths=asset_paths,
        variables=[str(dataset["variable"])],
        coverage=coverage,
        created_at=datetime.now(UTC),
        publication=ArtifactPublication(),
    )
    records = _load_records()
    records.append(record)
    _save_records(records)
    if publish:
        return publish_artifact_record(record.artifact_id)
    return record


def publish_artifact_record(artifact_id: str) -> ArtifactRecord:
    """Publish an artifact via pygeoapi and persist publication metadata."""
    records = _load_records()
    for index, record in enumerate(records):
        if record.artifact_id != artifact_id:
            continue
        published = publish_artifact(record)
        records[index] = published
        _save_records(records)
        return published
    raise HTTPException(status_code=404, detail=f"Artifact '{artifact_id}' not found")


def _load_records() -> list[ArtifactRecord]:
    ensure_store()
    raw = json.loads(ARTIFACTS_INDEX_PATH.read_text(encoding="utf-8"))
    return [ArtifactRecord.model_validate(item) for item in raw]


def _save_records(records: list[ArtifactRecord]) -> None:
    ensure_store()
    payload = [record.model_dump(mode="json") for record in records]
    ARTIFACTS_INDEX_PATH.write_text(f"{json.dumps(payload, indent=2)}\n", encoding="utf-8")
