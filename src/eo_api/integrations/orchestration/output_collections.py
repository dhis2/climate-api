"""Runtime helpers for publishing workflow outputs to OGC collection backends."""

from __future__ import annotations

import fcntl
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_PREVIEW_COLLECTION_ID = "generic-dhis2-datavalue-preview"
_PREVIEW_COLLECTION_PATH = Path(
    os.getenv("GENERIC_DHIS2_DATAVALUE_PREVIEW_PATH", "/tmp/generic_dhis2_datavalue_preview.geojson")
)


def _empty_feature_collection() -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": []}


def ensure_output_collections_seeded() -> Path:
    """Ensure the generic output collection file exists for OGC collection serving."""
    _PREVIEW_COLLECTION_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not _PREVIEW_COLLECTION_PATH.exists():
        _PREVIEW_COLLECTION_PATH.write_text(json.dumps(_empty_feature_collection()), encoding="utf-8")
    return _PREVIEW_COLLECTION_PATH


def publish_dhis2_datavalue_preview(
    *,
    dataset_type: str,
    rows: list[dict[str, Any]],
    job_id: str | None = None,
) -> dict[str, Any]:
    """Append DHIS2 dataValue preview rows to the output collection backend."""
    ensure_output_collections_seeded()

    effective_job_id = job_id or uuid.uuid4().hex
    published_at = datetime.now(UTC).isoformat()
    appended_features = []
    for idx, row in enumerate(rows):
        properties = dict(row)
        properties["dataset_type"] = dataset_type
        properties["job_id"] = effective_job_id
        properties["published_at"] = published_at
        appended_features.append(
            {
                "type": "Feature",
                "id": f"{effective_job_id}-{idx}",
                "geometry": None,
                "properties": properties,
            }
        )

    # Lock during read-modify-write to avoid clobbering concurrent runs.
    with _PREVIEW_COLLECTION_PATH.open("r+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            payload = json.load(handle)
            features = payload.get("features", [])
            if not isinstance(features, list):
                features = []
            features.extend(appended_features)
            payload = {"type": "FeatureCollection", "features": features}
            handle.seek(0)
            json.dump(payload, handle)
            handle.truncate()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    return {
        "collection_id": _PREVIEW_COLLECTION_ID,
        "path": str(_PREVIEW_COLLECTION_PATH),
        "job_id": effective_job_id,
        "item_count": len(appended_features),
        "total_item_count": len(payload["features"]),
    }
