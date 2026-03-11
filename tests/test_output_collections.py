import json
from pathlib import Path
from typing import Any

import pytest

from eo_api.integrations.orchestration import output_collections as module


@pytest.fixture(autouse=True)
def _force_file_backend(monkeypatch: Any) -> None:
    monkeypatch.delenv("EO_API_PG_DSN", raising=False)


def test_ensure_output_collections_seeded_creates_file(monkeypatch: Any, tmp_path: Path) -> None:
    target = tmp_path / "preview.geojson"
    monkeypatch.setattr(module, "_PREVIEW_COLLECTION_PATH", target)

    path = module.ensure_output_collections_seeded()
    assert path == target
    assert target.exists()

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["type"] == "FeatureCollection"
    assert payload["features"] == []


def test_ensure_output_collections_seeded_runs_startup_cleanup(monkeypatch: Any, tmp_path: Path) -> None:
    target = tmp_path / "preview.geojson"
    monkeypatch.setattr(module, "_PREVIEW_COLLECTION_PATH", target)
    calls: dict[str, int] = {"count": 0}

    def _fake_cleanup(*, file_path: Path | None = None) -> dict[str, object]:
        calls["count"] += 1
        assert file_path == target
        return {"backend": "file", "deleted_count": 0, "ttl_days": 90}

    monkeypatch.setattr(module.preview_store, "cleanup_preview_store", _fake_cleanup)
    monkeypatch.setattr(module.preview_store, "_cleanup_on_startup_enabled", lambda: True)

    module.ensure_output_collections_seeded()
    assert calls["count"] == 1


def test_publish_dhis2_datavalue_preview_writes_rows(monkeypatch: Any, tmp_path: Path) -> None:
    target = tmp_path / "preview.geojson"
    monkeypatch.setattr(module, "_PREVIEW_COLLECTION_PATH", target)

    result = module.publish_dhis2_datavalue_preview(
        dataset_type="chirps3",
        rows=[{"orgUnit": "OU_1", "period": "202501", "value": "1.23"}],
    )

    assert result["collection_id"] == "generic-dhis2-datavalue-preview"
    assert result["job_id"]
    assert result["item_count"] == 1
    assert result["total_item_count"] == 1

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["type"] == "FeatureCollection"
    assert len(payload["features"]) == 1
    feature = payload["features"][0]
    assert feature["geometry"] is None
    assert feature["properties"]["dataset_type"] == "chirps3"
    assert feature["properties"]["job_id"] == result["job_id"]


def test_publish_dhis2_datavalue_preview_appends_runs(monkeypatch: Any, tmp_path: Path) -> None:
    target = tmp_path / "preview.geojson"
    monkeypatch.setattr(module, "_PREVIEW_COLLECTION_PATH", target)

    first = module.publish_dhis2_datavalue_preview(
        dataset_type="chirps3",
        rows=[{"orgUnit": "OU_1", "period": "202501", "value": "1.23"}],
    )
    second = module.publish_dhis2_datavalue_preview(
        dataset_type="worldpop",
        rows=[{"orgUnit": "OU_2", "period": "2026", "value": "2.34"}],
    )

    assert first["job_id"] != second["job_id"]
    assert second["total_item_count"] == 2
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert len(payload["features"]) == 2


def test_publish_dhis2_datavalue_preview_uses_supplied_job_id(monkeypatch: Any, tmp_path: Path) -> None:
    target = tmp_path / "preview.geojson"
    monkeypatch.setattr(module, "_PREVIEW_COLLECTION_PATH", target)

    result = module.publish_dhis2_datavalue_preview(
        dataset_type="chirps3",
        rows=[{"orgUnit": "OU_1", "period": "202501", "value": "1.23"}],
        job_id="job-123",
    )
    assert result["job_id"] == "job-123"

    payload = json.loads(target.read_text(encoding="utf-8"))
    feature = payload["features"][0]
    assert feature["id"] == "job-123-0"
    assert feature["properties"]["job_id"] == "job-123"
