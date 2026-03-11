import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from eo_api.integrations.orchestration import preview_store


def test_publish_preview_rows_uses_file_backend_when_pg_dsn_unset(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.delenv("EO_API_PG_DSN", raising=False)
    result = preview_store.publish_preview_rows(
        dataset_type="chirps3",
        rows=[{"orgUnit": "OU_1", "period": "202501", "value": "1.23"}],
        job_id="job-file",
        file_path=tmp_path / "preview.geojson",
    )
    assert result["backend"] == "file"
    assert result["job_id"] == "job-file"


def test_publish_preview_rows_uses_postgres_backend_when_pg_dsn_set(monkeypatch: Any) -> None:
    monkeypatch.setenv("EO_API_PG_DSN", "postgresql://user:pass@localhost:5432/db")

    def _fake_run_async(coro: Any) -> dict[str, Any]:
        close = getattr(coro, "close", None)
        if callable(close):
            close()
        return {
            "collection_id": "generic-dhis2-datavalue-preview",
            "path": "generic_dhis2_datavalue_preview",
            "job_id": "job-pg",
            "item_count": 1,
            "total_item_count": 1,
            "backend": "postgresql",
        }

    monkeypatch.setattr(preview_store, "_run_async", _fake_run_async)
    result = preview_store.publish_preview_rows(
        dataset_type="chirps3",
        rows=[{"orgUnit": "OU_1", "period": "202501", "value": "1.23"}],
        job_id="job-pg",
    )
    assert result["backend"] == "postgresql"
    assert result["job_id"] == "job-pg"


def test_cleanup_preview_store_file_removes_expired_rows(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.delenv("EO_API_PG_DSN", raising=False)
    target = tmp_path / "preview.geojson"
    now = datetime.now(UTC)
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "old-1",
                "geometry": None,
                "properties": {
                    "job_id": "a",
                    "dataset_type": "chirps3",
                    "published_at": (now - timedelta(days=120)).isoformat(),
                },
            },
            {
                "type": "Feature",
                "id": "new-1",
                "geometry": None,
                "properties": {
                    "job_id": "b",
                    "dataset_type": "chirps3",
                    "published_at": (now - timedelta(days=10)).isoformat(),
                },
            },
        ],
    }
    target.write_text(json.dumps(payload), encoding="utf-8")

    result = preview_store.cleanup_preview_store(ttl_days=90, file_path=target)
    assert result["backend"] == "file"
    assert result["deleted_count"] == 1

    updated = json.loads(target.read_text(encoding="utf-8"))
    assert len(updated["features"]) == 1
    assert updated["features"][0]["id"] == "new-1"


def test_infer_preview_fields_uses_union_not_first_row(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.delenv("EO_API_PG_DSN", raising=False)
    target = tmp_path / "preview.geojson"
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "a-0",
                "geometry": None,
                "properties": {"job_id": "a", "dataset_type": "chirps3", "orgUnit": "OU_1"},
            },
            {
                "type": "Feature",
                "id": "a-1",
                "geometry": None,
                "properties": {"job_id": "a", "period": "202401", "value": 1.23, "dataElement": "DE_UID"},
            },
        ],
    }
    target.write_text(json.dumps(payload), encoding="utf-8")

    # infer_preview_fields reads default path; temporarily switch module path
    original = preview_store._PREVIEW_COLLECTION_PATH
    preview_store._PREVIEW_COLLECTION_PATH = target
    try:
        fields = preview_store.infer_preview_fields()
    finally:
        preview_store._PREVIEW_COLLECTION_PATH = original

    assert "orgUnit" in fields
    assert "period" in fields
    assert "value" in fields
    assert "dataElement" in fields


def test_get_latest_preview_job_id_file(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.delenv("EO_API_PG_DSN", raising=False)
    target = tmp_path / "preview.geojson"
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "old-0",
                "geometry": None,
                "properties": {
                    "job_id": "job-old",
                    "published_at": "2026-03-10T10:00:00+00:00",
                },
            },
            {
                "type": "Feature",
                "id": "new-0",
                "geometry": None,
                "properties": {
                    "job_id": "job-new",
                    "published_at": "2026-03-11T10:00:00+00:00",
                },
            },
        ],
    }
    target.write_text(json.dumps(payload), encoding="utf-8")

    assert preview_store.get_latest_preview_job_id(file_path=target) == "job-new"


def test_publish_preview_rows_file_persists_geometry_by_org_unit(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.delenv("EO_API_PG_DSN", raising=False)
    target = tmp_path / "preview.geojson"
    preview_store.publish_preview_rows(
        dataset_type="chirps3",
        rows=[{"orgUnit": "OU_1", "period": "202401", "value": "1.0"}],
        job_id="job-geo",
        geometry_by_org_unit={"OU_1": {"type": "Point", "coordinates": [1.0, 2.0]}},
        file_path=target,
    )
    features = preview_store.load_preview_features(job_id="job-geo", file_path=target)
    assert len(features) == 1
    assert features[0]["geometry"] == {"type": "Point", "coordinates": [1.0, 2.0]}


def test_list_preview_jobs_file_orders_by_latest(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.delenv("EO_API_PG_DSN", raising=False)
    target = tmp_path / "preview.geojson"
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "job-a-0",
                "geometry": None,
                "properties": {
                    "job_id": "job-a",
                    "dataset_type": "chirps3",
                    "published_at": "2026-03-10T10:00:00+00:00",
                },
            },
            {
                "type": "Feature",
                "id": "job-b-0",
                "geometry": None,
                "properties": {
                    "job_id": "job-b",
                    "dataset_type": "worldpop",
                    "published_at": "2026-03-11T10:00:00+00:00",
                },
            },
            {
                "type": "Feature",
                "id": "job-b-1",
                "geometry": None,
                "properties": {
                    "job_id": "job-b",
                    "dataset_type": "worldpop",
                    "published_at": "2026-03-11T10:05:00+00:00",
                },
            },
        ],
    }
    target.write_text(json.dumps(payload), encoding="utf-8")

    jobs = preview_store.list_preview_jobs(limit=10, file_path=target)
    assert len(jobs) == 2
    assert jobs[0]["job_id"] == "job-b"
    assert jobs[0]["row_count"] == 2
