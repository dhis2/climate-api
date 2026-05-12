from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from climate_api.ingestions.schemas import (
    ArtifactCoverage,
    CoverageSpatial,
    CoverageTemporal,
    DatasetPublication,
    DatasetRecord,
    PublicationStatus,
)
from climate_api.processing import services as processing_services


def _dataset_record(dataset_id: str) -> DatasetRecord:
    return DatasetRecord(
        dataset_id=dataset_id,
        source_dataset_id="chirps3_precipitation_daily_w_mon_sum",
        dataset_name="CHIRPS weekly precipitation",
        short_name="CHIRPS weekly",
        variable="precip",
        period_type="daily",
        units="mm",
        resolution="5 km x 5 km",
        source="CHIRPS v3",
        source_url="https://example.com/chirps",
        extent=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-05", end="2026-01-11"),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        last_updated=datetime(2026, 1, 21, tzinfo=UTC),
        links=[],
        publication=DatasetPublication(
            status=PublicationStatus.PUBLISHED,
            published_at=datetime(2026, 1, 21, tzinfo=UTC),
        ),
    )


def test_get_processes_lists_registered_processes(client: TestClient) -> None:
    response = client.get("/processes")

    assert response.status_code == 200
    payload = response.json()
    assert any(item["id"] == "resample" for item in payload["processes"])
    assert any(link["href"] == "/processes" for link in payload["links"])


def test_get_process_detail_returns_public_metadata(client: TestClient) -> None:
    response = client.get("/processes/resample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "resample"
    assert payload["title"] == "Temporal resampling"
    assert payload["jobControlOptions"] == ["sync-execute"]
    assert "execution_function" not in payload
    assert payload["inputs"]["source_dataset_id"]["required"] is True
    assert payload["outputs"]["artifact_id"]["type"] == "string"


def test_get_processes_omits_internal_only_processes(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "climate_api.processing.routes.process_registry.list_processes",
        lambda: [
            {
                "id": "public_process",
                "title": "Public process",
                "description": "Visible",
                "execution": {"function": "mypackage.public.execute"},
                "expose": True,
                "jobControlOptions": ["sync-execute"],
            },
            {
                "id": "internal_process",
                "title": "Internal process",
                "description": "Hidden",
                "execution": {"function": "mypackage.internal.execute"},
                "expose": False,
                "jobControlOptions": ["sync-execute"],
            },
        ],
    )

    response = client.get("/processes")

    assert response.status_code == 200
    payload = response.json()
    ids = {item["id"] for item in payload["processes"]}
    assert ids == {"public_process"}


def test_get_unknown_process_detail_returns_404(client: TestClient) -> None:
    response = client.get("/processes/unknown_process")

    assert response.status_code == 404


def test_get_internal_process_detail_returns_404(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "climate_api.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Internal process",
            "execution": {"function": "mypackage.internal.execute"},
            "expose": False,
            "jobControlOptions": ["sync-execute"],
        },
    )

    response = client.get("/processes/internal_process")

    assert response.status_code == 404


def test_post_resample_execution_returns_completed_response(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        processing_services,
        "run_resample_process",
        lambda **kwargs: ("artifact-123", _dataset_record("chirps3_precipitation_daily_w_mon_sum")),
    )

    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "sum",
            "start": "2026-01-05",
            "end": "2026-01-12",
            "publish": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["artifact_id"] == "artifact-123"
    assert payload["status"] == "completed"
    assert payload["dataset"]["dataset_id"] == "chirps3_precipitation_daily_w_mon_sum"


def test_post_internal_process_execution_returns_404(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "climate_api.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Internal process",
            "execution": {"function": "mypackage.internal.execute"},
            "expose": False,
            "jobControlOptions": ["sync-execute"],
        },
    )

    response = client.post("/processes/internal_process/execution", json={})

    assert response.status_code == 404


def test_post_resample_execution_passes_params_to_service(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run_resample_process(**kwargs: object) -> tuple[str, DatasetRecord]:
        captured.update(kwargs)
        return "artifact-456", _dataset_record("chirps3_precipitation_daily_w_mon_sum")

    monkeypatch.setattr(processing_services, "run_resample_process", fake_run_resample_process)

    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "sum",
            "start": "2026-01-05",
            "end": "2026-01-12",
            "overwrite": True,
            "publish": False,
        },
    )

    assert response.status_code == 200
    assert captured["source_dataset_id"] == "chirps3_precipitation_daily"
    assert captured["frequency"] == "W-MON"
    assert captured["method"] == "sum"
    assert captured["start"] == "2026-01-05"
    assert captured["end"] == "2026-01-12"
    assert captured["overwrite"] is True
    assert captured["publish"] is False


def test_post_resample_execution_returns_400_for_invalid_frequency(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "invalid",
            "method": "sum",
            "start": "2026-01-01",
        },
    )
    assert response.status_code == 400


def test_post_resample_execution_returns_400_for_unsupported_method(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "median",
            "start": "2026-01-05",
        },
    )
    assert response.status_code == 400


def test_post_unknown_process_id_returns_404(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = client.post(
        "/processes/unknown_process/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "sum",
            "start": "2026-01-05",
        },
    )
    assert response.status_code == 404


def test_post_process_execution_returns_500_for_invalid_execution_function(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "climate_api.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Broken process",
            "execution": {"function": "mypackage.broken.execute"},
            "expose": True,
            "jobControlOptions": ["sync-execute"],
        },
    )

    def _raise_import_error(full_path: str) -> object:
        raise ModuleNotFoundError(f"No module named for {full_path}")

    monkeypatch.setattr("climate_api.processing.routes.process_registry._get_dynamic_function", _raise_import_error)

    response = client.post("/processes/broken_process/execution", json={})

    assert response.status_code == 500
    assert "Failed to load execution.function" in response.json()["detail"]
