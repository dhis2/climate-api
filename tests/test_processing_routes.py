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
        source_dataset_id="chirps3_precipitation_daily_weekly_sum",
        dataset_name="CHIRPS weekly precipitation",
        short_name="CHIRPS weekly",
        variable="precip",
        period_type="weekly",
        units="mm",
        resolution="5 km x 5 km",
        source="CHIRPS v3",
        source_url="https://example.com/chirps",
        extent=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-W02", end="2026-W03"),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        last_updated=datetime(2026, 1, 21, tzinfo=UTC),
        links=[],
        publication=DatasetPublication(
            status=PublicationStatus.PUBLISHED,
            published_at=datetime(2026, 1, 21, tzinfo=UTC),
        ),
    )


def test_post_resample_execution_returns_completed_response(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        processing_services,
        "run_resample_process",
        lambda **kwargs: ("artifact-123", _dataset_record("chirps3_precipitation_daily_weekly_sum_sle")),
    )

    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "period_type": "weekly",
            "method": "sum",
            "start": "2026-W02",
            "end": "2026-W03",
            "extent_id": "sle",
            "publish": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["artifact_id"] == "artifact-123"
    assert payload["status"] == "completed"
    assert payload["dataset"]["dataset_id"] == "chirps3_precipitation_daily_weekly_sum_sle"


def test_post_resample_execution_passes_params_to_service(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run_resample_process(**kwargs: object) -> tuple[str, DatasetRecord]:
        captured.update(kwargs)
        return "artifact-456", _dataset_record("chirps3_precipitation_daily_weekly_sum_sle")

    monkeypatch.setattr(processing_services, "run_resample_process", fake_run_resample_process)

    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "period_type": "weekly",
            "method": "sum",
            "start": "2026-W02",
            "end": "2026-W03",
            "extent_id": "sle",
            "overwrite": True,
            "publish": False,
        },
    )

    assert response.status_code == 200
    assert captured["source_dataset_id"] == "chirps3_precipitation_daily"
    assert captured["period_type"] == "weekly"
    assert captured["method"] == "sum"
    assert captured["start"] == "2026-W02"
    assert captured["end"] == "2026-W03"
    assert captured["extent_id"] == "sle"
    assert captured["overwrite"] is True
    assert captured["publish"] is False


def test_post_resample_execution_returns_400_for_unsupported_period_type(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "period_type": "dekadal",
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
            "period_type": "weekly",
            "method": "median",
            "start": "2026-W01",
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
            "period_type": "weekly",
            "method": "sum",
            "start": "2026-W02",
        },
    )
    assert response.status_code == 404
