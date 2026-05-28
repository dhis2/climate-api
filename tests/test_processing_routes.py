from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_get_processes_lists_registered_processes(client: TestClient) -> None:
    response = client.get("/processes")

    assert response.status_code == 200
    payload = response.json()
    assert any(item["id"] == "ingestion" for item in payload["processes"])
    assert any(item["id"] == "sync" for item in payload["processes"])


def test_get_ingest_process_detail_returns_public_metadata(client: TestClient) -> None:
    response = client.get("/processes/ingestion")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "ingestion"
    assert len(payload["jobControlOptions"]) == 2
    assert set(payload["jobControlOptions"]) == {"sync-execute", "async-execute"}
    assert payload["inputs"]["dataset_id"]["required"] is True
    assert payload["inputs"]["publish"]["default"] is True
    assert payload["outputs"]["ingestion_id"]["type"] == "string"


def test_get_sync_process_detail_returns_public_metadata(client: TestClient) -> None:
    response = client.get("/processes/sync")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "sync"
    assert len(payload["jobControlOptions"]) == 2
    assert set(payload["jobControlOptions"]) == {"sync-execute", "async-execute"}
    assert payload["inputs"]["dataset_id"]["required"] is True
    assert payload["inputs"]["publish"]["default"] is True
    assert payload["outputs"]["sync_id"]["type"] == "string"


def test_get_processes_omits_internal_only_processes(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    # GET /processes is now the openEO endpoint; the expose flag is enforced on
    # GET /processes/{id} and POST /processes/{id}/execution (native routes).
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

    detail_response = client.get("/processes/internal_process")
    assert detail_response.status_code == 404

    exec_response = client.post("/processes/internal_process/execution", json={})
    assert exec_response.status_code == 404


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


def test_expose_false_yaml_fixture_is_hidden_from_execution_routes(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # GET /processes is the openEO endpoint and is unaffected by the native
    # expose flag.  The flag is still enforced on GET /processes/{id} and
    # POST /processes/{id}/execution.
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "internal.yaml").write_text(
        """
- id: internal_process
  title: Internal process
  expose: false
  execution:
    function: mypackage.internal.execute
""",
        encoding="utf-8",
    )
    monkeypatch.setattr("climate_api.data_registry.services.processes.CONFIGS_DIR", processes_subdir)

    detail_response = client.get("/processes/internal_process")
    assert detail_response.status_code == 404

    execution_response = client.post("/processes/internal_process/execution", json={})
    assert execution_response.status_code == 404


def test_post_ingest_execution_honors_respond_async(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from climate_api.ingestions.schemas import IngestionResponse
    from tests.helpers import dataset_record

    monkeypatch.setattr(
        "climate_api.ingestions.processes.registry_datasets.get_dataset",
        lambda dataset_id: {
            "id": dataset_id,
            "name": "CHIRPS3 precipitation",
            "variable": "precip",
            "period_type": "daily",
            "ingestion": {"plugin": "climate_api.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
    )
    monkeypatch.setattr(
        "climate_api.ingestions.processes.get_extent_or_404",
        lambda: {"bbox": [1.0, 2.0, 3.0, 4.0], "country_code": "SLE"},
    )
    monkeypatch.setattr(
        "climate_api.ingestions.processes.services.create_artifact",
        lambda **kwargs: type("Artifact", (), {"artifact_id": "artifact-123"})(),
    )
    monkeypatch.setattr(
        "climate_api.ingestions.processes.services.get_ingestion_or_404",
        lambda artifact_id: IngestionResponse(
            ingestion_id=artifact_id,
            status="completed",
            dataset=dataset_record("chirps3_precipitation_daily"),
        ),
    )

    response = client.post(
        "/processes/ingestion/execution",
        headers={"Prefer": "respond-async"},
        json={
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2026-01-01",
            "end": "2026-01-03",
            "publish": True,
        },
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] in {"accepted", "running", "successful"}
    assert response.headers["Location"].startswith("/internal/jobs/")


def test_post_ingest_execution_returns_404_for_unknown_dataset(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "climate_api.ingestions.processes.registry_datasets.get_dataset",
        lambda dataset_id: None,
    )

    response = client.post(
        "/processes/ingestion/execution",
        json={
            "dataset_id": "unknown_dataset",
            "start": "2026-01-01",
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Dataset 'unknown_dataset' not found"


def test_post_sync_execution_honors_respond_async(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.helpers import dataset_record

    monkeypatch.setattr(
        "climate_api.ingestions.processes.services.sync_dataset",
        lambda **kwargs: {
            "sync_id": "artifact-123",
            "status": "completed",
            "message": "Managed dataset was synced.",
            "dataset": dataset_record("chirps3_precipitation_daily"),
            "sync_detail": {
                "source_dataset_id": "chirps3_precipitation_daily",
                "sync_kind": "temporal",
                "action": "append",
                "reason": "new_periods_available_for_append",
                "message": "Sync will append new periods.",
                "current_start": "2026-01-01",
                "current_end": "2026-01-31",
                "target_end": "2026-02-10",
                "target_end_source": "request",
                "delta_start": "2026-02-01",
                "delta_end": "2026-02-10",
            },
        },
    )

    response = client.post(
        "/processes/sync/execution",
        headers={"Prefer": "respond-async"},
        json={
            "dataset_id": "chirps3_precipitation_daily_sle",
            "end": "2026-02-10",
            "publish": True,
        },
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] in {"accepted", "running", "successful"}
    assert response.headers["Location"].startswith("/internal/jobs/")


def test_post_process_execution_rejects_async_when_process_is_sync_only(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "climate_api.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Sync only process",
            "execution": {"function": "mypackage.sync_only.execute"},
            "expose": True,
            "jobControlOptions": ["sync-execute"],
        },
    )

    response = client.post(
        "/processes/sync-only/execution",
        headers={"Prefer": "respond-async"},
        json={},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "Process 'sync-only' does not support async execution"


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


def test_post_unknown_process_id_returns_404(client: TestClient) -> None:
    response = client.post(
        "/processes/unknown_process/execution",
        json={"dataset_id": "chirps3_precipitation_daily", "start": "2026-01-05"},
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


def test_post_process_execution_returns_400_for_execution_value_error(
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

    def _raise_value_error(full_path: str) -> object:
        def _func(**_: object) -> object:
            raise ValueError("Bad execution input")

        return _func

    monkeypatch.setattr("climate_api.processing.routes.process_registry._get_dynamic_function", _raise_value_error)

    response = client.post("/processes/broken_process/execution", json={})

    assert response.status_code == 400
    assert response.json()["detail"] == "Bad execution input"
