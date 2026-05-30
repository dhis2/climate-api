import pytest
from fastapi.testclient import TestClient


def test_get_unknown_process_detail_returns_404(client: TestClient) -> None:
    response = client.get("/processes/unknown_process")

    assert response.status_code == 404


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
