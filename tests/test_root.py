import pytest
from fastapi.testclient import TestClient

from eo_api.ingestions import services as ingestion_services
from eo_api.system.schemas import HealthStatus, RootResponse


def test_root_returns_200(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200


def test_root_returns_welcome_message(client: TestClient) -> None:
    response = client.get("/")
    result = RootResponse.model_validate(response.json())
    assert result.message == "Welcome to DHIS2 EO API"


def test_root_returns_links(client: TestClient) -> None:
    response = client.get("/")
    result = RootResponse.model_validate(response.json())
    rels = [link.rel for link in result.links]
    assert "extents" in rels
    assert "ingestions" in rels
    assert "datasets" in rels
    assert "prefect" in rels
    assert "docs" in rels


def test_health_returns_200(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200


def test_health_returns_healthy_status(client: TestClient) -> None:
    response = client.get("/health")
    result = HealthStatus.model_validate(response.json())
    assert result.status == "healthy"


def test_zarr_route_echoes_origin_for_browser_access(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "get_dataset_zarr_store_info_or_404",
        lambda _: {"kind": "ZarrListing", "dataset_id": "dataset-1", "path": ".", "entries": []},
    )

    response = client.get("/zarr/dataset-1", headers={"Origin": "https://inspect.geozarr.org"})

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://inspect.geozarr.org"


def test_zarr_route_allows_private_network_preflight(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "get_dataset_zarr_store_info_or_404",
        lambda _: {"kind": "ZarrListing", "dataset_id": "dataset-1", "path": ".", "entries": []},
    )

    response = client.options(
        "/zarr/dataset-1",
        headers={
            "Origin": "https://inspect.geozarr.org",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Private-Network": "true",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://inspect.geozarr.org"
    assert response.headers["access-control-allow-private-network"] == "true"
