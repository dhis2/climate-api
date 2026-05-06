from fastapi.testclient import TestClient


def test_get_extent_returns_configured_extent(client: TestClient) -> None:
    response = client.get("/extent")

    assert response.status_code == 200
    payload = response.json()
    assert payload["extent_id"] == "sle"
    assert payload["bbox"] == [-13.5, 6.9, -10.1, 10.0]
    assert "country_code" not in payload


def test_get_extent_by_id_returns_configured_extent(client: TestClient) -> None:
    response = client.get("/extent/sle")

    assert response.status_code == 200
    payload = response.json()
    assert payload["extent_id"] == "sle"
    assert payload["name"] == "Sierra Leone"


def test_get_extent_by_id_returns_404_for_unknown_extent(client: TestClient) -> None:
    response = client.get("/extent/unknown")

    assert response.status_code == 404
