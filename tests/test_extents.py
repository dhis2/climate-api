from fastapi.testclient import TestClient


def test_get_extent_returns_configured_extent(client: TestClient) -> None:
    response = client.get("/extent")

    assert response.status_code == 200
    payload = response.json()
    assert payload["name"] == "Sierra Leone"
    assert payload["bbox"] == [-13.5, 6.9, -10.1, 10.0]
    assert "extent_id" not in payload
    assert "country_code" not in payload
