from fastapi.testclient import TestClient


def test_list_extents_returns_configured_items(client: TestClient) -> None:
    response = client.get("/extents")

    assert response.status_code == 200
    payload = response.json()
    assert payload["kind"] == "ExtentList"
    assert "items" in payload
    assert payload["items"][0]["extent_id"] == "sle"
    assert payload["items"][0]["bbox"] == [-13.5, 6.9, -10.1, 10.0]
    assert "country_code" not in payload["items"][0]


def test_get_extent_returns_one_configured_extent(client: TestClient) -> None:
    response = client.get("/extents/sle")

    assert response.status_code == 200
    payload = response.json()
    assert payload["extent_id"] == "sle"
    assert payload["name"] == "Sierra Leone"


def test_get_extent_returns_404_for_unknown_extent(client: TestClient) -> None:
    response = client.get("/extents/unknown")

    assert response.status_code == 404
