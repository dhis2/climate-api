from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import xarray as xr

from climate_api.client import Client, _id_from_href


def _make_catalog(hrefs: list[str]) -> dict:
    return {
        "links": [{"rel": "child", "href": href, "title": href.split("/")[-1]} for href in hrefs]
        + [{"rel": "root", "href": "http://localhost/stac/catalog.json"}]
    }


def _make_collection(zarr_href: str) -> dict:
    return {
        "assets": {
            "zarr": {
                "href": zarr_href,
                "xarray:open_kwargs": {"consolidated": True},
            }
        }
    }


def _make_response(json_body: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


# ── _id_from_href ──────────────────────────────────────────────────────────────


def test_id_from_href_extracts_last_path_segment() -> None:
    assert _id_from_href("http://localhost/stac/collections/chirps3_daily_rwa") == "chirps3_daily_rwa"


def test_id_from_href_ignores_query_and_fragment() -> None:
    assert _id_from_href("http://localhost/stac/collections/ds?foo=bar#section") == "ds"


def test_id_from_href_strips_trailing_slash() -> None:
    assert _id_from_href("http://localhost/stac/collections/ds/") == "ds"


# ── Client class ───────────────────────────────────────────────────────────────


def _make_client(base_url: str = "http://localhost") -> tuple[Client, MagicMock]:
    """Return a Client with a mocked internal httpx.Client."""
    client = Client(base_url)
    mock_http = MagicMock()
    client._http = mock_http
    return client, mock_http


def test_client_catalog_uses_persistent_http_session() -> None:
    catalog = _make_catalog(["http://localhost/stac/collections/chirps3_precipitation_daily_rwa"])
    client, mock_http = _make_client()
    mock_http.get.return_value = _make_response(catalog)

    result = client.catalog()

    mock_http.get.assert_called_once_with("http://localhost/stac/catalog.json")
    assert len(result) == 1
    assert result[0]["rel"] == "child"
    assert result[0]["id"] == "chirps3_precipitation_daily_rwa"


def test_client_open_uses_persistent_http_session(tmp_path: Path) -> None:
    zarr_path = tmp_path / "test.zarr"
    ds = xr.Dataset(
        {"precip": (["time", "latitude", "longitude"], np.ones((2, 3, 3), dtype="float32"))},
        coords={
            "time": pd.date_range("2024-01-01", periods=2, freq="D"),
            "latitude": [3.0, 2.0, 1.0],
            "longitude": [10.0, 11.0, 12.0],
        },
    )
    ds.to_zarr(str(zarr_path), mode="w", consolidated=True)

    client, mock_http = _make_client()
    mock_http.get.return_value = _make_response(_make_collection(str(zarr_path)))

    result = client.open("chirps3_precipitation_daily_rwa")
    try:
        assert "precip" in result.data_vars
        assert result.sizes["time"] == 2
    finally:
        result.close()

    mock_http.get.assert_called_once_with("http://localhost/stac/collections/chirps3_precipitation_daily_rwa")


def test_client_context_manager_closes_http_session() -> None:
    with Client("http://localhost") as client:
        mock_http = MagicMock()
        client._http = mock_http

    mock_http.close.assert_called_once()


def test_client_accepts_custom_timeout() -> None:
    with patch("climate_api.client.httpx.Client") as mock_cls:
        Client("http://localhost", timeout=60)
    mock_cls.assert_called_once_with(timeout=60)


def test_client_strips_trailing_slash() -> None:
    client, mock_http = _make_client("http://localhost/")
    mock_http.get.return_value = _make_response(_make_catalog(["http://localhost/stac/collections/ds"]))
    client.catalog()
    mock_http.get.assert_called_once_with("http://localhost/stac/catalog.json")
