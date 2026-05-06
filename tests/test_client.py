from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_api.client import Client, list_datasets, open_dataset


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


def test_list_datasets_returns_child_links() -> None:
    catalog = _make_catalog(["http://localhost/stac/collections/chirps3_precipitation_daily_rwa"])
    with patch("climate_api.client.httpx.get", return_value=_make_response(catalog)) as mock_get:
        result = list_datasets("http://localhost")

    mock_get.assert_called_once_with("http://localhost/stac/catalog.json")
    assert len(result) == 1
    assert result[0]["rel"] == "child"
    assert "chirps3" in result[0]["href"]


def test_list_datasets_returns_empty_for_no_children() -> None:
    catalog = {"links": [{"rel": "root", "href": "http://localhost/stac/catalog.json"}]}
    with patch("climate_api.client.httpx.get", return_value=_make_response(catalog)):
        result = list_datasets("http://localhost")

    assert result == []


def test_list_datasets_raises_on_http_error() -> None:
    import httpx

    with patch("climate_api.client.httpx.get") as mock_get:
        mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404", request=MagicMock(), response=MagicMock()
        )
        with pytest.raises(httpx.HTTPStatusError):
            list_datasets("http://localhost")


def test_open_dataset_fetches_collection_and_opens_zarr(tmp_path: Path) -> None:
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

    collection = _make_collection(str(zarr_path))
    with patch("climate_api.client.httpx.get", return_value=_make_response(collection)):
        result = open_dataset("chirps3_precipitation_daily_rwa", base_url="http://localhost")

    try:
        assert "precip" in result.data_vars
        assert result.sizes["time"] == 2
        assert "latitude" in result.coords
        assert "longitude" in result.coords
    finally:
        result.close()


def test_open_dataset_raises_on_http_error() -> None:
    import httpx

    with patch("climate_api.client.httpx.get") as mock_get:
        mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404", request=MagicMock(), response=MagicMock()
        )
        with pytest.raises(httpx.HTTPStatusError):
            open_dataset("nonexistent", base_url="http://localhost")


def test_open_dataset_uses_default_base_url() -> None:
    with patch("climate_api.client.httpx.get", return_value=_make_response({})) as mock_get:
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.return_value.json.return_value = {
            "assets": {"zarr": {"href": "/dev/null", "xarray:open_kwargs": {"consolidated": False}}}
        }
        try:
            open_dataset("any_dataset")
        except Exception:
            pass

    mock_get.assert_called_once_with("http://127.0.0.1:8000/stac/collections/any_dataset")


def test_open_dataset_uses_env_var_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIMATE_API_BASE_URL", "http://env-host:9000")
    with patch("climate_api.client.httpx.get", return_value=_make_response({})) as mock_get:
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.return_value.json.return_value = {
            "assets": {"zarr": {"href": "/dev/null", "xarray:open_kwargs": {"consolidated": False}}}
        }
        try:
            open_dataset("any_dataset")
        except Exception:
            pass

    mock_get.assert_called_once_with("http://env-host:9000/stac/collections/any_dataset")


def test_client_catalog_delegates_to_module_function() -> None:
    catalog = _make_catalog(["http://localhost/stac/collections/chirps3_precipitation_daily_rwa"])
    with patch("climate_api.client.httpx.get", return_value=_make_response(catalog)) as mock_get:
        result = Client("http://localhost").catalog()

    mock_get.assert_called_once_with("http://localhost/stac/catalog.json")
    assert len(result) == 1
    assert result[0]["rel"] == "child"


def test_client_open_dataset_delegates_to_module_function(tmp_path: Path) -> None:
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

    collection = _make_collection(str(zarr_path))
    with patch("climate_api.client.httpx.get", return_value=_make_response(collection)):
        result = Client("http://localhost").open("chirps3_precipitation_daily_rwa")

    try:
        assert "precip" in result.data_vars
        assert result.sizes["time"] == 2
    finally:
        result.close()


def test_client_strips_trailing_slash() -> None:
    catalog = _make_catalog(["http://localhost/stac/collections/chirps3_precipitation_daily_rwa"])
    with patch("climate_api.client.httpx.get", return_value=_make_response(catalog)) as mock_get:
        Client("http://localhost/").catalog()

    mock_get.assert_called_once_with("http://localhost/stac/catalog.json")
