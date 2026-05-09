from pathlib import Path

import pytest

from climate_api.publications import services


def test_load_base_config_returns_mapping() -> None:
    config = services._load_base_config()
    assert isinstance(config, dict)
    assert "server" in config


def test_resolve_pygeoapi_dir_uses_data_dir_from_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from climate_api import config as api_config

    monkeypatch.setattr(api_config, "get_data_dir", lambda: tmp_path / "data")
    result = services._resolve_pygeoapi_dir()
    assert result == tmp_path / "data" / "pygeoapi"


def test_resolve_pygeoapi_dir_uses_xdg_data_home(monkeypatch: pytest.MonkeyPatch) -> None:
    import tempfile

    from climate_api import config as api_config

    monkeypatch.setattr(api_config, "get_data_dir", lambda: None)
    with tempfile.TemporaryDirectory() as xdg:
        monkeypatch.setenv("XDG_DATA_HOME", xdg)
        result = services._resolve_pygeoapi_dir()
        assert str(result) == f"{xdg}/climate-api/pygeoapi"


def test_native_dataset_href_defaults_to_relative_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLIMATE_API_BASE_URL", raising=False)
    monkeypatch.delenv("OGCAPI_BASE_URL", raising=False)

    assert services._native_dataset_href("dataset-1") == "/datasets/dataset-1"


def test_native_dataset_href_uses_ogcapi_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLIMATE_API_BASE_URL", raising=False)
    monkeypatch.setenv("OGCAPI_BASE_URL", "https://example.org/ogcapi")

    assert services._native_dataset_href("dataset-1") == "https://example.org/datasets/dataset-1"


def test_native_dataset_href_uses_climate_api_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIMATE_API_BASE_URL", "https://climate.example.org")
    monkeypatch.delenv("OGCAPI_BASE_URL", raising=False)

    assert services._native_dataset_href("dataset-1") == "https://climate.example.org/datasets/dataset-1"
