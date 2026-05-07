import pytest

from climate_api.publications import services


def test_load_base_config_returns_mapping() -> None:
    config = services._load_base_config()
    assert isinstance(config, dict)
    assert "server" in config


def test_resolve_pygeoapi_dir_uses_cache_override(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as override:
        monkeypatch.setenv("CACHE_OVERRIDE", override)
        monkeypatch.delenv("XDG_DATA_HOME", raising=False)
        result = services._resolve_pygeoapi_dir()
        assert str(result) == f"{override}/pygeoapi"


def test_resolve_pygeoapi_dir_uses_xdg_data_home(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as xdg:
        monkeypatch.delenv("CACHE_OVERRIDE", raising=False)
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
