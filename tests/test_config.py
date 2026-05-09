from pathlib import Path

import pytest

from climate_api.config import get_config, get_data_dir
from climate_api.data_registry.services import datasets as dataset_registry
from climate_api.extents import services as extent_services


def test_get_data_dir_returns_none_when_no_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLIMATE_API_CONFIG", raising=False)
    assert get_data_dir() is None


def test_get_data_dir_returns_none_when_config_path_set_but_file_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(tmp_path / "nonexistent.yaml"))
    assert get_data_dir() is None


def test_get_data_dir_raises_when_config_present_but_no_data_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text("extent:\n  id: nor\n", encoding="utf-8")
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))
    with pytest.raises(ValueError, match="data_dir is required"):
        get_data_dir()


def test_get_data_dir_resolves_relative_to_config_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text("data_dir: ./data\n", encoding="utf-8")
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))
    assert get_data_dir() == tmp_path / "data"


def test_get_config_returns_empty_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLIMATE_API_CONFIG", raising=False)
    assert get_config() == {}


def test_get_config_raises_when_path_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(tmp_path / "nonexistent.yaml"))
    with pytest.raises(FileNotFoundError, match="CLIMATE_API_CONFIG not found"):
        get_config()


def test_get_config_loads_extent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text(
        """
extent:
  id: rwa
  name: Rwanda
  bbox: [28.8, -2.9, 30.9, -1.0]
  country_code: RWA
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))

    config = get_config()
    assert config["extent"]["id"] == "rwa"
    assert config["extent"]["bbox"] == [28.8, -2.9, 30.9, -1.0]


def test_get_config_substitutes_env_vars(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text(
        """
extent:
  id: ${EXTENT_ID:-sle}
  name: ${EXTENT_NAME:-Sierra Leone}
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))
    monkeypatch.setenv("EXTENT_ID", "rwa")
    monkeypatch.delenv("EXTENT_NAME", raising=False)

    config = get_config()
    assert config["extent"]["id"] == "rwa"
    assert config["extent"]["name"] == "Sierra Leone"


def test_extents_service_reads_from_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text(
        """
extent:
  id: rwa
  name: Rwanda
  bbox: [28.8, -2.9, 30.9, -1.0]
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))

    extent = extent_services.get_extent()
    assert extent is not None
    assert extent["id"] == "rwa"


def test_extent_service_returns_none_when_config_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLIMATE_API_CONFIG", raising=False)

    assert extent_services.get_extent() is None


def test_builtin_datasets_include_chirps_era5_worldpop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dataset_registry, "CONFIGS_DIR", None)
    monkeypatch.delenv("CLIMATE_API_CONFIG", raising=False)

    ids = {d["id"] for d in dataset_registry.list_datasets()}
    assert "chirps3_precipitation_daily" in ids
    assert "era5land_temperature_hourly" in ids
    assert "era5land_precipitation_hourly" in ids
    assert "worldpop_population_yearly" in ids


def test_templates_dir_in_config_adds_to_bundled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    templates_dir = tmp_path / "datasets"
    templates_dir.mkdir()
    (templates_dir / "custom.yaml").write_text(
        """
- id: custom_dataset
  name: Custom dataset
  variable: val
  period_type: daily
  sync_kind: static
  ingestion:
    function: mypackage.sources.download
""",
        encoding="utf-8",
    )
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text(f"templates_dir: {templates_dir}\n", encoding="utf-8")

    monkeypatch.setattr(dataset_registry, "CONFIGS_DIR", None)
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))

    ids = {d["id"] for d in dataset_registry.list_datasets()}
    assert "custom_dataset" in ids
    assert "chirps3_precipitation_daily" in ids


def test_templates_dir_resolved_relative_to_config_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """templates_dir is resolved relative to the config file, not CWD.

    This matters when running the installed `climate-api` CLI from a directory
    other than the repo root, where a relative templates_dir in the config must
    still point at the correct sibling directory.
    """
    deployment_dir = tmp_path / "deployment"
    deployment_dir.mkdir()
    templates_dir = deployment_dir / "datasets"
    templates_dir.mkdir()
    (templates_dir / "custom.yaml").write_text(
        """
- id: deployed_dataset
  variable: val
  period_type: daily
  sync_kind: static
  ingestion:
    function: mypackage.sources.download
""",
        encoding="utf-8",
    )
    config_file = deployment_dir / "climate-api.yaml"
    config_file.write_text("templates_dir: ./datasets\n", encoding="utf-8")

    monkeypatch.setattr(dataset_registry, "CONFIGS_DIR", None)
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))

    ids = {d["id"] for d in dataset_registry.list_datasets()}
    assert "deployed_dataset" in ids


def test_templates_dir_in_config_overrides_bundled_by_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    templates_dir = tmp_path / "datasets"
    templates_dir.mkdir()
    (templates_dir / "chirps3.yaml").write_text(
        """
- id: chirps3_precipitation_daily
  name: Custom CHIRPS override
  variable: precip
  period_type: daily
  sync_kind: static
  ingestion:
    function: mypackage.sources.download
""",
        encoding="utf-8",
    )
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text(f"templates_dir: {templates_dir}\n", encoding="utf-8")

    monkeypatch.setattr(dataset_registry, "CONFIGS_DIR", None)
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))

    datasets = {d["id"]: d for d in dataset_registry.list_datasets()}
    assert datasets["chirps3_precipitation_daily"]["name"] == "Custom CHIRPS override"
