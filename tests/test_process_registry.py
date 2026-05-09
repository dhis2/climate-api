from pathlib import Path

import pytest

from climate_api.data_registry.services import processes as process_registry


def test_builtin_processes_include_resample(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.delenv("CLIMATE_API_CONFIG", raising=False)

    ids = {p["id"] for p in process_registry.list_processes()}
    assert "resample" in ids


def test_builtin_resample_has_execution_function(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.delenv("CLIMATE_API_CONFIG", raising=False)

    resample = process_registry.get_process("resample")
    assert resample is not None
    assert "execution_function" in resample
    assert resample["execution_function"] == "climate_api.processing.services.execute_resample"


def test_get_process_returns_none_for_unknown_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.delenv("CLIMATE_API_CONFIG", raising=False)

    assert process_registry.get_process("does_not_exist") is None


def test_plugins_dir_processes_subdir_adds_to_builtin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "plugins" / "processes"
    processes_subdir.mkdir(parents=True)
    (processes_subdir / "custom.yaml").write_text(
        """
- id: custom_process
  name: Custom process
  execution_function: mypackage.custom.execute
""",
        encoding="utf-8",
    )
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text(f"plugins_dir: {tmp_path / 'plugins'}\n", encoding="utf-8")

    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))

    ids = {p["id"] for p in process_registry.list_processes()}
    assert "custom_process" in ids
    assert "resample" in ids


def test_plugins_dir_process_overrides_builtin_by_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "plugins" / "processes"
    processes_subdir.mkdir(parents=True)
    (processes_subdir / "resample.yaml").write_text(
        """
- id: resample
  name: Custom resample override
  execution_function: mypackage.resample.execute
""",
        encoding="utf-8",
    )
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text(f"plugins_dir: {tmp_path / 'plugins'}\n", encoding="utf-8")

    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))

    processes = {p["id"]: p for p in process_registry.list_processes()}
    assert processes["resample"]["name"] == "Custom resample override"
    assert processes["resample"]["execution_function"] == "mypackage.resample.execute"
