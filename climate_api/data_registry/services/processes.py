"""Process registry backed by YAML config files."""

import importlib
import importlib.resources
import logging
import sys
from pathlib import Path
from typing import Any

import yaml

from climate_api import config as api_config

logger = logging.getLogger(__name__)

# Overridden in tests via monkeypatch to point to a temporary directory.
# When set, only this directory is loaded (no built-ins, no config override).
CONFIGS_DIR: Path | None = None


def list_processes() -> list[dict[str, Any]]:
    """Load all process definitions and return a flat list.

    Built-in definitions from climate_api/data/processes/ are always loaded. When
    plugins_dir is set in CLIMATE_API_CONFIG, definitions from plugins_dir/processes/
    are merged on top — a custom definition with the same id overrides the built-in.

    CONFIGS_DIR (test override via monkeypatch) bypasses this and loads only
    from the given directory.
    """
    if CONFIGS_DIR is not None:
        return _load_from_dir(CONFIGS_DIR)

    merged: dict[str, dict[str, Any]] = {p["id"]: p for p in _load_builtin_processes()}

    config = api_config.get_config()
    config_plugins_dir = config.get("plugins_dir")
    if config_plugins_dir:
        if not isinstance(config_plugins_dir, (str, Path)):
            raise ValueError(
                f"plugins_dir in CLIMATE_API_CONFIG must be a path string, got {type(config_plugins_dir).__name__}"
            )
        config_path = api_config.get_config_path()
        base = config_path.parent if config_path else Path()
        root = (base / config_plugins_dir).resolve()
        if not root.is_dir():
            raise ValueError(f"plugins_dir '{root}' does not exist or is not a directory")
        root_str = str(root)
        if root_str not in sys.path:
            sys.path.append(root_str)
        processes_subdir = root / "processes"
        if processes_subdir.is_dir():
            for process in _load_from_dir(processes_subdir):
                merged[process["id"]] = process

    return list(merged.values())


def get_process(process_id: str) -> dict[str, Any] | None:
    """Get process definition for a given id."""
    return {p["id"]: p for p in list_processes()}.get(process_id)


def _load_builtin_processes() -> list[dict[str, Any]]:
    """Load built-in process definitions from package data via importlib.resources."""
    pkg = importlib.resources.files("climate_api") / "data" / "processes"
    processes: list[dict[str, Any]] = []
    for resource in pkg.iterdir():
        if not resource.name.endswith((".yaml", ".yml")):
            continue
        try:
            content = resource.read_text(encoding="utf-8")
            file_processes = yaml.safe_load(content)
            if not isinstance(file_processes, list):
                raise ValueError(f"{resource.name} must contain a list of process definitions")
            for process in file_processes:
                _validate_process(process, source=resource.name)
            processes.extend(file_processes)
        except Exception:
            logger.exception("Error loading %s", resource.name)
            raise
    return processes


def _load_from_dir(folder: Path) -> list[dict[str, Any]]:
    """Load process definitions from a directory on disk."""
    processes: list[dict[str, Any]] = []

    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder}")

    for file_path in folder.glob("*.y*ml"):
        try:
            with open(file_path, encoding="utf-8") as f:
                file_processes = yaml.safe_load(f)
                if not isinstance(file_processes, list):
                    raise ValueError(f"{file_path.name} must contain a list of process definitions")
                for process in file_processes:
                    _validate_process(process, source=str(file_path))
                processes.extend(file_processes)
        except Exception:
            logger.exception("Error loading %s", file_path.name)
            raise

    return processes


def _validate_process(process: object, *, source: str) -> None:
    """Validate a process definition dict."""
    if not isinstance(process, dict):
        raise ValueError(f"{source} contains a non-object process definition")

    process_id = process.get("id")
    if not isinstance(process_id, str) or not process_id:
        raise ValueError(f"{source} contains a process definition with a missing or invalid id")

    execution_function = process.get("execution_function")
    if not isinstance(execution_function, str) or not execution_function:
        raise ValueError(f"Process '{process_id}' in {source} must define execution_function")


def _get_dynamic_function(full_path: str) -> Any:
    """Import and return a function given its dotted module path."""
    parts = [p for p in full_path.split(".") if p]
    if len(parts) < 2:
        raise ValueError(
            f"execution_function must be a dotted path with at least one module and one attribute, got '{full_path}'"
        )
    module_path = ".".join(parts[:-1])
    function_name = parts[-1]
    module = importlib.import_module(module_path)
    return getattr(module, function_name)
