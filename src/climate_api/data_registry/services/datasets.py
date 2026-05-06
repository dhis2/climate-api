"""Dataset registry backed by YAML config files."""

import logging
from pathlib import Path
from typing import Any

import yaml

from climate_api import config as api_config

logger = logging.getLogger(__name__)

_SCRIPT_DIR = Path(__file__).parent.resolve()
_BUILTIN_DATASETS_DIR = _SCRIPT_DIR.parent.parent.parent.parent / "data" / "datasets"

# Overridden in tests via monkeypatch to point to a temporary directory.
# When set, only this directory is loaded (no built-ins, no config override).
CONFIGS_DIR: Path | None = None

SUPPORTED_SYNC_KINDS = {"temporal", "release", "static"}
SUPPORTED_SYNC_EXECUTIONS = {"append", "rematerialize"}


def list_datasets() -> list[dict[str, Any]]:
    """Load all dataset templates and return a flat list.

    Built-in templates from data/datasets/ are always loaded. When datasets_dir
    is set in CLIMATE_API_CONFIG, templates from that directory are merged on
    top — a custom template with the same id overrides the built-in one.

    CONFIGS_DIR (test override via monkeypatch) bypasses this and loads only
    from the given directory, as tests supply a fully controlled set.
    """
    if CONFIGS_DIR is not None:
        return _load_from_dir(CONFIGS_DIR)

    merged: dict[str, dict[str, Any]] = {d["id"]: d for d in _load_from_dir(_BUILTIN_DATASETS_DIR)}

    config_datasets_dir = api_config.get_config().get("datasets_dir")
    if config_datasets_dir:
        config_path = api_config.get_config_path()
        resolved = (config_path.parent / config_datasets_dir).resolve() if config_path else Path(config_datasets_dir)
        for dataset in _load_from_dir(resolved):
            merged[dataset["id"]] = dataset

    return list(merged.values())


def get_dataset(dataset_id: str) -> dict[str, Any] | None:
    """Get dataset dict for a given id."""
    datasets_lookup = {d["id"]: d for d in list_datasets()}
    return datasets_lookup.get(dataset_id)


def _load_from_dir(folder: Path) -> list[dict[str, Any]]:
    """Load dataset templates from a directory on disk."""
    datasets: list[dict[str, Any]] = []

    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder}")

    for file_path in folder.glob("*.y*ml"):
        try:
            with open(file_path, encoding="utf-8") as f:
                file_datasets = yaml.safe_load(f)
                if not isinstance(file_datasets, list):
                    raise ValueError(f"{file_path.name} must contain a list of dataset templates")
                for dataset in file_datasets:
                    _validate_dataset_template(dataset, source=str(file_path))
                datasets.extend(file_datasets)
        except Exception:
            logger.exception("Error loading %s", file_path.name)
            raise

    return datasets


def _validate_dataset_template(dataset: object, *, source: str) -> None:
    """Validate registry fields required by runtime sync planning."""
    if not isinstance(dataset, dict):
        raise ValueError(f"{source} contains a non-object dataset template")

    dataset_id = str(dataset.get("id", "<missing id>"))
    sync_kind = dataset.get("sync_kind")
    if not isinstance(sync_kind, str) or not sync_kind:
        raise ValueError(f"Dataset template '{dataset_id}' in {source} must define sync_kind")
    if sync_kind not in SUPPORTED_SYNC_KINDS:
        supported = ", ".join(sorted(SUPPORTED_SYNC_KINDS))
        raise ValueError(
            f"Dataset template '{dataset_id}' in {source} has unsupported sync_kind "
            f"'{sync_kind}'. Supported values: {supported}"
        )

    sync_execution = dataset.get("sync_execution")
    if sync_execution is not None:
        if not isinstance(sync_execution, str) or not sync_execution:
            raise ValueError(f"Dataset template '{dataset_id}' in {source} has invalid sync_execution")
        if sync_execution not in SUPPORTED_SYNC_EXECUTIONS:
            supported = ", ".join(sorted(SUPPORTED_SYNC_EXECUTIONS))
            raise ValueError(
                f"Dataset template '{dataset_id}' in {source} has unsupported sync_execution "
                f"'{sync_execution}'. Supported values: {supported}"
            )

    sync_availability = dataset.get("sync_availability")
    if sync_availability is not None:
        _validate_sync_availability(sync_availability, dataset_id=dataset_id, source=source)


def _validate_sync_availability(sync_availability: object, *, dataset_id: str, source: str) -> None:
    """Validate optional source availability policy metadata."""
    if not isinstance(sync_availability, dict):
        raise ValueError(f"Dataset template '{dataset_id}' in {source} has invalid sync_availability")

    latest_available_function = sync_availability.get("latest_available_function")
    if latest_available_function is not None and (
        not isinstance(latest_available_function, str) or not latest_available_function
    ):
        raise ValueError(
            f"Dataset template '{dataset_id}' in {source} has invalid sync_availability.latest_available_function"
        )
