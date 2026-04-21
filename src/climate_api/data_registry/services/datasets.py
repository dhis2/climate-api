"""Dataset registry backed by YAML config files."""

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
CONFIGS_DIR = SCRIPT_DIR.parent.parent.parent.parent / "data" / "datasets"
SUPPORTED_SYNC_KINDS = {"temporal", "release", "static"}
SUPPORTED_SYNC_EXECUTIONS = {"append", "rematerialize"}


def list_datasets() -> list[dict[str, Any]]:
    """Load all YAML files in the registry folder and return a flat list of datasets."""
    datasets: list[dict[str, Any]] = []
    folder = CONFIGS_DIR

    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder}")

    for file_path in folder.glob("*.y*ml"):
        try:
            with open(file_path, encoding="utf-8") as f:
                file_datasets = yaml.safe_load(f)
                if not isinstance(file_datasets, list):
                    raise ValueError(f"{file_path.name} must contain a list of dataset templates")
                for dataset in file_datasets:
                    _validate_dataset_template(dataset, file_path=file_path)
                datasets.extend(file_datasets)
        except Exception:
            logger.exception("Error loading %s", file_path.name)
            raise

    return datasets


def get_dataset(dataset_id: str) -> dict[str, Any] | None:
    """Get dataset dict for a given id."""
    datasets_lookup = {d["id"]: d for d in list_datasets()}
    return datasets_lookup.get(dataset_id)


def _validate_dataset_template(dataset: object, *, file_path: Path) -> None:
    """Validate registry fields required by runtime sync planning."""
    if not isinstance(dataset, dict):
        raise ValueError(f"{file_path.name} contains a non-object dataset template")

    dataset_id = str(dataset.get("id", "<missing id>"))
    sync_kind = dataset.get("sync_kind")
    if not isinstance(sync_kind, str) or not sync_kind:
        raise ValueError(f"Dataset template '{dataset_id}' in {file_path.name} must define sync_kind")
    if sync_kind not in SUPPORTED_SYNC_KINDS:
        supported = ", ".join(sorted(SUPPORTED_SYNC_KINDS))
        raise ValueError(
            f"Dataset template '{dataset_id}' in {file_path.name} has unsupported sync_kind "
            f"'{sync_kind}'. Supported values: {supported}"
        )

    sync_execution = dataset.get("sync_execution")
    if sync_execution is not None:
        if sync_execution not in SUPPORTED_SYNC_EXECUTIONS:
            supported = ", ".join(sorted(SUPPORTED_SYNC_EXECUTIONS))
            raise ValueError(
                f"Dataset template '{dataset_id}' in {file_path.name} has unsupported sync_execution "
                f"'{sync_execution}'. Supported values: {supported}"
            )

    sync_availability = dataset.get("sync_availability")
    if sync_availability is not None:
        _validate_sync_availability(sync_availability, dataset_id=dataset_id, file_path=file_path)


def _validate_sync_availability(sync_availability: object, *, dataset_id: str, file_path: Path) -> None:
    """Validate optional source availability policy metadata."""
    if not isinstance(sync_availability, dict):
        raise ValueError(f"Dataset template '{dataset_id}' in {file_path.name} has invalid sync_availability")

    latest_available_function = sync_availability.get("latest_available_function")
    if latest_available_function is not None and (
        not isinstance(latest_available_function, str) or not latest_available_function
    ):
        raise ValueError(
            f"Dataset template '{dataset_id}' in {file_path.name} has invalid "
            "sync_availability.latest_available_function"
        )
