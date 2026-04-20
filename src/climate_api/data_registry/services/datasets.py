"""Dataset registry backed by YAML config files."""

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
CONFIGS_DIR = SCRIPT_DIR.parent.parent.parent.parent / "data" / "datasets"
SUPPORTED_SYNC_KINDS = {"temporal", "release", "static"}


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
