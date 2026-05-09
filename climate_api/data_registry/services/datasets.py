"""Dataset registry backed by YAML config files."""

import importlib.resources
import logging
from pathlib import Path
from typing import Any

import yaml

from climate_api import config as api_config

logger = logging.getLogger(__name__)

# Overridden in tests via monkeypatch to point to a temporary directory.
# When set, only this directory is loaded (no built-ins, no config override).
CONFIGS_DIR: Path | None = None

SUPPORTED_SYNC_KINDS = {"temporal", "release", "static", "derived"}
SUPPORTED_SYNC_EXECUTIONS = {"append", "rematerialize"}
SUPPORTED_RESAMPLE_METHODS = {"mean", "sum", "min", "max"}
SUPPORTED_WEEK_STARTS = {"monday", "sunday"}


def list_datasets() -> list[dict[str, Any]]:
    """Load all dataset templates and return a flat list.

    Built-in templates from climate_api/data/datasets/ are always loaded. When
    templates_dir is set in CLIMATE_API_CONFIG, templates from that directory are
    merged on top — a custom template with the same id overrides the built-in one.

    CONFIGS_DIR (test override via monkeypatch) bypasses this and loads only
    from the given directory, as tests supply a fully controlled set.
    """
    if CONFIGS_DIR is not None:
        return _load_from_dir(CONFIGS_DIR)

    merged: dict[str, dict[str, Any]] = {d["id"]: d for d in _load_builtin_datasets()}

    config_templates_dir = api_config.get_config().get("templates_dir")
    if config_templates_dir:
        if not isinstance(config_templates_dir, (str, Path)):
            raise ValueError(
                f"templates_dir in CLIMATE_API_CONFIG must be a path string, got {type(config_templates_dir).__name__}"
            )
        config_path = api_config.get_config_path()
        root = (config_path.parent / config_templates_dir).resolve() if config_path else Path(config_templates_dir)
        datasets_subdir = root / "datasets"
        if datasets_subdir.is_dir():
            for dataset in _load_from_dir(datasets_subdir):
                merged[dataset["id"]] = dataset

    return list(merged.values())


def get_dataset(dataset_id: str) -> dict[str, Any] | None:
    """Get dataset dict for a given id."""
    datasets_lookup = {d["id"]: d for d in list_datasets()}
    return datasets_lookup.get(dataset_id)


def _load_builtin_datasets() -> list[dict[str, Any]]:
    """Load built-in dataset templates from package data via importlib.resources.

    Using importlib.resources instead of __file__-relative path arithmetic ensures
    this works correctly in both editable installs and wheel installs, where the
    package lives inside site-packages with no guarantee that the project root
    directory (and its data/ folder) is accessible.
    """
    pkg = importlib.resources.files("climate_api") / "data" / "datasets"
    datasets: list[dict[str, Any]] = []
    for resource in pkg.iterdir():
        if not resource.name.endswith((".yaml", ".yml")):
            continue
        try:
            content = resource.read_text(encoding="utf-8")
            file_datasets = yaml.safe_load(content)
            if not isinstance(file_datasets, list):
                raise ValueError(f"{resource.name} must contain a list of dataset templates")
            for dataset in file_datasets:
                _validate_dataset_template(dataset, source=resource.name)
            datasets.extend(file_datasets)
        except Exception:
            logger.exception("Error loading %s", resource.name)
            raise
    return datasets


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

    dataset_id = dataset.get("id")
    if not isinstance(dataset_id, str) or not dataset_id:
        raise ValueError(f"{source} contains a dataset template with a missing or invalid id")
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

    if sync_kind != "derived":
        ingestion = dataset.get("ingestion")
        if not isinstance(ingestion, dict):
            raise ValueError(f"Dataset template '{dataset_id}' in {source} must define an 'ingestion' block")
        function = ingestion.get("function")
        if not isinstance(function, str) or not function:
            raise ValueError(f"Dataset template '{dataset_id}' in {source} must define ingestion.function")

    processing = dataset.get("processing")
    if sync_kind == "derived":
        if processing is None:
            raise ValueError(
                f"Dataset template '{dataset_id}' in {source} must define processing when sync_kind is derived"
            )
        _validate_processing_config(processing, dataset=dataset, dataset_id=dataset_id, source=source)
    elif processing is not None:
        raise ValueError(
            f"Dataset template '{dataset_id}' in {source} may only define processing when sync_kind is derived"
        )

    sync_availability = dataset.get("sync_availability")
    if sync_availability is not None:
        _validate_sync_availability(sync_availability, dataset_id=dataset_id, source=source)


def _validate_processing_config(processing: object, *, dataset: dict[str, Any], dataset_id: str, source: str) -> None:
    """Validate a processing block for a derived dataset."""
    if not isinstance(processing, dict):
        raise ValueError(f"Dataset template '{dataset_id}' in {source} has invalid processing block")
    process_id = processing.get("process_id")
    if not isinstance(process_id, str) or not process_id:
        raise ValueError(f"Dataset template '{dataset_id}' in {source} must define processing.process_id")
    if process_id == "resample":
        _validate_resample_params(processing, dataset=dataset, dataset_id=dataset_id, source=source)
    else:
        raise ValueError(
            f"Dataset template '{dataset_id}' in {source} has unsupported processing.process_id '{process_id}'"
        )


def _validate_resample_params(params: dict[str, Any], *, dataset: dict[str, Any], dataset_id: str, source: str) -> None:
    """Validate the parameters for a resample processing block."""
    source_dataset_id = params.get("source_dataset_id")
    if not isinstance(source_dataset_id, str) or not source_dataset_id:
        raise ValueError(f"Dataset template '{dataset_id}' in {source} must define processing.source_dataset_id")
    method = params.get("method")
    if not isinstance(method, str) or not method:
        raise ValueError(f"Dataset template '{dataset_id}' in {source} must define processing.method")
    if method not in SUPPORTED_RESAMPLE_METHODS:
        supported = ", ".join(sorted(SUPPORTED_RESAMPLE_METHODS))
        raise ValueError(
            f"Dataset template '{dataset_id}' in {source} has unsupported processing.method '{method}'. "
            f"Supported values: {supported}"
        )
    week_start = params.get("week_start")
    period_type = dataset.get("period_type")
    if week_start is not None:
        if period_type != "weekly":
            raise ValueError(
                f"Dataset template '{dataset_id}' in {source} may only define processing.week_start "
                "when period_type is weekly"
            )
        if week_start not in SUPPORTED_WEEK_STARTS:
            supported = ", ".join(sorted(SUPPORTED_WEEK_STARTS))
            raise ValueError(
                f"Dataset template '{dataset_id}' in {source} has unsupported processing.week_start "
                f"'{week_start}'. Supported values: {supported}"
            )


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
