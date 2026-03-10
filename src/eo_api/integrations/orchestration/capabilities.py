"""Dataset/service capability discovery for generic workflow orchestration."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.orchestration.definitions import WORKFLOW_DEFINITION_DIR, available_workflow_definitions
from eo_api.integrations.orchestration.registry import build_default_component_registry

_CAPABILITIES_FILE = WORKFLOW_DEFINITION_DIR / "dataset_capabilities.yaml"


class SupportCapabilities(BaseModel):
    """Boolean capability flags for one dataset."""

    temporal_downsample: bool = True
    temporal_disaggregate: bool = False


class ReducerCapabilities(BaseModel):
    """Supported reducers by aggregation stage."""

    temporal: list[str] = Field(default_factory=list)
    spatial: list[str] = Field(default_factory=list)


class StageBinding(BaseModel):
    """Adapter binding for one stage."""

    adapter: str


class CapabilityProfile(BaseModel):
    """Capability profile for either provider or current integration."""

    source_temporal_resolution: str
    supported_temporal_resolutions: list[str]
    supported_output_formats: list[str] = Field(default_factory=list)
    supports: SupportCapabilities
    reducers: ReducerCapabilities


class DatasetCapabilities(BaseModel):
    """Capabilities for one dataset, separated by provider vs integration."""

    title: str
    provider_capabilities: CapabilityProfile
    integration_capabilities: CapabilityProfile
    stages: dict[str, StageBinding]


class DatasetCapabilityCatalog(BaseModel):
    """Top-level capability catalog loaded from YAML."""

    version: str
    datasets: dict[str, DatasetCapabilities]


@lru_cache(maxsize=1)
def load_dataset_capabilities() -> DatasetCapabilityCatalog:
    """Load dataset capability catalog from YAML."""
    if not _CAPABILITIES_FILE.exists():
        raise ProcessorExecuteError(f"Dataset capability catalog not found: {_CAPABILITIES_FILE}")
    raw = _CAPABILITIES_FILE.read_text(encoding="utf-8")
    payload = yaml.safe_load(raw)
    if not isinstance(payload, dict):
        raise ProcessorExecuteError(f"Dataset capability catalog must be a YAML object: {_CAPABILITIES_FILE}")
    try:
        return DatasetCapabilityCatalog.model_validate(payload)
    except Exception as exc:
        raise ProcessorExecuteError(f"Invalid dataset capability catalog: {exc}") from exc


def list_supported_datasets() -> list[str]:
    """List dataset IDs available in capability catalog."""
    catalog = load_dataset_capabilities()
    return sorted(catalog.datasets.keys())


def build_generic_workflow_capabilities_document() -> dict[str, Any]:
    """Build a discoverable capability document for generic workflow."""
    catalog = load_dataset_capabilities()
    registry = build_default_component_registry()
    return {
        "processId": "generic-dhis2-workflow",
        "catalogVersion": catalog.version,
        "datasets": {dataset: item.model_dump() for dataset, item in catalog.datasets.items()},
        "components": [
            {"id": component_id, "description": registry.get(component_id).description}
            for component_id in registry.list_ids()
        ],
        "workflowDefinitions": available_workflow_definitions(),
    }
