"""Component service implementations and discovery metadata."""

from __future__ import annotations

import yaml
from typing import Any, Final
from pathlib import Path

import xarray as xr
from fastapi import HTTPException

from .schemas import ComponentDefinition

SCRIPT_DIR = Path(__file__).parent


# _ERROR_CODES_V1: Final[list[str]] = [
#     "INPUT_VALIDATION_FAILED",
#     "CONFIG_VALIDATION_FAILED",
#     "OUTPUT_VALIDATION_FAILED",
#     "UPSTREAM_UNREACHABLE",
#     "EXECUTION_FAILED",
# ]


def _discover_components() -> list[ComponentDefinition]:
    components = {}

    for path in SCRIPT_DIR.rglob('component.yaml'):
        with open(path, 'r') as file:
            data = yaml.safe_load(file)
        component = ComponentDefinition(**data)
        key = f'{component.name}@{component.version}'
        components[key] = component

    return components


_COMPONENT_REGISTRY = _discover_components()


def component_catalog() -> list[ComponentDefinition]:
    """Return all discoverable component definitions."""
    return list(_COMPONENT_REGISTRY.values())


def component_registry() -> dict[str, ComponentDefinition]:
    """Return registry entries keyed by component@version."""
    return dict(_COMPONENT_REGISTRY)
