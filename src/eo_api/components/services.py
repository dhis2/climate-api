"""Component service implementations and discovery metadata."""

from __future__ import annotations

from typing import Any, Final

import xarray as xr
from fastapi import HTTPException

from .schemas import ComponentDefinition

_ERROR_CODES_V1: Final[list[str]] = [
    "INPUT_VALIDATION_FAILED",
    "CONFIG_VALIDATION_FAILED",
    "OUTPUT_VALIDATION_FAILED",
    "UPSTREAM_UNREACHABLE",
    "EXECUTION_FAILED",
]

_COMPONENT_REGISTRY: Final[dict[str, ComponentDefinition]] = {
    "feature_source@v1": ComponentDefinition(
        name="feature_source",
        version="v1",
        description="Resolve feature source and compute bbox.",
        inputs=["feature_source"],
        outputs=["features", "bbox"],
        input_schema={
            "type": "object",
            "properties": {"feature_source": {"type": "object"}},
            "required": ["feature_source"],
        },
        config_schema={"type": "object", "properties": {}, "additionalProperties": False},
        output_schema={
            "type": "object",
            "properties": {
                "features": {"type": "object"},
                "bbox": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
            },
            "required": ["features", "bbox"],
        },
        error_codes=_ERROR_CODES_V1,
    ),
    "download_dataset@v1": ComponentDefinition(
        name="download_dataset",
        version="v1",
        description="Download dataset files for period and bbox.",
        inputs=["dataset_id", "start", "end", "overwrite", "country_code", "bbox"],
        outputs=["status"],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
                "overwrite": {"type": "boolean"},
                "country_code": {"type": ["string", "null"]},
                "bbox": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
            },
            "required": ["dataset_id", "start", "end", "overwrite", "bbox"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "overwrite": {"type": "boolean"},
                "country_code": {"type": ["string", "null"]},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object", "properties": {"status": {"type": "string"}}},
        error_codes=_ERROR_CODES_V1,
    ),
    "temporal_aggregation@v1": ComponentDefinition(
        name="temporal_aggregation",
        version="v1",
        description="Aggregate dataset over time dimension.",
        inputs=["dataset_id", "start", "end", "target_period_type", "method", "bbox"],
        outputs=["dataset"],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
                "target_period_type": {"type": "string"},
                "method": {"type": "string"},
                "bbox": {"type": ["array", "null"], "items": {"type": "number"}},
            },
            "required": ["dataset_id", "start", "end", "target_period_type", "method"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "target_period_type": {"type": "string"},
                "method": {"type": "string"},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object", "properties": {"dataset": {"type": "object"}}},
        error_codes=_ERROR_CODES_V1,
    ),
    "spatial_aggregation@v1": ComponentDefinition(
        name="spatial_aggregation",
        version="v1",
        description="Aggregate gridded dataset to features.",
        inputs=["dataset_id", "start", "end", "feature_source", "method"],
        outputs=["records"],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
                "feature_source": {"type": "object"},
                "method": {"type": "string"},
            },
            "required": ["dataset_id", "start", "end", "feature_source", "method"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "method": {"type": "string"},
                "feature_id_property": {"type": "string"},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object", "properties": {"records": {"type": "array"}}},
        error_codes=_ERROR_CODES_V1,
    ),
    "build_datavalueset@v1": ComponentDefinition(
        name="build_datavalueset",
        version="v1",
        description="Build and serialize DHIS2 DataValueSet JSON.",
        inputs=["dataset_id", "period_type", "records", "dhis2"],
        outputs=["data_value_set", "output_file"],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "period_type": {"type": "string"},
                "records": {"type": "array"},
                "dhis2": {"type": "object"},
            },
            "required": ["dataset_id", "period_type", "records", "dhis2"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "period_type": {"type": "string"},
            },
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"data_value_set": {"type": "object"}, "output_file": {"type": "string"}},
            "required": ["data_value_set", "output_file"],
        },
        error_codes=_ERROR_CODES_V1,
    ),
}


def component_catalog() -> list[ComponentDefinition]:
    """Return all discoverable component definitions."""
    return list(_COMPONENT_REGISTRY.values())


def component_registry() -> dict[str, ComponentDefinition]:
    """Return registry entries keyed by component@version."""
    return dict(_COMPONENT_REGISTRY)
