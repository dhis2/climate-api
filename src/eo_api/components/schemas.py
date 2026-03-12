"""Schemas for generic components."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from ..workflows.schemas import (
    AggregationMethod,
    Dhis2DataValueSetConfig,
    FeatureSourceConfig,
    PeriodType,
)


class ComponentDefinition(BaseModel):
    """Component metadata for discovery."""

    name: str
    version: str = "v1"
    description: str
    inputs: list[str]
    outputs: list[str]
    input_schema: dict[str, Any] = Field(default_factory=dict)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)
    error_codes: list[str] = Field(default_factory=list)


class ComponentCatalogResponse(BaseModel):
    """List of discoverable components."""

    components: list[ComponentDefinition]
